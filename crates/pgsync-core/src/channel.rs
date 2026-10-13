//! Bounded, byte-accounted channel between the reader and the applier.
//!
//! Two limits apply: a count of in-flight items (`capacity`) and a byte budget
//! (`max_bytes`). A producer must acquire both before an item is enqueued, so memory
//! held by undelivered events is bounded by `max_bytes + one oversized item`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Semaphore, mpsc};

pub struct Envelope<T> {
    pub item: T,
    _bytes: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl<T> Envelope<T> {
    pub fn into_inner(self) -> T {
        self.item
    }
}

#[derive(Clone)]
pub struct Sender<T> {
    tx: mpsc::Sender<Envelope<T>>,
    bytes: Arc<Semaphore>,
    max_bytes: usize,
    inflight: Arc<AtomicU64>,
}

pub struct Receiver<T> {
    rx: mpsc::Receiver<Envelope<T>>,
    inflight: Arc<AtomicU64>,
}

pub fn bounded<T>(capacity: usize, max_bytes: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = mpsc::channel(capacity);
    let inflight = Arc::new(AtomicU64::new(0));
    (
        Sender {
            tx,
            bytes: Arc::new(Semaphore::new(max_bytes)),
            max_bytes,
            inflight: inflight.clone(),
        },
        Receiver { rx, inflight },
    )
}

#[derive(Debug, thiserror::Error)]
#[error("channel closed")]
pub struct Closed;

impl<T> Sender<T> {
    /// Reserve a slot for an item of `bytes` bytes. Waits for both limits.
    /// Items larger than the whole budget are admitted alone (the budget is clamped) so a
    /// single oversized row cannot deadlock the pipeline.
    pub async fn reserve(&self, bytes: usize) -> Result<Permit<'_, T>, Closed> {
        let want = bytes.min(self.max_bytes).max(1) as u32;
        let slot = self.tx.reserve().await.map_err(|_| Closed)?;
        let permit = self
            .bytes
            .clone()
            .acquire_many_owned(want)
            .await
            .map_err(|_| Closed)?;
        self.inflight.fetch_add(1, Ordering::Relaxed);
        Ok(Permit {
            slot: Some(slot),
            permit: Some(permit),
            inflight: &self.inflight,
        })
    }

    pub fn inflight_items(&self) -> u64 {
        self.inflight.load(Ordering::Relaxed)
    }

    pub fn available_bytes(&self) -> usize {
        self.bytes.available_permits()
    }
}

pub struct Permit<'a, T> {
    slot: Option<mpsc::Permit<'a, Envelope<T>>>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    inflight: &'a AtomicU64,
}

impl<T> Permit<'_, T> {
    pub fn send(mut self, item: T) {
        let bytes = self.permit.take();
        if let Some(slot) = self.slot.take() {
            slot.send(Envelope {
                item,
                _bytes: bytes,
            });
        }
    }
}

impl<T> Drop for Permit<'_, T> {
    fn drop(&mut self) {
        if self.permit.is_some() {
            // Reserved but never sent.
            self.inflight.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl<T> Receiver<T> {
    /// Returns `None` once all senders are dropped and the queue is drained.
    pub async fn recv(&mut self) -> Option<Envelope<T>> {
        let e = self.rx.recv().await?;
        self.inflight.fetch_sub(1, Ordering::Relaxed);
        Some(e)
    }

    pub fn close(&mut self) {
        self.rx.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn blocks_when_bytes_exhausted_until_consumer_drains() {
        let (tx, mut rx) = bounded::<Vec<u8>>(100, 1000);
        tx.reserve(600).await.unwrap().send(vec![0; 600]);
        tx.reserve(300).await.unwrap().send(vec![0; 300]);
        assert_eq!(tx.available_bytes(), 100);
        assert_eq!(tx.inflight_items(), 2);

        let blocked = tokio::time::timeout(Duration::from_millis(50), tx.reserve(200)).await;
        assert!(blocked.is_err(), "third item should block on byte budget");

        let first = rx.recv().await.unwrap();
        assert_eq!(first.item.len(), 600);
        drop(first);
        let p = tokio::time::timeout(Duration::from_millis(500), tx.reserve(200)).await;
        assert!(p.is_ok(), "should unblock after consumer releases bytes");
    }

    #[tokio::test]
    async fn blocks_on_item_capacity() {
        let (tx, mut rx) = bounded::<u8>(2, 1 << 20);
        tx.reserve(1).await.unwrap().send(1);
        tx.reserve(1).await.unwrap().send(2);
        let blocked = tokio::time::timeout(Duration::from_millis(50), tx.reserve(1)).await;
        assert!(blocked.is_err());
        assert_eq!(rx.recv().await.unwrap().into_inner(), 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(500), tx.reserve(1))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn oversized_item_is_admitted_alone() {
        let (tx, mut rx) = bounded::<Vec<u8>>(10, 100);
        let p = tokio::time::timeout(Duration::from_millis(200), tx.reserve(10_000)).await;
        assert!(p.is_ok(), "oversized item must not deadlock");
        p.unwrap().unwrap().send(vec![1]);
        assert_eq!(tx.available_bytes(), 0);
        drop(rx.recv().await.unwrap());
        assert_eq!(tx.available_bytes(), 100);
    }

    #[tokio::test]
    async fn dropped_permit_releases_everything() {
        let (tx, _rx) = bounded::<u8>(1, 10);
        {
            let _p = tx.reserve(10).await.unwrap();
            assert_eq!(tx.inflight_items(), 1);
        }
        assert_eq!(tx.inflight_items(), 0);
        assert_eq!(tx.available_bytes(), 10);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), tx.reserve(10))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn recv_returns_none_after_senders_drop() {
        let (tx, mut rx) = bounded::<u8>(1, 10);
        tx.reserve(1).await.unwrap().send(5);
        drop(tx);
        assert_eq!(rx.recv().await.unwrap().into_inner(), 5);
        assert!(rx.recv().await.is_none());
    }
}
