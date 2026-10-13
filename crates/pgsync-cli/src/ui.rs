//! Terminal progress renderer: step lines during bootstrap, one line per applied
//! transaction while streaming. Used when stdout is a terminal and `--json` is off.

use pgsync_core::Lsn;
use pgsync_core::progress::{Progress, TxnSummary};
use std::io::Write;
use std::sync::Mutex;
use std::time::Duration;

pub struct Terminal {
    show_keys: bool,
    color: bool,
    out: Mutex<std::io::Stdout>,
}

impl Terminal {
    pub fn new(show_keys: bool, color: bool) -> Self {
        Terminal {
            show_keys,
            color,
            out: Mutex::new(std::io::stdout()),
        }
    }

    fn paint(&self, code: &str, s: &str) -> String {
        if self.color {
            format!("\x1b[{code}m{s}\x1b[0m")
        } else {
            s.to_owned()
        }
    }

    fn line(&self, s: String) {
        let mut o = self.out.lock().unwrap();
        let _ = writeln!(o, "{s}");
        let _ = o.flush();
    }
}

fn human_bytes(b: u64) -> String {
    const K: f64 = 1024.0;
    let b = b as f64;
    if b >= K * K * K {
        format!("{:.1} GB", b / K / K / K)
    } else if b >= K * K {
        format!("{:.1} MB", b / K / K)
    } else if b >= K {
        format!("{:.0} kB", b / K)
    } else {
        format!("{b:.0} B")
    }
}

fn human_rows(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.2}M", n as f64 / 1e6)
    } else if n >= 10_000 {
        format!("{}k", n / 1000)
    } else {
        pgsync_postgres::job::group_digits(n)
    }
}

fn now_hms() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs() % 86_400;
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        secs / 3600,
        (secs / 60) % 60,
        secs % 60,
        now.subsec_millis()
    )
}

fn ms(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms < 10_000.0 {
        format!("{ms:.0}ms")
    } else {
        format!("{:.1}s", ms / 1000.0)
    }
}

impl Progress for Terminal {
    fn step(&self, message: &str) {
        self.line(format!("  {} {message}", self.paint("32", "✓")));
    }

    fn copy_started(&self, table: &str) {
        self.line(format!("  {} snapshot {table} …", self.paint("36", "⠿")));
    }

    fn copy_done(&self, table: &str, rows: u64, bytes: u64, elapsed: Duration) {
        self.line(format!(
            "  {} snapshot {:<28} {:>8} rows  {:>9}  {}",
            self.paint("36", "⠿"),
            table,
            human_rows(rows),
            human_bytes(bytes),
            ms(elapsed)
        ));
    }

    fn streaming(&self, from: Lsn) {
        self.line(format!(
            "  {} streaming WAL from {from}",
            self.paint("33", "▸")
        ));
    }

    fn txn(&self, t: &TxnSummary) {
        // Single-table, single-kind transactions get the compact form of the example;
        // mixed transactions list each (table, kind) group.
        let what = if t.ops.len() == 1 {
            let o = &t.ops[0];
            let kind = o.kind.to_uppercase();
            let kind = if o.count > 1 {
                format!("{kind} ×{}", o.count)
            } else {
                kind
            };
            format!("{:<14} {:<28}", kind, o.table)
        } else {
            let groups: Vec<String> = t
                .ops
                .iter()
                .map(|o| format!("{} {}×{}", o.table, o.kind.to_uppercase(), o.count))
                .collect();
            format!("{:<14} {}", format!("TXN ×{}", t.rows), groups.join(", "))
        };
        let keys = if self.show_keys && !t.keys.is_empty() {
            let shown: Vec<&str> = t
                .keys
                .iter()
                .map(|k| k.rsplit(' ').next().unwrap_or(k))
                .collect();
            format!("  {}", shown.join(" "))
        } else {
            String::new()
        };
        let lsn = self.paint("2", &format!("{:<12}", t.end_lsn.to_string()));
        self.line(format!(
            "{}  {}{}  {}  {:>5}",
            self.paint("2", &now_hms()),
            what,
            keys,
            lsn,
            ms(t.apply_time)
        ));
    }

    fn skipped(&self, xid: u32, end_lsn: Lsn) {
        self.line(format!(
            "{}  {:<14} xid {xid} already applied        {end_lsn}",
            self.paint("2", &now_hms()),
            "SKIP"
        ));
    }

    fn wants_keys(&self) -> bool {
        self.show_keys
    }
}
