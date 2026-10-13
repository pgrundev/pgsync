//! PostgreSQL Log Sequence Numbers.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// A PostgreSQL WAL position. Ordered, copyable, and formatted as `XXXXXXXX/YYYYYYYY`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const ZERO: Lsn = Lsn(0);

    pub fn as_u64(self) -> u64 {
        self.0
    }

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// Bytes from `other` to `self`, saturating at zero when `self < other`.
    pub fn bytes_since(self, other: Lsn) -> u64 {
        self.0.saturating_sub(other.0)
    }

    pub fn saturating_add(self, n: u64) -> Lsn {
        Lsn(self.0.saturating_add(n))
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 as u32)
    }
}

impl fmt::Debug for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Lsn({self})")
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("invalid LSN {0:?}: expected hexadecimal `HIGH/LOW`")]
pub struct ParseLsnError(pub String);

impl FromStr for Lsn {
    type Err = ParseLsnError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (hi, lo) = s
            .split_once('/')
            .ok_or_else(|| ParseLsnError(s.to_owned()))?;
        if hi.is_empty() || lo.is_empty() || hi.len() > 8 || lo.len() > 8 {
            return Err(ParseLsnError(s.to_owned()));
        }
        let hi = u32::from_str_radix(hi, 16).map_err(|_| ParseLsnError(s.to_owned()))?;
        let lo = u32::from_str_radix(lo, 16).map_err(|_| ParseLsnError(s.to_owned()))?;
        Ok(Lsn(((hi as u64) << 32) | lo as u64))
    }
}

impl From<u64> for Lsn {
    fn from(v: u64) -> Self {
        Lsn(v)
    }
}

impl Serialize for Lsn {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Lsn {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_formats_round_trip() {
        for s in ["0/0", "0/16B3748", "1/0", "FFFFFFFF/FFFFFFFF", "A/B"] {
            let lsn: Lsn = s.parse().unwrap();
            assert_eq!(lsn.to_string(), s, "round trip of {s}");
        }
    }

    #[test]
    fn parses_numeric_value_correctly() {
        assert_eq!("0/16B3748".parse::<Lsn>().unwrap(), Lsn(0x16B3748));
        assert_eq!("1/0".parse::<Lsn>().unwrap(), Lsn(1 << 32));
        assert_eq!("1/5".parse::<Lsn>().unwrap(), Lsn((1 << 32) + 5));
    }

    #[test]
    fn rejects_malformed() {
        for s in [
            "",
            "/",
            "1",
            "1/",
            "/1",
            "G/1",
            "1/G",
            "123456789/0",
            "1/2/3",
            "0x1/0",
        ] {
            assert!(s.parse::<Lsn>().is_err(), "should reject {s:?}");
        }
    }

    #[test]
    fn ordering_follows_numeric_value() {
        let a: Lsn = "0/FFFFFFFF".parse().unwrap();
        let b: Lsn = "1/0".parse().unwrap();
        assert!(a < b);
        assert_eq!(b.bytes_since(a), 1);
        assert_eq!(a.bytes_since(b), 0);
        assert_eq!(Lsn::ZERO.max(a), a);
    }

    #[test]
    fn serde_uses_text_form() {
        let lsn: Lsn = "0/16B3748".parse().unwrap();
        let json = serde_json::to_string(&lsn).unwrap();
        assert_eq!(json, "\"0/16B3748\"");
        let back: Lsn = serde_json::from_str(&json).unwrap();
        assert_eq!(back, lsn);
        assert!(serde_json::from_str::<Lsn>("\"nope\"").is_err());
    }
}
