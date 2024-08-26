//! This package is used to control the switches of various functions of GreptimeDB,
//! such as wal, compaction, etc.

/// It indicates whether the wal is enabled globally.
pub fn enable_wal() -> bool {
    true
}

/// It indicates whether the compaction is enabled globally.
pub fn enable_compaction() -> bool {
    true
}
