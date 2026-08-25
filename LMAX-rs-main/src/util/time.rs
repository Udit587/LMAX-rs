// pub fn now_ns() -> u64 {
//     use std::time::{SystemTime, UNIX_EPOCH};
//     SystemTime::now()
//         .duration_since(UNIX_EPOCH)
//         .unwrap()
//         .as_nanos() as u64
// }

use std::time::Instant;
static START_INSTANT: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

pub fn now_ns() -> u64 {
    let start = START_INSTANT.get_or_init(Instant::now);
    start.elapsed().as_nanos() as u64
}