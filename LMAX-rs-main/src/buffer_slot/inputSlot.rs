use crate::order::command::Command;

pub struct InputSlot {
    pub raw_bytes: [u8; 256],
    pub len: usize,
    pub command: Option<Command>,
    pub timestamp_ns: u64,   // set by producer before publish
}