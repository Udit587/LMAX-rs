use crate::output_event::outputEvent::OutputEvent;

pub struct OutputSlot {
    pub event: Option<OutputEvent>,
    pub timestamp_ns: u64,   // copied from input slot by BLP before publishing output
}

impl OutputSlot {
    pub fn new() -> Self {
        Self { event: None, timestamp_ns: 0 }
    }
}