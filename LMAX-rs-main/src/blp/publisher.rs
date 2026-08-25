use std::sync::Arc;

use crate::buffer_slot::outputSlot::OutputSlot;
use crate::output_event::outputEvent::OutputEvent;
use crate::ring_buffer::ringBuffer::RingBuffer;
use crate::sequence::sequence::Sequence;

pub fn publish_output(
    ring: &Arc<RingBuffer<OutputSlot>>,
    producer_seq: &Arc<Sequence>,
    gating_sequence: &Arc<Sequence>,
    event: OutputEvent,
    timestamp_ns: u64,        // added
) {
    let next_seq = producer_seq.get() + 1;
    let buffer_size = ring.capacity() as i64;

    loop {
        let consumed = gating_sequence.get();
        if next_seq - consumed <= buffer_size {
            break;
        }
        std::hint::spin_loop();
    }

    let slot = unsafe { ring.slot_mut_ref(next_seq) };
    slot.event = Some(event);
    slot.timestamp_ns = timestamp_ns;  // added

    producer_seq.set(next_seq);
}