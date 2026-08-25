use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use crate::order::order::Order;
use crate::sequence::sequence::Sequence;
use crate::sequence_barrier::sequenceBarrier::SequenceBarrier;
use crate::ring_buffer::ringBuffer::RingBuffer;
use crate::buffer_slot::inputSlot::InputSlot;

pub struct JournalConsumer {
    ring: Arc<RingBuffer<InputSlot>>,
    barrier: Arc<SequenceBarrier>,
    sequence: Arc<Sequence>,
}

impl JournalConsumer {
    pub fn new(
        ring: Arc<RingBuffer<InputSlot>>,
        barrier: Arc<SequenceBarrier>,
        sequence: Arc<Sequence>,
    ) -> Self {
        Self {
            ring,
            barrier,
            sequence,
        }
    }

    pub fn run(self) -> JoinHandle<()> {
        thread::spawn(move || {
            loop {
                let next_seq = self.sequence.get() + 1;

                // wait until the upstream (producer or prior consumer) has reached next_seq
                self.barrier.wait_for(next_seq);

                // slot_ref now takes i64 directly — no cast needed here
                let input_slot = unsafe { self.ring.slot_ref(next_seq) };

                // --- process the slot ---
                // e.g. write input_slot.raw_bytes[..input_slot.len] to a journal file

                // advance our sequence so downstream consumers/producer know we're done
                self.sequence.set(next_seq);
            }
        })
    }
}