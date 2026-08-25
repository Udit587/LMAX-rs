use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use crate::sequence::sequence::Sequence;
use crate::sequence_barrier::sequenceBarrier::SequenceBarrier;
use crate::ring_buffer::ringBuffer::RingBuffer;
use crate::buffer_slot::inputSlot::InputSlot;

pub struct Replicator {
    ring: Arc<RingBuffer<InputSlot>>,
    barrier: Arc<SequenceBarrier>,
    sequence: Arc<Sequence>,
}

impl Replicator {
    pub fn new(
        ring: Arc<RingBuffer<InputSlot>>,
        barrier: Arc<SequenceBarrier>,
        sequence: Arc<Sequence>,
    ) -> Self {
        Self { ring, barrier, sequence }
    }

    pub fn run(self) -> JoinHandle<()> {
        thread::spawn(move || {
            loop {
                let next_seq = self.sequence.get() + 1;
                self.barrier.wait_for(next_seq);

                let slot = unsafe { self.ring.slot_ref(next_seq) };

                // stub — will send raw_bytes to replica node later
                // println!(
                //     "[Replicator] seq={} len={} bytes={:?}",
                //     next_seq,
                //     slot.len,
                //     &slot.raw_bytes[..slot.len]
                // );

                self.sequence.set(next_seq);
            }
        })
    }
}