use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use crate::buffer_slot::outputSlot::OutputSlot;
use crate::ring_buffer::ringBuffer::RingBuffer;
use crate::sequence::sequence::Sequence;
use crate::sequence_barrier::sequenceBarrier::SequenceBarrier;

pub struct OutputConsumer {
    output_ring: Arc<RingBuffer<OutputSlot>>,
    output_barrier: Arc<SequenceBarrier>,
    output_consumed_sequence: Arc<Sequence>,
}

impl OutputConsumer {
    pub fn new(
        output_ring: Arc<RingBuffer<OutputSlot>>,
        output_barrier: Arc<SequenceBarrier>,
        output_consumed_sequence: Arc<Sequence>,
    ) -> Self {
        Self {
            output_ring,
            output_barrier,
            output_consumed_sequence,
        }
    }

    pub fn run(self) -> JoinHandle<()> {
        thread::spawn(move || {
            loop {
                let next_seq = self.output_consumed_sequence.get() + 1;
                //println!("out: waiting for seq {}", next_seq);
                self.output_barrier.wait_for(next_seq);
                //println!("out: barrier released for seq {}", next_seq);

                let slot = unsafe { self.output_ring.slot_ref(next_seq) };
                match slot.event {
                    Some(event) => {
                        //println!("out: consumed seq {} -> {:?}", next_seq, event);
                    }
                    None => {
                        //println!("out: empty slot at seq {}", next_seq);
                    }
                }

                self.output_consumed_sequence.set(next_seq);
                //println!("out: advanced consumed/gating sequence to {}", next_seq);
            }
        })
    }
}