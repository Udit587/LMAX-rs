use crate::ring_buffer::ringBuffer::RingBuffer;
use crate::sequence::sequence::Sequence;
use crate::buffer_slot::inputSlot::InputSlot;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub struct SingleProducer {
    buffer: Arc<RingBuffer<InputSlot>>,
    blp_seq: Arc<Sequence>, // sequence of the last (slowest) consumer
    pub next_seq: i64,
}

impl SingleProducer {
    pub fn new(buffer: Arc<RingBuffer<InputSlot>>, blp_seq: Arc<Sequence>) -> Self {
        Self {
            buffer,
            blp_seq,
            next_seq: 0,
        }
    }

    fn claim_next(&mut self) -> i64 {
        // wrap_point: if the slowest consumer hasn't passed here yet, we'd overwrite unread data
        let wrap_point = self.next_seq - self.buffer.capacity() as i64;
        loop {
            if self.blp_seq.get() >= wrap_point {
                break;
            }
            core::hint::spin_loop();
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    pub fn publish(&mut self, raw: &[u8], producer_seq: &Arc<Sequence>) {
        let seq = self.claim_next();

        let mut slot_data = InputSlot {
            raw_bytes: [0u8; 256],
            len: raw.len().min(256),
            command: None,
            timestamp_ns: crate::util::time::now_ns(),
        };
        slot_data.raw_bytes[..slot_data.len].copy_from_slice(&raw[..slot_data.len]);

        unsafe {
            (*self.buffer.slot_mut(seq)).write(slot_data);
        }

        // publish: store seq so consumers can see this slot is ready
        self.buffer.cursor.0.store(seq, Ordering::Release);
        producer_seq.set(seq); 
    }
}