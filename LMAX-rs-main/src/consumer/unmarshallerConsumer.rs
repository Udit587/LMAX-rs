use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use crate::buffer_slot::inputSlot::InputSlot;
use crate::order::command::Command;
use crate::order::orderType::OrderType;
use crate::order::side::Side;
use crate::order::timeInForce::TimeInForce;
use crate::ring_buffer::ringBuffer::RingBuffer;
use crate::sequence::sequence::Sequence;
use crate::sequence_barrier::sequenceBarrier::SequenceBarrier;

pub struct Unmarshaller {
    ring: Arc<RingBuffer<InputSlot>>,
    barrier: Arc<SequenceBarrier>,
    consumed_sequence: Arc<Sequence>,
}

impl Unmarshaller {
    pub fn new(
        ring: Arc<RingBuffer<InputSlot>>,
        barrier: Arc<SequenceBarrier>,
        consumed_sequence: Arc<Sequence>,
    ) -> Self {
        Self {
            ring,
            barrier,
            consumed_sequence,
        }
    }

    pub fn run(self) -> JoinHandle<()> {
        thread::spawn(move || {
            loop {
                let next_seq = self.consumed_sequence.get() + 1;
                self.barrier.wait_for(next_seq);

                let slot = unsafe { self.ring.slot_mut_ref(next_seq) };

                // eprintln!(
                //     "unmarshaller: next_seq={}, len={}, first_tag={}",
                //     next_seq,
                //     slot.len,
                //     if slot.len > 0 { slot.raw_bytes[0] } else { 255 }
                // );

                match parse_command(&slot.raw_bytes[..slot.len]) {
                    Ok(cmd) => {
                        // eprintln!("unmarshaller: parse ok");
                        slot.command = Some(cmd);
                    }
                    Err(e) => {
                        // eprintln!("unmarshaller: parse failed: {}", e);
                        slot.command = None;
                    }
                }

                self.consumed_sequence.set(next_seq);
                // eprintln!("unmarshaller: advanced sequence to {}", next_seq);
            }
        })
    }
}

fn parse_command(buf: &[u8]) -> Result<Command, &'static str> {
    if buf.is_empty() {
        return Err("empty buffer");
    }

    let tag = buf[0];
    let mut off = 1usize;

    match tag {
        1 => parse_place(buf, &mut off),
        2 => parse_cancel(buf, &mut off),
        3 => parse_modify(buf, &mut off),
        _ => Err("unknown command tag"),
    }
}

fn parse_place(buf: &[u8], off: &mut usize) -> Result<Command, &'static str> {
    let client_id = read_u64(buf, off)?;
    let client_order_id = read_u64(buf, off)?;
    let symbol = read_symbol8(buf, off)?;
    let side = read_side(buf, off)?;
    let order_type = read_order_type(buf, off)?;
    let price = read_option_u64(buf, off)?;
    let quantity = read_u64(buf, off)?;
    let time_in_force = read_tif(buf, off)?;

    Ok(Command::Place {
        client_id,
        client_order_id,
        symbol,
        side,
        order_type,
        price,
        quantity,
        time_in_force,
    })
}

fn parse_cancel(buf: &[u8], off: &mut usize) -> Result<Command, &'static str> {
    let client_id = read_u64(buf, off)?;
    let client_order_id = read_u64(buf, off)?;

    Ok(Command::Cancel { client_id, client_order_id })
}

fn parse_modify(buf: &[u8], off: &mut usize) -> Result<Command, &'static str> {
    let client_id = read_u64(buf, off)?;
    let client_order_id = read_u64(buf, off)?;
    let new_price = read_option_u64(buf, off)?;
    let new_qty = read_option_u64(buf, off)?;

    Ok(Command::Modify { client_id, client_order_id, new_price, new_qty })
}

fn read_u8(buf: &[u8], off: &mut usize) -> Result<u8, &'static str> {
    if *off + 1 > buf.len() {
        return Err("buffer underflow");
    }
    let v = buf[*off];
    *off += 1;
    Ok(v)
}

fn read_u64(buf: &[u8], off: &mut usize) -> Result<u64, &'static str> {
    if *off + 8 > buf.len() {
        return Err("buffer underflow");
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&buf[*off..*off + 8]);
    *off += 8;
    Ok(u64::from_le_bytes(arr))
}

fn read_symbol8(buf: &[u8], off: &mut usize) -> Result<[u8; 8], &'static str> {
    if *off + 8 > buf.len() {
        return Err("buffer underflow");
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&buf[*off..*off + 8]);
    *off += 8;
    Ok(arr)
}

fn read_side(buf: &[u8], off: &mut usize) -> Result<Side, &'static str> {
    match read_u8(buf, off)? {
        0 => Ok(Side::BUY),
        1 => Ok(Side::SELL),
        _ => Err("invalid side"),
    }
}

fn read_order_type(buf: &[u8], off: &mut usize) -> Result<OrderType, &'static str> {
    match read_u8(buf, off)? {
        0 => Ok(OrderType::LIMIT),
        1 => Ok(OrderType::MARKET),
        _ => Err("invalid order type"),
    }
}

fn read_tif(buf: &[u8], off: &mut usize) -> Result<TimeInForce, &'static str> {
    match read_u8(buf, off)? {
        0 => Ok(TimeInForce::GTC),
        1 => Ok(TimeInForce::IOC),
        2 => Ok(TimeInForce::FOK),
        _ => Err("invalid time in force"),
    }
}

fn read_option_u64(buf: &[u8], off: &mut usize) -> Result<Option<u64>, &'static str> {
    match read_u8(buf, off)? {
        0 => Ok(None),
        1 => Ok(Some(read_u64(buf, off)?)),
        _ => Err("invalid option flag"),
    }
}

#[cfg(test)]
pub fn parse_command_for_test(buf: &[u8]) -> Result<Command, &'static str> {
    parse_command(buf)
}