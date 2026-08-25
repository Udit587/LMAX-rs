use crate::output_event::rejectReason::RejectReason;
use crate::order::side::Side;

#[derive(Clone, Copy,Debug)]
pub enum OutputEvent {
    OrderAccepted {
        order_id: u64,
        client_id: u64,
        client_order_id: u64,
        symbol: [u8; 8],
        side: Side,
        price: u64,
        quantity: u64,
    },
    OrderRejected {
        client_id: u64,
        client_order_id: u64,
        reason: RejectReason,
    },
    Fill {
        aggressor_order_id: u64,
        resting_order_id: u64,
        aggressor_client_id: u64,
        resting_client_id: u64,
        symbol: [u8; 8],
        price: u64,
        quantity: u64,
        aggressor_side: Side,
    },
    OrderCancelled {
        order_id: u64,
        client_id: u64,
        leaves_qty: u64,
    },
    CancelRejected {
        order_id: u64,
        client_id: u64,
    },
    OrderModified {
        order_id: u64,
        client_id: u64,
        new_price: u64,
        new_qty: u64,
    },
    ModifyRejected {
        order_id: u64,
        client_id: u64,
    },
}