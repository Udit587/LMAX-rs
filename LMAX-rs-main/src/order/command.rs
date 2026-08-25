use crate::order::side::Side;
use crate::order::orderType::OrderType;
use crate::order::timeInForce::TimeInForce;

#[derive(Clone, Copy,Debug)]
pub enum Command {
    Place {
        client_id: u64,
        client_order_id: u64,   // client's own ID
        symbol: [u8; 8],
        side: Side,
        order_type: OrderType,
        price: Option<u64>,     // None for Market
        quantity: u64,
        time_in_force: TimeInForce,
    },
    Cancel {
        client_id: u64,
        client_order_id: u64,   // client's own ID, not exchange-assigned
    },
    Modify {
        client_id: u64,
        client_order_id: u64,   // client's own ID, not exchange-assigned
        new_price: Option<u64>,
        new_qty: Option<u64>,
    },
}