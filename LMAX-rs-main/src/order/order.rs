use crate::order::side::Side;
use crate::order::orderType::OrderType;
use crate::order::timeInForce::TimeInForce;

// This is NOT what comes in from the wire
// This is what BLP creates after accepting a Place command
// and stores internally in the OrderBook/OrderStore
#[derive(Clone, Copy)]
pub struct Order {
    pub order_id: u64,          // exchange-assigned, monotonically increasing
    pub client_id: u64,
    pub client_order_id: u64,
    pub symbol: [u8; 8],
    pub side: Side,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub price: u64,             // no Option here — market orders never rest
    pub quantity: u64,          // original quantity
    pub leaves_qty: u64,        // remaining unfilled
    pub filled_qty: u64,        // how much filled so far
}