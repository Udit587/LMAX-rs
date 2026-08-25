#[derive(Clone, Copy,Debug)]
pub enum RejectReason {
    InvalidPrice,
    InvalidQuantity,
    InvalidSymbol,
    ArenaFull,
    ClientOrderLimitExceeded,
    NoLiquidityForMarketOrder,
}