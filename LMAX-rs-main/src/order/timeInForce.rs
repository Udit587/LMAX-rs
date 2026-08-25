
#[derive(Clone, Copy,Debug)]
pub enum TimeInForce{
    DAY,
    IOC,
    FOK,
    GTC,
    GTD
}