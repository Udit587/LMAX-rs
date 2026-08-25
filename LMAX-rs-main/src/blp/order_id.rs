#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderId {
    pub index:      u32,
    pub generation: u32,
}