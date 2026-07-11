use wincode::{SchemaRead, SchemaWrite};

pub const MAX_PACKET_SIZE: usize = 65507;
pub const PACKET_HEADER_SIZE: usize = 8; // bytes
pub const MAX_PACKET_BODY_SIZE: usize = MAX_PACKET_SIZE - PACKET_HEADER_SIZE;

#[derive(Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct Packet {
    pub timestamp: i64,
    pub body: Vec<u8>,
}

pub const CONNECT_PACKET: Packet = Packet {
    timestamp: 0,
    body: Vec::new(),
};
