use std::array::TryFromSliceError;

pub(crate) const MAX_UDP_PACKET_SIZE: usize = 65507;
pub(crate) const PACKET_HEADER_SIZE: usize = 8; // size in bytes of the fields before Packet.body
pub const MAX_PACKET_SIZE: usize = MAX_UDP_PACKET_SIZE - PACKET_HEADER_SIZE;

#[derive(Clone, PartialEq, Eq)]
pub struct Packet {
    pub timestamp: i64,
    pub body: Vec<u8>,
}

impl Packet {
    pub fn to_bytes(&self) -> Vec<u8> {
        let timestamp = i64::to_le_bytes(self.timestamp);
        [timestamp.as_slice(), self.body.as_slice()].concat()
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TryFromSliceError> {
        let timestamp = i64::from_le_bytes(<[u8; 8]>::try_from(&bytes[..8])?);
        let body = bytes[8..].to_vec();
        Ok(Self { timestamp, body })
    }
}

pub const CONNECT_PACKET: Packet = Packet {
    timestamp: 0,
    body: Vec::new(),
};
pub const KEEPALIVE_PACKET: Packet = Packet {
    timestamp: 1,
    body: Vec::new(),
};
