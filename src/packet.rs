use std::array::TryFromSliceError;

pub(crate) const MAX_UDP_PACKET_SIZE: usize = 65507;
pub(crate) const PACKET_HEADER_SIZE: usize = 8; // size in bytes of the fields before Packet.body
pub const MAX_PACKET_SIZE: usize = MAX_UDP_PACKET_SIZE - PACKET_HEADER_SIZE;

#[derive(Clone, PartialEq, Eq)]
pub struct Packet {
    pub id: u64,
    pub body: Vec<u8>,
}

impl Packet {
    pub fn to_bytes(&self) -> Vec<u8> {
        let id = u64::to_le_bytes(self.id);
        [id.as_slice(), self.body.as_slice()].concat()
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TryFromSliceError> {
        let id = u64::from_le_bytes(<[u8; 8]>::try_from(&bytes[..8])?);
        let body = bytes[8..].to_vec();
        Ok(Self { id, body })
    }
}

pub const CONNECT_PACKET: Packet = Packet {
    id: u64::MAX,
    body: Vec::new(),
};
pub const KEEPALIVE_PACKET: Packet = Packet {
    id: u64::MAX - 1,
    body: Vec::new(),
};
