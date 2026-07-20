pub struct ReliableSender(pub(crate) quinn::SendStream);

impl ReliableSender {
    /// This method is not cancellation safe. Even if this does not resolve, some prefix of data
    /// may have been written when previously polled.
    pub async fn send(&mut self, data: &[u8]) -> anyhow::Result<()> {
        let len: u32 = data.len().try_into().unwrap();
        self.0.write_all(&len.to_le_bytes()).await?;
        self.0.write_all(data).await?;
        Ok(())
    }
}

pub struct ReliableReceiver(pub(crate) quinn::RecvStream);

impl ReliableReceiver {
    pub async fn recv(&mut self) -> anyhow::Result<Vec<u8>> {
        let mut len_bytes = [0u8; 4];
        self.0.read_exact(&mut len_bytes).await?;
        let len = u32::from_le_bytes(len_bytes) as usize;
        let mut bytes = vec![0u8; len];
        self.0.read_exact(&mut bytes).await?;
        Ok(bytes)
    }
}
