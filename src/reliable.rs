pub struct ReliableSender {
    pub(crate) stream: quinn::SendStream,
    label: String,
}

impl ReliableSender {
    pub(crate) fn new(stream: quinn::SendStream, label: String) -> Self {
        Self { stream, label }
    }

    /// This method is not cancellation safe. Even if this does not resolve, some prefix of data
    /// may have been written when previously polled.
    pub async fn send(&mut self, data: &[u8]) -> anyhow::Result<()> {
        let len: u32 = data.len().try_into().unwrap();
        self.stream.write_all(&len.to_le_bytes()).await?;
        self.stream.write_all(data).await?;
        Ok(())
    }

    pub fn label(&self) -> &str {
        &self.label
    }
}

pub struct ReliableReceiver {
    pub(crate) stream: quinn::RecvStream,
    label: String,
}

impl ReliableReceiver {
    pub(crate) fn new(stream: quinn::RecvStream, label: String) -> Self {
        Self { stream, label }
    }

    pub async fn recv(&mut self) -> anyhow::Result<Vec<u8>> {
        let mut len_bytes = [0u8; 4];
        self.stream.read_exact(&mut len_bytes).await?;
        let len = u32::from_le_bytes(len_bytes) as usize;
        let mut bytes = vec![0u8; len];
        self.stream.read_exact(&mut bytes).await?;
        Ok(bytes)
    }

    pub fn label(&self) -> &str {
        &self.label
    }
}
