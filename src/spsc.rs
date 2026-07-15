use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crossbeam_queue::ArrayQueue;
use thiserror::Error;

fn is_closed<T>(channel: &Arc<ArrayQueue<T>>) -> bool {
    Arc::strong_count(channel) <= 1
}

pub struct SendError;

pub struct Sender<T>(Arc<ArrayQueue<T>>);

impl<T> Sender<T> {
    pub fn send(&self, value: T) -> Result<(), SendError> {
        if is_closed(&self.0) {
            return Err(SendError);
        }
        self.0.force_push(value);
        Ok(())
    }

    pub fn queue_len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Error, Debug)]
#[error("Channel sender disconnected")]
pub struct RecvError;

#[derive(Error, Debug)]
pub enum RecvTimeoutError {
    #[error("Call to recv timed out")]
    Timeout,
    #[error("Channel sender disconnected")]
    Disconnected,
}

pub struct Receiver<T>(Arc<ArrayQueue<T>>);

impl<T> Receiver<T> {
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let start = Instant::now();
        while Instant::now() - start < timeout {
            if is_closed(&self.0) {
                return Err(RecvTimeoutError::Disconnected);
            }
            if let Some(item) = self.0.pop() {
                return Ok(item);
            }
        }
        Err(RecvTimeoutError::Timeout)
    }

    pub fn recv(&self) -> Result<T, RecvError> {
        loop {
            if is_closed(&self.0) {
                return Err(RecvError);
            }
            if let Some(item) = self.0.pop() {
                return Ok(item);
            }
        }
    }

    pub fn queue_len(&self) -> usize {
        self.0.len()
    }
}

pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let queue = Arc::new(ArrayQueue::new(capacity));
    (Sender(queue.clone()), Receiver(queue))
}
