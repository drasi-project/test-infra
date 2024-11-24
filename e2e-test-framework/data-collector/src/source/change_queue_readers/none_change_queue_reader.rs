use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc::Receiver, RwLock};

use super::{SourceChangeQueueReader, SourceChangeQueueReaderMessage, SourceChangeQueueReaderStatus};

pub struct NoneSourceChangeQueueReader {
    status: Arc<RwLock<SourceChangeQueueReaderStatus>>,
}

impl NoneSourceChangeQueueReader {
    pub fn new() -> Box<dyn SourceChangeQueueReader + Send + Sync> {
        log::debug!("Creating NoneSourceChangeQueueReader");

        let status = Arc::new(RwLock::new(SourceChangeQueueReaderStatus::Uninitialized));

        Box::new(NoneSourceChangeQueueReader {
            status
        })
    }
}  

#[async_trait]
impl SourceChangeQueueReader for NoneSourceChangeQueueReader {
    async fn init(&self) -> anyhow::Result<Receiver<SourceChangeQueueReaderMessage>> {

        let mut status = self.status.write().await;
        match *status {
            SourceChangeQueueReaderStatus::Uninitialized => {
                let (_change_tx_channel, change_rx_channel) = tokio::sync::mpsc::channel(0);
                *status = SourceChangeQueueReaderStatus::Paused;
                Ok(change_rx_channel)
            },
            SourceChangeQueueReaderStatus::Running => {
                anyhow::bail!("Cant Init Reader, Reader currently Running");
            },
            SourceChangeQueueReaderStatus::Paused => {
                anyhow::bail!("Cant Init Reader, Reader currently Paused");
            },
            SourceChangeQueueReaderStatus::Stopped => {
                anyhow::bail!("Cant Init Reader, Reader currently Stopped");
            },            
            SourceChangeQueueReaderStatus::Error => {
                anyhow::bail!("Reader in Error state");
            },
        }
    }

    async fn start(&self) -> anyhow::Result<()> {

        let mut status = self.status.write().await;
        match *status {
            SourceChangeQueueReaderStatus::Uninitialized => {
                anyhow::bail!("Cant Start Reader, Reader Uninitialized");
            },
            SourceChangeQueueReaderStatus::Running => {
                Ok(())
            },
            SourceChangeQueueReaderStatus::Paused => {
                *status = SourceChangeQueueReaderStatus::Running;
                Ok(())
            },
            SourceChangeQueueReaderStatus::Stopped => {
                anyhow::bail!("Cant Start Reader, Reader already Stopped");
            },            
            SourceChangeQueueReaderStatus::Error => {
                anyhow::bail!("Reader in Error state");
            },
        }
    }

    async fn pause(&self) -> anyhow::Result<()> {

        let mut status = self.status.write().await;
        match *status {
            SourceChangeQueueReaderStatus::Uninitialized => {
                anyhow::bail!("Cant Pause Reader, Reader Uninitialized");
            },
            SourceChangeQueueReaderStatus::Running => {
                *status = SourceChangeQueueReaderStatus::Paused;
                Ok(())
            },
            SourceChangeQueueReaderStatus::Paused => {
                Ok(())
            },
            SourceChangeQueueReaderStatus::Stopped => {
                anyhow::bail!("Cant Pause Reader, Reader already Stopped");
            },            
            SourceChangeQueueReaderStatus::Error => {
                anyhow::bail!("Reader in Error state");
            },
        }
    }

    async fn stop(&self) -> anyhow::Result<()> {

        let mut status = self.status.write().await;
        match *status {
            SourceChangeQueueReaderStatus::Uninitialized => {
                anyhow::bail!("Reader not initialized, current status: Uninitialized");
            },
            SourceChangeQueueReaderStatus::Running => {
                *status = SourceChangeQueueReaderStatus::Stopped;
                Ok(())
            },
            SourceChangeQueueReaderStatus::Paused => {
                *status = SourceChangeQueueReaderStatus::Stopped;
                Ok(())
            },
            SourceChangeQueueReaderStatus::Stopped => {
                Ok(())
            },            
            SourceChangeQueueReaderStatus::Error => {
                anyhow::bail!("Reader in Error state");
            },
        }
    }
}