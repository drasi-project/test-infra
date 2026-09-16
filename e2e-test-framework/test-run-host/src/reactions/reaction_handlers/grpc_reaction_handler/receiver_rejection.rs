use std::path::PathBuf;

pub(super) struct ReceiverRejection {
    directory: PathBuf,
    port: u16,
    accepted_items: usize,
    rejected_requests: usize,
    restored: bool,
}

impl ReceiverRejection {
    pub(super) fn new(directory: PathBuf, port: u16) -> Self {
        Self {
            directory,
            port,
            accepted_items: 0,
            rejected_requests: 0,
            restored: false,
        }
    }

    pub(super) async fn before_request(&mut self) -> anyhow::Result<()> {
        if self.restored || self.accepted_items < 100 {
            return Ok(());
        }
        if tokio::fs::try_exists(self.directory.join(format!("{}.restore", self.port))).await? {
            self.write_evidence("restored").await?;
            self.restored = true;
            return Ok(());
        }
        self.rejected_requests += 1;
        self.write_evidence("rejecting").await?;
        anyhow::bail!("Injected receiver rejection before capture")
    }

    pub(super) fn accepted(&mut self, count: usize) {
        self.accepted_items += count;
    }

    async fn write_evidence(&self, phase: &str) -> anyhow::Result<()> {
        let evidence = serde_json::json!({
            "phase": phase,
            "port": self.port,
            "accepted_items_before_outage": self.accepted_items,
            "rejected_requests": self.rejected_requests,
        });
        let destination = self.directory.join(format!("{}.json", self.port));
        let temporary = destination.with_extension("tmp");
        tokio::fs::write(&temporary, serde_json::to_vec_pretty(&evidence)?).await?;
        tokio::fs::rename(temporary, destination).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn receivers_arm_independently_and_evidence_errors_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let mut first = ReceiverRejection::new(directory.path().to_owned(), 50052);
        let mut second = ReceiverRejection::new(directory.path().to_owned(), 50053);
        for _item in 0..100 {
            first.before_request().await.unwrap();
            first.accepted(1);
        }
        assert!(first.before_request().await.is_err());
        second.before_request().await.unwrap();
        second.accepted(100);
        tokio::fs::write(directory.path().join("50052.restore"), b"restore")
            .await
            .unwrap();
        first.before_request().await.unwrap();
        assert!(second.before_request().await.is_err());

        let mut missing = ReceiverRejection::new(directory.path().join("absent"), 50054);
        missing.accepted(100);
        assert!(missing.before_request().await.is_err());
        assert!(!missing.restored);
        assert_eq!(missing.accepted_items, 100);
    }
}
