use super::*;
use test_data_store::test_run_storage::TestRunId;
use tokio::sync::Notify;

struct DrainDispatcher {
    release: Arc<Notify>,
    fail: bool,
}

#[async_trait]
impl SourceChangeDispatcher for DrainDispatcher {
    async fn close(&mut self) -> anyhow::Result<()> {
        self.release.notified().await;
        if self.fail {
            anyhow::bail!("final batch failed");
        }
        Ok(())
    }

    async fn dispatch_source_change_events(
        &mut self,
        _events: Vec<&SourceChangeEvent>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn drain_barrier_waits_for_success_and_rejects_failure() {
    for fail in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let config: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../examples/stock_market/ci/drasi_server_http_grpc_join/config.json"
        )))
        .unwrap();
        let definition =
            config["data_store"]["test_repos"][0]["local_tests"][0]["sources"][0].clone();
        let model = serde_json::from_value(definition["model_data_generator"].clone()).unwrap();
        let input = TestSourceStorage {
            id: "stock-trades-db".into(),
            path: directory.path().to_owned(),
            repo_id: "repo".into(),
            test_id: "test".into(),
            test_source_definition: serde_json::from_value(definition).unwrap(),
        };
        let id = TestRunSourceId::new(&TestRunId::new("repo", "test", "run"), "stock-trades-db");
        let output = TestRunSourceStorage {
            id: id.clone(),
            path: directory.path().to_owned(),
            source_change_path: directory.path().join("changes"),
        };
        let settings = StockTradeDataGeneratorSettings::new(id, model, input, output, vec![])
            .await
            .unwrap();
        let market = Arc::new(Mutex::new(StockMarket::new(&settings).unwrap()));
        let (mut state, _receiver) =
            StockTradeDataGeneratorInternalState::initialize(settings, market)
                .await
                .unwrap();
        let release = Arc::new(Notify::new());
        state.dispatchers.push(Box::new(DrainDispatcher {
            release: release.clone(),
            fail,
        }));
        state.status = SourceChangeGeneratorStatus::Running;
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(20),
            state.transition_to_finished_state()
        )
        .await
        .is_err());
        assert_eq!(state.status, SourceChangeGeneratorStatus::Running);
        assert_eq!(state.stats.actual_end_time_ns, 0);
        release.notify_one();
        let result = state.transition_to_finished_state().await;
        if fail {
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("final batch failed"));
            assert_eq!(state.status, SourceChangeGeneratorStatus::Error);
        } else {
            result.unwrap();
            assert_eq!(state.status, SourceChangeGeneratorStatus::Finished);
            assert!(state.stats.actual_end_time_ns > 0);
        }
    }
}
