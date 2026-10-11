use super::*;
use serde_json::Value;
use test_data_store::test_run_storage::TestRunId;
use tokio::sync::Notify;

struct ControlledDispatcher {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    fail: bool,
}

#[async_trait]
impl SourceChangeDispatcher for ControlledDispatcher {
    async fn close(&mut self) -> anyhow::Result<()> {
        self.entered.notify_one();
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

async fn state(
    fail: bool,
) -> (
    BuildingHierarchyDataGeneratorInternalState,
    tempfile::TempDir,
    Arc<Notify>,
    Arc<Notify>,
) {
    let directory = tempfile::tempdir().unwrap();
    let config: Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../examples/building_comfort/dynamic/config.json"
    )))
    .unwrap();
    let definition = config["data_store"]["test_repos"][0]["local_tests"][0]["sources"][0].clone();
    let model = serde_json::from_value(definition["model_data_generator"].clone()).unwrap();
    let input = TestSourceStorage {
        id: "facilities-db".to_owned(),
        path: directory.path().to_owned(),
        repo_id: "repo".to_owned(),
        test_id: "test".to_owned(),
        test_source_definition: serde_json::from_value(definition).unwrap(),
    };
    let id = TestRunSourceId::new(&TestRunId::new("repo", "test", "run"), "facilities-db");
    let output = TestRunSourceStorage {
        id: id.clone(),
        path: directory.path().to_owned(),
        source_change_path: directory.path().join("changes"),
    };
    let settings = BuildingHierarchyDataGeneratorSettings::new(id, model, input, output, vec![])
        .await
        .unwrap();
    let graph = Arc::new(Mutex::new(BuildingGraph::new(&settings).unwrap()));
    let (mut state, _) = BuildingHierarchyDataGeneratorInternalState::initialize(settings, graph)
        .await
        .unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    state.dispatchers.push(Box::new(ControlledDispatcher {
        entered: entered.clone(),
        release: release.clone(),
        fail,
    }));
    state.status = SourceChangeGeneratorStatus::Running;
    (state, directory, entered, release)
}

#[tokio::test]
async fn finished_state_is_not_published_while_drain_is_pending() {
    let (mut state, _directory, entered, _release) = state(false).await;
    {
        let finish = state.transition_to_finished_state();
        tokio::pin!(finish);
        tokio::select! {
            result = &mut finish => panic!("Finished before drain: {result:?}"),
            _ = entered.notified() => {}
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut finish)
                .await
                .is_err()
        );
    }
    assert_eq!(state.status, SourceChangeGeneratorStatus::Running);
    assert_eq!(state.stats.actual_end_time_ns, 0);
}

#[tokio::test]
async fn successful_drain_then_finishes() {
    let (mut state, _directory, entered, release) = state(false).await;
    {
        let finish = state.transition_to_finished_state();
        tokio::pin!(finish);
        tokio::select! {
            result = &mut finish => panic!("Finished before drain: {result:?}"),
            _ = entered.notified() => {}
        }
        release.notify_one();
        finish.await.unwrap();
    }
    assert_eq!(state.status, SourceChangeGeneratorStatus::Finished);
    assert!(state.stats.actual_end_time_ns > 0);
    assert!(state.error_messages.is_empty());
}

#[tokio::test]
async fn failed_drain_enters_error_instead_of_finished() {
    let (mut state, _directory, _entered, release) = state(true).await;
    release.notify_one();
    let error = state.transition_to_finished_state().await.unwrap_err();
    assert!(error.to_string().contains("final batch failed"));
    assert_eq!(state.status, SourceChangeGeneratorStatus::Error);
    assert!(state
        .error_messages
        .iter()
        .any(|message| message.contains("Failed to close source dispatchers")));
}
