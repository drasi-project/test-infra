use std::{collections::HashMap, time::Instant};

use serde_json::json;
use test_data_store::test_run_storage::{TestRunId, TestRunReactionId, TestRunReactionStorage};
use test_run_host::{
    common::{HandlerPayload, HandlerRecord},
    reactions::output_loggers::{create_output_loggers, OutputLoggerConfig},
};

#[tokio::test]
#[ignore = "diagnostic logger timing; run in release mode with --nocapture"]
async fn measure_recovery_output_logger_cost() {
    let iterations = 100000;
    let configurations = [
        ("hash", json!([{"kind":"DeterminismHash"}])),
        (
            "jsonl",
            json!([{"kind":"JsonlFile","max_lines_per_file":15000}]),
        ),
        (
            "hash-jsonl-metrics",
            json!([{"kind":"DeterminismHash"},{"kind":"JsonlFile","max_lines_per_file":15000},{"kind":"PerformanceMetrics"}]),
        ),
    ];
    println!("round,loggers,iterations,microseconds_per_record,records_per_second");
    let mut expected_hash = None;
    for round in 0..3 {
        for (label, config) in &configurations {
            let directory = tempfile::tempdir().unwrap();
            let run = TestRunId::new("probe", "logger", "run");
            let reaction = TestRunReactionId::new(&run, "building-comfort");
            let storage = TestRunReactionStorage {
                id: reaction.clone(),
                path: directory.path().to_owned(),
                reaction_output_path: directory.path().join("output"),
            };
            let configs: Vec<OutputLoggerConfig> = serde_json::from_value(config.clone()).unwrap();
            let mut loggers = create_output_loggers(reaction, &configs, &storage)
                .await
                .unwrap();
            let mut record = HandlerRecord {
                id: "grpc-invocation-1".to_owned(),
                sequence: 1,
                created_time_ns: 0,
                processed_time_ns: 0,
                traceparent: None,
                tracestate: None,
                payload: HandlerPayload::ReactionInvocation {
                    reaction_type: "Grpc".to_owned(),
                    query_id: "unknown".to_owned(),
                    request_method: "POST".to_owned(),
                    request_path: "/".to_owned(),
                    headers: HashMap::from([
                        ("x-drasi-producer-sequence".to_owned(), "99000".to_owned()),
                        (
                            "x-drasi-producer-row-signature".to_owned(),
                            "13660005145781501189".to_owned(),
                        ),
                        (
                            "x-drasi-producer-key".to_owned(),
                            "[\"building-comfort\",\"99000\",\"13660005145781501189\",2]"
                                .to_owned(),
                        ),
                    ]),
                    request_body: json!({"query_id":"building-comfort","result":{"type":"UPDATE",
                        "before":{"RoomId":"R_000_001_000","Temperature":5110,"Humidity":5061,"Co2":4769},
                        "after":{"RoomId":"R_000_001_000","Temperature":5111,"Humidity":5061,"Co2":4769}}}),
                },
            };
            let start = Instant::now();
            for sequence in 1..=iterations {
                record.sequence = sequence;
                for logger in &mut loggers {
                    logger.log_handler_record(&record).await.unwrap();
                }
            }
            for logger in &mut loggers {
                let result = logger.end_test_run().await.unwrap();
                if result.logger_name == "DeterminismHash" {
                    let summary = result.summary.unwrap();
                    assert_eq!(summary["record_count"], iterations);
                    let hash = summary["sha256"].as_str().unwrap().to_owned();
                    if let Some(expected) = &expected_hash {
                        assert_eq!(expected, &hash);
                    } else {
                        expected_hash = Some(hash);
                    }
                }
            }
            let seconds = start.elapsed().as_secs_f64();
            println!(
                "{round},{label},{iterations},{:.3},{:.1}",
                seconds * 1e6 / iterations as f64,
                iterations as f64 / seconds
            );
        }
    }
}
