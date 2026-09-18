use anyhow::{bail, Context, Result};
use test_run_host::recovery_capture;
use test_run_host::recovery_comparison::{compare, Artifact, Verdict};

fn main() {
    match run() {
        Ok(code) => std::process::exit(code),
        Err(error) => {
            eprintln!("Invalid comparison: {error:#}");
            std::process::exit(2);
        }
    }
}

fn run() -> Result<i32> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() == 1 && args[0] == "--help" {
        println!("Usage: recovery-compare BASELINE.json RECOVERY.json");
        println!("       recovery-compare --import CAPTURE.json");
        println!("Writes a JSON report to stdout. Exit: 0 pass, 1 fail, 2 invalid/inconclusive.");
        println!("Requires exactly-once delivery in per-query order; duplicates and reordering fail.");
        return Ok(0);
    }
    if args.len() == 2 && args[0] == "--import" {
        let artifact = recovery_capture::load(std::path::Path::new(&args[1]))?;
        serde_json::to_writer_pretty(std::io::stdout(), &artifact)?;
        println!();
        return Ok(0);
    }
    if args.len() != 2 {
        bail!("Usage: recovery-compare BASELINE.json RECOVERY.json");
    }
    let baseline: Artifact = read_json(&args[0])?;
    let recovery: Artifact = read_json(&args[1])?;
    let report = compare(&baseline, &recovery)?;
    serde_json::to_writer_pretty(std::io::stdout(), &report)?;
    println!();
    Ok(match report.verdict {
        Verdict::Passed => 0,
        Verdict::Failed => 1,
        Verdict::Inconclusive => 2,
    })
}

fn read_json<T: serde::de::DeserializeOwned>(path: &str) -> Result<T> {
    let file = std::fs::File::open(path).with_context(|| format!("opening {path}"))?;
    serde_json::from_reader(std::io::BufReader::new(file))
        .with_context(|| format!("parsing {path}"))
}
