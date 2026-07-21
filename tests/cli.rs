//! Integration smoke tests for the `ftw` binary.
//!
//! These spawn the compiled CLI with `std::process::Command`, so they cover
//! the argument parsing, exit codes, and output formats that unit tests of
//! the library cannot reach — including one cross-process advisory-lock
//! check that in-process lock tests cannot prove.

use std::path::Path;
use std::process::{Command, Output};

fn ftw(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ftw"))
        .args(arguments)
        .output()
        .expect("spawning the ftw binary must succeed")
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("stdout must be UTF-8")
}

fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).expect("stderr must be UTF-8")
}

/// The digits immediately following `"key":` in one-line JSON output.
fn json_u64(output: &str, key: &str) -> u64 {
    let pattern = format!("\"{key}\":");
    let start = output
        .find(&pattern)
        .unwrap_or_else(|| panic!("missing {pattern} in {output}"))
        + pattern.len();
    output[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .expect("JSON numeric field")
}

/// The quoted string immediately following `"key":` in one-line JSON output.
fn json_string(output: &str, key: &str) -> String {
    let pattern = format!("\"{key}\":\"");
    let start = output
        .find(&pattern)
        .unwrap_or_else(|| panic!("missing {pattern} in {output}"))
        + pattern.len();
    output[start..]
        .chars()
        .take_while(|character| *character != '"')
        .collect()
}

fn path_str(path: &Path) -> &str {
    path.to_str().expect("tempdir paths are UTF-8")
}

#[test]
fn generate_bench_check_inspect_and_backup_round_trip() {
    let directory = tempfile::tempdir().unwrap();
    let workload = directory.path().join("workload");
    let store = directory.path().join("store");
    let backup = directory.path().join("backup");

    // generate: a small deterministic bundle.
    let generated = ftw(&[
        "generate",
        path_str(&workload),
        "--sites",
        "1",
        "--days",
        "1",
        "--cadence-seconds",
        "3600",
        "--seed",
        "7",
    ]);
    assert!(generated.status.success(), "{}", stderr(&generated));
    let generated_out = stdout(&generated);
    assert!(generated_out.contains("\"format\":\"ftwdb-energy-workload-v1\""));
    let dataset_crc = json_string(&generated_out, "crc32");
    let generated_points = json_u64(&generated_out, "points");
    assert!(generated_points > 0);
    assert!(workload.join("workload.postcard").is_file());

    // bench-ftwdb: ingests the bundle into a fresh store and validates the
    // cold/warm query checksums itself.
    let bench = ftw(&[
        "bench-ftwdb",
        path_str(&workload),
        path_str(&store),
        "--durability",
        "manual",
    ]);
    assert!(bench.status.success(), "{}", stderr(&bench));
    let bench_out = stdout(&bench);
    assert!(bench_out.contains("\"format\":\"ftwdb-benchmark-result-v1\""));
    assert_eq!(json_string(&bench_out, "dataset_crc32"), dataset_crc);
    assert_eq!(json_u64(&bench_out, "points"), generated_points);

    // check-store: read-only integrity JSON over the freshly benched store.
    let checked = ftw(&["check-store", path_str(&store)]);
    assert!(checked.status.success(), "{}", stderr(&checked));
    let checked_out = stdout(&checked);
    assert!(checked_out.contains("\"format\":\"ftwdb-integrity-v1\""));
    assert_eq!(json_u64(&checked_out, "raw_points"), generated_points);
    assert_eq!(json_u64(&checked_out, "raw_recovered_tail_bytes"), 0);
    assert_eq!(json_string(&checked_out, "raw_recovered_tail"), "none");
    assert!(json_u64(&checked_out, "active_rollup_files") > 0);

    // inspect: the raw log inside the store agrees with the integrity check.
    let inspected = ftw(&["inspect", path_str(&store.join("active.wlog"))]);
    assert!(inspected.status.success(), "{}", stderr(&inspected));
    let inspected_out = stdout(&inspected);
    assert!(inspected_out.contains("format: ftwdb-v1"));
    assert!(inspected_out.contains(&format!("points: {generated_points}\n")));
    assert!(inspected_out.contains("recovered_tail_bytes: 0"));
    assert!(inspected_out.contains("recovered_tail: none"));

    // backup: publish a snapshot, then verify the copy stands on its own.
    let backed_up = ftw(&["backup", path_str(&store), path_str(&backup)]);
    assert!(backed_up.status.success(), "{}", stderr(&backed_up));
    let backed_up_out = stdout(&backed_up);
    assert!(backed_up_out.contains("\"format\":\"ftwdb-backup-v1\""));
    assert!(json_u64(&backed_up_out, "files") >= 3);
    let checked_backup = ftw(&["check-store", path_str(&backup)]);
    assert!(
        checked_backup.status.success(),
        "{}",
        stderr(&checked_backup)
    );
    assert_eq!(
        json_u64(&stdout(&checked_backup), "raw_points"),
        generated_points
    );
}

#[test]
fn inspect_on_a_missing_path_fails_without_creating_the_file() {
    let directory = tempfile::tempdir().unwrap();
    let absent = directory.path().join("absent.ftwdb");

    let inspected = ftw(&["inspect", path_str(&absent)]);
    assert_eq!(inspected.status.code(), Some(1));
    assert!(stderr(&inspected).starts_with("error:"));
    // Regression for issue #5: inspection must never create the file it was
    // asked to examine.
    assert!(!absent.exists());
}

#[test]
fn missing_subcommand_prints_usage_and_exits_two() {
    let output = ftw(&[]);
    assert_eq!(output.status.code(), Some(2));
    assert!(stderr(&output).contains("usage:"));

    let unknown = ftw(&["frobnicate"]);
    assert_eq!(unknown.status.code(), Some(2));
    assert!(stderr(&unknown).contains("usage:"));
}

#[test]
fn check_store_from_another_process_respects_the_writer_lock() {
    let directory = tempfile::tempdir().unwrap();
    // Hold the exclusive writer lock in this process. The in-process lock
    // tests already cover same-process exclusion; spawning the binary proves
    // the advisory lock actually crosses a process boundary.
    let store = ftwdb::Store::open(directory.path()).unwrap();

    let checked = ftw(&["check-store", path_str(directory.path())]);
    assert_eq!(checked.status.code(), Some(1));
    assert!(stderr(&checked).contains("locked"), "{}", stderr(&checked));

    // Releasing the writer lets the same command succeed.
    store.close().unwrap();
    let rechecked = ftw(&["check-store", path_str(directory.path())]);
    assert!(rechecked.status.success(), "{}", stderr(&rechecked));
}
