//! Process-level tests for the `ftw` command-line contract.

use flate2::{Compression, write::GzEncoder};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};

struct CliOutput {
    arguments: String,
    status: ExitStatus,
    stdout: String,
    stderr: String,
}

impl CliOutput {
    fn assert_success(&self) {
        assert!(
            self.status.success(),
            "ftw {} failed with {:?}\nstdout:\n{}\nstderr:\n{}",
            self.arguments,
            self.status.code(),
            self.stdout,
            self.stderr
        );
    }

    fn assert_code(&self, expected: i32) {
        assert_eq!(
            self.status.code(),
            Some(expected),
            "ftw {} returned the wrong code\nstdout:\n{}\nstderr:\n{}",
            self.arguments,
            self.stdout,
            self.stderr
        );
    }
}

fn assert_runtime_error(output: &CliOutput) {
    output.assert_code(1);
    assert!(
        output.stdout.is_empty(),
        "ftw {} wrote to stdout on a runtime error: {}",
        output.arguments,
        output.stdout
    );
    assert!(
        output.stderr.starts_with("error:"),
        "ftw {} did not report a runtime error: {}",
        output.arguments,
        output.stderr
    );
    assert!(
        !output.stderr.contains("usage:\n"),
        "ftw {} printed usage for a runtime error: {}",
        output.arguments,
        output.stderr
    );
}

fn ftw(current_dir: &Path, arguments: &[&str]) -> CliOutput {
    let output = Command::new(env!("CARGO_BIN_EXE_ftw"))
        .current_dir(current_dir)
        .args(arguments)
        .output()
        .expect("spawning the ftw binary must succeed");
    CliOutput {
        arguments: arguments.join(" "),
        status: output.status,
        stdout: String::from_utf8(output.stdout).expect("stdout must be UTF-8"),
        stderr: String::from_utf8(output.stderr).expect("stderr must be UTF-8"),
    }
}

fn path_str(path: &Path) -> &str {
    path.to_str().expect("temporary paths must be UTF-8")
}

fn json_record(output: &CliOutput) -> Value {
    let mut lines = output.stdout.lines();
    let record = lines
        .next()
        .unwrap_or_else(|| panic!("ftw {} emitted no JSON", output.arguments));
    assert!(
        lines.next().is_none() && output.stdout == format!("{record}\n"),
        "ftw {} did not emit exactly one JSON line: {:?}",
        output.arguments,
        output.stdout
    );
    let value: Value = serde_json::from_str(record).unwrap_or_else(|error| {
        panic!(
            "ftw {} emitted invalid JSON: {error}\n{}",
            output.arguments, output.stdout
        )
    });
    assert!(
        value.is_object(),
        "ftw {} emitted JSON that is not an object: {}",
        output.arguments,
        output.stdout
    );
    value
}

fn json_u64(record: &Value, key: &str) -> u64 {
    record
        .get(key)
        .unwrap_or_else(|| panic!("missing JSON field {key} in {record}"))
        .as_u64()
        .unwrap_or_else(|| panic!("JSON field {key} is not an unsigned integer in {record}"))
}

fn json_string<'a>(record: &'a Value, key: &str) -> &'a str {
    record
        .get(key)
        .unwrap_or_else(|| panic!("missing JSON field {key} in {record}"))
        .as_str()
        .unwrap_or_else(|| panic!("JSON field {key} is not a string in {record}"))
}

fn text_fields(output: &CliOutput) -> BTreeMap<&str, &str> {
    let mut fields = BTreeMap::new();
    for line in output.stdout.lines() {
        let (key, value) = line
            .split_once(": ")
            .unwrap_or_else(|| panic!("invalid inspect line {line:?}"));
        assert!(fields.insert(key, value).is_none(), "duplicate field {key}");
    }
    fields
}

#[derive(Debug, Eq, PartialEq)]
enum TreeEntry {
    Directory,
    File(Vec<u8>),
}

fn snapshot_tree(root: &Path) -> Vec<(PathBuf, TreeEntry)> {
    fn visit(root: &Path, directory: &Path, snapshot: &mut Vec<(PathBuf, TreeEntry)>) {
        let mut entries = fs::read_dir(directory)
            .unwrap_or_else(|error| panic!("reading {} failed: {error}", directory.display()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        entries.sort_by_key(fs::DirEntry::file_name);
        for entry in entries {
            let path = entry.path();
            let relative = path.strip_prefix(root).unwrap().to_owned();
            let file_type = entry.file_type().unwrap();
            if file_type.is_dir() {
                snapshot.push((relative, TreeEntry::Directory));
                visit(root, &path, snapshot);
            } else if file_type.is_file() {
                snapshot.push((relative, TreeEntry::File(fs::read(&path).unwrap())));
            } else {
                panic!("unexpected file type at {}", path.display());
            }
        }
    }

    let mut snapshot = Vec::new();
    visit(root, root, &mut snapshot);
    snapshot
}

fn check_store(current_dir: &Path, store: &Path, points: u64, commits: u64) -> CliOutput {
    let checked = ftw(current_dir, &["check-store", path_str(store)]);
    checked.assert_success();
    let record = json_record(&checked);
    assert_eq!(json_string(&record, "format"), "ftwdb-integrity-v1");
    assert_eq!(json_u64(&record, "raw_points"), points);
    assert_eq!(json_u64(&record, "raw_commits"), commits);
    assert_eq!(json_u64(&record, "raw_recovered_tail_bytes"), 0);
    assert_eq!(json_string(&record, "raw_recovered_tail"), "none");
    checked
}

#[test]
fn usage_errors_exit_two_on_stderr_without_creating_files() {
    let directory = tempfile::tempdir().unwrap();
    let cases: &[&[&str]] = &[
        &[],
        &["unknown-command"],
        &["inspect"],
        &["inspect", "one", "two"],
        &["check-store"],
        &["check-store", "one", "two"],
        &["backup"],
        &["backup", "source"],
        &["backup", "source", "destination", "extra"],
        &["generate"],
        &["bench-ftwdb"],
        &["bench-ftwdb", "workload"],
        &["bench-real-fixture"],
        &["bench-real-fixture", "fixture.csv.gz"],
        &["bench-tsbs-iot"],
        &["bench-tsbs-iot", "fixture.influx"],
        &["generate", "workload", "--seed"],
        &["generate", "would-be-workload", "--sites"],
        &["generate", "workload", "--days"],
        &["generate", "workload", "--cadence-seconds"],
        &["generate", "workload", "--start-micros"],
        &["generate", "workload", "--unknown", "1"],
        &["generate", "workload", "--seed", "not-a-number"],
        &["generate", "workload", "--sites", "not-a-number"],
        &["generate", "workload", "--sites", "0"],
        &["generate", "workload", "--sites", "257"],
        &["generate", "workload", "--days", "0"],
        &["generate", "workload", "--days", "367"],
        &["generate", "workload", "--cadence-seconds", "0"],
        &["generate", "workload", "--cadence-seconds", "86401"],
        &[
            "generate",
            "workload",
            "--start-micros",
            "9223372036854775807",
        ],
        &[
            "generate",
            "workload",
            "--sites",
            "2",
            "--days",
            "366",
            "--cadence-seconds",
            "60",
        ],
        &["bench-ftwdb", "workload", "store", "--durability"],
        &["bench-ftwdb", "workload", "store", "--batch-points"],
        &["bench-ftwdb", "workload", "store", "--unknown", "1"],
        &[
            "bench-ftwdb",
            "workload",
            "store",
            "--durability",
            "sometimes",
        ],
        &[
            "bench-ftwdb",
            "workload",
            "store",
            "--batch-points",
            "not-a-number",
        ],
        &["bench-ftwdb", "workload", "store", "--batch-points", "0"],
        &[
            "bench-ftwdb",
            "workload",
            "store",
            "--batch-points",
            "262145",
        ],
        &[
            "bench-real-fixture",
            "fixture.csv.gz",
            "store",
            "--durability",
        ],
        &[
            "bench-real-fixture",
            "fixture.csv.gz",
            "store",
            "--batch-points",
        ],
        &[
            "bench-real-fixture",
            "fixture.csv.gz",
            "store",
            "--unknown",
            "1",
        ],
        &[
            "bench-real-fixture",
            "fixture.csv.gz",
            "store",
            "--durability",
            "sometimes",
        ],
        &[
            "bench-real-fixture",
            "fixture.csv.gz",
            "store",
            "--durability",
            "every-bytes:not-a-number",
        ],
        &[
            "bench-real-fixture",
            "fixture.csv.gz",
            "store",
            "--durability",
            "every-bytes:0",
        ],
        &[
            "bench-real-fixture",
            "fixture.csv.gz",
            "store",
            "--batch-points",
            "0",
        ],
        &[
            "bench-real-fixture",
            "fixture.csv.gz",
            "store",
            "--batch-points",
            "262145",
        ],
        &["bench-tsbs-iot", "fixture.influx", "store", "--durability"],
        &["bench-tsbs-iot", "fixture.influx", "store", "--batch-rows"],
        &[
            "bench-tsbs-iot",
            "fixture.influx",
            "store",
            "--unknown",
            "1",
        ],
        &[
            "bench-tsbs-iot",
            "fixture.influx",
            "store",
            "--durability",
            "sometimes",
        ],
        &[
            "bench-tsbs-iot",
            "fixture.influx",
            "store",
            "--durability",
            "every-bytes:not-a-number",
        ],
        &[
            "bench-tsbs-iot",
            "fixture.influx",
            "store",
            "--durability",
            "every-bytes:0",
        ],
        &[
            "bench-tsbs-iot",
            "fixture.influx",
            "store",
            "--batch-rows",
            "0",
        ],
        &[
            "bench-tsbs-iot",
            "fixture.influx",
            "store",
            "--batch-rows",
            "26215",
        ],
    ];

    for arguments in cases {
        let before = snapshot_tree(directory.path());
        let output = ftw(directory.path(), arguments);
        output.assert_code(2);
        assert!(output.stdout.is_empty(), "usage leaked to stdout");
        assert!(
            output.stderr.contains("usage:\n"),
            "ftw {} did not print usage on stderr: {}",
            output.arguments,
            output.stderr
        );
        assert_eq!(snapshot_tree(directory.path()), before);
    }
}

#[test]
fn nonempty_benchmark_destinations_are_runtime_errors() {
    let directory = tempfile::tempdir().unwrap();
    for (command, input) in [
        ("bench-ftwdb", "missing-workload"),
        ("bench-real-fixture", "missing.csv.gz"),
        ("bench-tsbs-iot", "missing.influx"),
    ] {
        let destination = directory.path().join(command);
        fs::create_dir(&destination).unwrap();
        fs::write(destination.join("keep"), b"unchanged").unwrap();
        let before = snapshot_tree(&destination);

        let output = ftw(directory.path(), &[command, input, path_str(&destination)]);
        assert_runtime_error(&output);
        assert!(
            output
                .stderr
                .contains("benchmark database directory must be absent or empty"),
            "ftw {} returned the wrong error: {}",
            output.arguments,
            output.stderr
        );
        assert_eq!(snapshot_tree(&destination), before);
    }
}

#[test]
fn generated_store_commands_round_trip_without_read_only_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let workload = directory.path().join("workload");
    let store = directory.path().join("store");
    let backup = directory.path().join("backup");

    let generated = ftw(
        directory.path(),
        &[
            "generate",
            path_str(&workload),
            "--seed",
            "7",
            "--sites",
            "1",
            "--days",
            "1",
            "--cadence-seconds",
            "3600",
        ],
    );
    generated.assert_success();
    let generated_json = json_record(&generated);
    assert_eq!(
        json_string(&generated_json, "format"),
        "ftwdb-energy-workload-v1"
    );
    let generated_points = json_u64(&generated_json, "points");
    let generated_crc = json_string(&generated_json, "crc32").to_owned();
    assert!(generated_points > 0);
    assert!(json_u64(&generated_json, "entities") > 0);
    assert!(json_u64(&generated_json, "series") > 0);
    assert!(json_u64(&generated_json, "runs") > 0);
    assert!(json_u64(&generated_json, "plans") > 0);
    let generated_commits = 1 + generated_points.div_ceil(100);

    let bench = ftw(
        directory.path(),
        &[
            "bench-ftwdb",
            path_str(&workload),
            path_str(&store),
            "--durability",
            "manual",
            "--batch-points",
            "100",
        ],
    );
    bench.assert_success();
    let bench_json = json_record(&bench);
    assert_eq!(
        json_string(&bench_json, "format"),
        "ftwdb-benchmark-result-v1"
    );
    assert_eq!(json_string(&bench_json, "engine"), "ftwdb");
    assert_eq!(json_string(&bench_json, "durability"), "manual");
    assert_eq!(json_string(&bench_json, "dataset_crc32"), generated_crc);
    assert_eq!(json_u64(&bench_json, "points"), generated_points);
    assert!(json_u64(&bench_json, "query_buckets") > 0);

    let before_check = snapshot_tree(&store);
    let checked = check_store(
        directory.path(),
        &store,
        generated_points,
        generated_commits,
    );
    let checked_json = json_record(&checked);
    assert!(json_u64(&checked_json, "active_rollup_files") > 0);
    let manifest_generation = json_u64(&checked_json, "manifest_generation");
    assert_eq!(snapshot_tree(&store), before_check);

    let before_inspect = snapshot_tree(&store);
    let inspected = ftw(
        directory.path(),
        &["inspect", path_str(&store.join("active.wlog"))],
    );
    inspected.assert_success();
    let fields = text_fields(&inspected);
    assert_eq!(fields.get("format"), Some(&"ftwdb-v1"));
    assert_eq!(
        fields.get("points").and_then(|value| value.parse().ok()),
        Some(generated_points)
    );
    assert_eq!(
        fields.get("commits").and_then(|value| value.parse().ok()),
        Some(generated_commits)
    );
    assert_eq!(fields.get("recovered_tail_bytes"), Some(&"0"));
    assert_eq!(fields.get("recovered_tail"), Some(&"none"));
    assert_eq!(snapshot_tree(&store), before_inspect);

    let before_backup = snapshot_tree(&store);
    let backed_up = ftw(
        directory.path(),
        &["backup", path_str(&store), path_str(&backup)],
    );
    backed_up.assert_success();
    let backup_json = json_record(&backed_up);
    assert_eq!(json_string(&backup_json, "format"), "ftwdb-backup-v1");
    let files = json_u64(&backup_json, "files");
    let linked = json_u64(&backup_json, "linked_files");
    let copied = json_u64(&backup_json, "copied_files");
    assert!(files > 0);
    assert!(json_u64(&backup_json, "bytes") > 0);
    assert_eq!(
        json_u64(&backup_json, "manifest_generation"),
        manifest_generation
    );
    assert!(linked > 0);
    assert!(copied > 0);
    assert_eq!(linked + copied, files);
    assert_eq!(json_u64(&backup_json, "hard_link_fallbacks"), 0);
    assert!(
        backup_json["hard_link_fallback_error_kinds"]
            .as_array()
            .expect("fallback error kinds must be a JSON array")
            .is_empty()
    );
    assert_eq!(snapshot_tree(&store), before_backup);

    let before_backup_check = snapshot_tree(&backup);
    check_store(
        directory.path(),
        &backup,
        generated_points,
        generated_commits,
    );
    assert_eq!(snapshot_tree(&backup), before_backup_check);
}

#[test]
fn read_only_commands_do_not_create_missing_paths() {
    let directory = tempfile::tempdir().unwrap();
    let missing_file = directory.path().join("missing.ftwdb");
    let missing_store = directory.path().join("missing-store");
    let before = snapshot_tree(directory.path());

    let inspected = ftw(directory.path(), &["inspect", path_str(&missing_file)]);
    assert_runtime_error(&inspected);
    assert!(!missing_file.exists());
    assert_eq!(snapshot_tree(directory.path()), before);

    let checked = ftw(directory.path(), &["check-store", path_str(&missing_store)]);
    assert_runtime_error(&checked);
    assert!(!missing_store.exists());
    assert_eq!(snapshot_tree(directory.path()), before);
}

#[test]
fn small_real_fixture_runs_through_the_cli() {
    const CSV: &str = concat!(
        "driver_id,series_id,offset_ms,value\n",
        "1,1,0,12.5\n",
        "1,2,5,0\n",
        "2,3,3,-4.25\n",
        "1,1,20,13.5\n",
    );

    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("points.csv.gz");
    let store = directory.path().join("real-store");
    let mut encoder = GzEncoder::new(fs::File::create(&input).unwrap(), Compression::fast());
    encoder.write_all(CSV.as_bytes()).unwrap();
    encoder.finish().unwrap();

    let loaded = ftw(
        directory.path(),
        &[
            "bench-real-fixture",
            path_str(&input),
            path_str(&store),
            "--durability",
            "manual",
            "--batch-points",
            "2",
        ],
    );
    loaded.assert_success();
    let record = json_record(&loaded);
    assert_eq!(json_string(&record, "format"), "ftwdb-real-fixture-load-v1");
    assert_eq!(json_string(&record, "durability"), "manual");
    assert_eq!(json_u64(&record, "points"), 4);
    assert_eq!(json_u64(&record, "entities"), 2);
    assert_eq!(json_u64(&record, "series"), 3);
    assert_eq!(json_u64(&record, "commits"), 2);
    assert_eq!(json_u64(&record, "commits_durable_immediately"), 0);
    check_store(directory.path(), &store, 4, 2);
}

#[test]
fn small_tsbs_input_runs_through_the_cli() {
    const INFLUX: &str = concat!(
        "readings,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 ",
        "load_capacity=1500,velocity=0 1451606400000000000\n",
        "diagnostics,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 ",
        "fuel_state=1 1451606401000000000\n",
    );

    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("tsbs.influx");
    let store = directory.path().join("tsbs-store");
    fs::write(&input, INFLUX).unwrap();

    let loaded = ftw(
        directory.path(),
        &[
            "bench-tsbs-iot",
            path_str(&input),
            path_str(&store),
            "--durability",
            "manual",
            "--batch-rows",
            "1",
        ],
    );
    loaded.assert_success();
    let record = json_record(&loaded);
    assert_eq!(json_string(&record, "format"), "ftwdb-tsbs-iot-load-v1");
    assert_eq!(json_string(&record, "durability"), "manual");
    assert_eq!(json_u64(&record, "rows"), 2);
    assert_eq!(json_u64(&record, "points"), 3);
    assert_eq!(json_u64(&record, "entities"), 1);
    assert_eq!(json_u64(&record, "series"), 3);
    assert_eq!(json_u64(&record, "commits"), 2);
    assert_eq!(json_u64(&record, "commits_durable_immediately"), 0);
    check_store(directory.path(), &store, 3, 2);
}

#[test]
fn invalid_fixture_data_is_a_runtime_error() {
    let directory = tempfile::tempdir().unwrap();
    let csv = directory.path().join("invalid.csv.gz");
    let mut encoder = GzEncoder::new(fs::File::create(&csv).unwrap(), Compression::fast());
    encoder.write_all(b"wrong,header\n1,2\n").unwrap();
    encoder.finish().unwrap();

    let real = ftw(
        directory.path(),
        &[
            "bench-real-fixture",
            path_str(&csv),
            "invalid-real-store",
            "--durability",
            "manual",
            "--batch-points",
            "1",
        ],
    );
    assert_runtime_error(&real);

    let influx = directory.path().join("invalid.influx");
    fs::write(&influx, b"not a valid influx row\n").unwrap();
    let tsbs = ftw(
        directory.path(),
        &[
            "bench-tsbs-iot",
            path_str(&influx),
            "invalid-tsbs-store",
            "--durability",
            "manual",
            "--batch-rows",
            "1",
        ],
    );
    assert_runtime_error(&tsbs);
}
