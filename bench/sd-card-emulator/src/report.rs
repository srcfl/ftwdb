use serde::Serialize;
use serde_json::Value;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

#[derive(Debug)]
pub struct VerifyInput<'a> {
    pub emulator: &'a Path,
    pub check: &'a Path,
    pub inspect: &'a Path,
    pub expected_points: u64,
    pub expected_commits: u64,
    pub writer_exit: Option<i32>,
    pub writer_signal: Option<i32>,
    pub checksum_ok: Option<bool>,
    pub ack_log: Option<&'a Path>,
    pub max_in_flight_commits: u64,
    pub max_in_flight_points: u64,
}

impl<'a> VerifyInput<'a> {
    fn mid_commit(&self) -> bool {
        self.ack_log.is_some()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AckWatermark {
    pub commits: u64,
    pub points: u64,
    pub durable: bool,
}

#[derive(Debug, Serialize)]
pub struct FaultRunReport {
    pub schema_version: &'static str,
    pub profile: String,
    pub seed: u64,
    pub fault_kind: Option<String>,
    pub fault_offset: Option<u64>,
    pub fault_operation: Option<u64>,
    pub writer_exit: Option<i32>,
    pub writer_signal: Option<i32>,
    pub acknowledged_commits: u64,
    pub acknowledged_points: u64,
    pub recovered_commits: u64,
    pub recovered_points: u64,
    pub recovered_tail_bytes: u64,
    pub in_flight_commits: u64,
    pub in_flight_points: u64,
    pub contains_every_ack: bool,
    pub has_at_most_one_unacknowledged: bool,
    pub silent_partial_frame: bool,
    pub write_amplification: f64,
    pub check_store_ok: bool,
    pub checksum_ok: Option<bool>,
    pub injected_operations: u64,
    pub injected_bytes: u64,
    pub reordered_operations: u64,
    pub dropped_operations: u64,
    pub dropped_bytes: u64,
    pub torn_operations: u64,
    pub torn_bytes: u64,
    pub max_erase_count: u64,
    pub bad_blocks: u64,
    pub emulator_version: String,
    pub emulator_commit: String,
    pub passed: bool,
}

pub fn last_durable_ack(path: &Path) -> Result<AckWatermark, VerifyError> {
    let contents = fs::read_to_string(path)?;
    let lines: Vec<_> = contents.split('\n').collect();
    let has_unterminated_final_line = !contents.ends_with('\n');
    let mut previous = None::<AckWatermark>;
    let mut last_durable = None;
    for (index, line) in lines.iter().enumerate() {
        // A JSONL record is complete only after its newline lands. A power
        // cut can leave a valid JSON prefix as the final unterminated line,
        // so never use that line as durability evidence.
        if has_unterminated_final_line && index + 1 == lines.len() {
            break;
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value = match serde_json::from_str::<Value>(line) {
            Ok(value) => value,
            Err(error) => {
                return Err(VerifyError::Invalid(format!(
                    "ack log line {} is not valid JSON: {error}",
                    index + 1
                )));
            }
        };
        if string(&value, "format")? != "ftwdb-ack-watermark-v1" {
            return Err(VerifyError::Invalid(format!(
                "ack log line {} has the wrong format",
                index + 1
            )));
        }
        let durable = value
            .get("durable")
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                VerifyError::Invalid(format!(
                    "ack log line {} lacks a boolean durable field",
                    index + 1
                ))
            })?;
        let watermark = AckWatermark {
            commits: unsigned(&value, "commits")?,
            points: unsigned(&value, "points")?,
            durable,
        };
        if let Some(previous) = previous
            && (watermark.commits <= previous.commits || watermark.points < previous.points)
        {
            return Err(VerifyError::Invalid(format!(
                "ack log line {} regresses its cumulative watermark",
                index + 1
            )));
        }
        previous = Some(watermark);
        if !durable {
            continue;
        }
        last_durable = Some(watermark);
    }
    last_durable.ok_or_else(|| {
        VerifyError::Invalid("ack log has no complete durable watermark line".to_owned())
    })
}

pub fn verify(input: &VerifyInput<'_>) -> Result<FaultRunReport, VerifyError> {
    let emulator = last_json_line(input.emulator)?;
    let stats = emulator.get("stats").unwrap_or(&emulator);
    let check = last_json_line(input.check)?;
    if string(stats, "schema_version")? != "ftw-sd-emulator-stats-v1" {
        return Err(VerifyError::Invalid(
            "emulator input has the wrong schema_version".to_owned(),
        ));
    }
    if string(&check, "format")? != "ftwdb-integrity-v1" {
        return Err(VerifyError::Invalid(
            "check input is not an ftwdb-integrity-v1 result".to_owned(),
        ));
    }
    let recovered_points = unsigned(&check, "raw_points")?;
    let recovered_commits = unsigned(&check, "raw_commits")?;
    let recovered_tail_bytes = parse_recovered_tail(&fs::read_to_string(input.inspect)?)?;
    let check_tail_bytes = unsigned(&check, "raw_recovered_tail_bytes")?;
    let recovered_tail_kind = string(&check, "raw_recovered_tail")?;
    let injected_torn_operations = unsigned(stats, "injected_torn_operations")?;
    let injected_torn_bytes = unsigned(stats, "injected_torn_bytes")?;
    let power_torn_operations = unsigned(stats, "torn_operations")?;
    let power_torn_bytes = unsigned(stats, "torn_bytes")?;
    let injected_eio = unsigned(stats, "injected_eio_operations")?;
    let corruptions = unsigned(stats, "injected_corruptions")?;
    let false_flushes = unsigned(stats, "false_flushes")?;
    let power_losses = unsigned(stats, "power_losses")?;
    let dropped_operations = unsigned(stats, "dropped_operations")?;
    let dropped_bytes = unsigned(stats, "dropped_bytes")?;
    let checksum_passed = input.checksum_ok != Some(false);
    let write_amplification = stats
        .get("write_amplification")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);

    let (acknowledged_commits, acknowledged_points) = if let Some(ack_log) = input.ack_log {
        let ack = last_durable_ack(ack_log)?;
        (ack.commits, ack.points)
    } else {
        (input.expected_commits, input.expected_points)
    };

    let contains_every_ack =
        recovered_commits >= acknowledged_commits && recovered_points >= acknowledged_points;
    let in_flight_commits = recovered_commits.saturating_sub(acknowledged_commits);
    let in_flight_points = recovered_points.saturating_sub(acknowledged_points);
    let has_at_most_one_unacknowledged = in_flight_commits <= input.max_in_flight_commits
        && in_flight_points <= input.max_in_flight_points;
    let silent_partial_frame = check_tail_bytes != recovered_tail_bytes
        || !matches!(
            recovered_tail_kind,
            "none" | "incomplete-header" | "incomplete-payload"
        )
        || (recovered_tail_kind == "none" && recovered_tail_bytes != 0);

    let passed = if input.mid_commit() {
        contains_every_ack
            && has_at_most_one_unacknowledged
            && !silent_partial_frame
            && checksum_passed
    } else {
        recovered_points == acknowledged_points
            && recovered_commits == acknowledged_commits
            && checksum_passed
    };

    Ok(FaultRunReport {
        schema_version: "ftw-sd-fault-run-v1",
        profile: string(stats, "profile")?.to_owned(),
        seed: unsigned(stats, "seed")?,
        fault_kind: optional_string(stats, "last_fault_kind"),
        fault_offset: optional_unsigned(stats, "last_fault_offset"),
        fault_operation: optional_unsigned(stats, "last_fault_operation"),
        writer_exit: input.writer_exit,
        writer_signal: input.writer_signal,
        acknowledged_commits,
        acknowledged_points,
        recovered_commits,
        recovered_points,
        recovered_tail_bytes,
        in_flight_commits,
        in_flight_points,
        contains_every_ack,
        has_at_most_one_unacknowledged,
        silent_partial_frame,
        write_amplification,
        check_store_ok: true,
        checksum_ok: input.checksum_ok,
        injected_operations: injected_eio
            + injected_torn_operations
            + corruptions
            + false_flushes
            + power_losses,
        injected_bytes: injected_torn_bytes + power_torn_bytes + dropped_bytes,
        reordered_operations: unsigned(stats, "reordered_operations")?,
        dropped_operations,
        dropped_bytes,
        torn_operations: injected_torn_operations + power_torn_operations,
        torn_bytes: injected_torn_bytes + power_torn_bytes,
        max_erase_count: unsigned(stats, "max_erase_count")?,
        bad_blocks: unsigned(stats, "bad_blocks")?,
        emulator_version: string(stats, "emulator_version")?.to_owned(),
        emulator_commit: string(stats, "emulator_commit")?.to_owned(),
        passed,
    })
}

pub fn append_report(path: impl AsRef<Path>, report: &FaultRunReport) -> Result<(), VerifyError> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, report)?;
    file.write_all(b"\n")?;
    file.sync_data()?;
    Ok(())
}

fn last_json_line(path: &Path) -> Result<Value, VerifyError> {
    let contents = fs::read_to_string(path)?;
    let line = contents
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .ok_or_else(|| VerifyError::Invalid(format!("{} is empty", path.display())))?;
    Ok(serde_json::from_str(line)?)
}

fn parse_recovered_tail(contents: &str) -> Result<u64, VerifyError> {
    contents
        .lines()
        .find_map(|line| line.strip_prefix("recovered_tail_bytes: "))
        .ok_or_else(|| {
            VerifyError::Invalid("inspect output lacks recovered_tail_bytes".to_owned())
        })?
        .parse()
        .map_err(|error| VerifyError::Invalid(format!("invalid recovered_tail_bytes: {error}")))
}

fn unsigned(value: &Value, field: &str) -> Result<u64, VerifyError> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| VerifyError::Invalid(format!("missing unsigned field {field:?}")))
}

fn optional_unsigned(value: &Value, field: &str) -> Option<u64> {
    value.get(field).and_then(Value::as_u64)
}

fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str, VerifyError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| VerifyError::Invalid(format!("missing string field {field:?}")))
}

fn optional_string(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_owned)
}

#[derive(Debug)]
pub enum VerifyError {
    Io(io::Error),
    Json(serde_json::Error),
    Invalid(String),
}

impl From<io::Error> for VerifyError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for VerifyError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl std::fmt::Display for VerifyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::Json(error) => write!(formatter, "JSON error: {error}"),
            Self::Invalid(error) => formatter.write_str(error),
        }
    }
}

impl std::error::Error for VerifyError {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn recovered_tail_parser_rejects_missing_field() {
        assert!(parse_recovered_tail("points: 10\n").is_err());
        assert_eq!(
            parse_recovered_tail("recovered_tail_bytes: 39\n").unwrap(),
            39
        );
    }

    #[test]
    fn verification_compares_the_acknowledged_watermark() {
        let directory =
            std::env::temp_dir().join(format!("ftw-sd-report-test-{}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        let emulator = directory.join("emulator.json");
        let check = directory.join("check.json");
        let inspect = directory.join("inspect.txt");
        fs::write(
            &emulator,
            serde_json::to_vec(&json!({
                "stats": {
                    "schema_version": "ftw-sd-emulator-stats-v1",
                    "profile": "test",
                    "seed": 42,
                    "last_fault_kind": "power_loss",
                    "last_fault_offset": 4096,
                    "last_fault_operation": 12,
                    "injected_torn_operations": 1,
                    "injected_torn_bytes": 10,
                    "torn_operations": 2,
                    "torn_bytes": 20,
                    "injected_eio_operations": 0,
                    "injected_corruptions": 0,
                    "false_flushes": 1,
                    "power_losses": 1,
                    "dropped_operations": 3,
                    "dropped_bytes": 30,
                    "reordered_operations": 4,
                    "max_erase_count": 5,
                    "bad_blocks": 0,
                    "emulator_version": "0.1.0",
                    "emulator_commit": "abc"
                }
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            &check,
            b"{\"format\":\"ftwdb-integrity-v1\",\"raw_points\":100,\"raw_commits\":10,\"raw_recovered_tail_bytes\":39,\"raw_recovered_tail\":\"incomplete-header\"}\n",
        )
        .unwrap();
        fs::write(&inspect, b"recovered_tail_bytes: 39\n").unwrap();
        let input = VerifyInput {
            emulator: &emulator,
            check: &check,
            inspect: &inspect,
            expected_points: 100,
            expected_commits: 10,
            writer_exit: Some(1),
            writer_signal: None,
            checksum_ok: Some(true),
            ack_log: None,
            max_in_flight_commits: 0,
            max_in_flight_points: 0,
        };
        let report = verify(&input).unwrap();
        assert!(report.passed);
        assert_eq!(report.injected_operations, 3);
        assert_eq!(report.torn_operations, 3);
        assert_eq!(report.injected_bytes, 60);

        let failed = verify(&VerifyInput {
            expected_points: 101,
            ..input
        })
        .unwrap();
        assert!(!failed.passed);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn last_durable_ack_skips_a_truncated_final_line() {
        let directory = std::env::temp_dir().join(format!("ftw-sd-ack-log-{}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("ack.jsonl");
        fs::write(
            &path,
            concat!(
                "{\"format\":\"ftwdb-ack-watermark-v1\",\"commits\":1,\"points\":10,\"durable\":true}\n",
                "{\"format\":\"ftwdb-ack-watermark-v1\",\"commits\":2,\"points\":20,\"durable\":true}\n",
                "{\"format\":\"ftwdb-ack-watermark-v1\",\"commits\":3,\"poi"
            ),
        )
        .unwrap();
        let ack = last_durable_ack(&path).unwrap();
        assert_eq!(ack.commits, 2);
        assert_eq!(ack.points, 20);

        fs::write(
            &path,
            concat!(
                "{\"format\":\"ftwdb-ack-watermark-v1\",\"commits\":1,\"points\":10,\"durable\":true}\n",
                "{\"format\":\"ftwdb-ack-watermark-v1\",\"commits\":2,\"points\":20,\"durable\":true}"
            ),
        )
        .unwrap();
        let ack = last_durable_ack(&path).unwrap();
        assert_eq!(ack.commits, 1);
        assert_eq!(ack.points, 10);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn last_durable_ack_rejects_malformed_complete_and_regressing_lines() {
        let directory =
            std::env::temp_dir().join(format!("ftw-sd-bad-ack-log-{}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("ack.jsonl");
        let first = "{\"format\":\"ftwdb-ack-watermark-v1\",\"commits\":2,\"points\":20,\"durable\":true}\n";

        fs::write(&path, format!("{first}not-json\n")).unwrap();
        assert!(last_durable_ack(&path).is_err());

        fs::write(
            &path,
            format!(
                "{first}{{\"format\":\"ftwdb-ack-watermark-v1\",\"commits\":1,\"points\":10,\"durable\":true}}\n"
            ),
        )
        .unwrap();
        assert!(last_durable_ack(&path).is_err());

        fs::write(
            &path,
            format!(
                "{first}{{\"format\":\"wrong\",\"commits\":3,\"points\":30,\"durable\":true}}\n"
            ),
        )
        .unwrap();
        assert!(last_durable_ack(&path).is_err());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn mid_commit_verify_accepts_one_in_flight_batch_and_rejects_lost_acks() {
        let directory =
            std::env::temp_dir().join(format!("ftw-sd-mid-commit-{}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        let emulator = directory.join("emulator.json");
        let check = directory.join("check.json");
        let inspect = directory.join("inspect.txt");
        let ack_log = directory.join("ack.jsonl");
        fs::write(
            &emulator,
            serde_json::to_vec(&json!({
                "schema_version": "ftw-sd-emulator-stats-v1",
                "profile": "healthy",
                "seed": 1,
                "injected_torn_operations": 0,
                "injected_torn_bytes": 0,
                "torn_operations": 0,
                "torn_bytes": 0,
                "injected_eio_operations": 0,
                "injected_corruptions": 0,
                "false_flushes": 0,
                "power_losses": 1,
                "dropped_operations": 0,
                "dropped_bytes": 0,
                "reordered_operations": 0,
                "max_erase_count": 0,
                "bad_blocks": 0,
                "write_amplification": 1.5,
                "emulator_version": "0.1.0",
                "emulator_commit": "abc"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            &check,
            b"{\"format\":\"ftwdb-integrity-v1\",\"raw_points\":30,\"raw_commits\":3,\"raw_recovered_tail_bytes\":7,\"raw_recovered_tail\":\"incomplete-header\"}\n",
        )
        .unwrap();
        fs::write(&inspect, b"recovered_tail_bytes: 7\n").unwrap();
        fs::write(
            &ack_log,
            "{\"format\":\"ftwdb-ack-watermark-v1\",\"commits\":2,\"points\":20,\"durable\":true}\n",
        )
        .unwrap();
        let input = VerifyInput {
            emulator: &emulator,
            check: &check,
            inspect: &inspect,
            expected_points: 0,
            expected_commits: 0,
            writer_exit: Some(1),
            writer_signal: None,
            checksum_ok: Some(true),
            ack_log: Some(&ack_log),
            max_in_flight_commits: 1,
            max_in_flight_points: 10,
        };
        let report = verify(&input).unwrap();
        assert!(report.passed);
        assert!(report.contains_every_ack);
        assert!(report.has_at_most_one_unacknowledged);
        assert!(!report.silent_partial_frame);
        assert_eq!(report.in_flight_commits, 1);
        assert_eq!(report.write_amplification, 1.5);

        fs::write(
            &ack_log,
            "{\"format\":\"ftwdb-ack-watermark-v1\",\"commits\":4,\"points\":40,\"durable\":true}\n",
        )
        .unwrap();
        let lost = verify(&input).unwrap();
        assert!(!lost.passed);
        assert!(!lost.contains_every_ack);

        fs::write(
            &ack_log,
            "{\"format\":\"ftwdb-ack-watermark-v1\",\"commits\":1,\"points\":10,\"durable\":true}\n",
        )
        .unwrap();
        fs::write(
            &check,
            b"{\"format\":\"ftwdb-integrity-v1\",\"raw_points\":40,\"raw_commits\":4,\"raw_recovered_tail_bytes\":0,\"raw_recovered_tail\":\"none\"}\n",
        )
        .unwrap();
        fs::write(&inspect, b"recovered_tail_bytes: 0\n").unwrap();
        let extra = verify(&input).unwrap();
        assert!(!extra.passed);
        assert!(!extra.has_at_most_one_unacknowledged);
        fs::remove_dir_all(directory).unwrap();
    }
}
