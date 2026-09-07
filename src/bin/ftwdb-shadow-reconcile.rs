use ftwdb::Store;
use ftwdb::shadow_protocol::{self, Request, WireMessage};
use ftwdb::shadow_reconcile::{ShadowReconcileLimits, ShadowReconciliationReport};
use std::env;
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;
use std::process::ExitCode;

const MAX_HEX_FILE_BYTES: usize = shadow_protocol::MAX_FRAME_BYTES * 2 + 2;
const MAX_TOTAL_FRAME_BYTES: usize = 256 * 1024 * 1024;

fn main() -> ExitCode {
    if env::args_os().len() == 2 && env::args_os().nth(1).as_deref() == Some("--version".as_ref()) {
        println!("ftwdb-shadow-reconcile {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    match run(env::args().skip(1)) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(3),
        Err(error) => {
            eprintln!("ftwdb-shadow-reconcile: {error}");
            usage();
            ExitCode::from(2)
        }
    }
}

fn run(arguments: impl IntoIterator<Item = String>) -> Result<bool, String> {
    let limits = ShadowReconcileLimits::default();
    let (store_argument, frame_paths) = collect_arguments(arguments, limits.max_batches)?;
    let store_path = Path::new(&store_argument);
    let mut batches = Vec::with_capacity(frame_paths.len());
    let mut total_frame_bytes = 0_usize;
    let mut total_metadata_records = 0_usize;
    let mut total_points = 0_usize;
    for frame_path in &frame_paths {
        let text = read_hex_file(Path::new(frame_path))?;
        let frame = decode_hex(&text).map_err(|error| format!("decode {frame_path}: {error}"))?;
        total_frame_bytes = add_with_limit(
            total_frame_bytes,
            frame.len(),
            MAX_TOTAL_FRAME_BYTES,
            "decoded frame bytes",
        )?;
        let message = shadow_protocol::decode(&frame)
            .map_err(|error| format!("parse {frame_path}: {error}"))?;
        let WireMessage::Request(Request::CommitBatch(batch)) = message else {
            return Err(format!("{frame_path} is not a commit-batch request"));
        };
        let metadata_records = batch
            .entities
            .len()
            .checked_add(batch.relations.len())
            .and_then(|count| count.checked_add(batch.series.len()))
            .and_then(|count| count.checked_add(batch.runs.len()))
            .and_then(|count| count.checked_add(batch.plans.len()))
            .ok_or_else(|| "metadata record count overflows".to_owned())?;
        total_metadata_records = add_with_limit(
            total_metadata_records,
            metadata_records,
            limits.max_metadata_records,
            "metadata records",
        )?;
        total_points = add_with_limit(
            total_points,
            batch.points.len(),
            limits.max_expected_points,
            "expected points",
        )?;
        batches.push(batch);
    }

    let store = Store::open_read_only(store_path)
        .map_err(|error| format!("open {} read-only: {error}", store_path.display()))?;
    let report = store
        .reconcile_shadow_batches(&batches, limits)
        .map_err(|error| format!("reconcile: {error}"))?;
    println!("{}", report_json(&report));
    for detail in &report.mismatch_details {
        eprintln!("mismatch: {detail:?}");
    }
    Ok(report.content_matches())
}

fn collect_arguments(
    arguments: impl IntoIterator<Item = String>,
    max_batches: usize,
) -> Result<(String, Vec<String>), String> {
    let mut arguments = arguments.into_iter();
    let Some(store) = arguments.next() else {
        return Err("a store directory and at least one commit frame are required".to_owned());
    };
    let mut frame_paths = Vec::new();
    for frame_path in arguments {
        if frame_paths.len() == max_batches {
            return Err(format!(
                "commit frame count exceeds the limit of {max_batches}"
            ));
        }
        frame_paths.push(frame_path);
    }
    if frame_paths.is_empty() {
        return Err("a store directory and at least one commit frame are required".to_owned());
    }
    Ok((store, frame_paths))
}

fn read_hex_file(path: &Path) -> Result<String, String> {
    let path_text = path.display();
    let metadata =
        fs::symlink_metadata(path).map_err(|error| format!("inspect {path_text}: {error}"))?;
    if !metadata.file_type().is_file() {
        return Err(format!("{path_text} is not a regular file"));
    }
    if metadata.len() > MAX_HEX_FILE_BYTES as u64 {
        return Err(format!("{path_text} exceeds the encoded frame limit"));
    }

    let mut file = File::open(path).map_err(|error| format!("open {path_text}: {error}"))?;
    let opened_metadata = file
        .metadata()
        .map_err(|error| format!("inspect opened {path_text}: {error}"))?;
    if !opened_metadata.file_type().is_file() {
        return Err(format!("{path_text} changed to a non-regular file"));
    }
    let mut bytes = Vec::with_capacity(
        usize::try_from(opened_metadata.len())
            .unwrap_or(MAX_HEX_FILE_BYTES)
            .min(MAX_HEX_FILE_BYTES),
    );
    file.by_ref()
        .take((MAX_HEX_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read {path_text}: {error}"))?;
    if bytes.len() > MAX_HEX_FILE_BYTES {
        return Err(format!("{path_text} exceeds the encoded frame limit"));
    }
    String::from_utf8(bytes).map_err(|_| format!("{path_text} is not UTF-8 hex text"))
}

fn add_with_limit(
    current: usize,
    addition: usize,
    maximum: usize,
    name: &str,
) -> Result<usize, String> {
    let total = current
        .checked_add(addition)
        .ok_or_else(|| format!("{name} count overflows"))?;
    if total > maximum {
        return Err(format!("{name} exceed the limit of {maximum}"));
    }
    Ok(total)
}

fn decode_hex(input: &str) -> Result<Vec<u8>, &'static str> {
    let input = input.trim();
    if !input.len().is_multiple_of(2) {
        return Err("hex has an odd number of digits");
    }
    if input.len() / 2 > shadow_protocol::MAX_FRAME_BYTES {
        return Err("frame exceeds the protocol limit");
    }
    input
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_digit(pair[0])?;
            let low = hex_digit(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

const fn hex_digit(value: u8) -> Result<u8, &'static str> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err("frame contains a non-hex byte"),
    }
}

fn report_json(report: &ShadowReconciliationReport) -> String {
    format!(
        concat!(
            "{{\"content_matches\":{},\"read_only_durability_proof\":false,",
            "\"expected_batches\":{},\"matching_receipts\":{},",
            "\"missing_receipts\":{},\"conflicting_receipts\":{},",
            "\"receipt_shape_mismatches\":{},\"receipt_payload_mismatches\":{},",
            "\"nondurable_receipts\":{},",
            "\"expected_catalog_objects\":{},\"matching_catalog_objects\":{},",
            "\"missing_catalog_objects\":{},\"different_catalog_objects\":{},",
            "\"expected_points\":{},\"scanned_points\":{},\"observed_points\":{},",
            "\"matching_points\":{},\"missing_points\":{},",
            "\"unexpected_points\":{},\"mismatch_groups\":{},",
            "\"reported_mismatch_details\":{},\"details_truncated\":{}}}"
        ),
        report.content_matches(),
        report.expected_batches,
        report.matching_receipts,
        report.missing_receipts,
        report.conflicting_receipts,
        report.receipt_shape_mismatches,
        report.receipt_payload_mismatches,
        report.nondurable_receipts,
        report.expected_catalog_objects,
        report.matching_catalog_objects,
        report.missing_catalog_objects,
        report.different_catalog_objects,
        report.expected_points,
        report.scanned_points,
        report.observed_points,
        report.matching_points,
        report.missing_points,
        report.unexpected_points,
        report.mismatch_groups,
        report.mismatch_details.len(),
        report.details_truncated,
    )
}

fn usage() {
    eprintln!(
        "usage: ftwdb-shadow-reconcile <store-directory> <commit-request.hex> [commit-request.hex ...]"
    );
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_HEX_FILE_BYTES, add_with_limit, collect_arguments, decode_hex, read_hex_file,
        report_json,
    };
    use ftwdb::shadow_reconcile::ShadowReconciliationReport;
    use std::fs::File;

    #[test]
    fn bounded_hex_decoder_rejects_bad_input() {
        assert_eq!(decode_hex("00aF").unwrap(), [0, 0xaf]);
        assert!(decode_hex("0").is_err());
        assert!(decode_hex("0g").is_err());
    }

    #[test]
    fn argument_file_and_total_limits_apply_before_reconciliation() {
        let error = collect_arguments(
            ["store", "one.hex", "two.hex", "three.hex"]
                .into_iter()
                .map(str::to_owned),
            2,
        )
        .unwrap_err();
        assert!(error.contains("frame count"));

        let directory = tempfile::tempdir().unwrap();
        assert!(
            read_hex_file(directory.path())
                .unwrap_err()
                .contains("regular file")
        );
        let oversized = directory.path().join("oversized.hex");
        File::create(&oversized)
            .unwrap()
            .set_len((MAX_HEX_FILE_BYTES + 1) as u64)
            .unwrap();
        assert!(
            read_hex_file(&oversized)
                .unwrap_err()
                .contains("encoded frame limit")
        );
        assert_eq!(add_with_limit(3, 2, 5, "items").unwrap(), 5);
        assert!(add_with_limit(5, 1, 5, "items").is_err());
    }

    #[test]
    fn report_line_is_stable_json() {
        let report = ShadowReconciliationReport {
            expected_batches: 2,
            receipt_payload_mismatches: 1,
            scanned_points: 3,
            missing_points: 1,
            mismatch_groups: 2,
            ..ShadowReconciliationReport::default()
        };
        let line = report_json(&report);
        assert!(line.starts_with("{\"content_matches\":false,"));
        assert!(line.contains("\"expected_batches\":2"));
        assert!(line.contains("\"receipt_payload_mismatches\":1"));
        assert!(line.contains("\"scanned_points\":3"));
        assert!(line.contains("\"missing_points\":1"));
        assert!(line.ends_with("\"details_truncated\":false}"));
    }
}
