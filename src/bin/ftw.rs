use flate2::read::GzDecoder;
use ftwdb::{
    BackupReport, Config, Database, Durability, EnergyWorkload, Error, MaintenanceReport,
    RestoreReport, RollupResolution, SalvageOptions, SalvageReport, SealReport, Store, Transaction,
    WorkloadConfig, gauge_bucket_checksum, load_real_fixture_with_ack, load_tsbs_iot,
};
use std::env;
use std::fs::File;
use std::io::{BufReader, Write, stdin};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

const SECOND: i64 = 1_000_000;
const DAY: i64 = 86_400 * SECOND;
const FIVE_MINUTES: i64 = 300 * SECOND;
const TSBS_MAX_FIELDS_PER_ROW: usize = 10;

type CliResult<T> = std::result::Result<T, CliError>;

enum CliError {
    Usage(String),
    Runtime(Error),
}

impl From<Error> for CliError {
    fn from(error: Error) -> Self {
        Self::Runtime(error)
    }
}

impl From<std::io::Error> for CliError {
    fn from(error: std::io::Error) -> Self {
        Self::Runtime(Error::Io(error))
    }
}

fn main() -> ExitCode {
    let arguments: Vec<_> = env::args().collect();
    let result = match arguments.get(1).map(String::as_str) {
        Some("inspect") => inspect(&arguments[2..]),
        Some("check-store") => check_store(&arguments[2..]),
        Some("seal") => seal(&arguments[2..]),
        Some("maintain") => maintain(&arguments[2..]),
        Some("backup") => backup(&arguments[2..]),
        Some("restore") => restore(&arguments[2..]),
        Some("salvage") => salvage(&arguments[2..]),
        Some("generate") => generate(&arguments[2..]),
        Some("bench-ftwdb") => bench_ftwdb(&arguments[2..]),
        Some("bench-real-fixture") => bench_real_fixture(&arguments[2..]),
        Some("bench-tsbs-iot") => bench_tsbs_iot(&arguments[2..]),
        _ => {
            usage(arguments.first().map_or("ftwdb", String::as_str));
            return ExitCode::from(2);
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(CliError::Usage(reason)) => {
            eprintln!("error: {reason}");
            usage(arguments.first().map_or("ftwdb", String::as_str));
            ExitCode::from(2)
        }
        Err(CliError::Runtime(error)) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn bench_real_fixture(arguments: &[String]) -> CliResult<()> {
    if arguments.len() < 2 {
        return Err(usage_error(
            "bench-real-fixture requires a points.csv.gz file and empty database directory",
        ));
    }
    let input = &arguments[0];
    let database_directory = Path::new(&arguments[1]);

    let mut durability = Durability::Always;
    let mut durability_name = "always".to_owned();
    let mut batch_points = 10_000_usize;
    let mut ack_log = None::<PathBuf>;
    let mut index = 2;
    while index < arguments.len() {
        let option = &arguments[index];
        let value = arguments
            .get(index + 1)
            .ok_or_else(|| usage_error(format!("missing value for {option}")))?;
        match option.as_str() {
            "--batch-points" => batch_points = parse(value, option)?,
            "--ack-log" => ack_log = Some(PathBuf::from(value)),
            "--durability" if value == "always" => {
                durability = Durability::Always;
                durability_name = value.clone();
            }
            "--durability" if value == "manual" => {
                durability = Durability::Manual;
                durability_name = value.clone();
            }
            "--durability" if value.starts_with("every-bytes:") => {
                let bytes = parse_every_bytes(value)?;
                durability = Durability::EveryBytes(bytes);
                durability_name = format!("every-bytes:{bytes}");
            }
            "--durability" => {
                return Err(usage_error(
                    "durability must be always, manual, or every-bytes:N",
                ));
            }
            _ => return Err(usage_error(format!("unknown benchmark option {option}"))),
        }
        index += 2;
    }
    validate_batch_size(
        batch_points,
        Config::default().max_batch_points,
        "batch-points",
    )?;
    if database_directory.exists() && std::fs::read_dir(database_directory)?.next().is_some() {
        return Err(runtime_invalid(
            "benchmark database directory must be absent or empty",
        ));
    }

    let mut store = Store::open_with(
        database_directory,
        Config {
            durability,
            ..Config::default()
        },
    )?;
    let file = File::open(input)?;
    let reader = BufReader::new(GzDecoder::new(file));
    let mut ack_file = ack_log.as_ref().map(File::create).transpose()?;
    let started = Instant::now();
    let report = load_real_fixture_with_ack(reader, &mut store, batch_points, |ack| {
        if let Some(file) = ack_file.as_mut() {
            writeln!(
                file,
                "{{\"format\":\"ftwdb-ack-watermark-v1\",\"commits\":{},\"points\":{},\"durable\":{}}}",
                ack.commits, ack.points, ack.durable
            )?;
            file.sync_data()?;
        }
        Ok(())
    })?;
    let ingest_seconds = started.elapsed().as_secs_f64();
    let stored_bytes = directory_bytes(database_directory)?;
    let points_per_second = report.points as f64 / ingest_seconds;
    let bytes_per_point = if report.points == 0 {
        0.0
    } else {
        stored_bytes as f64 / report.points as f64
    };

    println!(
        "{{\"format\":\"ftwdb-real-fixture-load-v1\",\"engine\":\"ftwdb\",\"scope\":\"sanitized_real_installation_write\",\"durability\":\"{}\",\"batch_points\":{},\"points\":{},\"entities\":{},\"series\":{},\"commits\":{},\"commits_durable_immediately\":{},\"input_bytes\":{},\"input_crc32\":\"{:08x}\",\"points_crc32\":\"{:08x}\",\"first_offset_millis\":{},\"last_offset_millis\":{},\"ingest_seconds\":{:.9},\"points_per_second\":{:.3},\"stored_bytes\":{},\"bytes_per_point\":{:.3}}}",
        durability_name,
        batch_points,
        report.points,
        report.entities,
        report.series,
        report.commits,
        report.commits_durable_immediately,
        report.input_bytes,
        report.input_crc32,
        report.points_crc32,
        report.first_offset_millis,
        report.last_offset_millis,
        ingest_seconds,
        points_per_second,
        stored_bytes,
        bytes_per_point
    );
    Ok(())
}

fn bench_tsbs_iot(arguments: &[String]) -> CliResult<()> {
    if arguments.len() < 2 {
        return Err(usage_error(
            "bench-tsbs-iot requires an input file (or -) and empty database directory",
        ));
    }
    let input = &arguments[0];
    let database_directory = Path::new(&arguments[1]);

    let mut durability = Durability::Always;
    let mut durability_name = "always".to_owned();
    let mut batch_rows = 10_000_usize;
    let mut index = 2;
    while index < arguments.len() {
        let option = &arguments[index];
        let value = arguments
            .get(index + 1)
            .ok_or_else(|| usage_error(format!("missing value for {option}")))?;
        match option.as_str() {
            "--batch-rows" => batch_rows = parse(value, option)?,
            "--durability" if value == "always" => {
                durability = Durability::Always;
                durability_name = value.clone();
            }
            "--durability" if value == "manual" => {
                durability = Durability::Manual;
                durability_name = value.clone();
            }
            "--durability" if value.starts_with("every-bytes:") => {
                let bytes = parse_every_bytes(value)?;
                durability = Durability::EveryBytes(bytes);
                durability_name = format!("every-bytes:{bytes}");
            }
            "--durability" => {
                return Err(usage_error(
                    "durability must be always, manual, or every-bytes:N",
                ));
            }
            _ => return Err(usage_error(format!("unknown benchmark option {option}"))),
        }
        index += 2;
    }
    validate_batch_size(
        batch_rows,
        Config::default().max_batch_points / TSBS_MAX_FIELDS_PER_ROW,
        "batch-rows",
    )?;
    if database_directory.exists() && std::fs::read_dir(database_directory)?.next().is_some() {
        return Err(runtime_invalid(
            "benchmark database directory must be absent or empty",
        ));
    }

    let mut store = Store::open_with(
        database_directory,
        Config {
            durability,
            ..Config::default()
        },
    )?;
    let started = Instant::now();
    let report = if input == "-" {
        let input = stdin();
        load_tsbs_iot(input.lock(), &mut store, batch_rows)?
    } else {
        load_tsbs_iot(BufReader::new(File::open(input)?), &mut store, batch_rows)?
    };
    let ingest_seconds = started.elapsed().as_secs_f64();
    let stored_bytes = directory_bytes(database_directory)?;
    let points_per_second = report.points as f64 / ingest_seconds;
    let rows_per_second = report.rows as f64 / ingest_seconds;
    let bytes_per_point = if report.points == 0 {
        0.0
    } else {
        stored_bytes as f64 / report.points as f64
    };

    println!(
        "{{\"format\":\"ftwdb-tsbs-iot-load-v1\",\"engine\":\"ftwdb\",\"scope\":\"tsbs_iot_write\",\"durability\":\"{}\",\"batch_rows\":{},\"rows\":{},\"points\":{},\"entities\":{},\"series\":{},\"commits\":{},\"commits_durable_immediately\":{},\"input_bytes\":{},\"input_crc32\":\"{:08x}\",\"points_crc32\":\"{:08x}\",\"ingest_seconds\":{:.9},\"rows_per_second\":{:.3},\"points_per_second\":{:.3},\"stored_bytes\":{},\"bytes_per_point\":{:.3}}}",
        durability_name,
        batch_rows,
        report.rows,
        report.points,
        report.entities,
        report.series,
        report.commits,
        report.commits_durable_immediately,
        report.input_bytes,
        report.input_crc32,
        report.points_crc32,
        ingest_seconds,
        rows_per_second,
        points_per_second,
        stored_bytes,
        bytes_per_point
    );
    Ok(())
}

fn check_store(arguments: &[String]) -> CliResult<()> {
    if arguments.len() != 1 {
        return Err(usage_error("check-store requires one store directory"));
    }
    // Read-only: a check may not publish, prune, sweep, or recover in place.
    let store = Store::open_read_only(&arguments[0])?;
    let report = store.check_integrity()?;
    println!(
        "{{\"format\":\"ftwdb-integrity-v1\",\"manifest_generation\":{},\"raw_points\":{},\"raw_commits\":{},\"raw_recovered_tail_bytes\":{},\"raw_recovered_tail\":\"{}\",\"active_rollup_files\":{},\"active_rollup_buckets\":{},\"active_rollup_bytes\":{},\"stale_rollup_files\":{}}}",
        report.manifest_generation,
        report.raw_points,
        report.raw_commits,
        report.raw_recovered_tail_bytes,
        report.raw_recovered_tail,
        report.active_rollup_files,
        report.active_rollup_buckets,
        report.active_rollup_bytes,
        report.stale_rollup_files
    );
    Ok(())
}

fn seal(arguments: &[String]) -> CliResult<()> {
    if arguments.len() != 1 {
        return Err(usage_error("seal requires one store directory"));
    }
    let mut store = Store::open(&arguments[0])?;
    let report = store.seal_and_reclaim()?;
    println!("{}", seal_report_json(&report));
    Ok(())
}

fn maintain(arguments: &[String]) -> CliResult<()> {
    let mut store_path = None;
    let mut now_micros = None;
    let mut index = 0;
    while index < arguments.len() {
        match arguments[index].as_str() {
            "--now-micros" => {
                let value = arguments
                    .get(index + 1)
                    .ok_or_else(|| usage_error("maintain --now-micros requires a value"))?;
                now_micros = Some(parse(value, "--now-micros")?);
                index += 2;
            }
            option if option.starts_with("--") => {
                return Err(usage_error(format!("maintain does not support {option}")));
            }
            path => {
                if store_path.is_some() {
                    return Err(usage_error("maintain accepts one store directory"));
                }
                store_path = Some(path.to_owned());
                index += 1;
            }
        }
    }
    let Some(store_path) = store_path else {
        return Err(usage_error("maintain requires one store directory"));
    };
    let mut store = Store::open(&store_path)?;
    let now_micros = now_micros.unwrap_or_else(current_time_micros);
    let report = store.maintain(now_micros)?;
    println!("{}", maintain_report_json(&report));
    Ok(())
}

fn current_time_micros() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros(),
    )
    .unwrap_or(i64::MAX)
}

fn seal_report_json(report: &SealReport) -> String {
    format!(
        "{{\"format\":\"ftwdb-seal-v1\",\"manifest_generation\":{},\"segment_file\":\"{}\",\"sealed_points\":{},\"live_points\":{},\"segment_bytes\":{},\"log_bytes\":{}}}",
        report.manifest_generation,
        report.segment_file,
        report.sealed_points,
        report.live_points,
        report.segment_bytes,
        report.log_bytes
    )
}

fn maintain_report_json(report: &MaintenanceReport) -> String {
    format!(
        "{{\"format\":\"ftwdb-maintain-v1\",\"manifest_generation\":{},\"rollup_files_written\":{},\"rollup_buckets_written\":{},\"rollup_bytes_written\":{},\"retention_gates\":{}}}",
        report.manifest_generation,
        report.rollup_files_written,
        report.rollup_buckets_written,
        report.rollup_bytes_written,
        report.retention_gates.len()
    )
}

fn backup(arguments: &[String]) -> CliResult<()> {
    if arguments.len() != 2 {
        return Err(usage_error(
            "backup requires source and absent destination directories",
        ));
    }
    // Read-only: backing up must never alter (or create) the source store.
    let mut store = Store::open_read_only(&arguments[0])?;
    let report = store.backup_to(&arguments[1])?;
    println!("{}", backup_report_json(&report));
    Ok(())
}

fn restore(arguments: &[String]) -> CliResult<()> {
    if arguments.len() != 2 {
        return Err(usage_error(
            "restore requires a backup and absent target directory",
        ));
    }
    let report = Store::restore_from(&arguments[0], &arguments[1])?;
    println!("{}", restore_report_json(&report));
    Ok(())
}

fn restore_report_json(report: &RestoreReport) -> String {
    format!(
        "{{\"format\":\"ftwdb-restore-v1\",\"files\":{},\"bytes\":{},\"manifest_generation\":{},\"raw_commits\":{},\"raw_points\":{},\"source_snapshot_crc32\":\"{:08x}\",\"destination_snapshot_crc32\":\"{:08x}\"}}",
        report.files,
        report.bytes,
        report.manifest_generation,
        report.raw_commits,
        report.raw_points,
        report.source_snapshot_crc32,
        report.destination_snapshot_crc32
    )
}

fn salvage(arguments: &[String]) -> CliResult<()> {
    let mut drop_orphan_segments = false;
    let mut positional = Vec::new();
    let mut index = 0;
    while index < arguments.len() {
        match arguments[index].as_str() {
            "--drop-orphan-segments" => {
                drop_orphan_segments = true;
                index += 1;
            }
            option if option.starts_with("--") => {
                return Err(usage_error(format!("salvage does not support {option}")));
            }
            value => {
                positional.push(value.to_owned());
                index += 1;
            }
        }
    }
    if positional.len() != 2 {
        return Err(usage_error(
            "salvage requires a damaged store and absent target directory",
        ));
    }
    let report = Store::salvage_from_with_options(
        &positional[0],
        &positional[1],
        SalvageOptions {
            drop_orphan_segments,
        },
    )?;
    println!("{}", salvage_report_json(&report));
    Ok(())
}

fn salvage_report_json(report: &SalvageReport) -> String {
    format!(
        "{{\"format\":\"ftwdb-salvage-v1\",\"status\":\"{}\",\"source_bytes\":{},\"recovered_prefix_bytes\":{},\"discarded_bytes\":{},\"stop_offset\":{},\"stop_reason\":\"{}\",\"recovered_commits\":{},\"recovered_points\":{},\"source_prefix_crc32\":\"{:08x}\",\"destination_snapshot_crc32\":\"{:08x}\"}}",
        report.status,
        report.source_bytes,
        report.recovered_prefix_bytes,
        report.discarded_bytes,
        report.stop_offset,
        report.stop_reason,
        report.recovered_commits,
        report.recovered_points,
        report.source_prefix_crc32,
        report.destination_snapshot_crc32
    )
}

fn backup_report_json(report: &BackupReport) -> String {
    let fallback_error_kinds = report
        .hard_link_fallback_error_kinds
        .iter()
        .map(|kind| format!("\"{}\"", io_error_kind_name(*kind)))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"format\":\"ftwdb-backup-v1\",\"files\":{},\"bytes\":{},\"manifest_generation\":{},\"linked_files\":{},\"copied_files\":{},\"hard_link_fallbacks\":{},\"hard_link_fallback_error_kinds\":[{}]}}",
        report.files,
        report.bytes,
        report.manifest_generation,
        report.linked_files,
        report.copied_files,
        report.hard_link_fallbacks,
        fallback_error_kinds
    )
}

fn io_error_kind_name(kind: std::io::ErrorKind) -> &'static str {
    use std::io::ErrorKind;

    match kind {
        ErrorKind::NotFound => "not-found",
        ErrorKind::PermissionDenied => "permission-denied",
        ErrorKind::AlreadyExists => "already-exists",
        ErrorKind::CrossesDevices => "crosses-devices",
        ErrorKind::ReadOnlyFilesystem => "read-only-filesystem",
        ErrorKind::StorageFull => "storage-full",
        ErrorKind::QuotaExceeded => "quota-exceeded",
        ErrorKind::Unsupported => "unsupported",
        ErrorKind::InvalidInput => "invalid-input",
        ErrorKind::InvalidData => "invalid-data",
        ErrorKind::FileTooLarge => "file-too-large",
        ErrorKind::ResourceBusy => "resource-busy",
        _ => "other",
    }
}

fn inspect(arguments: &[String]) -> CliResult<()> {
    if arguments.len() != 1 {
        return Err(usage_error("inspect requires one database file"));
    }
    // Read-only: inspection must not create a missing file or truncate a torn
    // tail in the file being examined; recovery is simulated in memory.
    let stats = Database::open_read_only(&arguments[0]).and_then(|database| database.stats())?;
    println!("format: ftwdb-v1");
    println!("points: {}", stats.points);
    println!("commits: {}", stats.commits);
    println!("series: {}", stats.series);
    println!("catalog_records: {}", stats.catalog_records);
    println!("file_bytes: {}", stats.file_bytes);
    println!("recovered_tail_bytes: {}", stats.recovered_tail_bytes);
    println!("recovered_tail: {}", stats.recovered_tail);
    Ok(())
}

fn generate(arguments: &[String]) -> CliResult<()> {
    let Some(output) = arguments.first() else {
        return Err(usage_error("generate requires an output directory"));
    };
    let mut config = WorkloadConfig::default();
    let mut index = 1;
    while index < arguments.len() {
        let option = &arguments[index];
        let value = arguments
            .get(index + 1)
            .ok_or_else(|| usage_error(format!("missing value for {option}")))?;
        match option.as_str() {
            "--seed" => config.seed = parse(value, option)?,
            "--sites" => config.sites = parse(value, option)?,
            "--days" => config.days = parse(value, option)?,
            "--cadence-seconds" => config.cadence_seconds = parse(value, option)?,
            "--start-micros" => config.start_micros = parse(value, option)?,
            _ => return Err(usage_error(format!("unknown generate option {option}"))),
        }
        index += 2;
    }
    config
        .validate()
        .map_err(|error| usage_error(error.to_string()))?;
    let workload = EnergyWorkload::generate(config)?;
    let summary = workload.write_bundle(output)?;
    println!(
        "{{\"format\":\"ftwdb-energy-workload-v1\",\"seed\":{},\"entities\":{},\"series\":{},\"runs\":{},\"plans\":{},\"points\":{},\"crc32\":\"{:08x}\"}}",
        config.seed,
        summary.entities,
        summary.series,
        summary.runs,
        summary.plans,
        summary.points,
        summary.crc32
    );
    Ok(())
}

fn bench_ftwdb(arguments: &[String]) -> CliResult<()> {
    if arguments.len() < 2 {
        return Err(usage_error(
            "bench-ftwdb requires workload and empty database directories",
        ));
    }
    let workload_directory = Path::new(&arguments[0]);
    let database_directory = Path::new(&arguments[1]);
    let mut durability = Durability::Always;
    let mut durability_name = "always";
    let mut batch_points = 10_000_usize;
    let mut index = 2;
    while index < arguments.len() {
        let option = &arguments[index];
        let value = arguments
            .get(index + 1)
            .ok_or_else(|| usage_error(format!("missing value for {option}")))?;
        match option.as_str() {
            "--durability" if value == "always" => {
                durability = Durability::Always;
                durability_name = "always";
            }
            "--durability" if value == "manual" => {
                durability = Durability::Manual;
                durability_name = "manual";
            }
            "--durability" => {
                return Err(usage_error("durability must be always or manual"));
            }
            "--batch-points" => batch_points = parse(value, option)?,
            _ => return Err(usage_error(format!("unknown benchmark option {option}"))),
        }
        index += 2;
    }
    validate_batch_size(
        batch_points,
        Config::default().max_batch_points,
        "batch-points",
    )?;
    if database_directory.exists() && std::fs::read_dir(database_directory)?.next().is_some() {
        return Err(runtime_invalid(
            "benchmark database directory must be absent or empty",
        ));
    }

    let workload = EnergyWorkload::read_bundle(workload_directory)?;
    let summary = workload.summary();
    let mut store = Store::open_with(
        database_directory,
        Config {
            durability,
            ..Config::default()
        },
    )?;
    let ingest_started = Instant::now();
    store.commit(workload.metadata_transaction())?;
    for points in workload.points.chunks(batch_points) {
        let mut transaction = Transaction::new();
        transaction.append_points(points.to_vec());
        store.commit(transaction)?;
    }
    store.flush()?;
    let ingest_seconds = ingest_started.elapsed().as_secs_f64();

    let end = workload
        .config
        .start_micros
        .checked_add(i64::from(workload.config.days) * DAY)
        .ok_or_else(|| runtime_invalid("workload end timestamp overflow"))?;
    let maintenance_started = Instant::now();
    let maintenance = store.maintain(end)?;
    let maintenance_seconds = maintenance_started.elapsed().as_secs_f64();
    let resolution = RollupResolution::FixedMicros(FIVE_MINUTES);
    let cold_query_started = Instant::now();
    let query = store.query_gauge(1, workload.config.start_micros, end, &resolution)?;
    let cold_query_seconds = cold_query_started.elapsed().as_secs_f64();
    let result_crc = gauge_bucket_checksum(&query.buckets);
    let warm_query_started = Instant::now();
    let warm_query = store.query_gauge(1, workload.config.start_micros, end, &resolution)?;
    let warm_query_seconds = warm_query_started.elapsed().as_secs_f64();
    if gauge_bucket_checksum(&warm_query.buckets) != result_crc {
        return Err(runtime_invalid("cold and warm rollup query results differ"));
    }
    let stored_bytes = directory_bytes(database_directory)?;
    let points_per_second = summary.points as f64 / ingest_seconds;

    println!(
        "{{\"format\":\"ftwdb-benchmark-result-v1\",\"engine\":\"ftwdb\",\"durability\":\"{}\",\"dataset_crc32\":\"{:08x}\",\"result_crc32\":\"{:08x}\",\"points\":{},\"batch_points\":{},\"ingest_seconds\":{:.9},\"points_per_second\":{:.3},\"maintenance_seconds\":{:.9},\"rollup_files_written\":{},\"cold_query_seconds\":{:.9},\"warm_query_seconds\":{:.9},\"query_buckets\":{},\"stored_bytes\":{}}}",
        durability_name,
        summary.crc32,
        result_crc,
        summary.points,
        batch_points,
        ingest_seconds,
        points_per_second,
        maintenance_seconds,
        maintenance.rollup_files_written,
        cold_query_seconds,
        warm_query_seconds,
        query.buckets.len(),
        stored_bytes
    );
    Ok(())
}

fn directory_bytes(path: &Path) -> CliResult<u64> {
    let mut total = 0_u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total = total.saturating_add(directory_bytes(&entry.path())?);
        } else {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

fn parse<T>(value: &str, option: &str) -> CliResult<T>
where
    T: std::str::FromStr,
{
    value
        .parse()
        .map_err(|_| usage_error(format!("invalid value for {option}: {value}")))
}

fn parse_every_bytes(value: &str) -> CliResult<u64> {
    let bytes = parse(
        value.trim_start_matches("every-bytes:"),
        "--durability every-bytes",
    )?;
    if bytes == 0 {
        return Err(usage_error(
            "--durability every-bytes threshold must be positive",
        ));
    }
    Ok(bytes)
}

fn validate_batch_size(value: usize, maximum: usize, option: &str) -> CliResult<()> {
    if value == 0 || value > maximum {
        return Err(usage_error(format!(
            "{option} must be between 1 and {maximum}"
        )));
    }
    Ok(())
}

fn usage_error(reason: impl Into<String>) -> CliError {
    CliError::Usage(reason.into())
}

fn runtime_invalid(reason: impl Into<String>) -> CliError {
    CliError::Runtime(Error::InvalidModel(reason.into()))
}

fn usage(program: &str) {
    eprintln!(
        "usage:\n  {program} inspect <database-file>\n  {program} check-store <store-directory>\n  {program} seal <store-directory>\n  {program} maintain <store-directory> [--now-micros N]\n  {program} backup <store-directory> <absent-destination>\n  {program} restore <backup-directory> <absent-target>\n  {program} salvage <damaged-store> <absent-target> [--drop-orphan-segments]\n  {program} generate <output-directory> [--seed N] [--sites N] [--days N] [--cadence-seconds N] [--start-micros N]\n  {program} bench-ftwdb <workload-directory> <empty-database-directory> [--durability always|manual] [--batch-points N]\n  {program} bench-real-fixture <points.csv.gz> <empty-database-directory> [--durability always|manual|every-bytes:N] [--batch-points N] [--ack-log FILE]\n  {program} bench-tsbs-iot <influx-line-file|-> <empty-database-directory> [--durability always|manual|every-bytes:N] [--batch-rows N]"
    );
}

#[cfg(test)]
mod tests {
    use super::{backup_report_json, restore_report_json, salvage_report_json};
    use ftwdb::{BackupReport, RestoreReport, SalvageReport, SalvageStatus, SalvageStopReason};

    #[test]
    fn backup_json_uses_fixed_error_kind_names() {
        let report = BackupReport {
            files: 4,
            bytes: 123,
            manifest_generation: 7,
            linked_files: 2,
            copied_files: 2,
            hard_link_fallbacks: 1,
            hard_link_fallback_error_kinds: vec![std::io::ErrorKind::CrossesDevices],
        };

        assert_eq!(
            backup_report_json(&report),
            "{\"format\":\"ftwdb-backup-v1\",\"files\":4,\"bytes\":123,\"manifest_generation\":7,\"linked_files\":2,\"copied_files\":2,\"hard_link_fallbacks\":1,\"hard_link_fallback_error_kinds\":[\"crosses-devices\"]}"
        );
    }

    #[test]
    fn restore_json_has_versioned_checksums_and_counts() {
        let report = RestoreReport {
            files: 4,
            bytes: 123,
            manifest_generation: 7,
            raw_commits: 8,
            raw_points: 9,
            source_snapshot_crc32: 0x0123_abcd,
            destination_snapshot_crc32: 0x0123_abcd,
        };

        assert_eq!(
            restore_report_json(&report),
            "{\"format\":\"ftwdb-restore-v1\",\"files\":4,\"bytes\":123,\"manifest_generation\":7,\"raw_commits\":8,\"raw_points\":9,\"source_snapshot_crc32\":\"0123abcd\",\"destination_snapshot_crc32\":\"0123abcd\"}"
        );
    }

    #[test]
    fn salvage_json_has_fixed_status_reason_checksums_and_counts() {
        let report = SalvageReport {
            status: SalvageStatus::Partial,
            source_bytes: 130,
            recovered_prefix_bytes: 123,
            discarded_bytes: 7,
            stop_offset: 123,
            stop_reason: SalvageStopReason::IncompleteFrameHeader,
            recovered_commits: 8,
            recovered_points: 9,
            source_prefix_crc32: 0x0123_abcd,
            destination_snapshot_crc32: 0x0123_abcd,
        };

        assert_eq!(
            salvage_report_json(&report),
            "{\"format\":\"ftwdb-salvage-v1\",\"status\":\"partial\",\"source_bytes\":130,\"recovered_prefix_bytes\":123,\"discarded_bytes\":7,\"stop_offset\":123,\"stop_reason\":\"incomplete-frame-header\",\"recovered_commits\":8,\"recovered_points\":9,\"source_prefix_crc32\":\"0123abcd\",\"destination_snapshot_crc32\":\"0123abcd\"}"
        );
    }
}
