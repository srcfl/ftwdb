use flate2::read::GzDecoder;
use ftwdb::{
    Config, Database, Durability, EnergyWorkload, Error, Result, RollupResolution, Store,
    Transaction, WorkloadConfig, gauge_bucket_checksum, load_real_fixture, load_tsbs_iot,
};
use std::env;
use std::fs::File;
use std::io::{BufReader, stdin};
use std::path::Path;
use std::process::ExitCode;
use std::time::Instant;

const SECOND: i64 = 1_000_000;
const DAY: i64 = 86_400 * SECOND;
const FIVE_MINUTES: i64 = 300 * SECOND;

fn main() -> ExitCode {
    let arguments: Vec<_> = env::args().collect();
    let result = match arguments.get(1).map(String::as_str) {
        Some("inspect") => inspect(&arguments[2..]),
        Some("check-store") => check_store(&arguments[2..]),
        Some("backup") => backup(&arguments[2..]),
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
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn bench_real_fixture(arguments: &[String]) -> Result<()> {
    if arguments.len() < 2 {
        return Err(invalid(
            "bench-real-fixture requires a points.csv.gz file and empty database directory",
        ));
    }
    let input = &arguments[0];
    let database_directory = Path::new(&arguments[1]);
    if database_directory.exists() && std::fs::read_dir(database_directory)?.next().is_some() {
        return Err(invalid(
            "benchmark database directory must be absent or empty",
        ));
    }

    let mut durability = Durability::Always;
    let mut durability_name = "always".to_owned();
    let mut batch_points = 10_000_usize;
    let mut index = 2;
    while index < arguments.len() {
        let option = &arguments[index];
        let value = arguments
            .get(index + 1)
            .ok_or_else(|| invalid(format!("missing value for {option}")))?;
        match option.as_str() {
            "--batch-points" => batch_points = parse(value, option)?,
            "--durability" if value == "always" => {
                durability = Durability::Always;
                durability_name = value.clone();
            }
            "--durability" if value == "manual" => {
                durability = Durability::Manual;
                durability_name = value.clone();
            }
            "--durability" if value.starts_with("every-bytes:") => {
                let encoded = value.trim_start_matches("every-bytes:");
                let bytes = parse(encoded, "--durability every-bytes")?;
                durability = Durability::EveryBytes(bytes);
                durability_name = format!("every-bytes:{bytes}");
            }
            "--durability" => {
                return Err(invalid(
                    "durability must be always, manual, or every-bytes:N",
                ));
            }
            _ => return Err(invalid(format!("unknown benchmark option {option}"))),
        }
        index += 2;
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
    let started = Instant::now();
    let report = load_real_fixture(reader, &mut store, batch_points)?;
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

fn bench_tsbs_iot(arguments: &[String]) -> Result<()> {
    if arguments.len() < 2 {
        return Err(invalid(
            "bench-tsbs-iot requires an input file (or -) and empty database directory",
        ));
    }
    let input = &arguments[0];
    let database_directory = Path::new(&arguments[1]);
    if database_directory.exists() && std::fs::read_dir(database_directory)?.next().is_some() {
        return Err(invalid(
            "benchmark database directory must be absent or empty",
        ));
    }

    let mut durability = Durability::Always;
    let mut durability_name = "always".to_owned();
    let mut batch_rows = 10_000_usize;
    let mut index = 2;
    while index < arguments.len() {
        let option = &arguments[index];
        let value = arguments
            .get(index + 1)
            .ok_or_else(|| invalid(format!("missing value for {option}")))?;
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
                let encoded = value.trim_start_matches("every-bytes:");
                let bytes = parse(encoded, "--durability every-bytes")?;
                durability = Durability::EveryBytes(bytes);
                durability_name = format!("every-bytes:{bytes}");
            }
            "--durability" => {
                return Err(invalid(
                    "durability must be always, manual, or every-bytes:N",
                ));
            }
            _ => return Err(invalid(format!("unknown benchmark option {option}"))),
        }
        index += 2;
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

fn check_store(arguments: &[String]) -> Result<()> {
    if arguments.len() != 1 {
        return Err(invalid("check-store requires one store directory"));
    }
    // Read-only: a check may not publish, prune, sweep, or recover in place.
    let store = Store::open_read_only(&arguments[0])?;
    let report = store.check_integrity()?;
    println!(
        "{{\"format\":\"ftwdb-integrity-v1\",\"manifest_generation\":{},\"raw_points\":{},\"raw_commits\":{},\"active_rollup_files\":{},\"active_rollup_buckets\":{},\"active_rollup_bytes\":{},\"stale_rollup_files\":{}}}",
        report.manifest_generation,
        report.raw_points,
        report.raw_commits,
        report.active_rollup_files,
        report.active_rollup_buckets,
        report.active_rollup_bytes,
        report.stale_rollup_files
    );
    Ok(())
}

fn backup(arguments: &[String]) -> Result<()> {
    if arguments.len() != 2 {
        return Err(invalid(
            "backup requires source and absent destination directories",
        ));
    }
    // Read-only: backing up must never alter (or create) the source store.
    let mut store = Store::open_read_only(&arguments[0])?;
    let report = store.backup_to(&arguments[1])?;
    println!(
        "{{\"format\":\"ftwdb-backup-v1\",\"files\":{},\"bytes\":{},\"manifest_generation\":{}}}",
        report.files, report.bytes, report.manifest_generation
    );
    Ok(())
}

fn inspect(arguments: &[String]) -> Result<()> {
    if arguments.len() != 1 {
        return Err(invalid("inspect requires one database file"));
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
    Ok(())
}

fn generate(arguments: &[String]) -> Result<()> {
    let Some(output) = arguments.first() else {
        return Err(invalid("generate requires an output directory"));
    };
    let mut config = WorkloadConfig::default();
    let mut index = 1;
    while index < arguments.len() {
        let option = &arguments[index];
        let value = arguments
            .get(index + 1)
            .ok_or_else(|| invalid(format!("missing value for {option}")))?;
        match option.as_str() {
            "--seed" => config.seed = parse(value, option)?,
            "--sites" => config.sites = parse(value, option)?,
            "--days" => config.days = parse(value, option)?,
            "--cadence-seconds" => config.cadence_seconds = parse(value, option)?,
            "--start-micros" => config.start_micros = parse(value, option)?,
            _ => return Err(invalid(format!("unknown generate option {option}"))),
        }
        index += 2;
    }
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

fn bench_ftwdb(arguments: &[String]) -> Result<()> {
    if arguments.len() < 2 {
        return Err(invalid(
            "bench-ftwdb requires workload and empty database directories",
        ));
    }
    let workload_directory = Path::new(&arguments[0]);
    let database_directory = Path::new(&arguments[1]);
    if database_directory.exists() && std::fs::read_dir(database_directory)?.next().is_some() {
        return Err(invalid(
            "benchmark database directory must be absent or empty",
        ));
    }
    let mut durability = Durability::Always;
    let mut durability_name = "always";
    let mut batch_points = 10_000_usize;
    let mut index = 2;
    while index < arguments.len() {
        let option = &arguments[index];
        let value = arguments
            .get(index + 1)
            .ok_or_else(|| invalid(format!("missing value for {option}")))?;
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
                return Err(invalid("durability must be always or manual"));
            }
            "--batch-points" => batch_points = parse(value, option)?,
            _ => return Err(invalid(format!("unknown benchmark option {option}"))),
        }
        index += 2;
    }
    if batch_points == 0 || batch_points > Config::default().max_batch_points {
        return Err(invalid("batch-points is outside the configured limit"));
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
        .ok_or_else(|| invalid("workload end timestamp overflow"))?;
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
        return Err(invalid("cold and warm rollup query results differ"));
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

fn directory_bytes(path: &Path) -> Result<u64> {
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

fn parse<T>(value: &str, option: &str) -> Result<T>
where
    T: std::str::FromStr,
{
    value
        .parse()
        .map_err(|_| invalid(format!("invalid value for {option}: {value}")))
}

fn invalid(reason: impl Into<String>) -> Error {
    Error::InvalidModel(reason.into())
}

fn usage(program: &str) {
    eprintln!(
        "usage:\n  {program} inspect <database-file>\n  {program} check-store <store-directory>\n  {program} backup <store-directory> <absent-destination>\n  {program} generate <output-directory> [--seed N] [--sites N] [--days N] [--cadence-seconds N] [--start-micros N]\n  {program} bench-ftwdb <workload-directory> <empty-database-directory> [--durability always|manual] [--batch-points N]\n  {program} bench-real-fixture <points.csv.gz> <empty-database-directory> [--durability always|manual|every-bytes:N] [--batch-points N]\n  {program} bench-tsbs-iot <influx-line-file|-> <empty-database-directory> [--durability always|manual|every-bytes:N] [--batch-rows N]"
    );
}
