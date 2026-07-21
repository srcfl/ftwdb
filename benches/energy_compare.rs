use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use rusqlite::{Connection, params};
use tempfile::{TempDir, tempdir};
use wattdb::{
    Config, Durability, EnergyWorkload, RollupResolution, Store, Transaction, WorkloadConfig,
};

const SECOND: i64 = 1_000_000;
const FIVE_MINUTES: i64 = 300 * SECOND;
const DAY: i64 = 86_400 * SECOND;

fn fixture() -> EnergyWorkload {
    EnergyWorkload::generate(WorkloadConfig {
        days: 1,
        cadence_seconds: 300,
        ..WorkloadConfig::default()
    })
    .unwrap()
}

fn wattdb() -> (TempDir, Store) {
    let directory = tempdir().unwrap();
    let store = Store::open_with(
        directory.path(),
        Config {
            durability: Durability::Manual,
            ..Config::default()
        },
    )
    .unwrap();
    (directory, store)
}

fn sqlite() -> (TempDir, Connection) {
    let directory = tempdir().unwrap();
    let connection = Connection::open(directory.path().join("energy.sqlite")).unwrap();
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .unwrap();
    connection
        .pragma_update(None, "synchronous", "OFF")
        .unwrap();
    connection
        .execute_batch(
            "CREATE TABLE records (kind TEXT NOT NULL, id TEXT NOT NULL, body TEXT NOT NULL);
             CREATE TABLE points (
                series_id INTEGER NOT NULL,
                valid_time INTEGER NOT NULL,
                valid_time_end INTEGER NOT NULL,
                knowledge_time INTEGER NOT NULL,
                change_time INTEGER NOT NULL,
                run_id TEXT NOT NULL,
                value REAL NOT NULL,
                quality INTEGER NOT NULL,
                flags INTEGER NOT NULL
             );
             CREATE INDEX points_series_time ON points(series_id, valid_time);",
        )
        .unwrap();
    (directory, connection)
}

fn ingest_wattdb(store: &mut Store, workload: &EnergyWorkload) {
    store.commit(workload.metadata_transaction()).unwrap();
    for points in workload.points.chunks(10_000) {
        let mut transaction = Transaction::new();
        transaction.append_points(points.to_vec());
        store.commit(transaction).unwrap();
    }
    store.flush().unwrap();
}

fn ingest_sqlite(connection: &mut Connection, workload: &EnergyWorkload) {
    let transaction = connection.transaction().unwrap();
    {
        let mut record = transaction
            .prepare("INSERT INTO records VALUES (?1, ?2, ?3)")
            .unwrap();
        for entity in &workload.entities {
            record
                .execute(params!["entity", entity.id.0.to_string(), entity.name])
                .unwrap();
        }
        for series in &workload.series {
            record
                .execute(params!["series", series.id.to_string(), series.name])
                .unwrap();
        }
        for run in &workload.runs {
            record
                .execute(params!["run", run.id.0.to_string(), run.workflow])
                .unwrap();
        }
        for plan in &workload.plans {
            record
                .execute(params!["plan", plan.id.to_string(), plan.scenario])
                .unwrap();
        }
        let mut point = transaction
            .prepare("INSERT INTO points VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)")
            .unwrap();
        for value in &workload.points {
            point
                .execute(params![
                    value.series_id as i64,
                    value.valid_time,
                    value.valid_time_end,
                    value.knowledge_time,
                    value.change_time,
                    value.run_id.to_string(),
                    value.value,
                    value.quality,
                    value.flags,
                ])
                .unwrap();
        }
    }
    transaction.commit().unwrap();
}

fn sqlite_5m(connection: &Connection, start: i64, end: i64) -> Vec<(i64, u64, f64, f64, f64)> {
    let mut statement = connection
        .prepare(
            "SELECT ((valid_time - ?1) / ?3) * ?3 + ?1 AS bucket,
                    COUNT(*), SUM(value), MIN(value), MAX(value)
             FROM points
             WHERE series_id = 1 AND valid_time >= ?1 AND valid_time < ?2
             GROUP BY bucket ORDER BY bucket",
        )
        .unwrap();
    statement
        .query_map(params![start, end, FIVE_MINUTES], |row| {
            Ok((
                row.get(0)?,
                row.get::<_, i64>(1)? as u64,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn energy_benchmarks(criterion: &mut Criterion) {
    let workload = fixture();
    let mut ingest = criterion.benchmark_group("energy_ingest_mixed");
    ingest.throughput(Throughput::Elements(workload.points.len() as u64));
    ingest.bench_function("wattdb_manual", |bencher| {
        bencher.iter_batched(
            wattdb,
            |(_directory, mut store)| ingest_wattdb(&mut store, &workload),
            BatchSize::LargeInput,
        )
    });
    ingest.bench_function("sqlite_wal_off", |bencher| {
        bencher.iter_batched(
            sqlite,
            |(_directory, mut connection)| ingest_sqlite(&mut connection, &workload),
            BatchSize::LargeInput,
        )
    });
    ingest.finish();

    let (_wattdb_directory, mut store) = wattdb();
    ingest_wattdb(&mut store, &workload);
    let start = workload.config.start_micros;
    let end = start + DAY;
    store.maintain(end).unwrap();
    let resolution = RollupResolution::FixedMicros(FIVE_MINUTES);
    let wattdb_result = store.query_gauge(1, start, end, &resolution).unwrap();
    let (_sqlite_directory, mut connection) = sqlite();
    ingest_sqlite(&mut connection, &workload);
    let sqlite_result = sqlite_5m(&connection, start, end);
    let wattdb_samples: Vec<_> = wattdb_result
        .buckets
        .iter()
        .filter(|bucket| bucket.count > 0)
        .map(|bucket| {
            (
                bucket.start,
                bucket.count,
                bucket.sum,
                bucket.min.unwrap(),
                bucket.max.unwrap(),
            )
        })
        .collect();
    assert_eq!(wattdb_samples, sqlite_result);

    let mut query = criterion.benchmark_group("energy_query_1d_5m");
    query.throughput(Throughput::Elements(wattdb_result.buckets.len() as u64));
    query.bench_function("wattdb_persistent_cached", |bencher| {
        bencher.iter(|| store.query_gauge(1, start, end, &resolution).unwrap())
    });
    query.bench_function("sqlite_group_by", |bencher| {
        bencher.iter(|| sqlite_5m(&connection, start, end))
    });
    query.finish();
}

criterion_group!(benches, energy_benchmarks);
criterion_main!(benches);
