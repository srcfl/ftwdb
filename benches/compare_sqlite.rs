use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use rusqlite::{Connection, params};
use tempfile::{TempDir, tempdir};
use wattdb::{Config, Database, Durability, Point};

const POINTS: usize = 1_000;
const SECOND: i64 = 1_000_000;

fn point(index: usize) -> Point {
    Point::actual(1, index as i64 * SECOND, (index % 100) as f64)
}

fn wattdb(durable: bool) -> (TempDir, Database) {
    let directory = tempdir().unwrap();
    let database = Database::open_with(
        directory.path().join("database.wattdb"),
        Config {
            durability: if durable {
                Durability::Always
            } else {
                Durability::Manual
            },
            ..Config::default()
        },
    )
    .unwrap();
    (directory, database)
}

fn sqlite(durable: bool) -> (TempDir, Connection) {
    let directory = tempdir().unwrap();
    let connection = Connection::open(directory.path().join("database.sqlite")).unwrap();
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .unwrap();
    connection
        .pragma_update(None, "synchronous", if durable { "FULL" } else { "OFF" })
        .unwrap();
    connection
        .execute_batch(
            "CREATE TABLE points (
                series_id INTEGER NOT NULL,
                valid_time INTEGER NOT NULL,
                value REAL NOT NULL,
                PRIMARY KEY (series_id, valid_time)
            ) WITHOUT ROWID;",
        )
        .unwrap();
    (directory, connection)
}

fn append_benchmarks(criterion: &mut Criterion) {
    let points: Vec<_> = (0..POINTS).map(point).collect();
    let mut group = criterion.benchmark_group("compare_append_1000");
    group.throughput(Throughput::Elements(POINTS as u64));

    group.bench_function("wattdb_manual", |bencher| {
        bencher.iter_batched(
            || wattdb(false),
            |(_directory, mut database)| database.append(&points).unwrap(),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("sqlite_wal_off", |bencher| {
        bencher.iter_batched(
            || sqlite(false),
            |(_directory, mut connection)| {
                let transaction = connection.transaction().unwrap();
                {
                    let mut statement = transaction
                        .prepare("INSERT INTO points VALUES (?1, ?2, ?3)")
                        .unwrap();
                    for point in &points {
                        statement
                            .execute(params![
                                point.series_id as i64,
                                point.valid_time,
                                point.value
                            ])
                            .unwrap();
                    }
                }
                transaction.commit().unwrap();
            },
            BatchSize::SmallInput,
        );
    });
    group.bench_function("wattdb_sync", |bencher| {
        bencher.iter_batched(
            || wattdb(true),
            |(_directory, mut database)| database.append(&points).unwrap(),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("sqlite_wal_full", |bencher| {
        bencher.iter_batched(
            || sqlite(true),
            |(_directory, mut connection)| {
                let transaction = connection.transaction().unwrap();
                {
                    let mut statement = transaction
                        .prepare("INSERT INTO points VALUES (?1, ?2, ?3)")
                        .unwrap();
                    for point in &points {
                        statement
                            .execute(params![
                                point.series_id as i64,
                                point.valid_time,
                                point.value
                            ])
                            .unwrap();
                    }
                }
                transaction.commit().unwrap();
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(benches, append_benchmarks);
criterion_main!(benches);
