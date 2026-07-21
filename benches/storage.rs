use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use tempfile::tempdir;
use wattdb::{Config, Database, Durability, Point};

fn batch(series: u64, start: i64, count: usize) -> Vec<Point> {
    (0..count)
        .map(|index| Point::actual(series, start + index as i64 * 1_000_000, index as f64))
        .collect()
}

fn storage_benchmarks(criterion: &mut Criterion) {
    let points = batch(1, 0, 1_000);
    let mut group = criterion.benchmark_group("append_1000");
    group.throughput(Throughput::Elements(points.len() as u64));
    group.bench_function("manual_sync", |bencher| {
        bencher.iter_batched(
            || {
                let directory = tempdir().unwrap();
                let database = Database::open_with(
                    directory.path().join("bench.wattdb"),
                    Config {
                        durability: Durability::Manual,
                        ..Config::default()
                    },
                )
                .unwrap();
                (directory, database)
            },
            |(_directory, mut database)| database.append(&points).unwrap(),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("sync_each_batch", |bencher| {
        bencher.iter_batched(
            || {
                let directory = tempdir().unwrap();
                let database = Database::open(directory.path().join("bench.wattdb")).unwrap();
                (directory, database)
            },
            |(_directory, mut database)| database.append(&points).unwrap(),
            BatchSize::SmallInput,
        );
    });
    group.finish();

    let directory = tempdir().unwrap();
    let mut database = Database::open_with(
        directory.path().join("query.wattdb"),
        Config {
            durability: Durability::Manual,
            ..Config::default()
        },
    )
    .unwrap();
    for chunk in 0..100 {
        database
            .append(&batch(1, chunk * 1_000_000_000, 1_000))
            .unwrap();
    }
    let mut group = criterion.benchmark_group("query_100k");
    group.throughput(Throughput::Elements(100_000));
    group.bench_function("latest", |bencher| {
        bencher.iter(|| database.query_latest(1, 0, i64::MAX));
    });
    group.bench_function("materialize_5m_gauge_rollup", |bencher| {
        bencher.iter(|| database.rollup_gauge(1, 0, i64::MAX, 300_000_000, 10_000_000));
    });
    group.finish();

    let rollup = database.rollup_gauge(1, 0, i64::MAX, 300_000_000, 10_000_000);
    let bucket_count = rollup.buckets().len() as u64;
    let mut group = criterion.benchmark_group("query_materialized_rollup");
    group.throughput(Throughput::Elements(bucket_count));
    group.bench_function("5m_full_range", |bencher| {
        bencher.iter(|| rollup.range(0, i64::MAX));
    });
    group.finish();
}

criterion_group!(benches, storage_benchmarks);
criterion_main!(benches);
