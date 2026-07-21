use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use ftwdb::{Point, Segment};
use tempfile::tempdir;

const POINT_COUNT: usize = 100_000;

fn points() -> Vec<Point> {
    (0..POINT_COUNT)
        .map(|index| Point {
            series_id: 1,
            valid_time: index as i64 * 1_000_000,
            valid_time_end: index as i64 * 1_000_000,
            knowledge_time: index as i64 * 1_000_000 + 10,
            change_time: index as i64 * 1_000_000 + 20,
            run_id: 0,
            value: 5_000.0 + ((index as f64) / 300.0).sin() * 2_000.0,
            quality: 0,
            flags: 0,
        })
        .collect()
}

fn segment_benchmarks(criterion: &mut Criterion) {
    let input = points();
    let mut group = criterion.benchmark_group("segment_100k");
    group.throughput(Throughput::Elements(POINT_COUNT as u64));
    group.bench_function("create_and_sync", |bencher| {
        bencher.iter_batched(
            || {
                let directory = tempdir().unwrap();
                let path = directory.path().join("raw.seg");
                (directory, path)
            },
            |(_directory, path)| Segment::create(path, &input, 4_096).unwrap(),
            BatchSize::SmallInput,
        );
    });
    group.finish();

    let directory = tempdir().unwrap();
    let path = directory.path().join("query.seg");
    let stats = Segment::create(&path, &input, 4_096).unwrap();
    eprintln!(
        "segment fixture: {} points, {} bytes, {:.2} logical/storage ratio",
        stats.points,
        stats.stored_bytes,
        stats.logical_point_bytes as f64 / stats.stored_bytes as f64
    );
    let mut segment = Segment::open(path).unwrap();
    let mut group = criterion.benchmark_group("segment_query_100k");
    group.throughput(Throughput::Elements(POINT_COUNT as u64));
    group.bench_function("full_series", |bencher| {
        bencher.iter(|| segment.query(1, i64::MIN, i64::MAX).unwrap());
    });
    group.finish();
}

criterion_group!(benches, segment_benchmarks);
criterion_main!(benches);
