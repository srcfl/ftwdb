use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::collections::BTreeMap;
use std::hint::black_box;
use tempfile::tempdir;
use wattdb::{
    CalendarUnit, Config, Durability, Entity, EntityId, Point, RollupPolicy, RollupResolution,
    RollupSource, RollupTier, SeriesDefinition, SeriesSemantics, Store, Transaction,
};

const MINUTE: i64 = 60_000_000;
const FIVE_MINUTES: i64 = 5 * MINUTE;
const DAYS: usize = 30;
const POINTS: usize = DAYS * 24 * 60 + 1;

fn rollup_benchmarks(criterion: &mut Criterion) {
    let directory = tempdir().unwrap();
    let mut store = Store::open_with(
        directory.path(),
        Config {
            durability: Durability::Manual,
            ..Config::default()
        },
    )
    .unwrap();
    let fixed = RollupResolution::FixedMicros(FIVE_MINUTES);
    let calendar = RollupResolution::Calendar {
        unit: CalendarUnit::Day,
        iana_timezone: "Europe/Stockholm".to_owned(),
    };
    let mut transaction = Transaction::new();
    transaction
        .upsert_entity(Entity {
            id: EntityId(1),
            kind: "site".to_owned(),
            name: "benchmark".to_owned(),
            parent: None,
            valid_from: 0,
            valid_to: None,
            properties: BTreeMap::new(),
        })
        .define_series(SeriesDefinition {
            id: 1,
            owner_entity: Some(EntityId(1)),
            owner_relation: None,
            name: "grid_power".to_owned(),
            physical_quantity: "power".to_owned(),
            canonical_unit: "W".to_owned(),
            semantics: SeriesSemantics::Gauge,
            maximum_gap_micros: Some(2 * MINUTE),
            rollup_policy: RollupPolicy {
                raw_retain_for_micros: Some(7 * 24 * 60 * MINUTE),
                tiers: vec![
                    RollupTier {
                        resolution: fixed.clone(),
                        retain_for_micros: None,
                    },
                    RollupTier {
                        resolution: calendar.clone(),
                        retain_for_micros: None,
                    },
                ],
            },
        });
    let points: Vec<_> = (0..POINTS)
        .map(|index| {
            let daily = ((index % (24 * 60)) as f64 / (24.0 * 60.0) * std::f64::consts::TAU).sin();
            Point::actual(1, index as i64 * MINUTE, 4_000.0 + 2_000.0 * daily)
        })
        .collect();
    transaction.append_points(points);
    store.commit(transaction).unwrap();
    let end = (POINTS as i64 - 1) * MINUTE;
    store.maintain(end).unwrap();

    // Warm and verify the exact persisted state before recording performance.
    let persisted = store.query_gauge(1, 0, end, &fixed).unwrap();
    assert_eq!(persisted.source, RollupSource::Materialized);
    let raw: Vec<_> = store
        .database()
        .rollup_gauge(1, 0, end + 1, FIVE_MINUTES, 2 * MINUTE)
        .range(0, end);
    assert_eq!(persisted.buckets, raw);

    let mut group = criterion.benchmark_group("rollup_query_30d");
    group.throughput(Throughput::Elements(persisted.buckets.len() as u64));
    group.bench_function("persistent_5m_cached", |bencher| {
        bencher.iter(|| black_box(store.query_gauge(1, 0, end, &fixed).unwrap()))
    });
    group.bench_function("raw_materialize_5m", |bencher| {
        bencher.iter(|| {
            black_box(
                store
                    .database()
                    .rollup_gauge(1, 0, end + 1, FIVE_MINUTES, 2 * MINUTE),
            )
        })
    });
    group.finish();

    let daily_descriptor = store
        .active_rollups()
        .find(|rollup| rollup.resolution == calendar)
        .unwrap();
    let daily_start = daily_descriptor.start;
    let daily_end = daily_descriptor.end;
    let daily = store
        .query_gauge(1, daily_start, daily_end, &calendar)
        .unwrap();
    assert_eq!(daily.source, RollupSource::Materialized);
    let mut group = criterion.benchmark_group("rollup_query_calendar_30d");
    group.throughput(Throughput::Elements(daily.buckets.len() as u64));
    group.bench_function("persistent_day_cached", |bencher| {
        bencher.iter(|| {
            black_box(
                store
                    .query_gauge(1, daily_start, daily_end, &calendar)
                    .unwrap(),
            )
        })
    });
    group.finish();
}

criterion_group!(benches, rollup_benchmarks);
criterion_main!(benches);
