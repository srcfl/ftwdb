use criterion::{Criterion, criterion_group, criterion_main};
use ftwdb::{
    Config, Durability, Entity, EntityId, FixedGaugeRollup, Plan, PlanOutcome, PlanStatus, Point,
    RollupPolicy, RollupResolution, RollupSource, RollupTier, Run, RunId, RunKind, RunStatus,
    SeriesDefinition, SeriesSemantics, Store, Transaction,
};
use std::collections::BTreeMap;
use std::hint::black_box;
use tempfile::{TempDir, tempdir};

const SECOND: i64 = 1_000_000;
const FIVE_MINUTES: i64 = 300 * SECOND;
const DAY: i64 = 86_400 * SECOND;
const ACTUAL_POINTS: usize = 100_000;
const FORECAST_TIMES: usize = 10_000;
const ACTUAL_SERIES: u64 = 1;
const FORECAST_SERIES: u64 = 2;
const PLANNED_SERIES: u64 = 3;
const ORIGINAL_FORECAST_RUN: u128 = 100;
const REVISED_FORECAST_RUN: u128 = 101;
const OPTIMIZATION_RUN: u128 = 200;

struct Fixture {
    _directory: TempDir,
    store: Store,
    forecast_start: i64,
    forecast_end: i64,
    actual_end: i64,
}

fn model_query_benchmarks(criterion: &mut Criterion) {
    let fixture = fixture();
    verify_answers(&fixture);

    let database = fixture.store.database();
    let mut group = criterion.benchmark_group("model_queries");
    group.bench_function("latest", |bencher| {
        bencher.iter(|| {
            black_box(
                database
                    .query_latest(
                        FORECAST_SERIES,
                        fixture.forecast_start,
                        fixture.forecast_end,
                    )
                    .unwrap(),
            )
        })
    });
    group.bench_function("history", |bencher| {
        bencher.iter(|| {
            black_box(
                database
                    .query_history(
                        FORECAST_SERIES,
                        fixture.forecast_start,
                        fixture.forecast_end,
                    )
                    .unwrap(),
            )
        })
    });
    group.bench_function("as_of", |bencher| {
        bencher.iter(|| {
            black_box(
                database
                    .query_as_of(
                        FORECAST_SERIES,
                        fixture.forecast_start,
                        fixture.forecast_end,
                        15,
                    )
                    .unwrap(),
            )
        })
    });
    group.bench_function("run", |bencher| {
        bencher.iter(|| {
            black_box(
                database
                    .query_run(
                        FORECAST_SERIES,
                        ORIGINAL_FORECAST_RUN,
                        fixture.forecast_start,
                        fixture.forecast_end,
                    )
                    .unwrap(),
            )
        })
    });
    group.bench_function("plan_outcome", |bencher| {
        bencher.iter(|| {
            black_box(
                database
                    .compare_plan_to_actual(
                        PLANNED_SERIES,
                        ACTUAL_SERIES,
                        OPTIMIZATION_RUN,
                        0,
                        fixture.actual_end,
                    )
                    .unwrap(),
            )
        })
    });
    group.bench_function("gauge_rollup_5m", |bencher| {
        bencher.iter(|| {
            black_box(
                fixture
                    .store
                    .query_gauge(
                        ACTUAL_SERIES,
                        0,
                        fixture.actual_end,
                        &RollupResolution::FixedMicros(FIVE_MINUTES),
                    )
                    .unwrap(),
            )
        })
    });
    group.finish();
}

fn fixture() -> Fixture {
    let directory = tempdir().unwrap();
    let mut store = Store::open_with(
        directory.path(),
        Config {
            durability: Durability::Manual,
            ..Config::default()
        },
    )
    .unwrap();

    let actual_end = ACTUAL_POINTS as i64 * SECOND;
    let forecast_end = FORECAST_TIMES as i64 * FIVE_MINUTES;
    let mut metadata = Transaction::new();
    metadata
        .upsert_entity(Entity {
            id: EntityId(1),
            kind: "site".to_owned(),
            name: "model-query-benchmark".to_owned(),
            parent: None,
            valid_from: 0,
            valid_to: None,
            properties: BTreeMap::new(),
        })
        .define_series(series(
            ACTUAL_SERIES,
            "actual_power",
            vec![RollupTier {
                resolution: RollupResolution::FixedMicros(FIVE_MINUTES),
                retain_for_micros: None,
            }],
        ))
        .define_series(series(FORECAST_SERIES, "forecast_power", Vec::new()))
        .define_series(series(PLANNED_SERIES, "planned_power", Vec::new()))
        .upsert_run(run(ORIGINAL_FORECAST_RUN, RunKind::Forecast, None))
        .upsert_run(run(
            REVISED_FORECAST_RUN,
            RunKind::Forecast,
            Some(RunId(ORIGINAL_FORECAST_RUN)),
        ))
        .upsert_run(run(
            OPTIMIZATION_RUN,
            RunKind::Optimization,
            Some(RunId(REVISED_FORECAST_RUN)),
        ))
        .upsert_plan(Plan {
            id: 300,
            run_id: RunId(OPTIMIZATION_RUN),
            status: PlanStatus::Deployed,
            horizon_start: 0,
            horizon_end: actual_end,
            resolution_micros: SECOND,
            scenario: "base".to_owned(),
            objective_terms: BTreeMap::new(),
            objective_value: Some(1.0),
            supersedes: None,
            attributes: BTreeMap::new(),
        });
    store.commit(metadata).unwrap();

    let mut points = actual_and_planned_points();
    points.extend(forecast_points());
    points.sort_by_key(|point| {
        (
            point.valid_time,
            point.series_id,
            point.knowledge_time,
            point.change_time,
        )
    });
    for batch in points.chunks(10_000) {
        let mut transaction = Transaction::new();
        transaction.append_points(batch.to_vec());
        store.commit(transaction).unwrap();
    }
    store.flush().unwrap();

    let maintenance_end = (actual_end.div_euclid(DAY) + 1) * DAY;
    store.maintain(maintenance_end).unwrap();

    Fixture {
        _directory: directory,
        store,
        forecast_start: 0,
        forecast_end,
        actual_end,
    }
}

fn series(id: u64, name: &str, tiers: Vec<RollupTier>) -> SeriesDefinition {
    SeriesDefinition {
        id,
        owner_entity: Some(EntityId(1)),
        owner_relation: None,
        name: name.to_owned(),
        physical_quantity: "power".to_owned(),
        canonical_unit: "W".to_owned(),
        semantics: SeriesSemantics::Gauge,
        maximum_gap_micros: Some(2 * SECOND),
        rollup_policy: RollupPolicy {
            raw_retain_for_micros: None,
            tiers,
        },
    }
}

fn run(id: u128, kind: RunKind, input_snapshot: Option<RunId>) -> Run {
    Run {
        id: RunId(id),
        kind,
        status: RunStatus::Succeeded,
        created_at: id as i64,
        knowledge_time: id as i64,
        workflow: "model-query-benchmark".to_owned(),
        model: "synthetic".to_owned(),
        model_version: "1".to_owned(),
        parent_run: None,
        input_snapshot,
        attributes: BTreeMap::new(),
    }
}

fn actual_and_planned_points() -> Vec<Point> {
    let mut points = Vec::with_capacity(ACTUAL_POINTS * 2);
    for index in 0..ACTUAL_POINTS {
        let timestamp = index as i64 * SECOND;
        let value = (index % 1_000) as f64;
        points.push(Point::actual(ACTUAL_SERIES, timestamp, value));
        points.push(Point {
            series_id: PLANNED_SERIES,
            valid_time: timestamp,
            valid_time_end: timestamp + SECOND,
            knowledge_time: 25,
            change_time: 25,
            run_id: OPTIMIZATION_RUN,
            value: value + 1.0,
            quality: 0,
            flags: 0,
        });
    }
    points
}

fn forecast_points() -> Vec<Point> {
    let mut points = Vec::with_capacity(FORECAST_TIMES * 3);
    for index in 0..FORECAST_TIMES {
        let valid_time = index as i64 * FIVE_MINUTES;
        let value = index as f64;
        points.extend([
            forecast_point(valid_time, 10, ORIGINAL_FORECAST_RUN, value),
            forecast_point(valid_time, 20, ORIGINAL_FORECAST_RUN, value + 0.25),
            forecast_point(valid_time, 30, REVISED_FORECAST_RUN, value + 0.5),
        ]);
    }
    points
}

fn forecast_point(valid_time: i64, revision_time: i64, run_id: u128, value: f64) -> Point {
    Point {
        series_id: FORECAST_SERIES,
        valid_time,
        valid_time_end: valid_time,
        knowledge_time: revision_time,
        change_time: revision_time,
        run_id,
        value,
        quality: 0,
        flags: 0,
    }
}

fn verify_answers(fixture: &Fixture) {
    let database = fixture.store.database();
    let history = database
        .query_history(
            FORECAST_SERIES,
            fixture.forecast_start,
            fixture.forecast_end,
        )
        .unwrap();
    assert_eq!(history.len(), FORECAST_TIMES * 3);
    for (index, revisions) in history.chunks_exact(3).enumerate() {
        assert_eq!(revisions, &forecast_points_for(index));
    }

    let latest = database
        .query_latest(
            FORECAST_SERIES,
            fixture.forecast_start,
            fixture.forecast_end,
        )
        .unwrap();
    assert_eq!(latest.len(), FORECAST_TIMES);
    for (index, point) in latest.iter().enumerate() {
        assert_eq!(*point, forecast_points_for(index)[2]);
    }

    let as_of = database
        .query_as_of(
            FORECAST_SERIES,
            fixture.forecast_start,
            fixture.forecast_end,
            15,
        )
        .unwrap();
    assert_eq!(as_of.len(), FORECAST_TIMES);
    for (index, point) in as_of.iter().enumerate() {
        assert_eq!(*point, forecast_points_for(index)[0]);
    }

    let run = database
        .query_run(
            FORECAST_SERIES,
            ORIGINAL_FORECAST_RUN,
            fixture.forecast_start,
            fixture.forecast_end,
        )
        .unwrap();
    assert_eq!(run.len(), FORECAST_TIMES);
    for (index, point) in run.iter().enumerate() {
        assert_eq!(*point, forecast_points_for(index)[1]);
    }

    let outcomes = database
        .compare_plan_to_actual(
            PLANNED_SERIES,
            ACTUAL_SERIES,
            OPTIMIZATION_RUN,
            0,
            fixture.actual_end,
        )
        .unwrap();
    assert_eq!(outcomes.len(), ACTUAL_POINTS);
    for (index, outcome) in outcomes.iter().enumerate() {
        assert_eq!(*outcome, expected_outcome(index));
    }

    let resolution = RollupResolution::FixedMicros(FIVE_MINUTES);
    let rollup = fixture
        .store
        .query_gauge(ACTUAL_SERIES, 0, fixture.actual_end, &resolution)
        .unwrap();
    let expected: Vec<_> = FixedGaugeRollup::build(
        &actual_and_planned_points()
            .into_iter()
            .filter(|point| point.series_id == ACTUAL_SERIES)
            .collect::<Vec<_>>(),
        FIVE_MINUTES,
        2 * SECOND,
    )
    .unwrap()
    .range(0, fixture.actual_end);
    assert_eq!(rollup.source, RollupSource::Materialized);
    assert_eq!(rollup.buckets, expected);
}

fn forecast_points_for(index: usize) -> [Point; 3] {
    let valid_time = index as i64 * FIVE_MINUTES;
    let value = index as f64;
    [
        forecast_point(valid_time, 10, ORIGINAL_FORECAST_RUN, value),
        forecast_point(valid_time, 20, ORIGINAL_FORECAST_RUN, value + 0.25),
        forecast_point(valid_time, 30, REVISED_FORECAST_RUN, value + 0.5),
    ]
}

fn expected_outcome(index: usize) -> PlanOutcome {
    let valid_time = index as i64 * SECOND;
    let value = (index % 1_000) as f64;
    let actual = Point::actual(ACTUAL_SERIES, valid_time, value);
    let planned = Point {
        series_id: PLANNED_SERIES,
        valid_time,
        valid_time_end: valid_time + SECOND,
        knowledge_time: 25,
        change_time: 25,
        run_id: OPTIMIZATION_RUN,
        value: value + 1.0,
        quality: 0,
        flags: 0,
    };
    PlanOutcome {
        valid_time,
        planned: Some(planned),
        actual: Some(actual),
        difference: Some(-1.0),
    }
}

criterion_group!(benches, model_query_benchmarks);
criterion_main!(benches);
