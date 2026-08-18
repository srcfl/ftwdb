//! Focused performance probes for ingest, reopen, and range-query scaling.
//!
//! Default sizes stay CI-friendly. Set `FTWDB_PERF_POINTS` for a larger local
//! soak (for example 200000).

use ftwdb::{Config, Database, Durability, Point};
use std::time::{Duration, Instant};
use tempfile::tempdir;

fn batch(series: u64, start: i64, count: usize) -> Vec<Point> {
    (0..count)
        .map(|index| Point::actual(series, start + index as i64 * 1_000_000, index as f64))
        .collect()
}

fn env_points(default: usize) -> usize {
    std::env::var("FTWDB_PERF_POINTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn p_latency(samples: &mut [Duration], quantile: f64) -> Duration {
    samples.sort_unstable();
    let index = ((samples.len() as f64 - 1.0) * quantile).round() as usize;
    samples[index]
}

#[test]
fn ingest_reopen_and_range_query_scale() {
    let points = env_points(20_000);
    let directory = tempdir().unwrap();
    let path = directory.path().join("perf.ftwdb");
    let mut database = Database::open_with(
        &path,
        Config {
            durability: Durability::Manual,
            ..Config::default()
        },
    )
    .unwrap();

    let ingest_started = Instant::now();
    for chunk in 0..(points / 1_000) {
        database
            .append(&batch(1, chunk as i64 * 1_000_000_000, 1_000))
            .unwrap();
    }
    database.flush().unwrap();
    let ingest = ingest_started.elapsed();

    let mut latest_full = Vec::with_capacity(40);
    let mut latest_tail = Vec::with_capacity(40);
    let tail_start = (points as i64 - 1_000) * 1_000_000;
    for _ in 0..40 {
        let started = Instant::now();
        let full = database.query_latest(1, 0, i64::MAX);
        latest_full.push(started.elapsed());
        assert_eq!(full.len(), points);

        let started = Instant::now();
        let tail = database.query_latest(1, tail_start, i64::MAX);
        latest_tail.push(started.elapsed());
        assert_eq!(tail.len(), 1_000);
    }

    drop(database);
    let reopen_started = Instant::now();
    let reopened = Database::open_with(
        &path,
        Config {
            durability: Durability::Manual,
            ..Config::default()
        },
    )
    .unwrap();
    let reopen = reopen_started.elapsed();
    assert_eq!(reopened.stats().unwrap().points, points as u64);
    assert_eq!(reopened.query_latest(1, tail_start, i64::MAX).len(), 1_000);
    drop(reopened);

    eprintln!(
        "perf_probe points={points} ingest={:.3}s ({:.0} points/s) reopen={:.3}s latest_full_p50={:?} latest_full_p95={:?} latest_tail_p50={:?} latest_tail_p95={:?}",
        ingest.as_secs_f64(),
        points as f64 / ingest.as_secs_f64(),
        reopen.as_secs_f64(),
        p_latency(&mut latest_full, 0.50),
        p_latency(&mut latest_full, 0.95),
        p_latency(&mut latest_tail, 0.50),
        p_latency(&mut latest_tail, 0.95),
    );
}

#[test]
fn many_small_frames_reopen_cost() {
    let frames = env_points(2_000).min(5_000);
    let directory = tempdir().unwrap();
    let path = directory.path().join("small-frames.ftwdb");
    let mut database = Database::open_with(
        &path,
        Config {
            durability: Durability::Manual,
            ..Config::default()
        },
    )
    .unwrap();
    for index in 0..frames {
        database
            .append(&[Point::actual(1, index as i64 * 1_000_000, index as f64)])
            .unwrap();
    }
    database.flush().unwrap();
    drop(database);

    let started = Instant::now();
    let reopened = Database::open_with(
        &path,
        Config {
            durability: Durability::Manual,
            ..Config::default()
        },
    )
    .unwrap();
    let reopen = started.elapsed();
    assert_eq!(reopened.stats().unwrap().points, frames as u64);
    eprintln!(
        "small_frame_reopen frames={frames} elapsed={:.3}s ({:.0} frames/s)",
        reopen.as_secs_f64(),
        frames as f64 / reopen.as_secs_f64(),
    );
}
