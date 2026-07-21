#![cfg(unix)]

use ftwdb::{Config, Database, Durability, Entity, EntityId, Error, Point, Transaction};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

const DEFAULT_SEED: u64 = 0x4654_5744_4250_4354;
const DEFAULT_ITERATIONS: usize = 32;
const DEFAULT_BATCH_POINTS: usize = 16_384;
const MAX_BATCHES: usize = 1_000_000;

fn numbered_batch(batch: usize, points: usize) -> Vec<Point> {
    (0..points)
        .map(|index| {
            let sequence = batch
                .checked_mul(points)
                .and_then(|base| base.checked_add(index))
                .expect("test sequence must fit usize");
            Point::actual(1, sequence as i64, sequence as f64)
        })
        .collect()
}

#[test]
fn writer_lock_holder_process() {
    if std::env::var_os("FTWDB_LOCK_HOLDER").is_none() {
        return;
    }

    let path = PathBuf::from(
        std::env::var_os("FTWDB_LOCK_DATABASE")
            .expect("FTWDB_LOCK_DATABASE must be set for the lock holder"),
    );
    let _database = Database::open(path).unwrap();
    println!("WRITER_LOCK_HELD");
    std::io::stdout().flush().unwrap();
    let mut release = [0_u8; 1];
    std::io::stdin().read_exact(&mut release).unwrap();
}

#[test]
fn writer_lock_blocks_another_process_until_holder_exits() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("process-lock.ftwdb");
    let executable = std::env::current_exe().unwrap();
    let mut child = Command::new(executable)
        .args([
            "--exact",
            "writer_lock_holder_process",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("FTWDB_LOCK_HOLDER", "1")
        .env("FTWDB_LOCK_DATABASE", &path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();

    let stdout = child.stdout.take().unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let reader = thread::spawn(move || {
        let mut ready_sent = false;
        for line in BufReader::new(stdout).lines() {
            match line {
                Ok(line) if !ready_sent && line.contains("WRITER_LOCK_HELD") => {
                    let _ = ready_tx.send(Ok(()));
                    ready_sent = true;
                }
                Ok(_) => {}
                Err(error) => {
                    if !ready_sent {
                        let _ = ready_tx.send(Err(error.to_string()));
                    }
                    return;
                }
            }
        }
        if !ready_sent {
            let _ = ready_tx.send(Err("lock holder exited before readiness".to_owned()));
        }
    });

    match ready_rx.recv_timeout(Duration::from_secs(15)) {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let _ = child.kill();
            let _ = child.wait();
            reader.join().unwrap();
            panic!("lock holder failed before readiness: {error}");
        }
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            reader.join().unwrap();
            panic!("lock holder readiness timed out: {error}");
        }
    }

    let second_open = Database::open(&path);
    let release = child.stdin.take().unwrap().write_all(&[1]);
    let status = child.wait().unwrap();
    reader.join().unwrap();
    release.unwrap();
    assert!(status.success());
    match second_open {
        Err(Error::Locked { path: reported }) => assert_eq!(reported, path),
        Err(other) => panic!("expected Error::Locked across processes, got {other:?}"),
        Ok(_) => panic!("second process opened the writer-locked file"),
    }

    let mut reopened = Database::open(&path).unwrap();
    reopened.append(&numbered_batch(0, 1)).unwrap();
    reopened.close().unwrap();
}

/// The ignored controller starts this test in a child process. Each ACK is
/// emitted only after an `Always` commit has returned as durable.
#[test]
fn sigkill_worker_process() {
    if std::env::var_os("FTWDB_POWER_CUT_WORKER").is_none() {
        return;
    }

    let path = PathBuf::from(
        std::env::var_os("FTWDB_POWER_CUT_DATABASE")
            .expect("FTWDB_POWER_CUT_DATABASE must be set for the worker"),
    );
    let batch_points = environment_usize("FTWDB_POWER_CUT_BATCH_POINTS", DEFAULT_BATCH_POINTS);
    let mut database = Database::open_with(
        path,
        Config {
            durability: Durability::Always,
            ..Config::default()
        },
    )
    .unwrap();

    println!("WORKER_READY");
    std::io::stdout().flush().unwrap();
    for batch in 0..MAX_BATCHES {
        let points = numbered_batch(batch, batch_points);
        println!("WORKER_BEGIN {batch}");
        std::io::stdout().flush().unwrap();
        let commit = database.append(&points).unwrap();
        assert!(commit.durable, "Always commit {batch} was not durable");
        println!("WORKER_ACK {batch}");
        std::io::stdout().flush().unwrap();
    }
}

/// This is a host-process crash test. It does not prove how an SD-card
/// controller or filesystem handles lost writes. Run it explicitly and keep
/// its JSONL result as evidence:
///
/// `cargo test --test power_cut always_recovers_every_acknowledged_batch_after_sigkill -- --ignored --nocapture`
#[test]
#[ignore = "process crash stress test; writes raw JSONL evidence under bench-results/power-cut"]
fn always_recovers_every_acknowledged_batch_after_sigkill() {
    let seed = environment_u64("FTWDB_POWER_CUT_SEED", DEFAULT_SEED);
    let iterations = environment_usize("FTWDB_POWER_CUT_ITERATIONS", DEFAULT_ITERATIONS);
    let batch_points = environment_usize("FTWDB_POWER_CUT_BATCH_POINTS", DEFAULT_BATCH_POINTS);
    assert!(iterations > 0);
    assert!(batch_points > 0 && batch_points <= Config::default().max_batch_points);

    let result_path = result_path(seed);
    let mut results = create_result_file(&result_path);
    writeln!(
        results,
        "{{\"format\":\"ftwdb-sigkill-v1\",\"seed\":{seed},\"iterations\":{iterations},\"batch_points\":{batch_points}}}"
    )
    .unwrap();
    results.flush().unwrap();

    let mut random = XorShift64::new(seed);
    for iteration in 0..iterations {
        let directory = tempdir().unwrap();
        let database_path = directory.path().join("power-cut.ftwdb");
        let kill_batch = random.below(17) as usize;
        let delay_micros = random.below(12_001);
        let outcome = run_one_sigkill(&database_path, batch_points, kill_batch, delay_micros);

        let reopened = match Database::open(&database_path) {
            Ok(database) => database,
            Err(error) => {
                writeln!(
                    results,
                    "{{\"iteration\":{iteration},\"kill_batch\":{kill_batch},\"delay_micros\":{delay_micros},\"signal\":{},\"acknowledged_batches\":{},\"reopen\":\"error\",\"error\":\"{}\"}}",
                    outcome.signal,
                    outcome.acknowledged_batches,
                    json_escape(&error.to_string()),
                )
                .unwrap();
                results.flush().unwrap();
                panic!("iteration {iteration} failed to reopen after SIGKILL: {error}");
            }
        };
        let stats = reopened.stats().unwrap();
        let points = reopened.query_history(1, i64::MIN, i64::MAX);
        let recovered_batches = points.len() / batch_points;
        let complete_batches = points.len().is_multiple_of(batch_points);
        let sequence_is_exact = points.iter().enumerate().all(|(sequence, point)| {
            point.valid_time == sequence as i64 && point.value == sequence as f64
        });
        let contains_every_ack = recovered_batches >= outcome.acknowledged_batches;
        let has_at_most_one_unacknowledged =
            recovered_batches <= outcome.acknowledged_batches.saturating_add(1);

        writeln!(
            results,
            "{{\"iteration\":{iteration},\"kill_batch\":{kill_batch},\"delay_micros\":{delay_micros},\"signal\":{},\"acknowledged_batches\":{},\"recovered_batches\":{recovered_batches},\"recovered_points\":{},\"recovered_tail_bytes\":{},\"complete_batches\":{complete_batches},\"sequence_is_exact\":{sequence_is_exact},\"contains_every_ack\":{contains_every_ack},\"has_at_most_one_unacknowledged\":{has_at_most_one_unacknowledged}}}",
            outcome.signal,
            outcome.acknowledged_batches,
            points.len(),
            stats.recovered_tail_bytes,
        )
        .unwrap();
        results.flush().unwrap();

        assert!(
            complete_batches,
            "iteration {iteration} exposed a partial batch"
        );
        assert!(
            sequence_is_exact,
            "iteration {iteration} exposed a gap or corrupt point"
        );
        assert!(
            contains_every_ack,
            "iteration {iteration} lost an acknowledged batch"
        );
        assert!(
            has_at_most_one_unacknowledged,
            "iteration {iteration} recovered more than the one in-flight batch"
        );
    }
    results.sync_all().unwrap();
    println!("power-cut raw results: {}", result_path.display());
}

#[test]
fn complete_final_frame_with_bad_payload_crc_is_corruption_and_preserved() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("final-crc.ftwdb");
    let first = numbered_batch(0, 3);

    let mut database = Database::open(&path).unwrap();
    database.append(&first).unwrap();
    let first_length = database.stats().unwrap().file_bytes;
    let mut catalog_change = Transaction::new();
    catalog_change.upsert_entity(Entity {
        id: EntityId(14),
        kind: "crc-test".to_owned(),
        name: "must-not-be-recovered".to_owned(),
        parent: None,
        valid_from: 0,
        valid_to: None,
        properties: BTreeMap::new(),
    });
    database.commit(catalog_change).unwrap();
    database.close().unwrap();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.seek(SeekFrom::End(-1)).unwrap();
    let mut last = [0_u8; 1];
    file.read_exact(&mut last).unwrap();
    file.seek(SeekFrom::End(-1)).unwrap();
    file.write_all(&[last[0] ^ 0xff]).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let corrupt_bytes = std::fs::read(&path).unwrap();

    for result in [Database::open(&path), Database::open_read_only(&path)] {
        match result {
            Err(Error::Corruption { offset, reason }) => {
                assert_eq!(offset, first_length);
                assert_eq!(reason, "payload checksum mismatch in complete final frame");
            }
            Err(other) => panic!("expected payload corruption, got {other:?}"),
            Ok(_) => panic!("expected payload corruption, got an open database"),
        }
        assert_eq!(std::fs::read(&path).unwrap(), corrupt_bytes);
    }

    let entries = std::fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(entries, [path.file_name().unwrap()]);
}

struct KillOutcome {
    signal: i32,
    acknowledged_batches: usize,
}

fn run_one_sigkill(
    database_path: &Path,
    batch_points: usize,
    kill_batch: usize,
    delay_micros: u64,
) -> KillOutcome {
    let executable = std::env::current_exe().unwrap();
    let mut child = Command::new(executable)
        .args([
            "--exact",
            "sigkill_worker_process",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("FTWDB_POWER_CUT_WORKER", "1")
        .env("FTWDB_POWER_CUT_DATABASE", database_path)
        .env("FTWDB_POWER_CUT_BATCH_POINTS", batch_points.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();

    let stdout = child.stdout.take().unwrap();
    let acknowledgements = Arc::new(Mutex::new(Vec::<usize>::new()));
    let reader_acknowledgements = Arc::clone(&acknowledgements);
    let (ready_tx, ready_rx) = mpsc::channel();
    let (begin_tx, begin_rx) = mpsc::channel();
    let reader = thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let line = line.unwrap();
            if line.contains("WORKER_READY") {
                let _ = ready_tx.send(());
            }
            if let Some(raw_batch) = line.split("WORKER_BEGIN ").nth(1)
                && let Some(token) = raw_batch.split_whitespace().next()
                && let Ok(batch) = token.parse::<usize>()
            {
                let _ = begin_tx.send(batch);
            }
            if let Some(raw_ack) = line.split("WORKER_ACK ").nth(1)
                && let Some(token) = raw_ack.split_whitespace().next()
                && let Ok(batch) = token.parse::<usize>()
            {
                reader_acknowledgements.lock().unwrap().push(batch);
            }
        }
    });

    ready_rx
        .recv_timeout(Duration::from_secs(15))
        .expect("worker did not become ready");
    for expected in 0..=kill_batch {
        let begun = begin_rx
            .recv_timeout(Duration::from_secs(15))
            .expect("worker stopped starting commits");
        assert_eq!(begun, expected);
    }
    thread::sleep(Duration::from_micros(delay_micros));
    child.kill().unwrap();
    let status = child.wait().unwrap();
    assert!(
        !status.success(),
        "worker exited cleanly instead of being killed"
    );
    let signal = status.signal().expect("worker did not exit from a signal");
    assert_eq!(signal, 9, "worker did not exit from SIGKILL");
    reader.join().unwrap();

    let acknowledgements = acknowledgements.lock().unwrap();
    for (expected, acknowledged) in acknowledgements.iter().enumerate() {
        assert_eq!(*acknowledged, expected, "worker ACK sequence has a gap");
    }
    KillOutcome {
        signal,
        acknowledged_batches: acknowledgements.len(),
    }
}

fn result_path(seed: u64) -> PathBuf {
    if let Some(path) = std::env::var_os("FTWDB_POWER_CUT_RESULTS") {
        return PathBuf::from(path);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("bench-results/power-cut")
        .join(format!(
            "always-sigkill-seed-{seed}-pid-{}.jsonl",
            std::process::id()
        ))
}

fn create_result_file(path: &Path) -> File {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    File::create(path).unwrap()
}

fn environment_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn environment_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

struct XorShift64(u64);

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }

    fn below(&mut self, upper: u64) -> u64 {
        self.next() % upper
    }
}
