#![cfg(unix)]

use ftwdb::shadow_protocol::{
    self, CommitBatchRequest, ErrorCode, HealthRequest, HealthStatus, HelloRequest, Request,
    Response, WireMessage,
};
use ftwdb::shadow_runtime::{ShadowRuntime, ShadowRuntimeConfig, ShadowStorageLimits};
use ftwdb::{Entity, EntityId, IngressIdentity, Point, Store};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn batch(sequence: u64) -> CommitBatchRequest {
    CommitBatchRequest {
        source_id: 7,
        sequence,
        commit_id: u128::from(sequence) + 100,
        entities: vec![Entity {
            id: EntityId(1),
            kind: "site".into(),
            name: "shadow limit test".into(),
            parent: None,
            valid_from: 0,
            valid_to: None,
            properties: Default::default(),
        }],
        relations: Vec::new(),
        series: vec![ftwdb::SeriesDefinition {
            id: 1,
            owner_entity: Some(EntityId(1)),
            owner_relation: None,
            name: "grid_power".into(),
            physical_quantity: "power".into(),
            canonical_unit: "W".into(),
            semantics: ftwdb::SeriesSemantics::Gauge,
            maximum_gap_micros: None,
            rollup_policy: ftwdb::RollupPolicy {
                raw_retain_for_micros: None,
                tiers: Vec::new(),
            },
        }],
        runs: Vec::new(),
        plans: Vec::new(),
        points: vec![Point::actual(1, sequence as i64, 50.0)],
    }
}

fn request(stream: &mut UnixStream, request: Request) -> Response {
    shadow_protocol::write_to(stream, &WireMessage::Request(request)).unwrap();
    match shadow_protocol::read_from(stream).unwrap() {
        WireMessage::Response(response) => response,
        other => panic!("expected response, got {other:?}"),
    }
}

fn start(store: &Path, socket: &Path, max_bytes: u64, min_free: u64) -> (Server, UnixStream) {
    let child = Command::new(env!("CARGO_BIN_EXE_ftwdb-shadow"))
        .args([store, socket])
        .env("FTWDB_SHADOW_MAX_STORE_BYTES", max_bytes.to_string())
        .env("FTWDB_SHADOW_MIN_FREE_BYTES", min_free.to_string())
        .env_remove("FTWDB_SHADOW_MAINTAIN_SECS")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut server = Server(child);
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut stream = loop {
        if let Ok(stream) = UnixStream::connect(socket) {
            break stream;
        }
        assert!(server.0.try_wait().unwrap().is_none(), "sidecar exited");
        assert!(Instant::now() < deadline, "sidecar did not bind");
        std::thread::sleep(Duration::from_millis(10));
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    assert!(matches!(
        request(
            &mut stream,
            Request::Hello(HelloRequest {
                source_id: 7,
                node_id: "limits".into(),
                client_version: "test".into(),
                capabilities: 0,
            })
        ),
        Response::Hello(_)
    ));
    (server, stream)
}

#[test]
fn storage_limits_reject_before_append_but_keep_exact_receipts_after_restart() {
    for disk_reserve in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("store");
        let socket = directory.path().join("run/shadow.sock");
        let first = batch(1);
        let mut store = Store::open(&root).unwrap();
        let mut transaction = ftwdb::Transaction::new();
        transaction.upsert_entity(first.entities[0].clone());
        transaction.define_series(first.series[0].clone());
        transaction.append_points(first.points.clone());
        store
            .commit_ingress(
                IngressIdentity::new(first.source_id, first.sequence, first.commit_id),
                transaction,
            )
            .unwrap();
        let bytes = store.stored_bytes().unwrap();
        store.close().unwrap();
        let before = std::fs::read(root.join("active.wlog")).unwrap();
        let (server, mut stream) = start(
            &root,
            &socket,
            if disk_reserve { u64::MAX } else { bytes },
            if disk_reserve { u64::MAX } else { 1 },
        );

        let Response::Error(error) = request(&mut stream, Request::CommitBatch(batch(2))) else {
            panic!("new write should exceed budget");
        };
        assert_eq!(error.code, ErrorCode::Overloaded);
        assert!(error.retryable);
        assert_eq!(
            error.message,
            if disk_reserve {
                "free disk reserve reached"
            } else {
                "store byte limit reached"
            }
        );
        let Response::Health(health) =
            request(&mut stream, Request::Health(HealthRequest { nonce: 1 }))
        else {
            panic!("expected health");
        };
        assert_eq!(health.status, HealthStatus::Degraded);
        assert_eq!(health.overload_count, 1);
        assert_eq!(health.database_points, 1);
        assert_eq!(health.durable_through_sequence, Some(1));

        let Response::Ack(ack) = request(&mut stream, Request::CommitBatch(first.clone())) else {
            panic!("an exact retry must still return its receipt");
        };
        assert!(ack.durable && ack.deduplicated);
        let mut conflict = first;
        conflict.points[0].value = 51.0;
        let Response::Error(error) = request(&mut stream, Request::CommitBatch(conflict)) else {
            panic!("changed retry must fail");
        };
        assert_eq!(error.code, ErrorCode::IdempotencyConflict);
        assert_eq!(std::fs::read(root.join("active.wlog")).unwrap(), before);
        drop(stream);
        drop(server); // SIGKILL: reopen must preserve the durable receipt.

        let (server, mut stream) = start(&root, &socket, u64::MAX, 1);
        let Response::Ack(ack) = request(&mut stream, Request::CommitBatch(batch(1))) else {
            panic!("retry failed after SIGKILL");
        };
        assert!(ack.durable && ack.deduplicated);
        let Response::Ack(ack) = request(&mut stream, Request::CommitBatch(batch(2))) else {
            panic!("write failed after budget was raised");
        };
        assert!(ack.durable && !ack.deduplicated);
        drop(stream);
        drop(server);
        assert_eq!(
            Store::open_read_only(&root)
                .unwrap()
                .database()
                .stats()
                .unwrap()
                .points,
            2
        );
    }
}

#[test]
fn bounded_runtime_rejects_maintenance_that_could_bypass_the_budget() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path()).unwrap();
    let result = ShadowRuntime::start_store(
        store,
        ShadowRuntimeConfig {
            storage_limits: Some(ShadowStorageLimits {
                max_store_bytes: 1024,
                minimum_free_bytes: 1,
            }),
            maintenance_interval: Some(Duration::from_secs(300)),
            ..ShadowRuntimeConfig::default()
        },
    );
    assert!(matches!(
        result,
        Err(ftwdb::shadow_runtime::ShadowStartError::InvalidStorageLimits)
    ));
}

#[test]
fn invalid_budget_never_opens_a_store() {
    for value in ["0", "-1", "", "512MiB", "18446744073709551616"] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("store");
        let output = Command::new(env!("CARGO_BIN_EXE_ftwdb-shadow"))
            .arg(&root)
            .arg(directory.path().join("run/shadow.sock"))
            .env("FTWDB_SHADOW_MAX_STORE_BYTES", value)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(!root.exists());
        assert!(String::from_utf8_lossy(&output.stderr).contains("must be a positive byte count"));
    }
}
