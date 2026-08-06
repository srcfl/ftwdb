//! Local Unix socket server for the FTWDB shadow protocol.
//!
//! Version one serves one client at a time. Every accepted stream has bounded
//! read and write times, so a stalled peer cannot hold the listener forever.
//! HELLO binds each connection to one ingress source. The durable store checks
//! source, sequence, commit ID, and transaction bytes.

use crate::shadow_protocol::{
    self, Ack, AckKind, CommitBatchRequest, ErrorCode, ErrorResponse, HealthResponse, HealthStatus,
    HelloResponse, Request, Response, WireMessage,
};
use crate::shadow_runtime::{
    AckWaitError, FlushSubmitError, ShadowFlushFailure, ShadowRuntimeState, ShadowSubmitter,
    ShadowWrite, ShadowWriteFailure, SubmitError,
};
use crate::{Error, IngressIdentity, Transaction};
use std::fmt;
use std::fs::{self, DirBuilder, FileType};
use std::io::{self, Read};
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Limits for the local sidecar endpoint.
#[derive(Clone, Debug)]
pub struct ShadowServerConfig {
    pub socket_path: PathBuf,
    pub io_timeout: Duration,
    pub acknowledgement_timeout: Duration,
    pub accept_poll_interval: Duration,
}

impl ShadowServerConfig {
    #[must_use]
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
            io_timeout: Duration::from_secs(2),
            acknowledgement_timeout: Duration::from_secs(5),
            accept_poll_interval: Duration::from_millis(20),
        }
    }
}

/// Cloneable stop flag for tests and clean service shutdown.
#[derive(Clone, Debug, Default)]
pub struct ShadowStopToken(Arc<AtomicBool>);

impl ShadowStopToken {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn stop(&self) {
        self.0.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ShadowServerReport {
    pub accepted_clients: u64,
    pub client_errors: u64,
}

#[derive(Debug)]
pub enum ShadowServerError {
    MissingSocketParent,
    UnsafeSocketParent(PathBuf),
    ExistingPathIsNotSocket(PathBuf),
    SocketInUse(PathBuf),
    CouldNotProveSocketStale { path: PathBuf, error: io::Error },
    Io(io::Error),
}

impl fmt::Display for ShadowServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSocketParent => {
                formatter.write_str("socket path needs a parent directory")
            }
            Self::UnsafeSocketParent(path) => write!(
                formatter,
                "socket parent is not a real directory: {}",
                path.display()
            ),
            Self::ExistingPathIsNotSocket(path) => write!(
                formatter,
                "refusing to replace a non-socket path: {}",
                path.display()
            ),
            Self::SocketInUse(path) => {
                write!(formatter, "a listener already owns {}", path.display())
            }
            Self::CouldNotProveSocketStale { path, error } => write!(
                formatter,
                "could not prove that {} is stale: {error}",
                path.display()
            ),
            Self::Io(error) => write!(formatter, "shadow server I/O error: {error}"),
        }
    }
}

impl std::error::Error for ShadowServerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CouldNotProveSocketStale { error, .. } | Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for ShadowServerError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

/// Serves local clients until `stop` is set.
pub fn serve(
    config: &ShadowServerConfig,
    submitter: ShadowSubmitter,
    stop: &ShadowStopToken,
) -> Result<ShadowServerReport, ShadowServerError> {
    validate_config(config)?;
    let bound = BoundSocket::bind(&config.socket_path)?;
    bound.listener.set_nonblocking(true)?;
    let mut report = ShadowServerReport::default();
    while !stop.is_stopped() {
        match bound.listener.accept() {
            Ok((mut stream, _)) => {
                report.accepted_clients = report.accepted_clients.saturating_add(1);
                if serve_connection(&mut stream, &submitter, config, stop).is_err() {
                    report.client_errors = report.client_errors.saturating_add(1);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(config.accept_poll_interval);
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(ShadowServerError::Io(error)),
        }
    }
    Ok(report)
}

fn validate_config(config: &ShadowServerConfig) -> Result<(), ShadowServerError> {
    if config.io_timeout.is_zero()
        || config.acknowledgement_timeout.is_zero()
        || config.accept_poll_interval.is_zero()
    {
        return Err(ShadowServerError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "shadow server timeouts must be positive",
        )));
    }
    Ok(())
}

fn serve_connection(
    stream: &mut UnixStream,
    submitter: &ShadowSubmitter,
    config: &ShadowServerConfig,
    stop: &ShadowStopToken,
) -> Result<(), ()> {
    stream
        .set_write_timeout(Some(config.io_timeout))
        .map_err(|_| ())?;
    let mut source_id = None;

    loop {
        if stop.is_stopped() {
            return Ok(());
        }
        let message = match read_frame_before(stream, config.io_timeout) {
            Ok(message) => message,
            Err(error) => {
                if stop.is_stopped() {
                    return Ok(());
                }
                let code = match error {
                    shadow_protocol::ProtocolError::UnsupportedVersion(_) => ErrorCode::Unsupported,
                    _ => ErrorCode::InvalidRequest,
                };
                let _ = write_error(stream, code, false);
                return Err(());
            }
        };

        let response = match message {
            WireMessage::Request(Request::Hello(hello)) if source_id.is_none() => {
                if hello.source_id == 0 {
                    stable_error(ErrorCode::InvalidRequest, false)
                } else {
                    source_id = Some(hello.source_id);
                    WireMessage::Response(Response::Hello(HelloResponse {
                        selected_version: shadow_protocol::PROTOCOL_VERSION,
                        session_id: next_session_id(),
                        server_time_micros: unix_time_micros(),
                    }))
                }
            }
            WireMessage::Request(Request::Hello(_)) => {
                stable_error(ErrorCode::InvalidRequest, false)
            }
            WireMessage::Request(_) if source_id.is_none() => {
                stable_error(ErrorCode::InvalidRequest, false)
            }
            WireMessage::Request(Request::CommitBatch(batch)) => {
                if source_id != Some(batch.source_id) {
                    stable_error(ErrorCode::InvalidRequest, false)
                } else {
                    handle_commit(batch, submitter, config)
                }
            }
            WireMessage::Request(Request::Flush(flush)) => {
                if source_id != Some(flush.source_id) {
                    stable_error(ErrorCode::InvalidRequest, false)
                } else {
                    handle_flush(flush.source_id, flush.through_sequence, submitter, config)
                }
            }
            WireMessage::Request(Request::Health(request)) => handle_health(
                request.nonce,
                source_id.expect("HELLO set a source"),
                submitter,
            ),
            WireMessage::Response(_) => stable_error(ErrorCode::InvalidRequest, false),
        };

        if shadow_protocol::write_to(stream, &response).is_err() {
            return Err(());
        }
    }
}

/// Reads one whole frame under one deadline. A socket timeout on each read is
/// not enough: a peer could otherwise send one byte per timeout and occupy the
/// only connection slot for hours.
fn read_frame_before(
    stream: &mut UnixStream,
    timeout: Duration,
) -> Result<WireMessage, shadow_protocol::ProtocolError> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "frame deadline overflows"))?;
    shadow_protocol::read_from(&mut DeadlineReader { stream, deadline })
}

struct DeadlineReader<'a> {
    stream: &'a mut UnixStream,
    deadline: Instant,
}

impl Read for DeadlineReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let remaining = self
            .deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "frame deadline expired"))?;
        self.stream.set_read_timeout(Some(remaining))?;
        self.stream.read(buffer)
    }
}

fn handle_commit(
    batch: CommitBatchRequest,
    submitter: &ShadowSubmitter,
    config: &ShadowServerConfig,
) -> WireMessage {
    let source_id = batch.source_id;
    let sequence = batch.sequence;
    let commit_id = batch.commit_id;
    let mut transaction = transaction_from_batch(batch);
    transaction.with_ingress_identity(IngressIdentity::new(source_id, sequence, commit_id));
    let write = ShadowWrite::from_identified(transaction)
        .expect("ingress identity always supplies a commit ID");
    let receipt = match submitter.try_submit(write) {
        Ok(receipt) => receipt,
        Err(error) => return map_submit_error(error),
    };
    let acknowledgement = match receipt.wait_timeout(config.acknowledgement_timeout) {
        Ok(Ok(acknowledgement)) => acknowledgement,
        Ok(Err(error)) => return map_write_failure(error),
        Err(error) => return map_wait_error(error),
    };

    let commit = acknowledgement.commit;
    WireMessage::Response(Response::Ack(Ack {
        kind: AckKind::CommitBatch,
        source_id,
        sequence,
        commit_id,
        accepted_through_sequence: acknowledgement.accepted_through,
        durable_through_sequence: acknowledgement.durable_through,
        durable: commit.durable,
        deduplicated: commit.deduplicated,
        frame_offset: commit.frame_offset,
        records: u32::try_from(commit.records).unwrap_or(u32::MAX),
        points: u32::try_from(commit.points).unwrap_or(u32::MAX),
        bytes_written: commit.bytes_written,
    }))
}

fn transaction_from_batch(batch: CommitBatchRequest) -> Transaction {
    let mut transaction = Transaction::new();
    for entity in batch.entities {
        transaction.upsert_entity(entity);
    }
    for relation in batch.relations {
        transaction.upsert_relation(relation);
    }
    for series in batch.series {
        transaction.define_series(series);
    }
    for run in batch.runs {
        transaction.upsert_run(run);
    }
    for plan in batch.plans {
        transaction.upsert_plan(plan);
    }
    if !batch.points.is_empty() {
        transaction.append_points(batch.points);
    }
    transaction
}

fn handle_flush(
    source_id: u128,
    through_sequence: u64,
    submitter: &ShadowSubmitter,
    config: &ShadowServerConfig,
) -> WireMessage {
    let receipt = match submitter.try_flush(source_id, through_sequence) {
        Ok(receipt) => receipt,
        Err(error) => return map_flush_submit_error(error),
    };
    let acknowledgement = match receipt.wait_timeout(config.acknowledgement_timeout) {
        Ok(Ok(acknowledgement)) => acknowledgement,
        Ok(Err(error)) => return map_flush_failure(error),
        Err(error) => return map_wait_error(error),
    };
    if acknowledgement
        .durable_through
        .is_none_or(|watermark| watermark < through_sequence)
    {
        return stable_error(ErrorCode::Internal, true);
    }
    WireMessage::Response(Response::Ack(Ack {
        kind: AckKind::Flush,
        source_id,
        sequence: through_sequence,
        commit_id: 0,
        accepted_through_sequence: acknowledgement.accepted_through,
        durable_through_sequence: acknowledgement.durable_through,
        durable: true,
        deduplicated: false,
        frame_offset: 0,
        records: 0,
        points: 0,
        bytes_written: 0,
    }))
}

fn handle_health(nonce: u64, source_id: u128, submitter: &ShadowSubmitter) -> WireMessage {
    let health = submitter.health();
    let watermarks = health
        .source_watermarks
        .get(&source_id)
        .copied()
        .unwrap_or_default();
    let status = match health.state {
        ShadowRuntimeState::Running => HealthStatus::Healthy,
        ShadowRuntimeState::Closing => HealthStatus::Degraded,
        ShadowRuntimeState::Poisoned | ShadowRuntimeState::Closed => HealthStatus::Unavailable,
    };
    WireMessage::Response(Response::Health(HealthResponse {
        nonce,
        source_id,
        status,
        queue_entries: u32::try_from(health.queued).unwrap_or(u32::MAX),
        accepted_through_sequence: watermarks.accepted_through,
        durable_through_sequence: watermarks.durable_through,
    }))
}

fn map_submit_error(error: SubmitError) -> WireMessage {
    match error {
        SubmitError::Overloaded(_) | SubmitError::PointBudgetExhausted(_) => {
            stable_error(ErrorCode::Overloaded, true)
        }
        SubmitError::DeadlineExceeded(_) => stable_error(ErrorCode::Overloaded, true),
        SubmitError::Closed(_) | SubmitError::Poisoned { .. } => {
            stable_error(ErrorCode::Internal, true)
        }
    }
}

fn map_write_failure(error: ShadowWriteFailure) -> WireMessage {
    match error {
        ShadowWriteFailure::Rejected(error) => map_store_error(&error),
        ShadowWriteFailure::Writer(_)
        | ShadowWriteFailure::Poisoned { .. }
        | ShadowWriteFailure::WriterPanicked { .. }
        | ShadowWriteFailure::WorkerStopped => stable_error(ErrorCode::Internal, true),
    }
}

fn map_store_error(error: &Error) -> WireMessage {
    match error {
        Error::IngressSourceSequenceConflict { .. } | Error::IngressCommitIdConflict { .. } => {
            stable_error(ErrorCode::IdempotencyConflict, false)
        }
        Error::IngressSequenceGap { .. }
        | Error::IngressSequenceExhausted { .. }
        | Error::BatchTooLarge { .. }
        | Error::InvalidArgument(_)
        | Error::InvalidModel(_)
        | Error::Serialization(_) => stable_error(ErrorCode::InvalidRequest, false),
        _ => stable_error(ErrorCode::Internal, true),
    }
}

fn map_flush_submit_error(error: FlushSubmitError) -> WireMessage {
    match error {
        FlushSubmitError::Overloaded | FlushSubmitError::DeadlineExceeded => {
            stable_error(ErrorCode::Overloaded, true)
        }
        FlushSubmitError::Closed | FlushSubmitError::Poisoned { .. } => {
            stable_error(ErrorCode::Internal, true)
        }
    }
}

fn map_flush_failure(error: ShadowFlushFailure) -> WireMessage {
    match error {
        ShadowFlushFailure::NotAccepted { .. } => stable_error(ErrorCode::InvalidRequest, true),
        ShadowFlushFailure::Rejected(error) => map_store_error(&error),
        ShadowFlushFailure::Writer(_)
        | ShadowFlushFailure::Poisoned { .. }
        | ShadowFlushFailure::WriterPanicked { .. }
        | ShadowFlushFailure::WorkerStopped => stable_error(ErrorCode::Internal, true),
    }
}

fn map_wait_error(_error: AckWaitError) -> WireMessage {
    stable_error(ErrorCode::Internal, true)
}

fn stable_error(code: ErrorCode, retryable: bool) -> WireMessage {
    let message = match code {
        ErrorCode::InvalidRequest => "invalid request",
        ErrorCode::Overloaded => "shadow writer overloaded",
        ErrorCode::Internal => "shadow writer unavailable",
        ErrorCode::Unsupported => "unsupported protocol version",
        ErrorCode::IdempotencyConflict => "idempotency conflict",
    };
    WireMessage::Response(Response::Error(ErrorResponse {
        code,
        retryable,
        message: message.to_owned(),
    }))
}

fn write_error(stream: &mut UnixStream, code: ErrorCode, retryable: bool) -> Result<(), ()> {
    shadow_protocol::write_to(stream, &stable_error(code, retryable)).map_err(|_| ())
}

fn next_session_id() -> [u8; 16] {
    let count = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = unix_time_micros() as u64;
    let mut id = [0; 16];
    id[..8].copy_from_slice(&now.to_be_bytes());
    id[8..].copy_from_slice(&(count ^ u64::from(std::process::id())).to_be_bytes());
    id
}

fn unix_time_micros() -> i64 {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    i64::try_from(micros).unwrap_or(i64::MAX)
}

struct BoundSocket {
    listener: UnixListener,
    _guard: SocketGuard,
}

impl BoundSocket {
    fn bind(path: &Path) -> Result<Self, ShadowServerError> {
        prepare_parent(path)?;
        remove_proven_stale_socket(path)?;
        let listener = UnixListener::bind(path)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        let metadata = fs::symlink_metadata(path)?;
        Ok(Self {
            listener,
            _guard: SocketGuard {
                path: path.to_owned(),
                device: metadata.dev(),
                inode: metadata.ino(),
            },
        })
    }
}

struct SocketGuard {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        let Ok(metadata) = fs::symlink_metadata(&self.path) else {
            return;
        };
        if metadata.file_type().is_socket()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn prepare_parent(path: &Path) -> Result<(), ShadowServerError> {
    let parent = path
        .parent()
        .ok_or(ShadowServerError::MissingSocketParent)?;
    match fs::symlink_metadata(parent) {
        Ok(metadata) if metadata.file_type().is_dir() => {
            if metadata.permissions().mode() & 0o022 != 0 {
                return Err(ShadowServerError::UnsafeSocketParent(parent.to_owned()));
            }
            return Ok(());
        }
        Ok(_) => return Err(ShadowServerError::UnsafeSocketParent(parent.to_owned())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(ShadowServerError::Io(error)),
    }
    let mut builder = DirBuilder::new();
    builder.recursive(true).mode(0o700).create(parent)?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
    let metadata = fs::symlink_metadata(parent)?;
    if !metadata.file_type().is_dir() {
        return Err(ShadowServerError::UnsafeSocketParent(parent.to_owned()));
    }
    Ok(())
}

fn remove_proven_stale_socket(path: &Path) -> Result<(), ShadowServerError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(ShadowServerError::Io(error)),
    };
    if !is_socket(metadata.file_type()) {
        return Err(ShadowServerError::ExistingPathIsNotSocket(path.to_owned()));
    }
    match UnixStream::connect(path) {
        Ok(_) => Err(ShadowServerError::SocketInUse(path.to_owned())),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
            ) =>
        {
            let current = fs::symlink_metadata(path)?;
            if !current.file_type().is_socket()
                || current.dev() != metadata.dev()
                || current.ino() != metadata.ino()
            {
                return Err(ShadowServerError::CouldNotProveSocketStale {
                    path: path.to_owned(),
                    error: io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "socket path changed during the stale check",
                    ),
                });
            }
            fs::remove_file(path)?;
            Ok(())
        }
        Err(error) => Err(ShadowServerError::CouldNotProveSocketStale {
            path: path.to_owned(),
            error,
        }),
    }
}

fn is_socket(file_type: FileType) -> bool {
    file_type.is_socket()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shadow_protocol::{HelloRequest, Response};
    use crate::shadow_runtime::{ShadowRuntime, ShadowRuntimeConfig};
    use crate::{Entity, EntityId, Store};
    use std::fs::File;

    fn runtime() -> (tempfile::TempDir, ShadowRuntime) {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path().join("store")).unwrap();
        let runtime = ShadowRuntime::start_store(
            store,
            ShadowRuntimeConfig {
                queue_capacity: 4,
                max_queued_points: 64,
            },
        )
        .unwrap();
        (directory, runtime)
    }

    #[test]
    fn hello_is_required_before_health() {
        let (_directory, runtime) = runtime();
        let submitter = runtime.submitter();
        let config = ShadowServerConfig::new("/tmp/unused-ftwdb-shadow.sock");
        let (mut client, mut server) = UnixStream::pair().unwrap();
        let join = thread::spawn(move || {
            serve_connection(&mut server, &submitter, &config, &ShadowStopToken::new())
        });
        shadow_protocol::write_to(
            &mut client,
            &WireMessage::Request(Request::Health(shadow_protocol::HealthRequest { nonce: 7 })),
        )
        .unwrap();
        let response = shadow_protocol::read_from(&mut client).unwrap();
        assert!(matches!(
            response,
            WireMessage::Response(Response::Error(ErrorResponse {
                code: ErrorCode::InvalidRequest,
                ..
            }))
        ));
        drop(client);
        assert!(join.join().unwrap().is_err());
        runtime.shutdown().unwrap();
    }

    #[test]
    fn hello_then_health_succeeds() {
        let (_directory, runtime) = runtime();
        let submitter = runtime.submitter();
        let config = ShadowServerConfig::new("/tmp/unused-ftwdb-shadow.sock");
        let (mut client, mut server) = UnixStream::pair().unwrap();
        let join = thread::spawn(move || {
            serve_connection(&mut server, &submitter, &config, &ShadowStopToken::new())
        });
        shadow_protocol::write_to(
            &mut client,
            &WireMessage::Request(Request::Hello(HelloRequest {
                source_id: 17,
                node_id: "box-test".to_owned(),
                client_version: "test".to_owned(),
                capabilities: 0,
            })),
        )
        .unwrap();
        assert!(matches!(
            shadow_protocol::read_from(&mut client).unwrap(),
            WireMessage::Response(Response::Hello(_))
        ));
        shadow_protocol::write_to(
            &mut client,
            &WireMessage::Request(Request::Health(shadow_protocol::HealthRequest {
                nonce: 11,
            })),
        )
        .unwrap();
        assert!(matches!(
            shadow_protocol::read_from(&mut client).unwrap(),
            WireMessage::Response(Response::Health(HealthResponse { nonce: 11, .. }))
        ));
        drop(client);
        assert!(join.join().unwrap().is_err());
        runtime.shutdown().unwrap();
    }

    #[test]
    fn one_deadline_bounds_the_whole_frame() {
        use std::io::Write;

        let message = WireMessage::Request(Request::Hello(HelloRequest {
            source_id: 17,
            node_id: "slow-client".to_owned(),
            client_version: "test".to_owned(),
            capabilities: 0,
        }));
        let frame = shadow_protocol::encode(&message).unwrap();
        let (mut client, mut server) = UnixStream::pair().unwrap();
        let writer = thread::spawn(move || {
            for byte in frame {
                if client.write_all(&[byte]).is_err() {
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }
        });

        let started = Instant::now();
        let result = read_frame_before(&mut server, Duration::from_millis(60));
        let elapsed = started.elapsed();
        drop(server);
        writer.join().unwrap();

        assert!(matches!(
            result,
            Err(shadow_protocol::ProtocolError::Io(error))
                if matches!(error.kind(), io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock)
        ));
        assert!(elapsed < Duration::from_millis(500));
    }

    #[test]
    fn stop_token_ends_an_active_connection_within_the_read_deadline() {
        let (_directory, runtime) = runtime();
        let submitter = runtime.submitter();
        let mut config = ShadowServerConfig::new("/tmp/unused-ftwdb-shadow.sock");
        config.io_timeout = Duration::from_millis(50);
        let stop = ShadowStopToken::new();
        let server_stop = stop.clone();
        let (mut client, mut server) = UnixStream::pair().unwrap();
        let join =
            thread::spawn(move || serve_connection(&mut server, &submitter, &config, &server_stop));
        shadow_protocol::write_to(
            &mut client,
            &WireMessage::Request(Request::Hello(HelloRequest {
                source_id: 17,
                node_id: "stop-test".to_owned(),
                client_version: "test".to_owned(),
                capabilities: 0,
            })),
        )
        .unwrap();
        assert!(matches!(
            shadow_protocol::read_from(&mut client).unwrap(),
            WireMessage::Response(Response::Hello(_))
        ));

        let started = Instant::now();
        stop.stop();
        assert!(join.join().unwrap().is_ok());
        assert!(started.elapsed() < Duration::from_millis(500));
        runtime.shutdown().unwrap();
    }

    #[test]
    fn commit_replay_and_conflict_use_durable_ingress_identity() {
        let (_directory, runtime) = runtime();
        let submitter = runtime.submitter();
        let config = ShadowServerConfig::new("/tmp/unused-ftwdb-shadow.sock");
        let (mut client, mut server) = UnixStream::pair().unwrap();
        let join = thread::spawn(move || {
            serve_connection(&mut server, &submitter, &config, &ShadowStopToken::new())
        });
        shadow_protocol::write_to(
            &mut client,
            &WireMessage::Request(Request::Hello(HelloRequest {
                source_id: 17,
                node_id: "box-test".to_owned(),
                client_version: "test".to_owned(),
                capabilities: 0,
            })),
        )
        .unwrap();
        let _ = shadow_protocol::read_from(&mut client).unwrap();

        let mut batch = CommitBatchRequest {
            source_id: 17,
            sequence: 5,
            commit_id: 99,
            entities: vec![Entity {
                id: EntityId(1),
                kind: "site".to_owned(),
                name: "test-site".to_owned(),
                parent: None,
                valid_from: 0,
                valid_to: None,
                properties: Default::default(),
            }],
            relations: Vec::new(),
            series: Vec::new(),
            runs: Vec::new(),
            plans: Vec::new(),
            points: Vec::new(),
        };
        let request = WireMessage::Request(Request::CommitBatch(batch.clone()));
        shadow_protocol::write_to(&mut client, &request).unwrap();
        assert!(matches!(
            shadow_protocol::read_from(&mut client).unwrap(),
            WireMessage::Response(Response::Ack(Ack {
                source_id: 17,
                sequence: 5,
                durable: true,
                deduplicated: false,
                ..
            }))
        ));

        shadow_protocol::write_to(&mut client, &request).unwrap();
        assert!(matches!(
            shadow_protocol::read_from(&mut client).unwrap(),
            WireMessage::Response(Response::Ack(Ack {
                source_id: 17,
                sequence: 5,
                durable: true,
                deduplicated: true,
                ..
            }))
        ));

        batch.entities[0].name = "changed".to_owned();
        shadow_protocol::write_to(
            &mut client,
            &WireMessage::Request(Request::CommitBatch(batch)),
        )
        .unwrap();
        assert!(matches!(
            shadow_protocol::read_from(&mut client).unwrap(),
            WireMessage::Response(Response::Error(ErrorResponse {
                code: ErrorCode::IdempotencyConflict,
                retryable: false,
                ..
            }))
        ));

        drop(client);
        assert!(join.join().unwrap().is_err());
        runtime.shutdown().unwrap();
    }

    #[test]
    fn bind_refuses_to_replace_regular_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("shadow.sock");
        File::create(&path).unwrap();
        let error = match BoundSocket::bind(&path) {
            Ok(_) => panic!("bind unexpectedly replaced the file"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            ShadowServerError::ExistingPathIsNotSocket(found) if found == path
        ));
        assert!(path.is_file());
    }

    #[test]
    fn bind_rejects_group_writable_existing_parent() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().join("shared-run");
        fs::create_dir(&parent).unwrap();
        if let Err(error) = fs::set_permissions(&parent, fs::Permissions::from_mode(0o770)) {
            if error.kind() == io::ErrorKind::PermissionDenied {
                return;
            }
            panic!("could not prepare shared parent: {error}");
        }
        let path = parent.join("shadow.sock");
        let error = match BoundSocket::bind(&path) {
            Ok(_) => panic!("bind unexpectedly used a shared parent"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            ShadowServerError::UnsafeSocketParent(found) if found == parent
        ));
        assert!(!path.exists());
    }

    #[test]
    fn bind_refuses_a_live_socket() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("shadow.sock");
        let listener = match UnixListener::bind(&path) {
            Ok(listener) => listener,
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("could not prepare live socket: {error}"),
        };
        let error = match BoundSocket::bind(&path) {
            Ok(_) => panic!("bind unexpectedly replaced the live socket"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            ShadowServerError::SocketInUse(found) if found == path
        ));
        drop(listener);
    }

    #[test]
    fn bind_replaces_a_proven_stale_socket() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("shadow.sock");
        let listener = match UnixListener::bind(&path) {
            Ok(listener) => listener,
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("could not prepare stale socket: {error}"),
        };
        drop(listener);
        let bound = BoundSocket::bind(&path).unwrap();
        assert!(path.exists());
        drop(bound);
        assert!(!path.exists());
    }

    #[test]
    fn bound_socket_has_private_mode_and_is_removed_on_drop() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().join("run");
        let path = parent.join("shadow.sock");
        let bound = match BoundSocket::bind(&path) {
            Ok(bound) => bound,
            Err(ShadowServerError::Io(error))
                if error.kind() == io::ErrorKind::PermissionDenied =>
            {
                return;
            }
            Err(error) => panic!("could not bind private socket: {error}"),
        };
        assert_eq!(
            fs::metadata(&parent).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        drop(bound);
        assert!(!path.exists());
    }

    #[test]
    fn malformed_client_does_not_stop_the_listener() {
        use std::io::Write;

        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path().join("store")).unwrap();
        let runtime = ShadowRuntime::start_store(
            store,
            ShadowRuntimeConfig {
                queue_capacity: 4,
                max_queued_points: 64,
            },
        )
        .unwrap();
        let socket_path = directory.path().join("run/shadow.sock");
        let mut config = ShadowServerConfig::new(&socket_path);
        config.io_timeout = Duration::from_millis(100);
        config.acknowledgement_timeout = Duration::from_millis(250);
        config.accept_poll_interval = Duration::from_millis(2);
        let stop = ShadowStopToken::new();
        let server_stop = stop.clone();
        let submitter = runtime.submitter();
        let mut join = Some(thread::spawn(move || {
            serve(&config, submitter, &server_stop)
        }));

        for _ in 0..100 {
            if socket_path.exists() {
                break;
            }
            if join.as_ref().unwrap().is_finished() {
                let result = join.take().unwrap().join().unwrap();
                runtime.shutdown().unwrap();
                if matches!(
                    result,
                    Err(ShadowServerError::Io(ref error))
                        if error.kind() == io::ErrorKind::PermissionDenied
                ) {
                    return;
                }
                panic!("shadow server stopped before bind: {result:?}");
            }
            thread::sleep(Duration::from_millis(2));
        }
        if !socket_path.exists() {
            stop.stop();
            let result = join.take().unwrap().join().unwrap();
            runtime.shutdown().unwrap();
            panic!("shadow socket did not appear; server result: {result:?}");
        }
        let mut malformed = UnixStream::connect(&socket_path).unwrap();
        malformed.write_all(b"bad").unwrap();
        drop(malformed);

        let mut client = UnixStream::connect(&socket_path).unwrap();
        shadow_protocol::write_to(
            &mut client,
            &WireMessage::Request(Request::Hello(HelloRequest {
                source_id: 18,
                node_id: "box-test".to_owned(),
                client_version: "test".to_owned(),
                capabilities: 0,
            })),
        )
        .unwrap();
        assert!(matches!(
            shadow_protocol::read_from(&mut client).unwrap(),
            WireMessage::Response(Response::Hello(_))
        ));
        stop.stop();
        drop(client);
        let report = join.take().unwrap().join().unwrap().unwrap();
        assert_eq!(report.accepted_clients, 2);
        assert!(!socket_path.exists());
        runtime.shutdown().unwrap();
    }
}
