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
use crate::{Error, IngressIdentity};
use std::fmt;
use std::fs::{self, DirBuilder, FileType};
use std::io::{self, Read};
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
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
    /// Effective user ID allowed to use the socket.
    pub allowed_peer_uid: u32,
    pub io_timeout: Duration,
    pub acknowledgement_timeout: Duration,
    pub accept_poll_interval: Duration,
}

impl ShadowServerConfig {
    #[must_use]
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
            allowed_peer_uid: rustix::process::geteuid().as_raw(),
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
    pub peer_auth_failures: u64,
    pub client_errors: u64,
}

#[derive(Debug)]
pub enum ShadowServerError {
    MissingSocketParent,
    UnsafeSocketParent(PathBuf),
    ExistingPathIsNotSocket(PathBuf),
    SocketInUse(PathBuf),
    CouldNotProveSocketStale { path: PathBuf, error: io::Error },
    PeerCredentials(io::Error),
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
            Self::PeerCredentials(error) => {
                write!(formatter, "could not read Unix peer credentials: {error}")
            }
            Self::Io(error) => write!(formatter, "shadow server I/O error: {error}"),
        }
    }
}

impl std::error::Error for ShadowServerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CouldNotProveSocketStale { error, .. }
            | Self::PeerCredentials(error)
            | Self::Io(error) => Some(error),
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
                // The listener is nonblocking so stop polling stays bounded.
                // Accepted streams must block under their own frame deadline.
                stream.set_nonblocking(false)?;
                let peer_uid =
                    peer_effective_uid(&stream).map_err(ShadowServerError::PeerCredentials)?;
                if peer_uid != config.allowed_peer_uid {
                    report.peer_auth_failures = report.peer_auth_failures.saturating_add(1);
                    continue;
                }
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

/// Returns the effective UID captured by the kernel for this connected peer.
/// The check does not trust socket-file mode alone: a process that received an
/// open descriptor still has to run as the configured service user.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn peer_effective_uid(stream: &UnixStream) -> io::Result<u32> {
    let mut credentials = MaybeUninit::<libc::ucred>::zeroed();
    let mut length = libc::socklen_t::try_from(std::mem::size_of::<libc::ucred>())
        .expect("ucred size fits socklen_t");
    // SAFETY: `credentials` points to writable storage of `length` bytes, and
    // the stream owns a valid socket descriptor for the whole call.
    let result = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            credentials.as_mut_ptr().cast(),
            &mut length,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    if usize::try_from(length).ok() != Some(std::mem::size_of::<libc::ucred>()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "kernel returned a truncated peer credential",
        ));
    }
    // SAFETY: getsockopt succeeded and reported the full `ucred` size.
    Ok(unsafe { credentials.assume_init() }.uid)
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "visionos",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
    target_os = "openbsd"
))]
fn peer_effective_uid(stream: &UnixStream) -> io::Result<u32> {
    let mut uid = MaybeUninit::<libc::uid_t>::uninit();
    let mut gid = MaybeUninit::<libc::gid_t>::uninit();
    // SAFETY: the stream owns a valid socket descriptor, and both output
    // pointers refer to initialized storage when getpeereid returns success.
    let result =
        unsafe { libc::getpeereid(stream.as_raw_fd(), uid.as_mut_ptr(), gid.as_mut_ptr()) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: getpeereid initialized both outputs on success.
    Ok(unsafe { uid.assume_init() })
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "visionos",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
    target_os = "openbsd"
)))]
fn peer_effective_uid(_stream: &UnixStream) -> io::Result<u32> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "peer credentials are not implemented on this Unix target",
    ))
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
    let mut source_id = None;

    loop {
        if stop.is_stopped() {
            return Ok(());
        }
        let message = match read_frame_before(stream, config.io_timeout) {
            Ok(message) => message,
            Err(shadow_protocol::ProtocolError::Truncated { actual: 0, .. }) => {
                // EOF before the next frame starts is a normal client close.
                // A partial header or body still takes the error path below.
                return Ok(());
            }
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

        if stream.set_write_timeout(Some(config.io_timeout)).is_err()
            || shadow_protocol::write_to(stream, &response).is_err()
        {
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
        let read = match self.stream.set_read_timeout(Some(remaining)) {
            Ok(()) => self.stream.read(buffer),
            // macOS can reject SO_RCVTIMEO with EINVAL after the peer has
            // already closed. A nonblocking read can classify that close
            // without risking an unbounded wait if the timeout itself was bad.
            Err(error) if error.kind() == io::ErrorKind::InvalidInput => {
                self.stream.set_nonblocking(true)?;
                let read = self.stream.read(buffer);
                self.stream.set_nonblocking(false)?;
                read
            }
            Err(error) => return Err(error),
        };
        match read {
            Ok(read) => Ok(read),
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => Ok(0),
            Err(error) => Err(error),
        }
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
    let mut transaction = shadow_protocol::transaction_from_batch(batch);
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
        Error::IngressSequenceNotIncreasing { .. }
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
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or(ShadowServerError::MissingSocketParent)?;
    match fs::symlink_metadata(parent) {
        Ok(metadata) => check_private_socket_parent(parent, &metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            create_private_socket_ancestors(parent)
        }
        Err(error) => Err(ShadowServerError::Io(error)),
    }
}

fn owned_by_effective_user(metadata: &fs::Metadata) -> bool {
    metadata.uid() == rustix::process::geteuid().as_raw()
}

/// The socket directory itself must be owner-only. A 0755 parent lets another
/// local user reach the inode during the bind-to-chmod window.
fn check_private_socket_parent(
    path: &Path,
    metadata: &fs::Metadata,
) -> Result<(), ShadowServerError> {
    if metadata.file_type().is_dir()
        && owned_by_effective_user(metadata)
        && metadata.permissions().mode() & 0o777 == 0o700
    {
        Ok(())
    } else {
        Err(ShadowServerError::UnsafeSocketParent(path.to_owned()))
    }
}

/// An existing ancestor may be 0755 (home, `/var/lib`) but must not be
/// group- or world-writable. Creating a 0700 child under `/tmp` is a
/// classic symlink race.
fn check_existing_ancestor(path: &Path, metadata: &fs::Metadata) -> Result<(), ShadowServerError> {
    if metadata.file_type().is_dir()
        && owned_by_effective_user(metadata)
        && metadata.permissions().mode() & 0o022 == 0
    {
        Ok(())
    } else {
        Err(ShadowServerError::UnsafeSocketParent(path.to_owned()))
    }
}

fn create_private_socket_ancestors(path: &Path) -> Result<(), ShadowServerError> {
    let mut missing = vec![path.to_path_buf()];
    let mut cursor = path;
    loop {
        let Some(parent) = cursor
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        else {
            let cwd = Path::new(".");
            check_existing_ancestor(cwd, &fs::symlink_metadata(cwd)?)?;
            break;
        };
        match fs::symlink_metadata(parent) {
            Ok(metadata) => {
                check_existing_ancestor(parent, &metadata)?;
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                missing.push(parent.to_path_buf());
                cursor = parent;
            }
            Err(error) => return Err(ShadowServerError::Io(error)),
        }
    }

    missing.reverse();
    for directory in missing {
        let mut builder = DirBuilder::new();
        builder.mode(0o700).create(&directory)?;
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
        check_private_socket_parent(&directory, &fs::symlink_metadata(&directory)?)?;
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
    use crate::shadow_protocol::{FlushRequest, HelloRequest, Response};
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

    fn private_socket_path(directory: &tempfile::TempDir) -> PathBuf {
        let parent = directory.path().join("run");
        fs::create_dir(&parent).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
        parent.join("shadow.sock")
    }

    #[test]
    fn reads_the_effective_uid_of_a_connected_peer() {
        let (client, server) = UnixStream::pair().unwrap();
        let expected = rustix::process::geteuid().as_raw();
        assert_eq!(peer_effective_uid(&client).unwrap(), expected);
        assert_eq!(peer_effective_uid(&server).unwrap(), expected);
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
        assert!(join.join().unwrap().is_ok());
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
        assert!(join.join().unwrap().is_ok());
        runtime.shutdown().unwrap();
    }

    #[test]
    fn frame_boundary_eof_is_clean_but_body_eof_is_an_error() {
        use std::io::Write;

        let config = ShadowServerConfig::new("/tmp/unused-ftwdb-shadow.sock");

        let (client, mut server) = UnixStream::pair().unwrap();
        drop(client);
        let boundary = read_frame_before(&mut server, config.io_timeout);
        assert!(
            matches!(
                boundary,
                Err(shadow_protocol::ProtocolError::Truncated { actual: 0, .. })
            ),
            "unexpected boundary result: {boundary:?}"
        );

        let frame = shadow_protocol::encode(&WireMessage::Request(Request::Hello(HelloRequest {
            source_id: 17,
            node_id: "partial-client".to_owned(),
            client_version: "test".to_owned(),
            capabilities: 0,
        })))
        .unwrap();
        let (mut client, mut server) = UnixStream::pair().unwrap();
        client.write_all(&frame[..12]).unwrap();
        drop(client);
        let partial = read_frame_before(&mut server, config.io_timeout);
        assert!(
            matches!(
                partial,
                Err(shadow_protocol::ProtocolError::Truncated { actual: 12, .. })
            ),
            "unexpected partial-body result: {partial:?}"
        );
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
        assert!(join.join().unwrap().is_ok());
        runtime.shutdown().unwrap();
    }

    #[test]
    fn bind_refuses_to_replace_regular_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = private_socket_path(&directory);
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
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o770)).unwrap();
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
    fn bind_rejects_world_accessible_existing_parent() {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().join("open-run");
        fs::create_dir(&parent).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o755)).unwrap();
        let path = parent.join("shadow.sock");
        let error = match BoundSocket::bind(&path) {
            Ok(_) => panic!("bind unexpectedly used a world-traversable parent"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            ShadowServerError::UnsafeSocketParent(found) if found == parent
        ));
        assert!(!path.exists());
    }

    #[test]
    fn bind_refuses_to_create_under_a_world_writable_ancestor() {
        let directory = tempfile::tempdir().unwrap();
        let world = directory.path().join("world");
        fs::create_dir(&world).unwrap();
        fs::set_permissions(&world, fs::Permissions::from_mode(0o777)).unwrap();
        let path = world.join("run/shadow.sock");
        let error = match BoundSocket::bind(&path) {
            Ok(_) => panic!("bind unexpectedly created a socket under a world-writable directory"),
            Err(error) => error,
        };
        assert!(matches!(error, ShadowServerError::UnsafeSocketParent(_)));
        assert!(!path.exists());
        assert!(!world.join("run").exists());
    }

    #[test]
    fn hello_binds_source_id_and_rejects_a_different_source() {
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
            &WireMessage::Request(Request::Flush(FlushRequest {
                source_id: 18,
                through_sequence: 1,
            })),
        )
        .unwrap();
        match shadow_protocol::read_from(&mut client).unwrap() {
            WireMessage::Response(Response::Error(ErrorResponse {
                code: ErrorCode::InvalidRequest,
                retryable: false,
                message,
            })) => assert_eq!(message, "invalid request"),
            other => panic!("expected a stable invalid-request error, got {other:?}"),
        }
        drop(client);
        assert!(join.join().unwrap().is_ok());
        runtime.shutdown().unwrap();
    }

    #[test]
    fn bind_refuses_a_live_socket() {
        let directory = tempfile::tempdir().unwrap();
        let path = private_socket_path(&directory);
        let listener = UnixListener::bind(&path).unwrap();
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
        let path = private_socket_path(&directory);
        let listener = UnixListener::bind(&path).unwrap();
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
        let bound = BoundSocket::bind(&path).unwrap();
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
        config.io_timeout = Duration::from_secs(2);
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
        let connect_deadline = Instant::now() + Duration::from_secs(1);
        let mut malformed = loop {
            match UnixStream::connect(&socket_path) {
                Ok(client) => break client,
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
                    ) && Instant::now() < connect_deadline =>
                {
                    thread::sleep(Duration::from_millis(2));
                }
                Err(error) => panic!("listener did not accept its first client: {error}"),
            }
        };
        malformed.write_all(b"bad").unwrap();
        drop(malformed);

        let reconnect_deadline = Instant::now() + Duration::from_secs(1);
        let mut client = loop {
            match UnixStream::connect(&socket_path) {
                Ok(client) => break client,
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
                    ) && Instant::now() < reconnect_deadline =>
                {
                    thread::sleep(Duration::from_millis(2));
                }
                Err(error) => panic!("listener did not recover from malformed client: {error}"),
            }
        };
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
        assert_eq!(report.client_errors, 1);
        assert!(!socket_path.exists());
        runtime.shutdown().unwrap();
    }

    #[test]
    fn listener_rejects_a_peer_with_the_wrong_effective_uid() {
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
        let current_uid = rustix::process::geteuid().as_raw();
        config.allowed_peer_uid = if current_uid == u32::MAX {
            0
        } else {
            current_uid + 1
        };
        config.io_timeout = Duration::from_millis(100);
        config.accept_poll_interval = Duration::from_millis(2);
        let stop = ShadowStopToken::new();
        let server_stop = stop.clone();
        let submitter = runtime.submitter();
        let join = thread::spawn(move || serve(&config, submitter, &server_stop));

        for _ in 0..100 {
            if socket_path.exists() || join.is_finished() {
                break;
            }
            thread::sleep(Duration::from_millis(2));
        }
        if !socket_path.exists() {
            stop.stop();
            let result = join.join().unwrap();
            runtime.shutdown().unwrap();
            panic!("shadow socket did not appear; server result: {result:?}");
        }

        let connect_deadline = Instant::now() + Duration::from_secs(1);
        let mut client = loop {
            match UnixStream::connect(&socket_path) {
                Ok(client) => break client,
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
                    ) && Instant::now() < connect_deadline =>
                {
                    thread::sleep(Duration::from_millis(2));
                }
                Err(error) => panic!("listener did not accept unauthorized client: {error}"),
            }
        };
        client
            .set_read_timeout(Some(Duration::from_millis(250)))
            .unwrap();
        let mut byte = [0_u8; 1];
        match client.read(&mut byte) {
            Ok(0) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
                ) => {}
            result => panic!("unauthorized connection remained open: {result:?}"),
        }

        stop.stop();
        let report = join.join().unwrap().unwrap();
        assert_eq!(report.accepted_clients, 1);
        assert_eq!(report.peer_auth_failures, 1);
        assert_eq!(report.client_errors, 0);
        runtime.shutdown().unwrap();
    }
}
