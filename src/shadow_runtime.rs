//! Bounded, single-writer runtime for local shadow ingestion.
//!
//! Producers submit identified transactions to a fixed-size queue. One worker
//! owns the [`Store`] or [`Database`], so commit order is the queue order and
//! callers never share a mutable storage handle. The runtime does not provide
//! a network protocol; an adapter can map its explicit submit and write
//! outcomes to its own wire format.
//!
//! Each write carries an [`IngressIdentity`]. FTWDB owns per-source ordering,
//! replay checks, conflicts, and durable watermarks, including after restart.
//! The runtime never acknowledges a replay from process-local state.

use crate::{
    Commit, Database, Durability, Error, IngressIdentity, IngressWatermarks, Store, Transaction,
};
use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Settings for one shadow writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShadowRuntimeConfig {
    /// Maximum number of operations waiting behind the active operation.
    pub queue_capacity: usize,
    /// Maximum total point count in writes waiting behind the active write.
    /// Metadata records and flush operations consume no point budget.
    pub max_queued_points: usize,
    /// Optional periodic [`Store::maintain`] interval for store backends.
    /// Database backends ignore background maintenance.
    pub maintenance_interval: Option<Duration>,
    /// When set on a store backend, seal and reclaim once the active log
    /// exceeds this many bytes after a maintenance tick.
    pub seal_log_bytes_threshold: Option<u64>,
}

impl Default for ShadowRuntimeConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 64,
            max_queued_points: 1_048_576,
            maintenance_interval: None,
            seal_log_bytes_threshold: None,
        }
    }
}

/// One ordered, idempotent storage request.
#[derive(Clone, Debug)]
pub struct ShadowWrite {
    identity: IngressIdentity,
    transaction: Transaction,
}

impl ShadowWrite {
    /// Builds a write and applies its durable FTWDB ingress identity.
    #[must_use]
    pub fn identified(identity: IngressIdentity, mut transaction: Transaction) -> Self {
        transaction.with_ingress_identity(identity);
        Self {
            identity,
            transaction,
        }
    }

    /// Builds a write from a transaction that already has an ingress identity.
    pub fn from_identified(transaction: Transaction) -> Result<Self, UnidentifiedWrite> {
        let Some(identity) = transaction.ingress_identity() else {
            return Err(UnidentifiedWrite {
                transaction: Box::new(transaction),
            });
        };
        Ok(Self {
            identity,
            transaction,
        })
    }

    #[must_use]
    pub const fn commit_id(&self) -> u128 {
        self.identity.commit_id
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.identity.sequence
    }

    #[must_use]
    pub const fn source_id(&self) -> u128 {
        self.identity.source_id
    }

    #[must_use]
    pub const fn identity(&self) -> IngressIdentity {
        self.identity
    }
}

/// A transaction without the commit ID required by the shadow runtime.
#[derive(Debug)]
pub struct UnidentifiedWrite {
    pub transaction: Box<Transaction>,
}

impl UnidentifiedWrite {
    #[must_use]
    pub fn into_transaction(self) -> Transaction {
        *self.transaction
    }
}

impl fmt::Display for UnidentifiedWrite {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("shadow writes require an ingress identity")
    }
}

impl std::error::Error for UnidentifiedWrite {}

/// The result of an acknowledged sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShadowAck {
    pub identity: IngressIdentity,
    pub accepted_through: Option<u64>,
    pub durable_through: Option<u64>,
    pub commit: Commit,
}

/// A write that reached the worker but could not be acknowledged.
#[derive(Debug)]
pub enum ShadowWriteFailure {
    /// FTWDB rejected this request before it could change durable state. The
    /// same sequence may be corrected and retried.
    Rejected(Error),
    /// FTWDB returned an error that may have happened after a durable raw-log
    /// append. The runtime stops making storage calls.
    Writer(Error),
    /// An earlier write failed, so this write never reached FTWDB.
    Poisoned {
        cause: String,
    },
    /// The storage implementation panicked. The panic stays in the worker and
    /// the runtime rejects later writes.
    WriterPanicked {
        cause: String,
    },
    WorkerStopped,
}

impl fmt::Display for ShadowWriteFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(error) => write!(formatter, "shadow write was rejected: {error}"),
            Self::Writer(error) => write!(formatter, "shadow writer failed: {error}"),
            Self::Poisoned { cause } => {
                write!(formatter, "shadow writer is poisoned: {cause}")
            }
            Self::WriterPanicked { cause } => {
                write!(formatter, "shadow writer panicked: {cause}")
            }
            Self::WorkerStopped => formatter.write_str("shadow writer stopped before replying"),
        }
    }
}

impl std::error::Error for ShadowWriteFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Rejected(error) | Self::Writer(error) => Some(error),
            _ => None,
        }
    }
}

/// A receipt for one accepted queue entry.
#[derive(Debug)]
pub struct ShadowReceipt {
    pub identity: IngressIdentity,
    receiver: Receiver<Result<ShadowAck, ShadowWriteFailure>>,
}

impl ShadowReceipt {
    pub fn wait(self) -> Result<ShadowAck, ShadowWriteFailure> {
        self.receiver
            .recv()
            .unwrap_or(Err(ShadowWriteFailure::WorkerStopped))
    }

    pub fn wait_timeout(
        self,
        timeout: Duration,
    ) -> Result<Result<ShadowAck, ShadowWriteFailure>, AckWaitError> {
        match self.receiver.recv_timeout(timeout) {
            Ok(result) => Ok(result),
            Err(mpsc::RecvTimeoutError::Timeout) => Err(AckWaitError::Timeout),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(AckWaitError::WorkerStopped),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AckWaitError {
    Timeout,
    WorkerStopped,
}

impl fmt::Display for AckWaitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Timeout => "timed out waiting for the shadow acknowledgement",
            Self::WorkerStopped => "shadow writer stopped before replying",
        })
    }
}

impl std::error::Error for AckWaitError {}

/// The result of an ordered FTWDB flush.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShadowFlushAck {
    pub source_id: u128,
    pub through_sequence: u64,
    pub accepted_through: Option<u64>,
    pub durable_through: Option<u64>,
}

#[derive(Debug)]
pub struct ShadowFlushReceipt {
    receiver: Receiver<Result<ShadowFlushAck, ShadowFlushFailure>>,
}

impl ShadowFlushReceipt {
    pub fn wait(self) -> Result<ShadowFlushAck, ShadowFlushFailure> {
        self.receiver
            .recv()
            .unwrap_or(Err(ShadowFlushFailure::WorkerStopped))
    }

    pub fn wait_timeout(
        self,
        timeout: Duration,
    ) -> Result<Result<ShadowFlushAck, ShadowFlushFailure>, AckWaitError> {
        match self.receiver.recv_timeout(timeout) {
            Ok(result) => Ok(result),
            Err(mpsc::RecvTimeoutError::Timeout) => Err(AckWaitError::Timeout),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(AckWaitError::WorkerStopped),
        }
    }
}

#[derive(Debug)]
pub enum ShadowFlushFailure {
    NotAccepted {
        source_id: u128,
        through_sequence: u64,
        accepted_through: Option<u64>,
    },
    Rejected(Error),
    Writer(Error),
    Poisoned {
        cause: String,
    },
    WriterPanicked {
        cause: String,
    },
    WorkerStopped,
}

impl fmt::Display for ShadowFlushFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAccepted {
                source_id,
                through_sequence,
                accepted_through,
            } => write!(
                formatter,
                "shadow source {source_id:032x} has accepted through {accepted_through:?}, not {through_sequence}"
            ),
            Self::Rejected(error) => write!(formatter, "shadow flush was rejected: {error}"),
            Self::Writer(error) => write!(formatter, "shadow flush failed: {error}"),
            Self::Poisoned { cause } => {
                write!(formatter, "shadow writer is poisoned: {cause}")
            }
            Self::WriterPanicked { cause } => {
                write!(formatter, "shadow writer panicked during flush: {cause}")
            }
            Self::WorkerStopped => formatter.write_str("shadow writer stopped before flushing"),
        }
    }
}

impl std::error::Error for ShadowFlushFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Rejected(error) | Self::Writer(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FlushSubmitError {
    Overloaded,
    DeadlineExceeded,
    Closed,
    Poisoned { cause: String },
}

impl fmt::Display for FlushSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overloaded => formatter.write_str("shadow writer queue is full"),
            Self::DeadlineExceeded => formatter.write_str("shadow writer queue deadline expired"),
            Self::Closed => formatter.write_str("shadow writer is closed"),
            Self::Poisoned { cause } => {
                write!(formatter, "shadow writer is poisoned: {cause}")
            }
        }
    }
}

impl std::error::Error for FlushSubmitError {}

/// Why a write did not enter the bounded queue.
#[derive(Debug)]
pub enum SubmitError {
    Overloaded(Box<ShadowWrite>),
    PointBudgetExhausted(Box<ShadowWrite>),
    DeadlineExceeded(Box<ShadowWrite>),
    Closed(Box<ShadowWrite>),
    Poisoned {
        write: Box<ShadowWrite>,
        cause: String,
    },
}

impl SubmitError {
    #[must_use]
    pub fn into_write(self) -> ShadowWrite {
        match self {
            Self::Overloaded(write)
            | Self::PointBudgetExhausted(write)
            | Self::DeadlineExceeded(write)
            | Self::Closed(write)
            | Self::Poisoned { write, .. } => *write,
        }
    }
}

impl fmt::Display for SubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overloaded(_) => formatter.write_str("shadow writer queue is full"),
            Self::PointBudgetExhausted(_) => {
                formatter.write_str("shadow writer queued-point limit is full")
            }
            Self::DeadlineExceeded(_) => {
                formatter.write_str("shadow writer queue deadline expired")
            }
            Self::Closed(_) => formatter.write_str("shadow writer is closed"),
            Self::Poisoned { cause, .. } => {
                write!(formatter, "shadow writer is poisoned: {cause}")
            }
        }
    }
}

impl std::error::Error for SubmitError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShadowRuntimeState {
    Running,
    Poisoned,
    Closing,
    Closed,
}

/// A point-in-time view of writer health.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShadowHealth {
    pub state: ShadowRuntimeState,
    pub queue_capacity: usize,
    pub max_queued_points: usize,
    /// Operations waiting in the queue. The active storage call is excluded.
    pub queued: usize,
    /// Points in waiting writes. Points in the active write are excluded.
    pub queued_points: usize,
    pub accepted: u64,
    pub acknowledged: u64,
    pub failed: u64,
    /// Latest authoritative storage watermarks for sources observed by this
    /// runtime. Sources not yet seen by this process are absent.
    pub source_watermarks: BTreeMap<u128, IngressWatermarks>,
    /// Latest fatal writer or close error. Rejected client input increments
    /// `failed` but does not mark a healthy writer as degraded.
    pub last_error: Option<String>,
    pub database_bytes: u64,
    pub database_points: u64,
    pub database_commits: u64,
    pub recovered_tail_bytes: u64,
    pub durability: Durability,
    pub last_ack_durable: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct StoreOpsSnapshot {
    bytes: u64,
    points: u64,
    commits: u64,
    recovered_tail_bytes: u64,
    durability: Durability,
    last_ack_durable: bool,
}

struct HealthState {
    state: ShadowRuntimeState,
    accepted: u64,
    acknowledged: u64,
    failed: u64,
    source_watermarks: BTreeMap<u128, IngressWatermarks>,
    last_error: Option<String>,
    store: StoreOpsSnapshot,
}

struct Shared {
    health: Mutex<HealthState>,
    send_gate: Mutex<()>,
    queue_usage: Mutex<QueueUsage>,
    queue_capacity: usize,
    max_queued_points: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct QueueUsage {
    queued: usize,
    points: usize,
}

impl QueueUsage {
    fn reserve(
        &mut self,
        queue_capacity: usize,
        max_queued_points: usize,
        points: usize,
    ) -> Result<(), QueueReservationError> {
        let next_queued = self
            .queued
            .checked_add(1)
            .ok_or(QueueReservationError::Entries)?;
        if next_queued > queue_capacity {
            return Err(QueueReservationError::Entries);
        }
        let next_points = self
            .points
            .checked_add(points)
            .ok_or(QueueReservationError::Points)?;
        if next_points > max_queued_points {
            return Err(QueueReservationError::Points);
        }
        self.queued = next_queued;
        self.points = next_points;
        Ok(())
    }

    fn release(&mut self, points: usize) {
        debug_assert!(self.queued > 0);
        debug_assert!(self.points >= points);
        self.queued = self.queued.saturating_sub(1);
        self.points = self.points.saturating_sub(points);
    }
}

/// Cloneable producer side of a shadow runtime.
#[derive(Clone)]
pub struct ShadowSubmitter {
    sender: SyncSender<Command>,
    shared: Arc<Shared>,
}

impl ShadowSubmitter {
    /// Tries once without waiting for queue space.
    pub fn try_submit(&self, write: ShadowWrite) -> Result<ShadowReceipt, SubmitError> {
        self.try_submit_inner(write)
    }

    /// Retries until the write enters the queue or the deadline passes.
    pub fn submit_until(
        &self,
        mut write: ShadowWrite,
        deadline: Instant,
    ) -> Result<ShadowReceipt, SubmitError> {
        loop {
            match self.try_submit_inner(write) {
                Ok(receipt) => return Ok(receipt),
                Err(
                    SubmitError::Overloaded(returned) | SubmitError::PointBudgetExhausted(returned),
                ) => {
                    write = *returned;
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(SubmitError::DeadlineExceeded(Box::new(write)));
                    }
                    thread::sleep(
                        deadline
                            .saturating_duration_since(now)
                            .min(Duration::from_millis(1)),
                    );
                }
                Err(other) => return Err(other),
            }
        }
    }

    /// Places a flush after all operations already accepted into the queue.
    pub fn try_flush(
        &self,
        source_id: u128,
        through_sequence: u64,
    ) -> Result<ShadowFlushReceipt, FlushSubmitError> {
        let _gate = match self.shared.send_gate.try_lock() {
            Ok(gate) => gate,
            Err(TryLockError::WouldBlock) => return Err(FlushSubmitError::Overloaded),
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
        };
        let (state, last_error) = {
            let health = lock(&self.shared.health);
            (health.state, health.last_error.clone())
        };
        match state {
            ShadowRuntimeState::Running => {}
            ShadowRuntimeState::Poisoned => {
                let cause = last_error.unwrap_or_else(|| "unknown writer failure".to_owned());
                return Err(FlushSubmitError::Poisoned { cause });
            }
            ShadowRuntimeState::Closing | ShadowRuntimeState::Closed => {
                return Err(FlushSubmitError::Closed);
            }
        }
        if reserve_queue_slot(&self.shared, 0).is_err() {
            return Err(FlushSubmitError::Overloaded);
        }
        let (reply, receiver) = mpsc::sync_channel(1);
        match self.sender.try_send(Command::Flush {
            source_id,
            through_sequence,
            reply,
        }) {
            Ok(()) => Ok(ShadowFlushReceipt { receiver }),
            Err(TrySendError::Full(Command::Flush { .. })) => {
                release_queue_slot(&self.shared, 0);
                Err(FlushSubmitError::Overloaded)
            }
            Err(TrySendError::Disconnected(Command::Flush { .. })) => {
                release_queue_slot(&self.shared, 0);
                Err(FlushSubmitError::Closed)
            }
            Err(
                TrySendError::Full(Command::Write { .. } | Command::Shutdown { .. })
                | TrySendError::Disconnected(Command::Write { .. } | Command::Shutdown { .. }),
            ) => unreachable!(),
        }
    }

    /// Retries an ordered flush until it enters the queue or its deadline passes.
    pub fn flush_until(
        &self,
        source_id: u128,
        through_sequence: u64,
        deadline: Instant,
    ) -> Result<ShadowFlushReceipt, FlushSubmitError> {
        loop {
            match self.try_flush(source_id, through_sequence) {
                Ok(receipt) => return Ok(receipt),
                Err(FlushSubmitError::Overloaded) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(FlushSubmitError::DeadlineExceeded);
                    }
                    thread::sleep(
                        deadline
                            .saturating_duration_since(now)
                            .min(Duration::from_millis(1)),
                    );
                }
                Err(other) => return Err(other),
            }
        }
    }

    #[must_use]
    pub fn health(&self) -> ShadowHealth {
        health_snapshot(&self.shared)
    }

    fn try_submit_inner(&self, write: ShadowWrite) -> Result<ShadowReceipt, SubmitError> {
        let _gate = match self.shared.send_gate.try_lock() {
            Ok(gate) => gate,
            Err(TryLockError::WouldBlock) => {
                return Err(SubmitError::Overloaded(Box::new(write)));
            }
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
        };

        let (state, last_error) = {
            let health = lock(&self.shared.health);
            (health.state, health.last_error.clone())
        };
        match state {
            ShadowRuntimeState::Running => {}
            ShadowRuntimeState::Poisoned => {
                let cause = last_error.unwrap_or_else(|| "unknown writer failure".to_owned());
                return Err(SubmitError::Poisoned {
                    write: Box::new(write),
                    cause,
                });
            }
            ShadowRuntimeState::Closing | ShadowRuntimeState::Closed => {
                return Err(SubmitError::Closed(Box::new(write)));
            }
        }

        let queued_points = write.transaction.point_count();
        match reserve_queue_slot(&self.shared, queued_points) {
            Ok(()) => {}
            Err(QueueReservationError::Entries) => {
                return Err(SubmitError::Overloaded(Box::new(write)));
            }
            Err(QueueReservationError::Points) => {
                return Err(SubmitError::PointBudgetExhausted(Box::new(write)));
            }
        }
        let identity = write.identity;
        let (reply, receiver) = mpsc::sync_channel(1);
        lock(&self.shared.health).accepted += 1;
        match self.sender.try_send(Command::Write { write, reply }) {
            Ok(()) => Ok(ShadowReceipt { identity, receiver }),
            Err(TrySendError::Full(Command::Write { write, .. })) => {
                release_queue_slot(&self.shared, queued_points);
                lock(&self.shared.health).accepted -= 1;
                Err(SubmitError::Overloaded(Box::new(write)))
            }
            Err(TrySendError::Disconnected(Command::Write { write, .. })) => {
                release_queue_slot(&self.shared, queued_points);
                lock(&self.shared.health).accepted -= 1;
                Err(SubmitError::Closed(Box::new(write)))
            }
            Err(
                TrySendError::Full(Command::Flush { .. } | Command::Shutdown { .. })
                | TrySendError::Disconnected(Command::Flush { .. } | Command::Shutdown { .. }),
            ) => unreachable!(),
        }
    }
}

/// Owns the worker lifecycle. Producers should hold [`ShadowSubmitter`] clones.
pub struct ShadowRuntime {
    submitter: ShadowSubmitter,
    join: Option<JoinHandle<()>>,
}

impl ShadowRuntime {
    pub fn start_store(
        store: Store,
        config: ShadowRuntimeConfig,
    ) -> Result<Self, ShadowStartError> {
        if store.is_read_only() {
            return Err(ShadowStartError::ReadOnlyBackend);
        }
        Self::start_backend(Box::new(store), config)
    }

    pub fn start_database(
        database: Database,
        config: ShadowRuntimeConfig,
    ) -> Result<Self, ShadowStartError> {
        if database.is_read_only() {
            return Err(ShadowStartError::ReadOnlyBackend);
        }
        Self::start_backend(Box::new(database), config)
    }

    fn start_backend(
        backend: Box<dyn WriterBackend>,
        config: ShadowRuntimeConfig,
    ) -> Result<Self, ShadowStartError> {
        if config.queue_capacity == 0 {
            return Err(ShadowStartError::ZeroQueueCapacity);
        }

        let source_watermarks = backend.all_ingress_watermarks();
        let store = backend.store_snapshot().unwrap_or_default();
        let (sender, receiver) = mpsc::sync_channel(config.queue_capacity);
        let shared = Arc::new(Shared {
            health: Mutex::new(HealthState {
                state: ShadowRuntimeState::Running,
                accepted: 0,
                acknowledged: 0,
                failed: 0,
                source_watermarks,
                last_error: None,
                store,
            }),
            send_gate: Mutex::new(()),
            queue_usage: Mutex::new(QueueUsage::default()),
            queue_capacity: config.queue_capacity,
            max_queued_points: config.max_queued_points,
        });
        let worker_shared = Arc::clone(&shared);
        let join = thread::Builder::new()
            .name("ftwdb-shadow-writer".to_owned())
            .spawn(move || worker_loop(backend, receiver, worker_shared, config))
            .map_err(ShadowStartError::Spawn)?;
        Ok(Self {
            submitter: ShadowSubmitter { sender, shared },
            join: Some(join),
        })
    }

    #[must_use]
    pub fn submitter(&self) -> ShadowSubmitter {
        self.submitter.clone()
    }

    #[must_use]
    pub fn health(&self) -> ShadowHealth {
        self.submitter.health()
    }

    pub fn shutdown(mut self) -> Result<ShutdownReport, ShutdownError> {
        self.shutdown_inner()
    }

    fn shutdown_inner(&mut self) -> Result<ShutdownReport, ShutdownError> {
        let Some(join) = self.join.take() else {
            return Ok(ShutdownReport {
                health: self.health(),
            });
        };

        let (reply, receiver) = mpsc::sync_channel(1);
        let send_result = {
            let _gate = lock(&self.submitter.shared.send_gate);
            let mut health = lock(&self.submitter.shared.health);
            if health.state == ShadowRuntimeState::Running {
                health.state = ShadowRuntimeState::Closing;
            }
            drop(health);
            self.submitter.sender.send(Command::Shutdown { reply })
        };

        if send_result.is_err() {
            let _ = join.join();
            let report = ShutdownReport {
                health: self.health(),
            };
            return Err(ShutdownError::WorkerStopped(Box::new(report)));
        }

        let worker_result = receiver.recv();
        let join_result = join.join();
        if let Err(panic) = join_result {
            let report = ShutdownReport {
                health: self.health(),
            };
            return Err(ShutdownError::WorkerPanicked {
                cause: panic_message(panic),
                report: Box::new(report),
            });
        }
        worker_result.unwrap_or_else(|_| {
            Err(ShutdownError::WorkerStopped(Box::new(ShutdownReport {
                health: self.health(),
            })))
        })
    }
}

impl Drop for ShadowRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown_inner();
    }
}

#[derive(Debug)]
pub enum ShadowStartError {
    ZeroQueueCapacity,
    ReadOnlyBackend,
    Spawn(std::io::Error),
}

impl fmt::Display for ShadowStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroQueueCapacity => {
                formatter.write_str("shadow queue capacity must be positive")
            }
            Self::ReadOnlyBackend => formatter.write_str("shadow writer requires writable storage"),
            Self::Spawn(error) => write!(formatter, "could not start shadow writer: {error}"),
        }
    }
}

impl std::error::Error for ShadowStartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Spawn(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShutdownReport {
    pub health: ShadowHealth,
}

#[derive(Debug)]
pub enum ShutdownError {
    WriterPoisoned {
        cause: String,
        report: Box<ShutdownReport>,
    },
    Close {
        error: Error,
        report: Box<ShutdownReport>,
    },
    WriterPanicked {
        cause: String,
        report: Box<ShutdownReport>,
    },
    WorkerPanicked {
        cause: String,
        report: Box<ShutdownReport>,
    },
    WorkerStopped(Box<ShutdownReport>),
}

impl ShutdownError {
    #[must_use]
    pub fn report(&self) -> &ShutdownReport {
        match self {
            Self::WriterPoisoned { report, .. }
            | Self::Close { report, .. }
            | Self::WriterPanicked { report, .. }
            | Self::WorkerPanicked { report, .. }
            | Self::WorkerStopped(report) => report,
        }
    }
}

impl fmt::Display for ShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WriterPoisoned { cause, .. } => {
                write!(
                    formatter,
                    "shadow writer closed after a write error: {cause}"
                )
            }
            Self::Close { error, .. } => write!(formatter, "shadow close failed: {error}"),
            Self::WriterPanicked { cause, .. } => {
                write!(formatter, "shadow writer closed after a panic: {cause}")
            }
            Self::WorkerPanicked { cause, .. } => {
                write!(formatter, "shadow worker panicked: {cause}")
            }
            Self::WorkerStopped(_) => formatter.write_str("shadow worker stopped during shutdown"),
        }
    }
}

impl std::error::Error for ShutdownError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Close { error, .. } => Some(error),
            _ => None,
        }
    }
}

enum Command {
    Write {
        write: ShadowWrite,
        reply: SyncSender<Result<ShadowAck, ShadowWriteFailure>>,
    },
    Flush {
        source_id: u128,
        through_sequence: u64,
        reply: SyncSender<Result<ShadowFlushAck, ShadowFlushFailure>>,
    },
    Shutdown {
        reply: SyncSender<Result<ShutdownReport, ShutdownError>>,
    },
}

trait WriterBackend: Send + 'static {
    fn commit_ingress(
        &mut self,
        identity: IngressIdentity,
        transaction: Transaction,
    ) -> crate::Result<Commit>;
    fn ingress_watermarks(&self, source_id: u128) -> IngressWatermarks;
    fn all_ingress_watermarks(&self) -> BTreeMap<u128, IngressWatermarks>;
    fn flush(&mut self) -> crate::Result<()>;
    fn close(self: Box<Self>) -> crate::Result<BTreeMap<u128, IngressWatermarks>>;
    fn background_maintenance(
        &mut self,
        now_micros: i64,
        config: &ShadowRuntimeConfig,
    ) -> crate::Result<()> {
        let _ = (now_micros, config);
        Ok(())
    }
    fn store_snapshot(&self) -> crate::Result<StoreOpsSnapshot> {
        Ok(StoreOpsSnapshot::default())
    }
}

impl WriterBackend for Store {
    fn commit_ingress(
        &mut self,
        identity: IngressIdentity,
        transaction: Transaction,
    ) -> crate::Result<Commit> {
        Store::commit_ingress(self, identity, transaction)
    }

    fn ingress_watermarks(&self, source_id: u128) -> IngressWatermarks {
        Store::ingress_watermarks(self, source_id)
    }

    fn all_ingress_watermarks(&self) -> BTreeMap<u128, IngressWatermarks> {
        Store::all_ingress_watermarks(self)
    }

    fn flush(&mut self) -> crate::Result<()> {
        Store::flush(self)
    }

    fn close(mut self: Box<Self>) -> crate::Result<BTreeMap<u128, IngressWatermarks>> {
        Store::flush(&mut self)?;
        Ok(Store::all_ingress_watermarks(&self))
    }

    fn background_maintenance(
        &mut self,
        now_micros: i64,
        config: &ShadowRuntimeConfig,
    ) -> crate::Result<()> {
        Store::maintain(self, now_micros)?;
        if let Some(threshold) = config.seal_log_bytes_threshold
            && self.database().stats()?.file_bytes > threshold
        {
            Store::seal_and_reclaim(self)?;
        }
        Ok(())
    }

    fn store_snapshot(&self) -> crate::Result<StoreOpsSnapshot> {
        let stats = self.database().stats()?;
        Ok(StoreOpsSnapshot {
            bytes: self.stored_bytes()?,
            points: stats.points,
            commits: stats.commits,
            recovered_tail_bytes: stats.recovered_tail_bytes,
            durability: self.database().durability(),
            last_ack_durable: false,
        })
    }
}

impl WriterBackend for Database {
    fn commit_ingress(
        &mut self,
        identity: IngressIdentity,
        transaction: Transaction,
    ) -> crate::Result<Commit> {
        Database::commit_ingress(self, identity, transaction)
    }

    fn ingress_watermarks(&self, source_id: u128) -> IngressWatermarks {
        Database::ingress_watermarks(self, source_id)
    }

    fn all_ingress_watermarks(&self) -> BTreeMap<u128, IngressWatermarks> {
        Database::all_ingress_watermarks(self)
    }

    fn flush(&mut self) -> crate::Result<()> {
        Database::flush(self)
    }

    fn close(mut self: Box<Self>) -> crate::Result<BTreeMap<u128, IngressWatermarks>> {
        Database::flush(&mut self)?;
        Ok(Database::all_ingress_watermarks(&self))
    }

    fn store_snapshot(&self) -> crate::Result<StoreOpsSnapshot> {
        let stats = self.stats()?;
        Ok(StoreOpsSnapshot {
            bytes: stats.file_bytes,
            points: stats.points,
            commits: stats.commits,
            recovered_tail_bytes: stats.recovered_tail_bytes,
            durability: self.durability(),
            last_ack_durable: false,
        })
    }
}

fn worker_loop(
    mut backend: Box<dyn WriterBackend>,
    receiver: Receiver<Command>,
    shared: Arc<Shared>,
    config: ShadowRuntimeConfig,
) {
    let mut poison: Option<WorkerPoison> = None;
    let mut last_maintenance = None::<Instant>;
    while let Ok(command) = receiver.recv() {
        match command {
            Command::Write { write, reply } => {
                let points = write.transaction.point_count();
                release_queue_slot(&shared, points);
                let result = process_write(&mut backend, write, &shared, &mut poison);
                if result.is_ok() {
                    maybe_run_background_maintenance(
                        &mut backend,
                        &config,
                        &shared,
                        &mut last_maintenance,
                        &mut poison,
                    );
                }
                let _ = reply.send(result);
            }
            Command::Flush {
                source_id,
                through_sequence,
                reply,
            } => {
                release_queue_slot(&shared, 0);
                let result = process_flush(
                    &mut backend,
                    source_id,
                    through_sequence,
                    &shared,
                    &mut poison,
                );
                let _ = reply.send(result);
            }
            Command::Shutdown { reply } => {
                let result = close_worker(backend, &shared, poison);
                let _ = reply.send(result);
                return;
            }
        }
    }
    lock(&shared.health).state = ShadowRuntimeState::Closed;
}

enum WorkerPoison {
    Error(String),
    Panic(String),
}

fn writer_error_requires_poison(error: &Error) -> bool {
    matches!(
        error,
        Error::Io(_)
            | Error::InvalidHeader
            | Error::UnsupportedVersion(_)
            | Error::Corruption { .. }
            | Error::Poisoned
            | Error::SnapshotPublication { .. }
    )
}

fn observe_source(
    backend: &dyn WriterBackend,
    shared: &Shared,
    source_id: u128,
) -> IngressWatermarks {
    let watermarks = backend.ingress_watermarks(source_id);
    lock(&shared.health)
        .source_watermarks
        .insert(source_id, watermarks);
    watermarks
}

fn refresh_store_snapshot(backend: &dyn WriterBackend, shared: &Shared, last_ack_durable: bool) {
    if let Ok(mut snapshot) = backend.store_snapshot() {
        snapshot.last_ack_durable = last_ack_durable;
        lock(&shared.health).store = snapshot;
    } else {
        lock(&shared.health).store.last_ack_durable = last_ack_durable;
    }
}

fn maybe_run_background_maintenance(
    backend: &mut Box<dyn WriterBackend>,
    config: &ShadowRuntimeConfig,
    shared: &Shared,
    last_maintenance: &mut Option<Instant>,
    poison: &mut Option<WorkerPoison>,
) {
    let Some(interval) = config.maintenance_interval else {
        return;
    };
    let now = Instant::now();
    if last_maintenance.is_some_and(|previous| now.duration_since(previous) < interval) {
        return;
    }
    let now_micros = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros(),
    )
    .unwrap_or(i64::MAX);
    match backend.background_maintenance(now_micros, config) {
        Ok(()) => *last_maintenance = Some(now),
        Err(error) if writer_error_requires_poison(&error) => {
            let cause = error.to_string();
            *poison = Some(WorkerPoison::Error(cause.clone()));
            let mut health = lock(&shared.health);
            health.state = ShadowRuntimeState::Poisoned;
            health.last_error = Some(cause);
        }
        Err(_) => *last_maintenance = Some(now),
    }
}

fn process_write(
    backend: &mut Box<dyn WriterBackend>,
    write: ShadowWrite,
    shared: &Shared,
    poison: &mut Option<WorkerPoison>,
) -> Result<ShadowAck, ShadowWriteFailure> {
    if let Some(poison) = poison {
        lock(&shared.health).failed += 1;
        return Err(match poison {
            WorkerPoison::Error(cause) => ShadowWriteFailure::Poisoned {
                cause: cause.clone(),
            },
            WorkerPoison::Panic(cause) => ShadowWriteFailure::WriterPanicked {
                cause: cause.clone(),
            },
        });
    }

    let identity = write.identity;
    let commit = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        backend.commit_ingress(identity, write.transaction)
    }));
    match commit {
        Ok(Ok(commit)) => {
            let watermarks = observe_source(&**backend, shared, identity.source_id);
            refresh_store_snapshot(&**backend, shared, commit.durable);
            let mut health = lock(&shared.health);
            health.acknowledged += 1;
            Ok(ShadowAck {
                identity,
                accepted_through: watermarks.accepted_through,
                durable_through: watermarks.durable_through,
                commit,
            })
        }
        Ok(Err(error)) => {
            observe_source(&**backend, shared, identity.source_id);
            if !writer_error_requires_poison(&error) {
                let mut health = lock(&shared.health);
                health.failed += 1;
                return Err(ShadowWriteFailure::Rejected(error));
            }
            let cause = error.to_string();
            *poison = Some(WorkerPoison::Error(cause.clone()));
            let mut health = lock(&shared.health);
            health.state = ShadowRuntimeState::Poisoned;
            health.failed += 1;
            health.last_error = Some(cause);
            Err(ShadowWriteFailure::Writer(error))
        }
        Err(panic) => {
            let cause = panic_message(panic);
            *poison = Some(WorkerPoison::Panic(cause.clone()));
            let mut health = lock(&shared.health);
            health.state = ShadowRuntimeState::Poisoned;
            health.failed += 1;
            health.last_error = Some(cause.clone());
            Err(ShadowWriteFailure::WriterPanicked { cause })
        }
    }
}

fn process_flush(
    backend: &mut Box<dyn WriterBackend>,
    source_id: u128,
    through_sequence: u64,
    shared: &Shared,
    poison: &mut Option<WorkerPoison>,
) -> Result<ShadowFlushAck, ShadowFlushFailure> {
    if let Some(poison) = poison {
        lock(&shared.health).failed += 1;
        return Err(match poison {
            WorkerPoison::Error(cause) => ShadowFlushFailure::Poisoned {
                cause: cause.clone(),
            },
            WorkerPoison::Panic(cause) => ShadowFlushFailure::WriterPanicked {
                cause: cause.clone(),
            },
        });
    }

    let before = observe_source(&**backend, shared, source_id);
    if before
        .accepted_through
        .is_none_or(|accepted| accepted < through_sequence)
    {
        lock(&shared.health).failed += 1;
        return Err(ShadowFlushFailure::NotAccepted {
            source_id,
            through_sequence,
            accepted_through: before.accepted_through,
        });
    }

    let flush = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| backend.flush()));
    match flush {
        Ok(Ok(())) => {
            let source_ids: Vec<_> = lock(&shared.health)
                .source_watermarks
                .keys()
                .copied()
                .collect();
            for observed_source_id in source_ids {
                observe_source(&**backend, shared, observed_source_id);
            }
            let watermarks = backend.ingress_watermarks(source_id);
            refresh_store_snapshot(&**backend, shared, true);
            Ok(ShadowFlushAck {
                source_id,
                through_sequence,
                accepted_through: watermarks.accepted_through,
                durable_through: watermarks.durable_through,
            })
        }
        Ok(Err(error)) => {
            if !writer_error_requires_poison(&error) {
                let mut health = lock(&shared.health);
                health.failed += 1;
                return Err(ShadowFlushFailure::Rejected(error));
            }
            let cause = error.to_string();
            *poison = Some(WorkerPoison::Error(cause.clone()));
            let mut health = lock(&shared.health);
            health.state = ShadowRuntimeState::Poisoned;
            health.failed += 1;
            health.last_error = Some(cause);
            Err(ShadowFlushFailure::Writer(error))
        }
        Err(panic) => {
            let cause = panic_message(panic);
            *poison = Some(WorkerPoison::Panic(cause.clone()));
            let mut health = lock(&shared.health);
            health.state = ShadowRuntimeState::Poisoned;
            health.failed += 1;
            health.last_error = Some(cause.clone());
            Err(ShadowFlushFailure::WriterPanicked { cause })
        }
    }
}

fn close_worker(
    backend: Box<dyn WriterBackend>,
    shared: &Shared,
    poison: Option<WorkerPoison>,
) -> Result<ShutdownReport, ShutdownError> {
    if let Some(poison) = poison {
        lock(&shared.health).state = ShadowRuntimeState::Closed;
        let report = ShutdownReport {
            health: health_snapshot(shared),
        };
        return Err(match poison {
            WorkerPoison::Error(cause) => ShutdownError::WriterPoisoned {
                cause,
                report: Box::new(report),
            },
            WorkerPoison::Panic(cause) => ShutdownError::WriterPanicked {
                cause,
                report: Box::new(report),
            },
        });
    }

    let close = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| backend.close()));
    match close {
        Ok(Ok(source_watermarks)) => {
            let mut health = lock(&shared.health);
            health.source_watermarks = source_watermarks;
            health.state = ShadowRuntimeState::Closed;
            drop(health);
            Ok(ShutdownReport {
                health: health_snapshot(shared),
            })
        }
        Ok(Err(error)) => {
            let cause = error.to_string();
            let mut health = lock(&shared.health);
            health.state = ShadowRuntimeState::Closed;
            health.failed += 1;
            health.last_error = Some(cause);
            drop(health);
            let report = ShutdownReport {
                health: health_snapshot(shared),
            };
            Err(ShutdownError::Close {
                error,
                report: Box::new(report),
            })
        }
        Err(panic) => {
            let cause = panic_message(panic);
            let mut health = lock(&shared.health);
            health.state = ShadowRuntimeState::Closed;
            health.failed += 1;
            health.last_error = Some(cause.clone());
            drop(health);
            let report = ShutdownReport {
                health: health_snapshot(shared),
            };
            Err(ShutdownError::WriterPanicked {
                cause,
                report: Box::new(report),
            })
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueueReservationError {
    Entries,
    Points,
}

fn reserve_queue_slot(shared: &Shared, points: usize) -> Result<(), QueueReservationError> {
    let mut usage = lock(&shared.queue_usage);
    usage.reserve(shared.queue_capacity, shared.max_queued_points, points)
}

fn release_queue_slot(shared: &Shared, points: usize) {
    lock(&shared.queue_usage).release(points);
}

fn health_snapshot(shared: &Shared) -> ShadowHealth {
    let health = lock(&shared.health);
    let usage = *lock(&shared.queue_usage);
    ShadowHealth {
        state: health.state,
        queue_capacity: shared.queue_capacity,
        max_queued_points: shared.max_queued_points,
        queued: usage.queued,
        queued_points: usage.points,
        accepted: health.accepted,
        acknowledged: health.acknowledged,
        failed: health.failed,
        source_watermarks: health.source_watermarks.clone(),
        last_error: health.last_error.clone(),
        database_bytes: health.store.bytes,
        database_points: health.store.points,
        database_commits: health.store.commits,
        recovered_tail_bytes: health.store.recovered_tail_bytes,
        durability: health.store.durability,
        last_ack_durable: health.store.last_ack_durable,
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

fn panic_message(panic: Box<dyn Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FlushSubmitError, QueueReservationError, QueueUsage, ShadowFlushFailure, ShadowRuntime,
        ShadowRuntimeConfig, ShadowRuntimeState, ShadowWrite, ShadowWriteFailure, SubmitError,
        WriterBackend,
    };
    use crate::{
        Commit, Config, Database, Durability, Entity, EntityId, IngressIdentity, IngressWatermarks,
        Point, Properties, RollupPolicy, SeriesDefinition, SeriesSemantics, Store, Transaction,
    };
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::{Duration, Instant};

    #[derive(Default)]
    struct FakeState {
        commit_ids: Vec<u128>,
        entered: bool,
        released: bool,
        reject_next: bool,
        fail_next: bool,
        closed: bool,
        watermarks: std::collections::BTreeMap<u128, IngressWatermarks>,
    }

    struct FakeWriter {
        state: Arc<(Mutex<FakeState>, Condvar)>,
        block: bool,
    }

    impl WriterBackend for FakeWriter {
        fn commit_ingress(
            &mut self,
            identity: IngressIdentity,
            transaction: Transaction,
        ) -> crate::Result<Commit> {
            let (state, changed) = &*self.state;
            let mut state = state.lock().unwrap();
            state.entered = true;
            changed.notify_all();
            while self.block && !state.released {
                state = changed.wait(state).unwrap();
            }
            if state.reject_next {
                state.reject_next = false;
                return Err(crate::Error::InvalidModel(
                    "injected invalid model".to_owned(),
                ));
            }
            if state.fail_next {
                state.fail_next = false;
                return Err(crate::Error::Io(std::io::Error::other(
                    "injected writer error",
                )));
            }
            state.commit_ids.push(identity.commit_id);
            state.watermarks.insert(
                identity.source_id,
                IngressWatermarks {
                    accepted_through: Some(identity.sequence),
                    durable_through: Some(identity.sequence),
                },
            );
            Ok(Commit {
                frame_offset: state.commit_ids.len() as u64,
                points: transaction.point_count(),
                records: transaction.record_count(),
                bytes_written: 1,
                durable: true,
                deduplicated: false,
            })
        }

        fn ingress_watermarks(&self, source_id: u128) -> IngressWatermarks {
            self.state
                .0
                .lock()
                .unwrap()
                .watermarks
                .get(&source_id)
                .copied()
                .unwrap_or_default()
        }

        fn all_ingress_watermarks(&self) -> std::collections::BTreeMap<u128, IngressWatermarks> {
            self.state.0.lock().unwrap().watermarks.clone()
        }

        fn flush(&mut self) -> crate::Result<()> {
            Ok(())
        }

        fn close(
            self: Box<Self>,
        ) -> crate::Result<std::collections::BTreeMap<u128, IngressWatermarks>> {
            let mut state = self.state.0.lock().unwrap();
            state.closed = true;
            Ok(state.watermarks.clone())
        }
    }

    fn fake_runtime(
        capacity: usize,
        block: bool,
        fail_next: bool,
    ) -> (ShadowRuntime, Arc<(Mutex<FakeState>, Condvar)>) {
        fake_runtime_with_config(
            ShadowRuntimeConfig {
                queue_capacity: capacity,
                ..ShadowRuntimeConfig::default()
            },
            block,
            fail_next,
        )
    }

    fn fake_runtime_with_config(
        config: ShadowRuntimeConfig,
        block: bool,
        fail_next: bool,
    ) -> (ShadowRuntime, Arc<(Mutex<FakeState>, Condvar)>) {
        let state = Arc::new((
            Mutex::new(FakeState {
                fail_next,
                ..FakeState::default()
            }),
            Condvar::new(),
        ));
        let runtime = ShadowRuntime::start_backend(
            Box::new(FakeWriter {
                state: Arc::clone(&state),
                block,
            }),
            config,
        )
        .unwrap();
        (runtime, state)
    }

    fn write(sequence: u64, commit_id: u128) -> ShadowWrite {
        write_points(sequence, commit_id, 1)
    }

    fn write_points(sequence: u64, commit_id: u128, points: usize) -> ShadowWrite {
        write_points_for_source(1, sequence, commit_id, points)
    }

    fn write_points_for_source(
        source_id: u128,
        sequence: u64,
        commit_id: u128,
        points: usize,
    ) -> ShadowWrite {
        let mut transaction = Transaction::new();
        transaction.append_points(vec![Point::actual(1, sequence as i64, 1.0); points]);
        ShadowWrite::identified(
            IngressIdentity::new(source_id, sequence, commit_id),
            transaction,
        )
    }

    #[test]
    fn queue_capacity_is_a_hard_bound() {
        let (runtime, state) = fake_runtime(1, true, false);
        let submitter = runtime.submitter();
        let active = submitter.try_submit(write(0, 10)).unwrap();
        let (lock, changed) = &*state;
        let mut state = lock.lock().unwrap();
        while !state.entered {
            state = changed.wait(state).unwrap();
        }
        drop(state);

        let queued = submitter.try_submit(write(1, 11)).unwrap();
        assert!(matches!(
            submitter.try_submit(write(2, 12)),
            Err(SubmitError::Overloaded(_))
        ));
        assert_eq!(submitter.health().queued, 1);
        assert_eq!(submitter.health().queued_points, 1);

        let mut state = lock.lock().unwrap();
        state.released = true;
        changed.notify_all();
        drop(state);
        active.wait().unwrap();
        queued.wait().unwrap();
        runtime.shutdown().unwrap();
    }

    #[test]
    fn read_only_database_cannot_start_a_writer_runtime() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("read-only-runtime.wlog");
        Database::open(&path).unwrap().close().unwrap();
        let database = Database::open_read_only(path).unwrap();
        assert!(matches!(
            ShadowRuntime::start_database(database, ShadowRuntimeConfig::default()),
            Err(super::ShadowStartError::ReadOnlyBackend)
        ));
    }

    #[test]
    fn read_only_store_cannot_start_a_writer_runtime() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("read-only-runtime-store");
        Store::open(&path).unwrap().close().unwrap();
        let store = Store::open_read_only(path).unwrap();
        assert!(matches!(
            ShadowRuntime::start_store(store, ShadowRuntimeConfig::default()),
            Err(super::ShadowStartError::ReadOnlyBackend)
        ));
    }

    #[test]
    fn queued_point_limit_is_a_hard_bound_and_excludes_active_write() {
        let (runtime, state) = fake_runtime_with_config(
            ShadowRuntimeConfig {
                queue_capacity: 3,
                max_queued_points: 2,
                ..ShadowRuntimeConfig::default()
            },
            true,
            false,
        );
        let submitter = runtime.submitter();
        let active = submitter.try_submit(write(0, 10)).unwrap();
        let (lock, changed) = &*state;
        let mut state = lock.lock().unwrap();
        while !state.entered {
            state = changed.wait(state).unwrap();
        }
        drop(state);

        let queued = submitter.try_submit(write_points(1, 11, 2)).unwrap();
        assert!(matches!(
            submitter.try_submit(write(2, 12)),
            Err(SubmitError::PointBudgetExhausted(_))
        ));
        let health = submitter.health();
        assert_eq!(health.max_queued_points, 2);
        assert_eq!(health.queued, 1);
        assert_eq!(health.queued_points, 2);

        let mut state = lock.lock().unwrap();
        state.released = true;
        changed.notify_all();
        drop(state);
        active.wait().unwrap();
        queued.wait().unwrap();
        runtime.shutdown().unwrap();
    }

    #[test]
    fn queue_reservation_overflow_changes_neither_counter() {
        let mut entries_full = QueueUsage {
            queued: usize::MAX,
            points: 7,
        };
        assert_eq!(
            entries_full.reserve(usize::MAX, usize::MAX, 1),
            Err(QueueReservationError::Entries)
        );
        assert_eq!(
            entries_full,
            QueueUsage {
                queued: usize::MAX,
                points: 7,
            }
        );

        let mut points_full = QueueUsage {
            queued: 0,
            points: usize::MAX,
        };
        assert_eq!(
            points_full.reserve(usize::MAX, usize::MAX, 1),
            Err(QueueReservationError::Points)
        );
        assert_eq!(
            points_full,
            QueueUsage {
                queued: 0,
                points: usize::MAX,
            }
        );
    }

    #[test]
    fn writer_preserves_sequence_and_queue_order() {
        let (runtime, state) = fake_runtime(4, false, false);
        let submitter = runtime.submitter();
        let receipts: Vec<_> = (40..44)
            .map(|sequence| {
                submitter
                    .submit_until(
                        write(sequence, 100 + u128::from(sequence)),
                        Instant::now() + Duration::from_secs(1),
                    )
                    .unwrap()
            })
            .collect();
        for (offset, receipt) in receipts.into_iter().enumerate() {
            assert_eq!(
                receipt.wait().unwrap().identity.sequence,
                40 + offset as u64
            );
        }
        let report = runtime.shutdown().unwrap();
        assert_eq!(
            report.health.source_watermarks.get(&1),
            Some(&IngressWatermarks {
                accepted_through: Some(43),
                durable_through: Some(43),
            })
        );
        assert_eq!(state.0.lock().unwrap().commit_ids, vec![140, 141, 142, 143]);
    }

    #[test]
    fn duplicate_sequence_always_reaches_the_writer_backend() {
        let (runtime, state) = fake_runtime(2, false, false);
        let submitter = runtime.submitter();
        submitter.try_submit(write(0, 7)).unwrap().wait().unwrap();
        submitter.try_submit(write(0, 7)).unwrap().wait().unwrap();
        assert_eq!(state.0.lock().unwrap().commit_ids, vec![7, 7]);
        runtime.shutdown().unwrap();
    }

    #[test]
    fn ftwdb_exact_ingress_replay_is_deduplicated() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("shadow.wlog");
        let mut database = Database::open(&path).unwrap();
        initialize_catalog(&mut database);
        let runtime =
            ShadowRuntime::start_database(database, ShadowRuntimeConfig::default()).unwrap();
        let submitter = runtime.submitter();
        let first = submitter.try_submit(write(0, 77)).unwrap().wait().unwrap();
        let replay = submitter.try_submit(write(0, 77)).unwrap().wait().unwrap();
        assert!(!first.commit.deduplicated);
        assert!(replay.commit.deduplicated);
        runtime.shutdown().unwrap();

        let restarted = ShadowRuntime::start_database(
            Database::open(&path).unwrap(),
            ShadowRuntimeConfig::default(),
        )
        .unwrap();
        assert_eq!(
            restarted.health().source_watermarks.get(&1),
            Some(&IngressWatermarks {
                accepted_through: Some(0),
                durable_through: Some(0),
            })
        );
        restarted.shutdown().unwrap();

        let database = Database::open_read_only(path).unwrap();
        assert_eq!(database.stats().unwrap().points, 1);
    }

    #[test]
    fn ingress_conflict_is_nonfatal_and_next_sequence_succeeds() {
        let directory = tempfile::tempdir().unwrap();
        let mut database = Database::open(directory.path().join("conflict.wlog")).unwrap();
        initialize_catalog(&mut database);
        let runtime =
            ShadowRuntime::start_database(database, ShadowRuntimeConfig::default()).unwrap();
        let submitter = runtime.submitter();
        submitter.try_submit(write(0, 77)).unwrap().wait().unwrap();

        let mut changed = Transaction::new();
        changed.append_points(vec![Point::actual(1, 0, 2.0)]);
        let conflict = ShadowWrite::identified(IngressIdentity::new(1, 0, 77), changed);
        assert!(matches!(
            submitter.try_submit(conflict).unwrap().wait(),
            Err(ShadowWriteFailure::Rejected(
                crate::Error::IngressSourceSequenceConflict {
                    source_id: 1,
                    sequence: 0,
                }
            ))
        ));
        submitter.try_submit(write(1, 78)).unwrap().wait().unwrap();
        assert_eq!(submitter.health().state, ShadowRuntimeState::Running);
        assert!(submitter.health().last_ack_durable);
        assert!(submitter.health().database_points >= 1);
        runtime.shutdown().unwrap();
    }

    #[test]
    fn ftwdb_batch_rejection_does_not_stop_a_corrected_retry() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("shadow.wlog");
        let mut database = Database::open_with(
            &path,
            Config {
                max_batch_points: 1,
                ..Config::default()
            },
        )
        .unwrap();
        initialize_catalog(&mut database);
        let runtime =
            ShadowRuntime::start_database(database, ShadowRuntimeConfig::default()).unwrap();
        let submitter = runtime.submitter();

        let rejected = submitter.try_submit(write_points(5, 91, 2)).unwrap().wait();
        assert!(matches!(
            rejected,
            Err(ShadowWriteFailure::Rejected(
                crate::Error::BatchTooLarge { .. }
            ))
        ));
        assert_eq!(submitter.health().state, ShadowRuntimeState::Running);
        submitter.try_submit(write(5, 91)).unwrap().wait().unwrap();
        runtime.shutdown().unwrap();

        assert_eq!(
            Database::open_read_only(path)
                .unwrap()
                .stats()
                .unwrap()
                .points,
            1
        );
    }

    #[test]
    fn writer_error_poisons_runtime_and_contains_later_writes() {
        let (runtime, state) = fake_runtime(2, false, true);
        let submitter = runtime.submitter();
        let first = submitter.try_submit(write(0, 1)).unwrap();
        let second = submitter.try_submit(write(0, 2)).unwrap();
        assert!(matches!(first.wait(), Err(ShadowWriteFailure::Writer(_))));
        assert!(matches!(
            second.wait(),
            Err(ShadowWriteFailure::Poisoned { .. })
        ));
        assert_eq!(state.0.lock().unwrap().commit_ids, Vec::<u128>::new());
        assert_eq!(submitter.health().state, ShadowRuntimeState::Poisoned);
        assert!(matches!(
            submitter.try_submit(write(1, 3)),
            Err(SubmitError::Poisoned { .. })
        ));
        assert!(matches!(
            submitter.try_flush(1, 0),
            Err(FlushSubmitError::Poisoned { .. })
        ));
        assert!(runtime.shutdown().is_err());
    }

    #[test]
    fn request_error_allows_a_corrected_retry_and_next_write() {
        let (runtime, state) = fake_runtime(2, false, false);
        state.0.lock().unwrap().reject_next = true;
        let submitter = runtime.submitter();
        let rejected = submitter.try_submit(write(20, 1)).unwrap().wait();
        assert!(matches!(
            rejected,
            Err(ShadowWriteFailure::Rejected(crate::Error::InvalidModel(_)))
        ));
        assert_eq!(submitter.health().state, ShadowRuntimeState::Running);
        assert_eq!(submitter.health().last_error, None);

        submitter.try_submit(write(20, 2)).unwrap().wait().unwrap();
        submitter.try_submit(write(21, 3)).unwrap().wait().unwrap();
        assert_eq!(state.0.lock().unwrap().commit_ids, vec![2, 3]);
        runtime.shutdown().unwrap();
    }

    #[test]
    fn clean_shutdown_flushes_manual_durability_and_closes_submissions() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("shadow.wlog");
        let mut database = Database::open_with(
            &path,
            Config {
                durability: Durability::Manual,
                ..Config::default()
            },
        )
        .unwrap();
        initialize_catalog(&mut database);
        let runtime =
            ShadowRuntime::start_database(database, ShadowRuntimeConfig::default()).unwrap();
        let submitter = runtime.submitter();
        let ack = submitter.try_submit(write(0, 88)).unwrap().wait().unwrap();
        assert_eq!(ack.durable_through, None);
        let report = runtime.shutdown().unwrap();
        assert_eq!(report.health.state, ShadowRuntimeState::Closed);
        assert_eq!(
            report
                .health
                .source_watermarks
                .get(&1)
                .unwrap()
                .durable_through,
            Some(0)
        );
        assert!(matches!(
            submitter.try_submit(write(1, 89)),
            Err(SubmitError::Closed(_))
        ));
        assert_eq!(
            Database::open_read_only(path)
                .unwrap()
                .stats()
                .unwrap()
                .points,
            1
        );
    }

    #[test]
    fn source_flush_checks_through_and_refreshes_all_observed_sources() {
        let directory = tempfile::tempdir().unwrap();
        let mut database = Database::open_with(
            directory.path().join("two-sources.wlog"),
            Config {
                durability: Durability::Manual,
                ..Config::default()
            },
        )
        .unwrap();
        initialize_catalog(&mut database);
        let runtime =
            ShadowRuntime::start_database(database, ShadowRuntimeConfig::default()).unwrap();
        let submitter = runtime.submitter();
        submitter
            .try_submit(write_points_for_source(1, 40, 140, 1))
            .unwrap()
            .wait()
            .unwrap();
        submitter
            .try_submit(write_points_for_source(2, 900, 2900, 1))
            .unwrap()
            .wait()
            .unwrap();

        assert!(matches!(
            submitter.try_flush(1, 41).unwrap().wait(),
            Err(ShadowFlushFailure::NotAccepted {
                source_id: 1,
                through_sequence: 41,
                accepted_through: Some(40),
            })
        ));
        let flushed = submitter.try_flush(1, 40).unwrap().wait().unwrap();
        assert_eq!(flushed.durable_through, Some(40));
        let health = submitter.health();
        assert_eq!(
            health.source_watermarks.get(&1).unwrap().durable_through,
            Some(40)
        );
        assert_eq!(
            health.source_watermarks.get(&2).unwrap().durable_through,
            Some(900)
        );
        runtime.shutdown().unwrap();
    }

    fn initialize_catalog(database: &mut Database) {
        let mut transaction = Transaction::new();
        transaction
            .upsert_entity(Entity {
                id: EntityId(1),
                kind: "site".to_owned(),
                name: "test".to_owned(),
                parent: None,
                valid_from: 0,
                valid_to: None,
                properties: Properties::new(),
            })
            .define_series(SeriesDefinition {
                id: 1,
                owner_entity: Some(EntityId(1)),
                owner_relation: None,
                name: "shadow".to_owned(),
                physical_quantity: "power".to_owned(),
                canonical_unit: "W".to_owned(),
                semantics: SeriesSemantics::Gauge,
                maximum_gap_micros: None,
                rollup_policy: RollupPolicy {
                    raw_retain_for_micros: None,
                    tiers: Vec::new(),
                },
            });
        database.commit(transaction).unwrap();
    }
}
