//! Bounded version-one wire protocol for the FTWDB shadow sidecar.
//!
//! Version 1 is a draft until the Go sidecar passes the same golden fixtures.
//! All integers and IEEE-754 bit patterns use big-endian byte order. A frame is
//! `magic[4] | version:u16 | kind:u8 | reserved:u8 | payload_len:u32 | payload
//! | crc32:u32`; CRC-32 covers the header and payload.
//!
//! A commit payload starts with `source_id:u128 | sequence:u64 | commit_id:u128`,
//! then six collections in this exact order: entities, relations, series,
//! runs, plans, points. Each collection starts with a `u32` count. Metadata
//! fields follow their Rust declaration order. Text is `u16 bytes | UTF-8`.
//! Optional values are `presence:u8` (`0` or `1`) followed by the value when
//! present. Maps are `u32 count` followed by key/value pairs in ascending key
//! order. Enums use the explicit one-byte tags in the codec below. A point is
//! exactly 72 bytes: `u64 | i64 | i64 | i64 | i64 | u128 | f64-bits | u32 |
//! u32`, retaining all UTC-microsecond timestamps and provenance.
//!
//! Message kinds are request `1=hello, 2=commit, 3=flush, 4=health` and
//! response `128=hello, 129=ack, 130=health, 131=error`. Hello request is
//! `source_id | node_id | client_version | capabilities`; hello response is
//! `selected_version | session_id[16] | server_time_micros`. Flush is
//! `source_id | through_sequence`; a health request is `nonce`. Ack fields and
//! health-response fields follow their public struct order. An optional
//! watermark uses the normal presence encoding. Error is
//! `code:u8 | retryable:u8 | message`.
//!
//! Metadata layouts are: Entity `id | kind | name | parent | valid_from |
//! valid_to | properties`; Relation `id | kind | source | target | valid_from
//! | valid_to | properties`; SeriesDefinition `id | owner_entity |
//! owner_relation | name | physical_quantity | canonical_unit | semantics |
//! maximum_gap | rollup_policy`; Run `id | kind | status | created_at |
//! knowledge_time | workflow | model | model_version | parent_run |
//! input_snapshot | attributes`; Plan `id | run_id | status | horizon_start |
//! horizon_end | resolution_micros | scenario | objective_terms |
//! objective_value | supersedes | attributes`. Properties use tags
//! `0=null, 1=bool, 2=i64, 3=f64, 4=text`.

use crate::{
    CalendarUnit, Entity, EntityId, Plan, PlanStatus, Point, Properties, PropertyValue, Relation,
    RelationId, RollupPolicy, RollupResolution, RollupTier, Run, RunId, RunKind, RunStatus,
    SeriesDefinition, SeriesSemantics, Transaction,
};
use crc32fast::Hasher;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::io::{self, Read, Write};

pub const PROTOCOL_VERSION: u16 = 1;
pub const FRAME_MAGIC: [u8; 4] = *b"FTWS";
pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_BATCH_POINTS: usize = 16_384;
pub const MAX_METADATA_RECORDS: usize = 16_384;
pub const MAX_QUEUE_ENTRIES: u32 = 65_536;
pub const MAX_PROPERTIES: usize = 1_024;
pub const MAX_ROLLUP_TIERS: usize = 64;
pub const MAX_TEXT_BYTES: usize = 4_096;

const HEADER_BYTES: usize = 12;
const CHECKSUM_BYTES: usize = 4;
const MAX_PAYLOAD_BYTES: usize = MAX_FRAME_BYTES - HEADER_BYTES - CHECKSUM_BYTES;
const MAX_KEY_BYTES: usize = 256;
const MAX_ERROR_TEXT_BYTES: usize = 512;
const HELLO_REQUEST: u8 = 1;
const COMMIT_BATCH_REQUEST: u8 = 2;
const FLUSH_REQUEST: u8 = 3;
const HEALTH_REQUEST: u8 = 4;
const HELLO_RESPONSE: u8 = 128;
const ACK_RESPONSE: u8 = 129;
const HEALTH_RESPONSE: u8 = 130;
const ERROR_RESPONSE: u8 = 131;

#[derive(Clone, Debug, PartialEq)]
pub enum WireMessage {
    Request(Request),
    Response(Response),
}
#[derive(Clone, Debug, PartialEq)]
pub enum Request {
    Hello(HelloRequest),
    CommitBatch(CommitBatchRequest),
    Flush(FlushRequest),
    Health(HealthRequest),
}
#[derive(Clone, Debug, PartialEq)]
pub enum Response {
    Hello(HelloResponse),
    Ack(Ack),
    Health(HealthResponse),
    Error(ErrorResponse),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HelloRequest {
    pub source_id: u128,
    pub node_id: String,
    pub client_version: String,
    pub capabilities: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HelloResponse {
    pub selected_version: u16,
    pub session_id: [u8; 16],
    pub server_time_micros: i64,
}

/// One sidecar identity and sequence form the retry key. `commit_id` maps to
/// `Transaction::with_commit_id`; the receiver must reject the same identity
/// with different canonical bytes as an idempotency conflict.
#[derive(Clone, Debug, PartialEq)]
pub struct CommitBatchRequest {
    pub source_id: u128,
    pub sequence: u64,
    pub commit_id: u128,
    pub entities: Vec<Entity>,
    pub relations: Vec<Relation>,
    pub series: Vec<SeriesDefinition>,
    pub runs: Vec<Run>,
    pub plans: Vec<Plan>,
    /// Each point contains all 72 FTWDB point bytes and UTC microsecond times.
    pub points: Vec<Point>,
}

/// Builds the one canonical storage transaction used by both the sidecar and
/// read-only reconciliation. Keeping this mapping in one place prevents the
/// verifier from blessing bytes that the server would store differently.
pub(crate) fn transaction_from_batch(batch: CommitBatchRequest) -> Transaction {
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlushRequest {
    pub source_id: u128,
    pub through_sequence: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HealthRequest {
    pub nonce: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AckKind {
    CommitBatch,
    Flush,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Ack {
    pub kind: AckKind,
    pub source_id: u128,
    pub sequence: u64,
    /// Zero for a flush acknowledgement.
    pub commit_id: u128,
    pub accepted_through_sequence: Option<u64>,
    pub durable_through_sequence: Option<u64>,
    pub durable: bool,
    pub deduplicated: bool,
    pub frame_offset: u64,
    pub records: u32,
    pub points: u32,
    pub bytes_written: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unavailable,
}
/// Sidecar sync policy reported on health. Matches [`crate::Durability`]
/// tags so operators can see the live writer without a second endpoint.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SyncPolicy {
    #[default]
    Always,
    Manual,
    EveryBytes(u64),
}
impl fmt::Display for SyncPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Always => formatter.write_str("always"),
            Self::Manual => formatter.write_str("manual"),
            Self::EveryBytes(bytes) => write!(formatter, "every-bytes:{bytes}"),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HealthResponse {
    pub nonce: u64,
    pub source_id: u128,
    pub status: HealthStatus,
    pub queue_entries: u32,
    pub accepted_through_sequence: Option<u64>,
    pub durable_through_sequence: Option<u64>,
    pub overload_count: u64,
    pub protocol_error_count: u64,
    pub database_bytes: u64,
    pub database_points: u64,
    pub database_commits: u64,
    pub recovered_tail_bytes: u64,
    pub sync_policy: SyncPolicy,
    pub last_ack_durable: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErrorResponse {
    pub code: ErrorCode,
    pub retryable: bool,
    pub message: String,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorCode {
    InvalidRequest,
    Overloaded,
    Internal,
    Unsupported,
    IdempotencyConflict,
}

#[derive(Debug)]
pub enum ProtocolError {
    Io(io::Error),
    Truncated { expected: usize, actual: usize },
    TrailingBytes { count: usize },
    InvalidMagic,
    UnsupportedVersion(u16),
    UnknownMessageType(u8),
    ReservedBitsSet(u8),
    FrameTooLarge { declared: usize, maximum: usize },
    ChecksumMismatch { expected: u32, actual: u32 },
    InvalidField(&'static str),
    InvalidEnumValue { field: &'static str, value: u8 },
}
impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Truncated { expected, actual } => write!(
                f,
                "truncated frame: expected {expected} bytes, got {actual}"
            ),
            Self::TrailingBytes { count } => write!(f, "frame has {count} trailing bytes"),
            Self::InvalidMagic => write!(f, "invalid shadow protocol magic"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported shadow protocol version {v}"),
            Self::UnknownMessageType(v) => write!(f, "unknown shadow message type {v}"),
            Self::ReservedBitsSet(v) => write!(f, "reserved header bits are set: {v}"),
            Self::FrameTooLarge { declared, maximum } => {
                write!(f, "frame declares {declared} bytes; maximum is {maximum}")
            }
            Self::ChecksumMismatch { expected, actual } => write!(
                f,
                "shadow frame checksum mismatch: expected {expected:#010x}, got {actual:#010x}"
            ),
            Self::InvalidField(v) => write!(f, "invalid shadow protocol field: {v}"),
            Self::InvalidEnumValue { field, value } => {
                write!(f, "invalid {field} enum value {value}")
            }
        }
    }
}
impl Error for ProtocolError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Self::Io(e) = self {
            Some(e)
        } else {
            None
        }
    }
}
impl From<io::Error> for ProtocolError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

pub fn encode(message: &WireMessage) -> Result<Vec<u8>, ProtocolError> {
    let mut payload = Vec::new();
    let kind = encode_payload(message, &mut payload)?;
    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(too_large(payload.len()));
    }
    let mut frame = Vec::with_capacity(HEADER_BYTES + payload.len() + CHECKSUM_BYTES);
    frame.extend_from_slice(&FRAME_MAGIC);
    put_u16(&mut frame, PROTOCOL_VERSION);
    frame.push(kind);
    frame.push(0);
    put_u32(&mut frame, payload.len() as u32);
    frame.extend_from_slice(&payload);
    let sum = crc32(&frame);
    put_u32(&mut frame, sum);
    Ok(frame)
}
pub fn decode(frame: &[u8]) -> Result<WireMessage, ProtocolError> {
    let (kind, total) = parse_header(frame)?;
    if frame.len() < total {
        return Err(ProtocolError::Truncated {
            expected: total,
            actual: frame.len(),
        });
    }
    if frame.len() > total {
        return Err(ProtocolError::TrailingBytes {
            count: frame.len() - total,
        });
    }
    let actual = u32::from_be_bytes(frame[total - 4..total].try_into().unwrap());
    let expected = crc32(&frame[..total - 4]);
    if actual != expected {
        return Err(ProtocolError::ChecksumMismatch { expected, actual });
    }
    decode_payload(kind, &frame[HEADER_BYTES..total - CHECKSUM_BYTES])
}
pub fn write_to<W: Write>(writer: &mut W, message: &WireMessage) -> Result<(), ProtocolError> {
    writer.write_all(&encode(message)?)?;
    Ok(())
}
/// Validates magic, version, type, reserved byte, and body limit before it allocates or reads a body.
pub fn read_from<R: Read>(reader: &mut R) -> Result<WireMessage, ProtocolError> {
    let mut header = [0; HEADER_BYTES];
    read_exact(reader, &mut header, 0)?;
    let (_, total) = parse_header(&header)?;
    let mut frame = Vec::with_capacity(total);
    frame.extend_from_slice(&header);
    let mut tail = vec![0; total - HEADER_BYTES];
    read_exact(reader, &mut tail, HEADER_BYTES)?;
    frame.extend_from_slice(&tail);
    decode(&frame)
}

fn parse_header(frame: &[u8]) -> Result<(u8, usize), ProtocolError> {
    if frame.len() < HEADER_BYTES {
        return Err(ProtocolError::Truncated {
            expected: HEADER_BYTES,
            actual: frame.len(),
        });
    }
    if frame[..4] != FRAME_MAGIC {
        return Err(ProtocolError::InvalidMagic);
    }
    let version = u16::from_be_bytes(frame[4..6].try_into().unwrap());
    if version != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion(version));
    }
    let kind = frame[6];
    validate_kind(kind)?;
    if frame[7] != 0 {
        return Err(ProtocolError::ReservedBitsSet(frame[7]));
    }
    let payload = u32::from_be_bytes(frame[8..12].try_into().unwrap()) as usize;
    let total = HEADER_BYTES
        .checked_add(payload)
        .and_then(|n| n.checked_add(CHECKSUM_BYTES))
        .ok_or_else(|| too_large(usize::MAX))?;
    if total > MAX_FRAME_BYTES {
        return Err(too_large(payload));
    }
    Ok((kind, total))
}
fn validate_kind(kind: u8) -> Result<(), ProtocolError> {
    match kind {
        HELLO_REQUEST | COMMIT_BATCH_REQUEST | FLUSH_REQUEST | HEALTH_REQUEST | HELLO_RESPONSE
        | ACK_RESPONSE | HEALTH_RESPONSE | ERROR_RESPONSE => Ok(()),
        v => Err(ProtocolError::UnknownMessageType(v)),
    }
}
fn too_large(payload: usize) -> ProtocolError {
    ProtocolError::FrameTooLarge {
        declared: payload.saturating_add(HEADER_BYTES + CHECKSUM_BYTES),
        maximum: MAX_FRAME_BYTES,
    }
}

fn encode_payload(message: &WireMessage, o: &mut Vec<u8>) -> Result<u8, ProtocolError> {
    match message {
        WireMessage::Request(Request::Hello(v)) => {
            if v.source_id == 0 {
                return Err(ProtocolError::InvalidField("source_id"));
            }
            put_u128(o, v.source_id);
            string(o, &v.node_id, 128, "node_id")?;
            string(o, &v.client_version, 64, "client_version")?;
            put_u64(o, v.capabilities);
            Ok(HELLO_REQUEST)
        }
        WireMessage::Request(Request::CommitBatch(v)) => {
            validate_batch(v)?;
            put_u128(o, v.source_id);
            put_u64(o, v.sequence);
            put_u128(o, v.commit_id);
            put_u32(o, v.entities.len() as u32);
            for value in &v.entities {
                encode_entity(o, value)?;
            }
            put_u32(o, v.relations.len() as u32);
            for value in &v.relations {
                encode_relation(o, value)?;
            }
            put_u32(o, v.series.len() as u32);
            for value in &v.series {
                encode_series(o, value)?;
            }
            put_u32(o, v.runs.len() as u32);
            for value in &v.runs {
                encode_run(o, value)?;
            }
            put_u32(o, v.plans.len() as u32);
            for value in &v.plans {
                encode_plan(o, value)?;
            }
            put_u32(o, v.points.len() as u32);
            for p in &v.points {
                point(o, *p);
            }
            Ok(COMMIT_BATCH_REQUEST)
        }
        WireMessage::Request(Request::Flush(v)) => {
            if v.source_id == 0 {
                return Err(ProtocolError::InvalidField("source_id"));
            }
            put_u128(o, v.source_id);
            put_u64(o, v.through_sequence);
            Ok(FLUSH_REQUEST)
        }
        WireMessage::Request(Request::Health(v)) => {
            put_u64(o, v.nonce);
            Ok(HEALTH_REQUEST)
        }
        WireMessage::Response(Response::Hello(v)) => {
            if v.selected_version != PROTOCOL_VERSION {
                return Err(ProtocolError::UnsupportedVersion(v.selected_version));
            }
            put_u16(o, v.selected_version);
            o.extend_from_slice(&v.session_id);
            put_i64(o, v.server_time_micros);
            Ok(HELLO_RESPONSE)
        }
        WireMessage::Response(Response::Ack(v)) => {
            if v.source_id == 0 {
                return Err(ProtocolError::InvalidField("source_id"));
            }
            o.push(match v.kind {
                AckKind::CommitBatch => 1,
                AckKind::Flush => 2,
            });
            put_u128(o, v.source_id);
            put_u64(o, v.sequence);
            put_u128(o, v.commit_id);
            put_watermark(o, v.accepted_through_sequence);
            put_watermark(o, v.durable_through_sequence);
            o.push(v.durable as u8);
            o.push(v.deduplicated as u8);
            put_u64(o, v.frame_offset);
            put_u32(o, v.records);
            put_u32(o, v.points);
            put_u64(o, v.bytes_written);
            Ok(ACK_RESPONSE)
        }
        WireMessage::Response(Response::Health(v)) => {
            if v.queue_entries > MAX_QUEUE_ENTRIES {
                return Err(ProtocolError::InvalidField("queue_entries"));
            }
            put_u64(o, v.nonce);
            if v.source_id == 0 {
                return Err(ProtocolError::InvalidField("source_id"));
            }
            put_u128(o, v.source_id);
            o.push(status_byte(v.status));
            put_u32(o, v.queue_entries);
            put_watermark(o, v.accepted_through_sequence);
            put_watermark(o, v.durable_through_sequence);
            put_u64(o, v.overload_count);
            put_u64(o, v.protocol_error_count);
            put_u64(o, v.database_bytes);
            put_u64(o, v.database_points);
            put_u64(o, v.database_commits);
            put_u64(o, v.recovered_tail_bytes);
            put_sync_policy(o, v.sync_policy)?;
            o.push(v.last_ack_durable as u8);
            Ok(HEALTH_RESPONSE)
        }
        WireMessage::Response(Response::Error(v)) => {
            o.push(error_byte(v.code));
            o.push(v.retryable as u8);
            string(o, &v.message, MAX_ERROR_TEXT_BYTES, "error message")?;
            Ok(ERROR_RESPONSE)
        }
    }
}

fn decode_payload(kind: u8, bytes: &[u8]) -> Result<WireMessage, ProtocolError> {
    let mut i = Input::new(bytes);
    let m = match kind {
        HELLO_REQUEST => WireMessage::Request(Request::Hello(HelloRequest {
            source_id: i.u128()?,
            node_id: i.string(128, "node_id")?,
            client_version: i.string(64, "client_version")?,
            capabilities: i.u64()?,
        })),
        COMMIT_BATCH_REQUEST => {
            let mut remaining = MAX_METADATA_RECORDS;
            let v = CommitBatchRequest {
                source_id: i.u128()?,
                sequence: i.u64()?,
                commit_id: i.u128()?,
                entities: decode_collection(&mut i, &mut remaining, decode_entity)?,
                relations: decode_collection(&mut i, &mut remaining, decode_relation)?,
                series: decode_collection(&mut i, &mut remaining, decode_series)?,
                runs: decode_collection(&mut i, &mut remaining, decode_run)?,
                plans: decode_collection(&mut i, &mut remaining, decode_plan)?,
                points: i.points()?,
            };
            validate_batch(&v)?;
            WireMessage::Request(Request::CommitBatch(v))
        }
        FLUSH_REQUEST => WireMessage::Request(Request::Flush(FlushRequest {
            source_id: i.u128()?,
            through_sequence: i.u64()?,
        })),
        HEALTH_REQUEST => WireMessage::Request(Request::Health(HealthRequest { nonce: i.u64()? })),
        HELLO_RESPONSE => {
            let selected_version = i.u16()?;
            if selected_version != PROTOCOL_VERSION {
                return Err(ProtocolError::UnsupportedVersion(selected_version));
            }
            let mut session_id = [0; 16];
            session_id.copy_from_slice(i.take(16)?);
            WireMessage::Response(Response::Hello(HelloResponse {
                selected_version,
                session_id,
                server_time_micros: i.i64()?,
            }))
        }
        ACK_RESPONSE => WireMessage::Response(Response::Ack(Ack {
            kind: match i.u8()? {
                1 => AckKind::CommitBatch,
                2 => AckKind::Flush,
                value => return Err(enum_error("ack kind", value)),
            },
            source_id: i.u128()?,
            sequence: i.u64()?,
            commit_id: i.u128()?,
            accepted_through_sequence: i.watermark("accepted watermark")?,
            durable_through_sequence: i.watermark("durable watermark")?,
            durable: boolean(&mut i, "durable")?,
            deduplicated: boolean(&mut i, "deduplicated")?,
            frame_offset: i.u64()?,
            records: i.u32()?,
            points: i.u32()?,
            bytes_written: i.u64()?,
        })),
        HEALTH_RESPONSE => {
            let mut v = HealthResponse {
                nonce: i.u64()?,
                source_id: i.u128()?,
                status: status_from(i.u8()?)?,
                queue_entries: i.u32()?,
                accepted_through_sequence: i.watermark("accepted watermark")?,
                durable_through_sequence: i.watermark("durable watermark")?,
                overload_count: 0,
                protocol_error_count: 0,
                database_bytes: 0,
                database_points: 0,
                database_commits: 0,
                recovered_tail_bytes: 0,
                sync_policy: SyncPolicy::Always,
                last_ack_durable: false,
            };
            if i.remaining() > 0 {
                v.overload_count = i.u64()?;
                v.protocol_error_count = i.u64()?;
                v.database_bytes = i.u64()?;
                v.database_points = i.u64()?;
                v.database_commits = i.u64()?;
                v.recovered_tail_bytes = i.u64()?;
                v.sync_policy = sync_policy_from(&mut i)?;
                v.last_ack_durable = boolean(&mut i, "last_ack_durable")?;
            }
            if v.queue_entries > MAX_QUEUE_ENTRIES {
                return Err(ProtocolError::InvalidField("queue_entries"));
            }
            WireMessage::Response(Response::Health(v))
        }
        ERROR_RESPONSE => WireMessage::Response(Response::Error(ErrorResponse {
            code: error_from(i.u8()?)?,
            retryable: boolean(&mut i, "retryable")?,
            message: i.string(MAX_ERROR_TEXT_BYTES, "error message")?,
        })),
        _ => unreachable!(),
    };
    match &m {
        WireMessage::Request(Request::Hello(v)) if v.source_id == 0 => {
            return Err(ProtocolError::InvalidField("source_id"));
        }
        WireMessage::Request(Request::Flush(v)) if v.source_id == 0 => {
            return Err(ProtocolError::InvalidField("source_id"));
        }
        WireMessage::Response(Response::Ack(v)) if v.source_id == 0 => {
            return Err(ProtocolError::InvalidField("source_id"));
        }
        WireMessage::Response(Response::Health(v)) if v.source_id == 0 => {
            return Err(ProtocolError::InvalidField("source_id"));
        }
        _ => {}
    }
    i.finish()?;
    Ok(m)
}

fn validate_batch(v: &CommitBatchRequest) -> Result<(), ProtocolError> {
    if v.source_id == 0 {
        return Err(ProtocolError::InvalidField("source_id"));
    }
    let meta = v.entities.len() + v.relations.len() + v.series.len() + v.runs.len() + v.plans.len();
    if meta > MAX_METADATA_RECORDS {
        return Err(ProtocolError::InvalidField("too many metadata records"));
    }
    if v.points.len() > MAX_BATCH_POINTS {
        return Err(ProtocolError::InvalidField("too many points"));
    }
    if meta == 0 && v.points.is_empty() {
        return Err(ProtocolError::InvalidField("empty transaction"));
    }
    for s in &v.series {
        s.validate().map_err(ProtocolError::InvalidField)?;
    }
    for p in &v.plans {
        p.validate().map_err(ProtocolError::InvalidField)?;
    }
    for p in &v.points {
        if p.series_id == 0 || p.valid_time_end < p.valid_time || !p.value.is_finite() {
            return Err(ProtocolError::InvalidField("invalid point"));
        }
    }
    Ok(())
}
fn encode_entity(o: &mut Vec<u8>, v: &Entity) -> Result<(), ProtocolError> {
    put_u128(o, v.id.0);
    string(o, &v.kind, MAX_KEY_BYTES, "entity kind")?;
    string(o, &v.name, MAX_TEXT_BYTES, "entity name")?;
    put_option_u128(o, v.parent.map(|id| id.0));
    put_i64(o, v.valid_from);
    put_option_i64(o, v.valid_to);
    encode_properties(o, &v.properties)
}
fn decode_entity(i: &mut Input<'_>) -> Result<Entity, ProtocolError> {
    Ok(Entity {
        id: EntityId(i.u128()?),
        kind: i.string(MAX_KEY_BYTES, "entity kind")?,
        name: i.string(MAX_TEXT_BYTES, "entity name")?,
        parent: i.option_u128("entity parent")?.map(EntityId),
        valid_from: i.i64()?,
        valid_to: i.option_i64("entity valid_to")?,
        properties: decode_properties(i)?,
    })
}
fn encode_relation(o: &mut Vec<u8>, v: &Relation) -> Result<(), ProtocolError> {
    put_u128(o, v.id.0);
    string(o, &v.kind, MAX_KEY_BYTES, "relation kind")?;
    put_u128(o, v.source.0);
    put_u128(o, v.target.0);
    put_i64(o, v.valid_from);
    put_option_i64(o, v.valid_to);
    encode_properties(o, &v.properties)
}
fn decode_relation(i: &mut Input<'_>) -> Result<Relation, ProtocolError> {
    Ok(Relation {
        id: RelationId(i.u128()?),
        kind: i.string(MAX_KEY_BYTES, "relation kind")?,
        source: EntityId(i.u128()?),
        target: EntityId(i.u128()?),
        valid_from: i.i64()?,
        valid_to: i.option_i64("relation valid_to")?,
        properties: decode_properties(i)?,
    })
}
fn encode_series(o: &mut Vec<u8>, v: &SeriesDefinition) -> Result<(), ProtocolError> {
    v.validate().map_err(ProtocolError::InvalidField)?;
    put_u64(o, v.id);
    put_option_u128(o, v.owner_entity.map(|id| id.0));
    put_option_u128(o, v.owner_relation.map(|id| id.0));
    string(o, &v.name, MAX_KEY_BYTES, "series name")?;
    string(o, &v.physical_quantity, MAX_KEY_BYTES, "physical quantity")?;
    string(o, &v.canonical_unit, MAX_KEY_BYTES, "canonical unit")?;
    o.push(series_semantics_byte(v.semantics));
    put_option_i64(o, v.maximum_gap_micros);
    encode_rollup_policy(o, &v.rollup_policy)
}
fn decode_series(i: &mut Input<'_>) -> Result<SeriesDefinition, ProtocolError> {
    let v = SeriesDefinition {
        id: i.u64()?,
        owner_entity: i.option_u128("owner entity")?.map(EntityId),
        owner_relation: i.option_u128("owner relation")?.map(RelationId),
        name: i.string(MAX_KEY_BYTES, "series name")?,
        physical_quantity: i.string(MAX_KEY_BYTES, "physical quantity")?,
        canonical_unit: i.string(MAX_KEY_BYTES, "canonical unit")?,
        semantics: series_semantics_from(i.u8()?)?,
        maximum_gap_micros: i.option_i64("maximum gap")?,
        rollup_policy: decode_rollup_policy(i)?,
    };
    v.validate().map_err(ProtocolError::InvalidField)?;
    Ok(v)
}
fn encode_rollup_policy(o: &mut Vec<u8>, v: &RollupPolicy) -> Result<(), ProtocolError> {
    if v.tiers.len() > MAX_ROLLUP_TIERS {
        return Err(ProtocolError::InvalidField("too many rollup tiers"));
    }
    put_option_i64(o, v.raw_retain_for_micros);
    put_u32(o, v.tiers.len() as u32);
    for tier in &v.tiers {
        match &tier.resolution {
            RollupResolution::FixedMicros(value) => {
                o.push(1);
                put_i64(o, *value);
            }
            RollupResolution::Calendar {
                unit,
                iana_timezone,
            } => {
                o.push(2);
                o.push(calendar_unit_byte(*unit));
                string(o, iana_timezone, MAX_KEY_BYTES, "IANA timezone")?;
            }
        }
        put_option_i64(o, tier.retain_for_micros);
    }
    Ok(())
}
fn decode_rollup_policy(i: &mut Input<'_>) -> Result<RollupPolicy, ProtocolError> {
    let raw_retain_for_micros = i.option_i64("raw retention")?;
    let count = i.count(MAX_ROLLUP_TIERS, "rollup tier count")?;
    let mut tiers = Vec::with_capacity(count);
    for _ in 0..count {
        let resolution = match i.u8()? {
            1 => RollupResolution::FixedMicros(i.i64()?),
            2 => RollupResolution::Calendar {
                unit: calendar_unit_from(i.u8()?)?,
                iana_timezone: i.string(MAX_KEY_BYTES, "IANA timezone")?,
            },
            value => return Err(enum_error("rollup resolution", value)),
        };
        tiers.push(RollupTier {
            resolution,
            retain_for_micros: i.option_i64("tier retention")?,
        });
    }
    Ok(RollupPolicy {
        raw_retain_for_micros,
        tiers,
    })
}
fn encode_run(o: &mut Vec<u8>, v: &Run) -> Result<(), ProtocolError> {
    put_u128(o, v.id.0);
    o.push(run_kind_byte(v.kind));
    o.push(run_status_byte(v.status));
    put_i64(o, v.created_at);
    put_i64(o, v.knowledge_time);
    string(o, &v.workflow, MAX_TEXT_BYTES, "workflow")?;
    text(o, &v.model, MAX_TEXT_BYTES, "model")?;
    text(o, &v.model_version, MAX_TEXT_BYTES, "model version")?;
    put_option_u128(o, v.parent_run.map(|id| id.0));
    put_option_u128(o, v.input_snapshot.map(|id| id.0));
    encode_properties(o, &v.attributes)
}
fn decode_run(i: &mut Input<'_>) -> Result<Run, ProtocolError> {
    Ok(Run {
        id: RunId(i.u128()?),
        kind: run_kind_from(i.u8()?)?,
        status: run_status_from(i.u8()?)?,
        created_at: i.i64()?,
        knowledge_time: i.i64()?,
        workflow: i.string(MAX_TEXT_BYTES, "workflow")?,
        model: i.text(MAX_TEXT_BYTES, "model")?,
        model_version: i.text(MAX_TEXT_BYTES, "model version")?,
        parent_run: i.option_u128("parent run")?.map(RunId),
        input_snapshot: i.option_u128("input snapshot")?.map(RunId),
        attributes: decode_properties(i)?,
    })
}
fn encode_plan(o: &mut Vec<u8>, v: &Plan) -> Result<(), ProtocolError> {
    v.validate().map_err(ProtocolError::InvalidField)?;
    put_u128(o, v.id);
    put_u128(o, v.run_id.0);
    o.push(plan_status_byte(v.status));
    put_i64(o, v.horizon_start);
    put_i64(o, v.horizon_end);
    put_i64(o, v.resolution_micros);
    string(o, &v.scenario, MAX_TEXT_BYTES, "scenario")?;
    if v.objective_terms.len() > MAX_PROPERTIES {
        return Err(ProtocolError::InvalidField("too many objective terms"));
    }
    put_u32(o, v.objective_terms.len() as u32);
    for (key, value) in &v.objective_terms {
        string(o, key, MAX_KEY_BYTES, "objective key")?;
        put_f64(o, *value, "objective value")?;
    }
    put_option_f64(o, v.objective_value, "objective value")?;
    put_option_u128(o, v.supersedes);
    encode_properties(o, &v.attributes)
}
fn decode_plan(i: &mut Input<'_>) -> Result<Plan, ProtocolError> {
    let id = i.u128()?;
    let run_id = RunId(i.u128()?);
    let status = plan_status_from(i.u8()?)?;
    let horizon_start = i.i64()?;
    let horizon_end = i.i64()?;
    let resolution_micros = i.i64()?;
    let scenario = i.string(MAX_TEXT_BYTES, "scenario")?;
    let count = i.count(MAX_PROPERTIES, "objective term count")?;
    let mut objective_terms = BTreeMap::new();
    let mut previous: Option<String> = None;
    for _ in 0..count {
        let key = i.string(MAX_KEY_BYTES, "objective key")?;
        ensure_sorted(&previous, &key, "objective keys")?;
        previous = Some(key.clone());
        let value = i.f64("objective value")?;
        objective_terms.insert(key, value);
    }
    let v = Plan {
        id,
        run_id,
        status,
        horizon_start,
        horizon_end,
        resolution_micros,
        scenario,
        objective_terms,
        objective_value: i.option_f64("objective value")?,
        supersedes: i.option_u128("supersedes")?,
        attributes: decode_properties(i)?,
    };
    v.validate().map_err(ProtocolError::InvalidField)?;
    Ok(v)
}
fn encode_properties(o: &mut Vec<u8>, values: &Properties) -> Result<(), ProtocolError> {
    if values.len() > MAX_PROPERTIES {
        return Err(ProtocolError::InvalidField("too many properties"));
    }
    put_u32(o, values.len() as u32);
    for (key, value) in values {
        string(o, key, MAX_KEY_BYTES, "property key")?;
        match value {
            PropertyValue::Null => o.push(0),
            PropertyValue::Bool(v) => {
                o.push(1);
                o.push(*v as u8);
            }
            PropertyValue::Integer(v) => {
                o.push(2);
                put_i64(o, *v);
            }
            PropertyValue::Float(v) => {
                o.push(3);
                put_f64(o, *v, "property float")?;
            }
            PropertyValue::Text(v) => {
                o.push(4);
                text(o, v, MAX_TEXT_BYTES, "property text")?;
            }
        }
    }
    Ok(())
}
fn decode_properties(i: &mut Input<'_>) -> Result<Properties, ProtocolError> {
    let count = i.count(MAX_PROPERTIES, "property count")?;
    let mut values = BTreeMap::new();
    let mut previous: Option<String> = None;
    for _ in 0..count {
        let key = i.string(MAX_KEY_BYTES, "property key")?;
        ensure_sorted(&previous, &key, "property keys")?;
        previous = Some(key.clone());
        let value = match i.u8()? {
            0 => PropertyValue::Null,
            1 => PropertyValue::Bool(boolean(i, "property bool")?),
            2 => PropertyValue::Integer(i.i64()?),
            3 => PropertyValue::Float(i.f64("property float")?),
            4 => PropertyValue::Text(i.text(MAX_TEXT_BYTES, "property text")?),
            value => return Err(enum_error("property value", value)),
        };
        values.insert(key, value);
    }
    Ok(values)
}
fn decode_collection<T>(
    i: &mut Input<'_>,
    remaining: &mut usize,
    decode: fn(&mut Input<'_>) -> Result<T, ProtocolError>,
) -> Result<Vec<T>, ProtocolError> {
    let count = i.count(*remaining, "metadata record count")?;
    *remaining -= count;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(decode(i)?);
    }
    Ok(values)
}
fn ensure_sorted(
    previous: &Option<String>,
    current: &str,
    field: &'static str,
) -> Result<(), ProtocolError> {
    if previous.as_deref().is_some_and(|value| value >= current) {
        Err(ProtocolError::InvalidField(field))
    } else {
        Ok(())
    }
}
fn point(o: &mut Vec<u8>, p: Point) {
    put_u64(o, p.series_id);
    put_i64(o, p.valid_time);
    put_i64(o, p.valid_time_end);
    put_i64(o, p.knowledge_time);
    put_i64(o, p.change_time);
    put_u128(o, p.run_id);
    put_u64(o, p.value.to_bits());
    put_u32(o, p.quality);
    put_u32(o, p.flags);
}
fn string(o: &mut Vec<u8>, v: &str, max: usize, field: &'static str) -> Result<(), ProtocolError> {
    if v.is_empty() || v.len() > max {
        return Err(ProtocolError::InvalidField(field));
    }
    text(o, v, max, field)
}
fn text(o: &mut Vec<u8>, v: &str, max: usize, field: &'static str) -> Result<(), ProtocolError> {
    if v.len() > max || v.len() > u16::MAX as usize {
        return Err(ProtocolError::InvalidField(field));
    }
    put_u16(o, v.len() as u16);
    o.extend_from_slice(v.as_bytes());
    Ok(())
}
fn put_option_u128(o: &mut Vec<u8>, value: Option<u128>) {
    match value {
        Some(v) => {
            o.push(1);
            put_u128(o, v);
        }
        None => o.push(0),
    }
}
fn put_option_i64(o: &mut Vec<u8>, value: Option<i64>) {
    match value {
        Some(v) => {
            o.push(1);
            put_i64(o, v);
        }
        None => o.push(0),
    }
}
fn put_f64(o: &mut Vec<u8>, value: f64, field: &'static str) -> Result<(), ProtocolError> {
    if !value.is_finite() {
        return Err(ProtocolError::InvalidField(field));
    }
    put_u64(o, value.to_bits());
    Ok(())
}
fn put_option_f64(
    o: &mut Vec<u8>,
    value: Option<f64>,
    field: &'static str,
) -> Result<(), ProtocolError> {
    match value {
        Some(v) => {
            o.push(1);
            put_f64(o, v, field)?;
        }
        None => o.push(0),
    }
    Ok(())
}
fn put_u16(o: &mut Vec<u8>, v: u16) {
    o.extend_from_slice(&v.to_be_bytes())
}
fn put_u32(o: &mut Vec<u8>, v: u32) {
    o.extend_from_slice(&v.to_be_bytes())
}
fn put_u64(o: &mut Vec<u8>, v: u64) {
    o.extend_from_slice(&v.to_be_bytes())
}
fn put_u128(o: &mut Vec<u8>, v: u128) {
    o.extend_from_slice(&v.to_be_bytes())
}
fn put_i64(o: &mut Vec<u8>, v: i64) {
    o.extend_from_slice(&v.to_be_bytes())
}
fn put_watermark(o: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            o.push(1);
            put_u64(o, value);
        }
        None => o.push(0),
    }
}
fn crc32(v: &[u8]) -> u32 {
    let mut h = Hasher::new();
    h.update(v);
    h.finalize()
}
fn boolean(i: &mut Input<'_>, field: &'static str) -> Result<bool, ProtocolError> {
    match i.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        value => Err(enum_error(field, value)),
    }
}
fn enum_error(field: &'static str, value: u8) -> ProtocolError {
    ProtocolError::InvalidEnumValue { field, value }
}
fn series_semantics_byte(v: SeriesSemantics) -> u8 {
    match v {
        SeriesSemantics::Gauge => 1,
        SeriesSemantics::IntervalTotal => 2,
        SeriesSemantics::Counter => 3,
        SeriesSemantics::State => 4,
        SeriesSemantics::Event => 5,
    }
}
fn series_semantics_from(v: u8) -> Result<SeriesSemantics, ProtocolError> {
    match v {
        1 => Ok(SeriesSemantics::Gauge),
        2 => Ok(SeriesSemantics::IntervalTotal),
        3 => Ok(SeriesSemantics::Counter),
        4 => Ok(SeriesSemantics::State),
        5 => Ok(SeriesSemantics::Event),
        _ => Err(enum_error("series semantics", v)),
    }
}
fn calendar_unit_byte(v: CalendarUnit) -> u8 {
    match v {
        CalendarUnit::Day => 1,
        CalendarUnit::Month => 2,
        CalendarUnit::Year => 3,
    }
}
fn calendar_unit_from(v: u8) -> Result<CalendarUnit, ProtocolError> {
    match v {
        1 => Ok(CalendarUnit::Day),
        2 => Ok(CalendarUnit::Month),
        3 => Ok(CalendarUnit::Year),
        _ => Err(enum_error("calendar unit", v)),
    }
}
fn run_kind_byte(v: RunKind) -> u8 {
    match v {
        RunKind::Forecast => 1,
        RunKind::Optimization => 2,
        RunKind::Import => 3,
        RunKind::Control => 4,
        RunKind::Reconciliation => 5,
    }
}
fn run_kind_from(v: u8) -> Result<RunKind, ProtocolError> {
    match v {
        1 => Ok(RunKind::Forecast),
        2 => Ok(RunKind::Optimization),
        3 => Ok(RunKind::Import),
        4 => Ok(RunKind::Control),
        5 => Ok(RunKind::Reconciliation),
        _ => Err(enum_error("run kind", v)),
    }
}
fn run_status_byte(v: RunStatus) -> u8 {
    match v {
        RunStatus::Pending => 1,
        RunStatus::Running => 2,
        RunStatus::Succeeded => 3,
        RunStatus::Failed => 4,
        RunStatus::Cancelled => 5,
    }
}
fn run_status_from(v: u8) -> Result<RunStatus, ProtocolError> {
    match v {
        1 => Ok(RunStatus::Pending),
        2 => Ok(RunStatus::Running),
        3 => Ok(RunStatus::Succeeded),
        4 => Ok(RunStatus::Failed),
        5 => Ok(RunStatus::Cancelled),
        _ => Err(enum_error("run status", v)),
    }
}
fn plan_status_byte(v: PlanStatus) -> u8 {
    match v {
        PlanStatus::Candidate => 1,
        PlanStatus::Approved => 2,
        PlanStatus::Deployed => 3,
        PlanStatus::Superseded => 4,
        PlanStatus::Cancelled => 5,
    }
}
fn plan_status_from(v: u8) -> Result<PlanStatus, ProtocolError> {
    match v {
        1 => Ok(PlanStatus::Candidate),
        2 => Ok(PlanStatus::Approved),
        3 => Ok(PlanStatus::Deployed),
        4 => Ok(PlanStatus::Superseded),
        5 => Ok(PlanStatus::Cancelled),
        _ => Err(enum_error("plan status", v)),
    }
}
fn status_byte(v: HealthStatus) -> u8 {
    match v {
        HealthStatus::Healthy => 1,
        HealthStatus::Degraded => 2,
        HealthStatus::Unavailable => 3,
    }
}
fn status_from(v: u8) -> Result<HealthStatus, ProtocolError> {
    match v {
        1 => Ok(HealthStatus::Healthy),
        2 => Ok(HealthStatus::Degraded),
        3 => Ok(HealthStatus::Unavailable),
        _ => Err(enum_error("health status", v)),
    }
}
fn put_sync_policy(o: &mut Vec<u8>, v: SyncPolicy) -> Result<(), ProtocolError> {
    match v {
        SyncPolicy::Always => {
            o.push(1);
            put_u64(o, 0);
        }
        SyncPolicy::Manual => {
            o.push(2);
            put_u64(o, 0);
        }
        SyncPolicy::EveryBytes(0) => {
            return Err(ProtocolError::InvalidField("sync every-bytes"));
        }
        SyncPolicy::EveryBytes(bytes) => {
            o.push(3);
            put_u64(o, bytes);
        }
    }
    Ok(())
}
fn sync_policy_from(i: &mut Input<'_>) -> Result<SyncPolicy, ProtocolError> {
    match i.u8()? {
        1 => {
            if i.u64()? != 0 {
                return Err(ProtocolError::InvalidField("sync every-bytes"));
            }
            Ok(SyncPolicy::Always)
        }
        2 => {
            if i.u64()? != 0 {
                return Err(ProtocolError::InvalidField("sync every-bytes"));
            }
            Ok(SyncPolicy::Manual)
        }
        3 => {
            let bytes = i.u64()?;
            if bytes == 0 {
                return Err(ProtocolError::InvalidField("sync every-bytes"));
            }
            Ok(SyncPolicy::EveryBytes(bytes))
        }
        value => Err(enum_error("sync policy", value)),
    }
}
fn error_byte(v: ErrorCode) -> u8 {
    match v {
        ErrorCode::InvalidRequest => 1,
        ErrorCode::Overloaded => 2,
        ErrorCode::Internal => 3,
        ErrorCode::Unsupported => 4,
        ErrorCode::IdempotencyConflict => 5,
    }
}
fn error_from(v: u8) -> Result<ErrorCode, ProtocolError> {
    match v {
        1 => Ok(ErrorCode::InvalidRequest),
        2 => Ok(ErrorCode::Overloaded),
        3 => Ok(ErrorCode::Internal),
        4 => Ok(ErrorCode::Unsupported),
        5 => Ok(ErrorCode::IdempotencyConflict),
        _ => Err(enum_error("error code", v)),
    }
}
fn read_exact<R: Read>(r: &mut R, b: &mut [u8], base: usize) -> Result<(), ProtocolError> {
    let mut n = 0;
    while n < b.len() {
        match r.read(&mut b[n..]) {
            Ok(0) => {
                return Err(ProtocolError::Truncated {
                    expected: base + b.len(),
                    actual: base + n,
                });
            }
            Ok(m) => n += m,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
struct Input<'a> {
    b: &'a [u8],
    p: usize,
}
impl<'a> Input<'a> {
    fn new(b: &'a [u8]) -> Self {
        Self { b, p: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], ProtocolError> {
        let e = self.p.checked_add(n).ok_or(ProtocolError::Truncated {
            expected: usize::MAX,
            actual: self.b.len(),
        })?;
        if e > self.b.len() {
            return Err(ProtocolError::Truncated {
                expected: e,
                actual: self.b.len(),
            });
        }
        let v = &self.b[self.p..e];
        self.p = e;
        Ok(v)
    }
    fn remaining(&self) -> usize {
        self.b.len().saturating_sub(self.p)
    }
    fn finish(&self) -> Result<(), ProtocolError> {
        if self.p == self.b.len() {
            Ok(())
        } else {
            Err(ProtocolError::TrailingBytes {
                count: self.b.len() - self.p,
            })
        }
    }
    fn u8(&mut self) -> Result<u8, ProtocolError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, ProtocolError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, ProtocolError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, ProtocolError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn u128(&mut self) -> Result<u128, ProtocolError> {
        Ok(u128::from_be_bytes(self.take(16)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64, ProtocolError> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn watermark(&mut self, field: &'static str) -> Result<Option<u64>, ProtocolError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u64()?)),
            value => Err(enum_error(field, value)),
        }
    }
    fn string(&mut self, max: usize, field: &'static str) -> Result<String, ProtocolError> {
        let n = self.u16()? as usize;
        if n == 0 || n > max {
            return Err(ProtocolError::InvalidField(field));
        }
        self.text_bytes(n, field)
    }
    fn text(&mut self, max: usize, field: &'static str) -> Result<String, ProtocolError> {
        let n = self.u16()? as usize;
        if n > max {
            return Err(ProtocolError::InvalidField(field));
        }
        self.text_bytes(n, field)
    }
    fn text_bytes(&mut self, n: usize, field: &'static str) -> Result<String, ProtocolError> {
        let s =
            std::str::from_utf8(self.take(n)?).map_err(|_| ProtocolError::InvalidField(field))?;
        Ok(s.into())
    }
    fn count(&mut self, maximum: usize, field: &'static str) -> Result<usize, ProtocolError> {
        let count = self.u32()? as usize;
        if count > maximum {
            Err(ProtocolError::InvalidField(field))
        } else {
            Ok(count)
        }
    }
    fn option_u128(&mut self, field: &'static str) -> Result<Option<u128>, ProtocolError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u128()?)),
            value => Err(enum_error(field, value)),
        }
    }
    fn option_i64(&mut self, field: &'static str) -> Result<Option<i64>, ProtocolError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.i64()?)),
            value => Err(enum_error(field, value)),
        }
    }
    fn f64(&mut self, field: &'static str) -> Result<f64, ProtocolError> {
        let value = f64::from_bits(self.u64()?);
        if value.is_finite() {
            Ok(value)
        } else {
            Err(ProtocolError::InvalidField(field))
        }
    }
    fn option_f64(&mut self, field: &'static str) -> Result<Option<f64>, ProtocolError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.f64(field)?)),
            value => Err(enum_error(field, value)),
        }
    }
    fn points(&mut self) -> Result<Vec<Point>, ProtocolError> {
        let n = self.u32()? as usize;
        if n > MAX_BATCH_POINTS {
            return Err(ProtocolError::InvalidField("too many points"));
        }
        let need = n
            .checked_mul(72)
            .ok_or(ProtocolError::InvalidField("point count"))?;
        if self.b.len() - self.p != need {
            return Err(ProtocolError::InvalidField("point payload length"));
        }
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(Point {
                series_id: self.u64()?,
                valid_time: self.i64()?,
                valid_time_end: self.i64()?,
                knowledge_time: self.i64()?,
                change_time: self.i64()?,
                run_id: self.u128()?,
                value: f64::from_bits(self.u64()?),
                quality: self.u32()?,
                flags: self.u32()?,
            })
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    fn metadata() -> (Entity, Relation, SeriesDefinition, Run, Plan) {
        let properties = BTreeMap::from([
            ("a".into(), PropertyValue::Null),
            ("b".into(), PropertyValue::Bool(true)),
            ("c".into(), PropertyValue::Integer(-2)),
            ("d".into(), PropertyValue::Float(1.5)),
            ("e".into(), PropertyValue::Text(String::new())),
        ]);
        let entity = Entity {
            id: EntityId(1),
            kind: "site".into(),
            name: "Alpha".into(),
            parent: None,
            valid_from: 10,
            valid_to: Some(20),
            properties,
        };
        let relation = Relation {
            id: RelationId(2),
            kind: "contains".into(),
            source: EntityId(1),
            target: EntityId(3),
            valid_from: 10,
            valid_to: None,
            properties: BTreeMap::new(),
        };
        let series = SeriesDefinition {
            id: 4,
            owner_entity: Some(EntityId(1)),
            owner_relation: None,
            name: "power".into(),
            physical_quantity: "power".into(),
            canonical_unit: "W".into(),
            semantics: SeriesSemantics::Gauge,
            maximum_gap_micros: Some(5),
            rollup_policy: RollupPolicy {
                raw_retain_for_micros: Some(100),
                tiers: vec![
                    RollupTier {
                        resolution: RollupResolution::FixedMicros(60),
                        retain_for_micros: None,
                    },
                    RollupTier {
                        resolution: RollupResolution::Calendar {
                            unit: CalendarUnit::Day,
                            iana_timezone: "UTC".into(),
                        },
                        retain_for_micros: Some(1_000),
                    },
                ],
            },
        };
        let run = Run {
            id: RunId(5),
            kind: RunKind::Forecast,
            status: RunStatus::Succeeded,
            created_at: 11,
            knowledge_time: 12,
            workflow: "wf".into(),
            model: String::new(),
            model_version: "v1".into(),
            parent_run: None,
            input_snapshot: None,
            attributes: BTreeMap::new(),
        };
        let plan = Plan {
            id: 6,
            run_id: RunId(5),
            status: PlanStatus::Candidate,
            horizon_start: 20,
            horizon_end: 80,
            resolution_micros: 60,
            scenario: "base".into(),
            objective_terms: BTreeMap::from([("cost".into(), 1.5)]),
            objective_value: Some(1.5),
            supersedes: None,
            attributes: BTreeMap::new(),
        };
        (entity, relation, series, run, plan)
    }
    fn batch() -> WireMessage {
        let (entity, relation, series, run, plan) = metadata();
        WireMessage::Request(Request::CommitBatch(CommitBatchRequest {
            source_id: 1,
            sequence: 2,
            commit_id: 3,
            entities: vec![entity],
            relations: vec![relation],
            series: vec![series],
            runs: vec![run],
            plans: vec![plan],
            points: vec![Point {
                series_id: 4,
                valid_time: 1_754_382_400_123_456,
                valid_time_end: 1_754_382_700_123_456,
                knowledge_time: 1_754_382_401_123_456,
                change_time: 1_754_382_402_123_456,
                run_id: 5,
                value: -12.5,
                quality: 7,
                flags: 8,
            }],
        }))
    }
    #[test]
    fn round_trip_full_point() {
        let m = batch();
        let frame = encode(&m).unwrap();
        assert_eq!(decode(&frame).unwrap(), m)
    }
    #[test]
    fn all_message_kinds_round_trip_with_frozen_tags() {
        let messages = vec![
            (
                1,
                WireMessage::Request(Request::Hello(HelloRequest {
                    source_id: 1,
                    node_id: "n".into(),
                    client_version: "v".into(),
                    capabilities: 1,
                })),
            ),
            (2, batch()),
            (
                3,
                WireMessage::Request(Request::Flush(FlushRequest {
                    source_id: 1,
                    through_sequence: 2,
                })),
            ),
            (
                4,
                WireMessage::Request(Request::Health(HealthRequest { nonce: 3 })),
            ),
            (
                128,
                WireMessage::Response(Response::Hello(HelloResponse {
                    selected_version: 1,
                    session_id: [4; 16],
                    server_time_micros: 5,
                })),
            ),
            (
                129,
                WireMessage::Response(Response::Ack(Ack {
                    kind: AckKind::CommitBatch,
                    source_id: 1,
                    sequence: 2,
                    commit_id: 3,
                    accepted_through_sequence: Some(2),
                    durable_through_sequence: None,
                    durable: false,
                    deduplicated: true,
                    frame_offset: 6,
                    records: 5,
                    points: 1,
                    bytes_written: 7,
                })),
            ),
            (
                130,
                WireMessage::Response(Response::Health(HealthResponse {
                    nonce: 3,
                    source_id: 1,
                    status: HealthStatus::Degraded,
                    queue_entries: 4,
                    accepted_through_sequence: Some(2),
                    durable_through_sequence: None,
                    overload_count: 0,
                    protocol_error_count: 0,
                    database_bytes: 0,
                    database_points: 0,
                    database_commits: 0,
                    recovered_tail_bytes: 0,
                    sync_policy: SyncPolicy::Always,
                    last_ack_durable: false,
                })),
            ),
            (
                131,
                WireMessage::Response(Response::Error(ErrorResponse {
                    code: ErrorCode::IdempotencyConflict,
                    retryable: false,
                    message: "conflict".into(),
                })),
            ),
        ];
        for (kind, message) in messages {
            let frame = encode(&message).unwrap();
            assert_eq!(frame[6], kind);
            assert_eq!(decode(&frame).unwrap(), message);
        }
    }
    #[test]
    fn enum_tags_are_frozen() {
        assert_eq!(
            [
                SeriesSemantics::Gauge,
                SeriesSemantics::IntervalTotal,
                SeriesSemantics::Counter,
                SeriesSemantics::State,
                SeriesSemantics::Event
            ]
            .map(series_semantics_byte),
            [1, 2, 3, 4, 5]
        );
        assert_eq!(
            [CalendarUnit::Day, CalendarUnit::Month, CalendarUnit::Year].map(calendar_unit_byte),
            [1, 2, 3]
        );
        assert_eq!(
            [
                RunKind::Forecast,
                RunKind::Optimization,
                RunKind::Import,
                RunKind::Control,
                RunKind::Reconciliation
            ]
            .map(run_kind_byte),
            [1, 2, 3, 4, 5]
        );
        assert_eq!(
            [
                RunStatus::Pending,
                RunStatus::Running,
                RunStatus::Succeeded,
                RunStatus::Failed,
                RunStatus::Cancelled
            ]
            .map(run_status_byte),
            [1, 2, 3, 4, 5]
        );
        assert_eq!(
            [
                PlanStatus::Candidate,
                PlanStatus::Approved,
                PlanStatus::Deployed,
                PlanStatus::Superseded,
                PlanStatus::Cancelled
            ]
            .map(plan_status_byte),
            [1, 2, 3, 4, 5]
        );
        assert_eq!(
            [
                ErrorCode::InvalidRequest,
                ErrorCode::Overloaded,
                ErrorCode::Internal,
                ErrorCode::Unsupported,
                ErrorCode::IdempotencyConflict
            ]
            .map(error_byte),
            [1, 2, 3, 4, 5]
        );
        let mut always = Vec::new();
        put_sync_policy(&mut always, SyncPolicy::Always).unwrap();
        let mut manual = Vec::new();
        put_sync_policy(&mut manual, SyncPolicy::Manual).unwrap();
        let mut every = Vec::new();
        put_sync_policy(&mut every, SyncPolicy::EveryBytes(64)).unwrap();
        assert_eq!(always[0], 1);
        assert_eq!(manual[0], 2);
        assert_eq!(every[0], 3);
    }
    #[test]
    fn health_response_decodes_the_legacy_prefix_without_ops_fields() {
        let frame = decode_hex(
            "465457530001820000000027112233445566778800112233445566778899aabbccddeeff02000000030101020304050607080053c62c77",
        );
        match decode(&frame).unwrap() {
            WireMessage::Response(Response::Health(health)) => {
                assert_eq!(health.nonce, 0x1122_3344_5566_7788);
                assert_eq!(health.queue_entries, 3);
                assert_eq!(
                    health.accepted_through_sequence,
                    Some(0x0102_0304_0506_0708)
                );
                assert_eq!(health.durable_through_sequence, None);
                assert_eq!(health.overload_count, 0);
                assert_eq!(health.protocol_error_count, 0);
                assert_eq!(health.sync_policy, SyncPolicy::Always);
                assert!(!health.last_ack_durable);
            }
            other => panic!("expected health, got {other:?}"),
        }
    }
    fn decode_hex(text: &str) -> Vec<u8> {
        let text = text.trim();
        text.as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let digits = std::str::from_utf8(pair).unwrap();
                u8::from_str_radix(digits, 16).unwrap()
            })
            .collect()
    }
    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
    #[test]
    fn frozen_metadata_encodings() {
        let (entity, relation, series, run, plan) = metadata();
        let mut encoded = Vec::new();
        encode_entity(&mut encoded, &entity).unwrap();
        assert_eq!(
            hex(&encoded),
            "000000000000000000000000000000010004736974650005416c70686100000000000000000a0100000000000000140000000500016100000162010100016302fffffffffffffffe000164033ff8000000000000000165040000"
        );
        encoded.clear();
        encode_relation(&mut encoded, &relation).unwrap();
        assert_eq!(
            hex(&encoded),
            "000000000000000000000000000000020008636f6e7461696e730000000000000000000000000000000100000000000000000000000000000003000000000000000a0000000000"
        );
        encoded.clear();
        encode_series(&mut encoded, &series).unwrap();
        assert_eq!(
            hex(&encoded),
            "00000000000000040100000000000000000000000000000001000005706f7765720005706f776572000157010100000000000000050100000000000000640000000201000000000000003c00020100035554430100000000000003e8"
        );
        encoded.clear();
        encode_run(&mut encoded, &run).unwrap();
        assert_eq!(
            hex(&encoded),
            "000000000000000000000000000000050103000000000000000b000000000000000c00027766000000027631000000000000"
        );
        encoded.clear();
        encode_plan(&mut encoded, &plan).unwrap();
        assert_eq!(
            hex(&encoded),
            "00000000000000000000000000000006000000000000000000000000000000050100000000000000140000000000000050000000000000003c000462617365000000010004636f73743ff8000000000000013ff80000000000000000000000"
        );
    }
    #[test]
    fn stream_round_trip() {
        let m = batch();
        let mut b = Vec::new();
        write_to(&mut b, &m).unwrap();
        assert_eq!(read_from(&mut Cursor::new(b)).unwrap(), m)
    }
    #[test]
    fn rejects_corruption() {
        let mut b = encode(&batch()).unwrap();
        b[HEADER_BYTES + 50] ^= 1;
        assert!(matches!(
            decode(&b),
            Err(ProtocolError::ChecksumMismatch { .. })
        ))
    }
    #[test]
    fn bounds() {
        let mut b = vec![0; HEADER_BYTES];
        b[..4].copy_from_slice(&FRAME_MAGIC);
        b[4..6].copy_from_slice(&PROTOCOL_VERSION.to_be_bytes());
        b[6] = HEALTH_REQUEST;
        b[8..12].copy_from_slice(&((MAX_PAYLOAD_BYTES + 1) as u32).to_be_bytes());
        assert!(matches!(
            read_from(&mut Cursor::new(b)),
            Err(ProtocolError::FrameTooLarge { .. })
        ));
        let mut v = match batch() {
            WireMessage::Request(Request::CommitBatch(v)) => v,
            _ => unreachable!(),
        };
        v.points = vec![v.points[0]; MAX_BATCH_POINTS + 1];
        assert!(encode(&WireMessage::Request(Request::CommitBatch(v))).is_err())
    }
    #[test]
    fn adversarial_counts_lengths_and_trailing_bytes_are_rejected() {
        let mut payload = Vec::new();
        put_u128(&mut payload, 1);
        put_u64(&mut payload, 1);
        put_u128(&mut payload, 1);
        put_u32(&mut payload, (MAX_METADATA_RECORDS + 1) as u32);
        assert!(matches!(
            decode_payload(COMMIT_BATCH_REQUEST, &payload),
            Err(ProtocolError::InvalidField("metadata record count"))
        ));

        let mut oversized_name = 1_u128.to_be_bytes().to_vec();
        oversized_name.extend_from_slice(&[0, 129]);
        assert!(matches!(
            decode_payload(HELLO_REQUEST, &oversized_name),
            Err(ProtocolError::InvalidField("node_id"))
        ));

        let mut health = 1_u64.to_be_bytes().to_vec();
        health.push(0);
        assert!(matches!(
            decode_payload(HEALTH_REQUEST, &health),
            Err(ProtocolError::TrailingBytes { count: 1 })
        ));

        let mut properties = Vec::new();
        put_u32(&mut properties, 2);
        string(&mut properties, "b", MAX_KEY_BYTES, "key").unwrap();
        properties.push(0);
        string(&mut properties, "a", MAX_KEY_BYTES, "key").unwrap();
        properties.push(0);
        assert!(matches!(
            decode_properties(&mut Input::new(&properties)),
            Err(ProtocolError::InvalidField("property keys"))
        ));

        let (mut entity, _, _, _, _) = metadata();
        entity
            .properties
            .insert("z".into(), PropertyValue::Float(f64::NAN));
        assert!(matches!(
            encode_entity(&mut Vec::new(), &entity),
            Err(ProtocolError::InvalidField("property float"))
        ));
    }
    #[test]
    fn bad_header_rejected_before_body() {
        for (at, value, expected) in [(4, 2, "version"), (6, 99, "type"), (7, 1, "reserved")] {
            let mut h = [0; HEADER_BYTES];
            h[..4].copy_from_slice(&FRAME_MAGIC);
            h[4..6].copy_from_slice(&PROTOCOL_VERSION.to_be_bytes());
            h[6] = HEALTH_REQUEST;
            h[at] = value;
            let e = read_from(&mut Cursor::new(h)).unwrap_err();
            assert!(e.to_string().contains(expected));
        }
    }
    #[test]
    fn frozen_golden_health_frame() {
        let m = WireMessage::Request(Request::Health(HealthRequest {
            nonce: 0x0102_0304_0506_0708,
        }));
        let expected: Vec<u8> = vec![
            0x46, 0x54, 0x57, 0x53, 0, 1, 4, 0, 0, 0, 0, 8, 1, 2, 3, 4, 5, 6, 7, 8, 36, 198, 92,
            216,
        ];
        assert_eq!(encode(&m).unwrap(), expected);
        assert_eq!(decode(&expected).unwrap(), m)
    }
}
