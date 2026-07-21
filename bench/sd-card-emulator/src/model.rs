use crate::config::{IoProfile, PROBABILITY_SCALE, Profile};
use crate::{COMMIT, VERSION};
use serde::Serialize;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceStatus {
    Online,
    ReadOnly,
    Offline,
}

#[derive(Clone, Debug, Serialize)]
pub struct Stats {
    pub schema_version: &'static str,
    pub emulator_version: &'static str,
    pub emulator_commit: &'static str,
    pub profile: String,
    pub seed: u64,
    pub started_unix_ms: u128,
    pub status: DeviceStatus,
    pub generation: u64,
    pub operations: u64,
    pub read_operations: u64,
    pub write_operations: u64,
    pub flush_operations: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub persisted_bytes: u64,
    pub write_amplification: f64,
    pub cached_operations: u64,
    pub cached_bytes: u64,
    pub latency_spikes: u64,
    pub injected_eio_operations: u64,
    pub injected_torn_operations: u64,
    pub injected_torn_bytes: u64,
    pub injected_corruptions: u64,
    pub false_flushes: u64,
    pub power_losses: u64,
    pub dropped_operations: u64,
    pub dropped_bytes: u64,
    pub torn_operations: u64,
    pub torn_bytes: u64,
    pub reordered_operations: u64,
    pub erase_operations: u64,
    pub max_erase_count: u64,
    pub bad_blocks: u64,
    pub last_fault_kind: Option<String>,
    pub last_fault_operation: Option<u64>,
    pub last_fault_offset: Option<u64>,
}

impl Stats {
    fn new(profile: &Profile, seed: u64) -> Self {
        Self {
            schema_version: "ftw-sd-emulator-stats-v1",
            emulator_version: VERSION,
            emulator_commit: COMMIT,
            profile: profile.name.clone(),
            seed,
            started_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            status: DeviceStatus::Online,
            generation: 0,
            operations: 0,
            read_operations: 0,
            write_operations: 0,
            flush_operations: 0,
            read_bytes: 0,
            write_bytes: 0,
            persisted_bytes: 0,
            write_amplification: 0.0,
            cached_operations: 0,
            cached_bytes: 0,
            latency_spikes: 0,
            injected_eio_operations: 0,
            injected_torn_operations: 0,
            injected_torn_bytes: 0,
            injected_corruptions: 0,
            false_flushes: 0,
            power_losses: 0,
            dropped_operations: 0,
            dropped_bytes: 0,
            torn_operations: 0,
            torn_bytes: 0,
            reordered_operations: 0,
            erase_operations: 0,
            max_erase_count: 0,
            bad_blocks: 0,
            last_fault_kind: None,
            last_fault_operation: None,
            last_fault_offset: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorCode {
    Io,
    Invalid,
    NoSpace,
    ReadOnly,
    Disconnected,
}

impl ErrorCode {
    #[must_use]
    pub const fn errno(self) -> u32 {
        match self {
            Self::Io | Self::Disconnected => 5,
            Self::Invalid => 22,
            Self::NoSpace => 28,
            Self::ReadOnly => 30,
        }
    }
}

#[derive(Debug)]
pub struct DeviceError {
    pub code: ErrorCode,
    message: String,
}

impl DeviceError {
    fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn from_code(code: ErrorCode, message: impl Into<String>) -> Self {
        Self::new(code, message)
    }

    fn io(context: &str, error: io::Error) -> Self {
        Self::new(ErrorCode::Io, format!("{context}: {error}"))
    }

    #[must_use]
    pub const fn disconnects(&self) -> bool {
        matches!(self.code, ErrorCode::Disconnected)
    }
}

impl std::fmt::Display for DeviceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for DeviceError {}

#[derive(Clone, Debug)]
struct PendingWrite {
    offset: u64,
    data: Vec<u8>,
}

#[derive(Debug)]
struct Pacer {
    next_ready: Instant,
}

impl Pacer {
    fn new() -> Self {
        Self {
            next_ready: Instant::now(),
        }
    }

    fn wait(&mut self, profile: &IoProfile, bytes: usize, added_latency_us: u64) {
        let transfer_us = if profile.bandwidth_bytes_per_second == 0 {
            0
        } else {
            (bytes as u128 * 1_000_000_u128)
                .div_ceil(profile.bandwidth_bytes_per_second as u128)
                .min(u64::MAX as u128) as u64
        };
        let iop_us = if profile.iops == 0 {
            0
        } else {
            1_000_000_u64.div_ceil(profile.iops)
        };
        let service = Duration::from_micros(transfer_us.max(iop_us));
        let now = Instant::now();
        let start = self.next_ready.max(now);
        self.next_ready = start.checked_add(service).unwrap_or(start);
        let queued = start.saturating_duration_since(now);
        let latency = Duration::from_micros(added_latency_us);
        let wait = queued.saturating_add(service).saturating_add(latency);
        if !wait.is_zero() {
            thread::sleep(wait);
        }
    }
}

#[derive(Clone, Debug)]
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
        if upper == 0 { 0 } else { self.next() % upper }
    }

    fn chance(&mut self, probability_ppm: u32) -> bool {
        probability_ppm != 0 && self.below(PROBABILITY_SCALE as u64) < probability_ppm as u64
    }
}

pub struct SdCard {
    profile: Profile,
    file: File,
    rng: XorShift64,
    pending: VecDeque<PendingWrite>,
    wear_counts: Vec<u64>,
    bad_blocks: Vec<bool>,
    status: DeviceStatus,
    generation: u64,
    read_pacer: Pacer,
    write_pacer: Pacer,
    stats: Stats,
}

impl SdCard {
    pub fn open(path: impl AsRef<Path>, profile: Profile, seed: u64) -> Result<Self, DeviceError> {
        profile
            .validate()
            .map_err(|error| DeviceError::new(ErrorCode::Invalid, error.to_string()))?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .map_err(|error| DeviceError::io("open backing file", error))?;
        let current_size = file
            .metadata()
            .map_err(|error| DeviceError::io("inspect backing file", error))?
            .len();
        if current_size == 0 {
            file.set_len(profile.device.size_bytes)
                .map_err(|error| DeviceError::io("size backing file", error))?;
        } else if current_size != profile.device.size_bytes {
            return Err(DeviceError::new(
                ErrorCode::Invalid,
                format!(
                    "backing file has {current_size} bytes; profile needs {}",
                    profile.device.size_bytes
                ),
            ));
        }

        let erase_blocks = profile
            .device
            .size_bytes
            .div_ceil(profile.device.erase_block_bytes);
        let erase_blocks = usize::try_from(erase_blocks)
            .map_err(|_| DeviceError::new(ErrorCode::Invalid, "erase-block map is too large"))?;
        Ok(Self {
            stats: Stats::new(&profile, seed),
            profile,
            file,
            rng: XorShift64::new(seed),
            pending: VecDeque::new(),
            wear_counts: vec![0; erase_blocks],
            bad_blocks: vec![false; erase_blocks],
            status: DeviceStatus::Online,
            generation: 0,
            read_pacer: Pacer::new(),
            write_pacer: Pacer::new(),
        })
    }

    #[must_use]
    pub const fn size(&self) -> u64 {
        self.profile.device.size_bytes
    }

    #[must_use]
    pub const fn logical_block_bytes(&self) -> u64 {
        self.profile.device.logical_block_bytes
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn status(&self) -> DeviceStatus {
        self.status
    }

    #[must_use]
    pub fn stats(&self) -> Stats {
        let mut stats = self.stats.clone();
        stats.status = self.status;
        stats.generation = self.generation;
        stats.cached_operations = self.pending.len() as u64;
        stats.cached_bytes = self
            .pending
            .iter()
            .map(|write| write.data.len() as u64)
            .sum();
        stats.write_amplification = if stats.write_bytes == 0 {
            0.0
        } else {
            stats.persisted_bytes as f64 / stats.write_bytes as f64
        };
        stats
    }

    pub fn read(&mut self, offset: u64, length: usize) -> Result<Vec<u8>, DeviceError> {
        self.check_range(offset, length)?;
        self.begin_operation(offset)?;
        if self.touches_bad_block(offset, length) {
            return self.injected_error("bad_block_read", offset);
        }
        self.wait_for_io(false, length);
        let mut data = vec![0; length];
        self.read_backing(offset, &mut data)?;
        for write in &self.pending {
            overlay(offset, &mut data, write);
        }
        self.stats.read_operations += 1;
        self.stats.read_bytes += length as u64;
        Ok(data)
    }

    pub fn write(&mut self, offset: u64, mut data: Vec<u8>, fua: bool) -> Result<(), DeviceError> {
        self.check_range(offset, data.len())?;
        let operation = self.begin_operation(offset)?;
        if self.status == DeviceStatus::ReadOnly {
            return Err(DeviceError::new(ErrorCode::ReadOnly, "device is read-only"));
        }
        if self.touches_bad_block(offset, data.len()) {
            return self.injected_error("bad_block_write", offset);
        }
        self.wait_for_io(true, data.len());
        self.apply_wear(offset, data.len())?;

        if self
            .rng
            .chance(self.profile.faults.torn_write_probability_ppm)
        {
            let torn_length = torn_length(&mut self.rng, data.len());
            self.persist(offset, &data[..torn_length])?;
            self.file
                .sync_data()
                .map_err(|error| DeviceError::io("sync torn write", error))?;
            self.stats.injected_torn_operations += 1;
            self.stats.injected_torn_bytes += torn_length as u64;
            self.record_fault("torn_write", operation, offset + torn_length as u64);
            return Err(DeviceError::new(
                ErrorCode::Io,
                "injected torn write returned EIO",
            ));
        }

        if self
            .rng
            .chance(self.profile.faults.silent_corruption_probability_ppm)
            && !data.is_empty()
        {
            let index = self.rng.below(data.len() as u64) as usize;
            let bit = 1_u8 << self.rng.below(8) as u8;
            data[index] ^= bit;
            self.stats.injected_corruptions += 1;
            self.record_fault("silent_corruption", operation, offset + index as u64);
        }

        self.stats.write_operations += 1;
        self.stats.write_bytes += data.len() as u64;
        if self.profile.cache.enabled {
            if fua
                && !self
                    .rng
                    .chance(self.profile.cache.false_flush_probability_ppm)
            {
                self.persist_pending()?;
                self.persist(offset, &data)?;
                self.file
                    .sync_data()
                    .map_err(|error| DeviceError::io("sync FUA write", error))?;
            } else {
                if fua {
                    self.stats.false_flushes += 1;
                    self.record_fault("false_fua", operation, offset);
                }
                self.pending.push_back(PendingWrite { offset, data });
                self.evict_cache()?;
            }
        } else {
            self.persist(offset, &data)?;
            self.file
                .sync_data()
                .map_err(|error| DeviceError::io("sync write", error))?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), DeviceError> {
        let operation = self.begin_operation(0)?;
        self.wait_for_io(true, 0);
        self.stats.flush_operations += 1;
        if self
            .rng
            .chance(self.profile.cache.false_flush_probability_ppm)
        {
            self.stats.false_flushes += 1;
            self.record_fault("false_flush", operation, 0);
            return Ok(());
        }
        self.persist_pending()?;
        self.file
            .sync_data()
            .map_err(|error| DeviceError::io("flush backing file", error))?;
        Ok(())
    }

    pub fn power_loss(&mut self) -> Result<(), DeviceError> {
        self.apply_power_loss("power_loss")
    }

    pub fn detach(&mut self) -> Result<(), DeviceError> {
        self.apply_power_loss("device_disappeared")
    }

    pub fn reset(&mut self) {
        self.status = DeviceStatus::Online;
        self.generation = self.generation.wrapping_add(1);
    }

    pub fn set_read_only(&mut self, read_only: bool) {
        self.status = if read_only {
            DeviceStatus::ReadOnly
        } else {
            DeviceStatus::Online
        };
        self.generation = self.generation.wrapping_add(1);
    }

    fn begin_operation(&mut self, offset: u64) -> Result<u64, DeviceError> {
        if self.status == DeviceStatus::Offline {
            return Err(DeviceError::new(
                ErrorCode::Disconnected,
                "device is offline",
            ));
        }
        self.stats.operations += 1;
        let operation = self.stats.operations;
        if self.profile.faults.power_loss_after_ops == Some(operation) {
            self.apply_power_loss("scheduled_power_loss")?;
            return Err(DeviceError::new(
                ErrorCode::Disconnected,
                "scheduled power loss",
            ));
        }
        if self.profile.faults.disappear_after_ops == Some(operation) {
            self.apply_power_loss("scheduled_disappearance")?;
            return Err(DeviceError::new(
                ErrorCode::Disconnected,
                "scheduled device disappearance",
            ));
        }
        if self.profile.faults.read_only_after_ops == Some(operation) {
            self.status = DeviceStatus::ReadOnly;
            self.record_fault("scheduled_read_only", operation, offset);
        }
        if self.rng.chance(self.profile.faults.eio_probability_ppm) {
            return self.injected_error("random_eio", offset);
        }
        Ok(operation)
    }

    fn apply_power_loss(&mut self, kind: &str) -> Result<(), DeviceError> {
        let operation = self.stats.operations;
        let pending = std::mem::take(&mut self.pending);
        let mut writes = Vec::new();
        for write in pending {
            let roll = self.rng.below(PROBABILITY_SCALE as u64) as u32;
            if roll < self.cache_persist_probability() {
                writes.push(write);
            } else if roll
                < self
                    .cache_persist_probability()
                    .saturating_add(self.profile.cache.power_loss_torn_probability_ppm)
            {
                let length = torn_length(&mut self.rng, write.data.len());
                if length != 0 {
                    self.persist(write.offset, &write.data[..length])?;
                }
                self.stats.torn_operations += 1;
                self.stats.torn_bytes += length as u64;
                self.stats.dropped_bytes += (write.data.len() - length) as u64;
                self.stats.last_fault_offset = Some(write.offset + length as u64);
            } else {
                self.stats.dropped_operations += 1;
                self.stats.dropped_bytes += write.data.len() as u64;
            }
        }

        if writes.len() > 1
            && self
                .rng
                .chance(self.profile.cache.power_loss_reorder_probability_ppm)
        {
            shuffle(&mut writes, &mut self.rng);
            self.stats.reordered_operations += writes.len() as u64;
        }
        for write in writes {
            self.persist(write.offset, &write.data)?;
        }
        self.file
            .sync_data()
            .map_err(|error| DeviceError::io("sync power-loss outcome", error))?;
        self.stats.power_losses += 1;
        self.status = DeviceStatus::Offline;
        self.generation = self.generation.wrapping_add(1);
        self.record_fault(kind, operation, self.stats.last_fault_offset.unwrap_or(0));
        Ok(())
    }

    const fn cache_persist_probability(&self) -> u32 {
        self.profile.cache.power_loss_persist_probability_ppm
    }

    fn evict_cache(&mut self) -> Result<(), DeviceError> {
        let mut evicted = false;
        while self.pending_bytes() > self.profile.cache.max_bytes {
            let Some(write) = self.pending.pop_front() else {
                break;
            };
            self.persist(write.offset, &write.data)?;
            evicted = true;
        }
        if evicted {
            self.file
                .sync_data()
                .map_err(|error| DeviceError::io("sync evicted cache", error))?;
        }
        Ok(())
    }

    fn pending_bytes(&self) -> u64 {
        self.pending
            .iter()
            .map(|write| write.data.len() as u64)
            .sum()
    }

    fn persist_pending(&mut self) -> Result<(), DeviceError> {
        let pending = std::mem::take(&mut self.pending);
        for write in pending {
            self.persist(write.offset, &write.data)?;
        }
        Ok(())
    }

    fn wait_for_io(&mut self, write: bool, bytes: usize) {
        let profile = if write {
            self.profile.write.clone()
        } else {
            self.profile.read.clone()
        };
        let mut latency = profile.base_latency_us;
        latency = latency.saturating_add(self.rng.below(profile.jitter_latency_us + 1));
        if self.rng.chance(profile.spike_probability_ppm) {
            let width = profile
                .spike_latency_us_max
                .saturating_sub(profile.spike_latency_us_min)
                .saturating_add(1);
            latency = latency.saturating_add(
                profile
                    .spike_latency_us_min
                    .saturating_add(self.rng.below(width)),
            );
            self.stats.latency_spikes += 1;
        }
        if self.profile.wear.enabled
            && self.stats.max_erase_count >= self.profile.wear.warning_cycles
        {
            latency = latency.saturating_add(profile.base_latency_us.saturating_mul(4));
        }
        if write {
            self.write_pacer.wait(&profile, bytes, latency);
        } else {
            self.read_pacer.wait(&profile, bytes, latency);
        }
    }

    fn apply_wear(&mut self, offset: u64, length: usize) -> Result<(), DeviceError> {
        if !self.profile.wear.enabled || length == 0 {
            return Ok(());
        }
        let (first, last) = self.erase_block_range(offset, length);
        for block in first..=last {
            let count =
                self.wear_counts[block].saturating_add(self.profile.wear.accelerated_factor);
            self.wear_counts[block] = count;
            self.stats.erase_operations = self
                .stats
                .erase_operations
                .saturating_add(self.profile.wear.accelerated_factor);
            self.stats.max_erase_count = self.stats.max_erase_count.max(count);
            if count >= self.profile.wear.failure_cycles && !self.bad_blocks[block] {
                self.bad_blocks[block] = true;
                self.stats.bad_blocks += 1;
            }
        }
        if self.stats.bad_blocks >= self.profile.wear.read_only_after_bad_blocks
            && self.profile.wear.read_only_after_bad_blocks != 0
        {
            self.status = DeviceStatus::ReadOnly;
            self.record_fault("wear_read_only", self.stats.operations, offset);
            return Err(DeviceError::new(
                ErrorCode::ReadOnly,
                "wear threshold made the device read-only",
            ));
        }
        if self.stats.max_erase_count >= self.profile.wear.warning_cycles
            && self.rng.chance(self.profile.wear.worn_eio_probability_ppm)
        {
            return self.injected_error("worn_block_eio", offset);
        }
        Ok(())
    }

    fn touches_bad_block(&self, offset: u64, length: usize) -> bool {
        if length == 0 {
            return false;
        }
        let (first, last) = self.erase_block_range(offset, length);
        self.bad_blocks[first..=last].iter().any(|bad| *bad)
    }

    fn erase_block_range(&self, offset: u64, length: usize) -> (usize, usize) {
        let size = self.profile.device.erase_block_bytes;
        let first = (offset / size) as usize;
        let last = ((offset + length as u64 - 1) / size) as usize;
        (first, last)
    }

    fn check_range(&self, offset: u64, length: usize) -> Result<(), DeviceError> {
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| DeviceError::new(ErrorCode::Invalid, "request range overflows u64"))?;
        if end > self.profile.device.size_bytes {
            return Err(DeviceError::new(
                ErrorCode::NoSpace,
                format!("request ends at {end}, past device size {}", self.size()),
            ));
        }
        Ok(())
    }

    fn read_backing(&mut self, offset: u64, data: &mut [u8]) -> Result<(), DeviceError> {
        self.file
            .seek(SeekFrom::Start(offset))
            .and_then(|_| self.file.read_exact(data))
            .map_err(|error| DeviceError::io("read backing file", error))
    }

    fn persist(&mut self, offset: u64, data: &[u8]) -> Result<(), DeviceError> {
        self.file
            .seek(SeekFrom::Start(offset))
            .and_then(|_| self.file.write_all(data))
            .map_err(|error| DeviceError::io("write backing file", error))?;
        self.stats.persisted_bytes += data.len() as u64;
        Ok(())
    }

    fn injected_error<T>(&mut self, kind: &str, offset: u64) -> Result<T, DeviceError> {
        self.stats.injected_eio_operations += 1;
        self.record_fault(kind, self.stats.operations, offset);
        Err(DeviceError::new(ErrorCode::Io, format!("injected {kind}")))
    }

    fn record_fault(&mut self, kind: &str, operation: u64, offset: u64) {
        self.stats.last_fault_kind = Some(kind.to_owned());
        self.stats.last_fault_operation = Some(operation);
        self.stats.last_fault_offset = Some(offset);
    }
}

fn overlay(read_offset: u64, data: &mut [u8], write: &PendingWrite) {
    let read_end = read_offset + data.len() as u64;
    let write_end = write.offset + write.data.len() as u64;
    let start = read_offset.max(write.offset);
    let end = read_end.min(write_end);
    if start >= end {
        return;
    }
    let read_start = (start - read_offset) as usize;
    let write_start = (start - write.offset) as usize;
    let length = (end - start) as usize;
    data[read_start..read_start + length]
        .copy_from_slice(&write.data[write_start..write_start + length]);
}

fn torn_length(rng: &mut XorShift64, length: usize) -> usize {
    match length {
        0 | 1 => 0,
        _ => 1 + rng.below((length - 1) as u64) as usize,
    }
}

fn shuffle<T>(values: &mut [T], rng: &mut XorShift64) {
    for index in (1..values.len()).rev() {
        let other = rng.below((index + 1) as u64) as usize;
        values.swap(index, other);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CacheProfile, DeviceProfile, FaultProfile, IoProfile, WearProfile};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

    fn profile() -> Profile {
        Profile {
            schema_version: 1,
            name: "test".to_owned(),
            device: DeviceProfile {
                size_bytes: 64 * 1024,
                logical_block_bytes: 512,
                erase_block_bytes: 4096,
            },
            read: no_delay(),
            write: no_delay(),
            cache: CacheProfile {
                enabled: true,
                max_bytes: 16 * 1024,
                false_flush_probability_ppm: 0,
                power_loss_persist_probability_ppm: 0,
                power_loss_torn_probability_ppm: 0,
                power_loss_reorder_probability_ppm: 0,
            },
            wear: WearProfile {
                enabled: false,
                accelerated_factor: 1,
                warning_cycles: 100,
                failure_cycles: 200,
                worn_eio_probability_ppm: 0,
                read_only_after_bad_blocks: 0,
            },
            faults: FaultProfile {
                eio_probability_ppm: 0,
                torn_write_probability_ppm: 0,
                silent_corruption_probability_ppm: 0,
                power_loss_after_ops: None,
                read_only_after_ops: None,
                disappear_after_ops: None,
            },
        }
    }

    fn no_delay() -> IoProfile {
        IoProfile {
            bandwidth_bytes_per_second: 0,
            iops: 0,
            base_latency_us: 0,
            jitter_latency_us: 0,
            spike_probability_ppm: 0,
            spike_latency_us_min: 0,
            spike_latency_us_max: 0,
        }
    }

    fn temporary_file(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ftw-sd-emulator-{label}-{}-{}",
            std::process::id(),
            NEXT_FILE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn flush_moves_cached_data_to_the_backing_file() {
        let path = temporary_file("flush");
        let mut card = SdCard::open(&path, profile(), 42).unwrap();
        card.write(1024, vec![1, 2, 3, 4], false).unwrap();
        assert_eq!(card.read(1024, 4).unwrap(), [1, 2, 3, 4]);
        let mut raw = fs::read(&path).unwrap();
        assert_eq!(&raw[1024..1028], &[0, 0, 0, 0]);
        card.flush().unwrap();
        raw = fs::read(&path).unwrap();
        assert_eq!(&raw[1024..1028], &[1, 2, 3, 4]);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn fua_preserves_order_with_older_overlapping_cached_writes() {
        let path = temporary_file("fua-order");
        let mut card = SdCard::open(&path, profile(), 43).unwrap();
        card.write(1024, vec![1; 512], false).unwrap();
        card.write(1024, vec![2; 512], true).unwrap();

        assert_eq!(card.read(1024, 512).unwrap(), vec![2; 512]);
        card.power_loss().unwrap();
        card.reset();
        assert_eq!(card.read(1024, 512).unwrap(), vec![2; 512]);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn false_flush_can_lose_acknowledged_data_on_power_loss() {
        let path = temporary_file("false-flush");
        let mut settings = profile();
        settings.cache.false_flush_probability_ppm = PROBABILITY_SCALE;
        let mut card = SdCard::open(&path, settings, 7).unwrap();
        card.write(0, vec![9; 512], false).unwrap();
        card.flush().unwrap();
        card.power_loss().unwrap();
        card.reset();
        assert_eq!(card.read(0, 512).unwrap(), vec![0; 512]);
        let stats = card.stats();
        assert_eq!(stats.false_flushes, 1);
        assert_eq!(stats.dropped_operations, 1);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn explicit_torn_write_returns_eio_and_persists_only_a_prefix() {
        let path = temporary_file("torn");
        let mut settings = profile();
        settings.faults.torn_write_probability_ppm = PROBABILITY_SCALE;
        let mut card = SdCard::open(&path, settings, 11).unwrap();
        let error = card.write(0, vec![5; 512], false).unwrap_err();
        assert_eq!(error.code, ErrorCode::Io);
        let raw = fs::read(&path).unwrap();
        let written = raw[..512].iter().take_while(|byte| **byte == 5).count();
        assert!(written > 0 && written < 512);
        assert_eq!(card.stats().injected_torn_operations, 1);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn wear_can_make_the_device_read_only() {
        let path = temporary_file("wear");
        let mut settings = profile();
        settings.cache.enabled = false;
        settings.wear.enabled = true;
        settings.wear.warning_cycles = 1;
        settings.wear.failure_cycles = 2;
        settings.wear.read_only_after_bad_blocks = 1;
        let mut card = SdCard::open(&path, settings, 13).unwrap();
        card.write(0, vec![1; 512], false).unwrap();
        let error = card.write(0, vec![2; 512], false).unwrap_err();
        assert_eq!(error.code, ErrorCode::ReadOnly);
        assert_eq!(card.status(), DeviceStatus::ReadOnly);
        assert_eq!(card.stats().bad_blocks, 1);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn scheduled_power_loss_is_reproducible() {
        let mut settings = profile();
        settings.faults.power_loss_after_ops = Some(2);
        for suffix in ["scheduled-a", "scheduled-b"] {
            let path = temporary_file(suffix);
            let mut card = SdCard::open(&path, settings.clone(), 99).unwrap();
            card.write(0, vec![1; 512], false).unwrap();
            let error = card.write(512, vec![2; 512], false).unwrap_err();
            assert!(error.disconnects());
            let stats = card.stats();
            assert_eq!(stats.last_fault_operation, Some(2));
            assert_eq!(stats.dropped_operations, 1);
            fs::remove_file(path).unwrap();
        }
    }
}
