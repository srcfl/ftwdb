use crate::manifest::{Manifest, RollupDescriptor};
use crate::rollup::calendar_bucket_bounds;
use crate::transaction::Record;
use crate::{
    CalendarGaugeRollup, Commit, Config, Database, Error, FixedGaugeRollup, GaugeBucket, Point,
    Result, RollupResolution, RollupSegment, SeriesSemantics, Transaction,
};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

const ACTIVE_LOG: &str = "active.wlog";
const MANIFEST_DIRECTORY: &str = "manifests";
const ROLLUP_DIRECTORY: &str = "rollups";
const UTC_DAY_MICROS: i64 = 86_400_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RollupSource {
    Materialized,
    Hybrid,
    Raw,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RollupQuery {
    pub buckets: Vec<GaugeBucket>,
    pub source: RollupSource,
    pub manifest_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionGate {
    pub series_id: u64,
    pub raw_before: i64,
    pub eligible: bool,
    pub reason: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MaintenanceReport {
    pub manifest_generation: u64,
    pub rollup_files_written: usize,
    pub rollup_buckets_written: u64,
    pub rollup_bytes_written: u64,
    pub retention_gates: Vec<RetentionGate>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IntegrityReport {
    pub manifest_generation: u64,
    pub raw_points: u64,
    pub raw_commits: u64,
    pub active_rollup_files: usize,
    pub active_rollup_buckets: u64,
    pub active_rollup_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BackupReport {
    pub files: usize,
    pub bytes: u64,
    pub manifest_generation: u64,
}

/// A directory-level WattDB store with a commit log and durable rollup
/// generations. Maintenance is explicit so an embedded caller can schedule it
/// around flash, CPU, and power constraints.
pub struct Store {
    root: PathBuf,
    rollup_directory: PathBuf,
    manifest_directory: PathBuf,
    database: Database,
    manifest: Manifest,
    rollup_cache: RwLock<HashMap<String, RollupSegment>>,
    poisoned: bool,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, Config::default())
    }

    pub fn open_with(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        let manifest_directory = root.join(MANIFEST_DIRECTORY);
        let rollup_directory = root.join(ROLLUP_DIRECTORY);
        std::fs::create_dir_all(&manifest_directory)?;
        std::fs::create_dir_all(&rollup_directory)?;
        sync_directory(&root)?;

        let database = Database::open_with(root.join(ACTIVE_LOG), config)?;
        let manifest = Manifest::load(&manifest_directory)?;
        let mut store = Self {
            root,
            rollup_directory,
            manifest_directory,
            database,
            manifest,
            rollup_cache: RwLock::new(HashMap::new()),
            poisoned: false,
        };
        store.verify_and_reconcile_manifest()?;
        Ok(store)
    }

    #[must_use]
    pub const fn database(&self) -> &Database {
        &self.database
    }

    #[must_use]
    pub const fn manifest_generation(&self) -> u64 {
        self.manifest.generation
    }

    pub fn active_rollups(&self) -> impl Iterator<Item = &RollupDescriptor> {
        self.manifest.rollups.iter().filter(|rollup| rollup.active)
    }

    /// Commits catalog and data records atomically, then durably advances the
    /// rollup manifest if new points affect or supersede materialized state.
    pub fn commit(&mut self, transaction: Transaction) -> Result<Commit> {
        self.ensure_healthy()?;
        let committed_points: Vec<Point> = transaction
            .records
            .iter()
            .filter_map(|record| match record {
                Record::Points(points) => Some(points.as_slice()),
                _ => None,
            })
            .flatten()
            .copied()
            .collect();
        let mut commit = self.database.commit(transaction)?;
        if !committed_points.is_empty() && self.manifest.rollups.iter().any(|rollup| rollup.active)
        {
            if !commit.durable {
                self.database.flush()?;
                commit.durable = true;
            }
            self.advance_after_points(&committed_points)?;
        }
        Ok(commit)
    }

    /// Compatibility append for a previously initialized catalog. New code
    /// should prefer a mixed `Transaction` so metadata and values are atomic.
    pub fn append(&mut self, points: &[Point]) -> Result<Commit> {
        self.ensure_healthy()?;
        let mut commit = self.database.append(points)?;
        if !points.is_empty() && self.manifest.rollups.iter().any(|rollup| rollup.active) {
            if !commit.durable {
                self.database.flush()?;
                commit.durable = true;
            }
            self.advance_after_points(points)?;
        }
        Ok(commit)
    }

    /// Builds every completed configured gauge bucket and atomically publishes
    /// one manifest generation after all new segment files are durable.
    pub fn maintain(&mut self, now_micros: i64) -> Result<MaintenanceReport> {
        self.ensure_healthy()?;
        // A durable rollup may never get ahead of the raw source it summarizes.
        self.database.flush()?;
        let stats = self.database.stats()?;
        let definitions: Vec<_> = self
            .database
            .catalog()
            .series_definitions()
            .cloned()
            .collect();
        let next_generation = self.manifest.generation.saturating_add(1);
        let mut next = self.manifest.clone();
        let mut files_written = 0_usize;
        let mut buckets_written = 0_u64;
        let mut bytes_written = 0_u64;
        let mut changed = false;

        for definition in definitions {
            if definition.semantics != SeriesSemantics::Gauge {
                continue;
            }
            let points = self
                .database
                .query_latest(definition.id, i64::MIN, i64::MAX);
            let max_gap = definition.maximum_gap_micros.unwrap_or(0);
            for tier in &definition.rollup_policy.tiers {
                let mut buckets = materialize(&points, &tier.resolution, max_gap)?;
                buckets.retain(|bucket| bucket.end <= now_micros);
                let retention_cutoff = tier
                    .retain_for_micros
                    .map(|retention| now_micros.saturating_sub(retention));
                for rollup in &mut next.rollups {
                    if rollup.active
                        && rollup.series_id == definition.id
                        && rollup.resolution == tier.resolution
                        && retention_cutoff.is_some_and(|cutoff| rollup.end < cutoff)
                    {
                        rollup.active = false;
                        changed = true;
                    }
                }
                let shards = rollup_shards(&buckets, &tier.resolution, now_micros)?;
                for shard in shards
                    .into_iter()
                    .filter(|shard| retention_cutoff.is_none_or(|cutoff| shard.end >= cutoff))
                {
                    let already_current = next.rollups.iter().any(|rollup| {
                        rollup.active
                            && rollup.series_id == definition.id
                            && rollup.resolution == tier.resolution
                            && rollup.start == shard.start
                            && rollup.end == shard.end
                            && rollup.source_points == stats.points
                    });
                    if already_current {
                        continue;
                    }
                    for rollup in &mut next.rollups {
                        if rollup.active
                            && rollup.series_id == definition.id
                            && rollup.resolution == tier.resolution
                            && ranges_overlap(rollup.start, rollup.end, shard.start, shard.end)
                        {
                            rollup.active = false;
                        }
                    }
                    let file = rollup_file_name(
                        next_generation,
                        definition.id,
                        files_written,
                        &tier.resolution,
                    );
                    let segment_stats =
                        RollupSegment::create(self.rollup_directory.join(&file), &shard.buckets)?;
                    next.rollups.push(RollupDescriptor {
                        file,
                        series_id: definition.id,
                        resolution: tier.resolution.clone(),
                        start: shard.start,
                        end: shard.end,
                        source_commit: stats.commits,
                        source_points: stats.points,
                        active: true,
                    });
                    files_written += 1;
                    buckets_written += u64::from(segment_stats.buckets);
                    bytes_written += segment_stats.stored_bytes;
                    changed = true;
                }
            }
        }

        if changed {
            next.generation = next_generation;
            self.publish_or_poison(next)?;
        }
        let retention_gates = self.retention_gates(now_micros)?;
        Ok(MaintenanceReport {
            manifest_generation: self.manifest.generation,
            rollup_files_written: files_written,
            rollup_buckets_written: buckets_written,
            rollup_bytes_written: bytes_written,
            retention_gates,
        })
    }

    /// Uses a fully covering current materialization, otherwise computes the
    /// same aggregate state from the latest raw revisions.
    pub fn query_gauge(
        &self,
        series_id: u64,
        start: i64,
        end: i64,
        resolution: &RollupResolution,
    ) -> Result<RollupQuery> {
        self.ensure_healthy()?;
        if end <= start {
            return Err(Error::InvalidModel(
                "rollup query range must have positive duration".to_owned(),
            ));
        }
        let definition = self
            .database
            .catalog()
            .series(series_id)
            .ok_or_else(|| Error::InvalidModel(format!("undefined series {series_id}")))?;
        if definition.semantics != SeriesSemantics::Gauge {
            return Err(Error::InvalidModel(format!(
                "series {series_id} is not a gauge"
            )));
        }
        let (required_start, required_end) = query_envelope(start, end, resolution)?;
        let current_points = self.database.stats()?.points;
        let candidates: Vec<_> = self
            .manifest
            .rollups
            .iter()
            .filter(|rollup| {
                rollup.active
                    && rollup.series_id == series_id
                    && &rollup.resolution == resolution
                    && rollup.source_points == current_points
                    && ranges_overlap(rollup.start, rollup.end, required_start, required_end)
            })
            .collect();
        let coverage = coverage_plan(candidates, required_start, required_end);
        if !coverage.descriptors.is_empty() {
            let mut cache = self.rollup_cache.write().map_err(|_| Error::Poisoned)?;
            for descriptor in &coverage.descriptors {
                if !cache.contains_key(&descriptor.file) {
                    cache.insert(
                        descriptor.file.clone(),
                        RollupSegment::open(self.rollup_directory.join(&descriptor.file))?,
                    );
                }
            }
            let mut buckets = Vec::new();
            for descriptor in coverage.descriptors {
                buckets.extend(
                    cache
                        .get(&descriptor.file)
                        .expect("rollup was inserted")
                        .query(start, end),
                );
            }
            drop(cache);
            for (gap_start, gap_end) in &coverage.gaps {
                buckets.extend(self.materialize_raw_range(
                    series_id,
                    *gap_start,
                    *gap_end,
                    resolution,
                    definition.maximum_gap_micros.unwrap_or(0),
                )?);
            }
            buckets.sort_by_key(|bucket| bucket.start);
            return Ok(RollupQuery {
                buckets,
                source: if coverage.gaps.is_empty() {
                    RollupSource::Materialized
                } else {
                    RollupSource::Hybrid
                },
                manifest_generation: self.manifest.generation,
            });
        }

        let buckets = self.materialize_raw_range(
            series_id,
            required_start,
            required_end,
            resolution,
            definition.maximum_gap_micros.unwrap_or(0),
        )?;
        Ok(RollupQuery {
            buckets,
            source: RollupSource::Raw,
            manifest_generation: self.manifest.generation,
        })
    }

    /// Returns safety decisions only. M3 deliberately does not delete raw log
    /// frames because catalog records and points still share the active log.
    pub fn retention_gates(&self, now_micros: i64) -> Result<Vec<RetentionGate>> {
        self.ensure_healthy()?;
        let source_points = self.database.stats()?.points;
        let mut gates = Vec::new();
        for definition in self.database.catalog().series_definitions() {
            let Some(retention) = definition.rollup_policy.raw_retain_for_micros else {
                continue;
            };
            let cutoff = now_micros.saturating_sub(retention);
            let raw = self
                .database
                .query_history(definition.id, i64::MIN, i64::MAX);
            let Some(oldest) = raw.iter().map(|point| point.valid_time).min() else {
                gates.push(RetentionGate {
                    series_id: definition.id,
                    raw_before: cutoff,
                    eligible: true,
                    reason: "no raw points exist".to_owned(),
                });
                continue;
            };
            if oldest >= cutoff {
                gates.push(RetentionGate {
                    series_id: definition.id,
                    raw_before: cutoff,
                    eligible: true,
                    reason: "no raw points are older than the cutoff".to_owned(),
                });
                continue;
            }
            let missing = definition.rollup_policy.tiers.iter().find(|tier| {
                let candidates = self
                    .manifest
                    .rollups
                    .iter()
                    .filter(|rollup| {
                        rollup.active
                            && rollup.series_id == definition.id
                            && rollup.resolution == tier.resolution
                            && rollup.source_points == source_points
                    })
                    .collect();
                covering_descriptors(candidates, oldest, cutoff).is_none()
            });
            gates.push(match missing {
                Some(tier) => RetentionGate {
                    series_id: definition.id,
                    raw_before: cutoff,
                    eligible: false,
                    reason: format!(
                        "durable current {:?} rollup does not cover raw data through the cutoff",
                        tier.resolution
                    ),
                },
                None if definition.rollup_policy.tiers.is_empty() => RetentionGate {
                    series_id: definition.id,
                    raw_before: cutoff,
                    eligible: false,
                    reason: "raw retention requires at least one durable rollup tier".to_owned(),
                },
                None => RetentionGate {
                    series_id: definition.id,
                    raw_before: cutoff,
                    eligible: true,
                    reason: "all configured durable rollup tiers cover the deletion range"
                        .to_owned(),
                },
            });
        }
        Ok(gates)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.ensure_healthy()?;
        self.database.flush()
    }

    pub fn close(mut self) -> Result<()> {
        self.flush()
    }

    /// Re-opens and verifies every active immutable rollup in the selected
    /// manifest. Opening the Store has already recovered/validated the raw log.
    pub fn check_integrity(&self) -> Result<IntegrityReport> {
        self.ensure_healthy()?;
        let raw = self.database.stats()?;
        let mut report = IntegrityReport {
            manifest_generation: self.manifest.generation,
            raw_points: raw.points,
            raw_commits: raw.commits,
            ..IntegrityReport::default()
        };
        for descriptor in self.active_rollups() {
            let segment = RollupSegment::open(self.rollup_directory.join(&descriptor.file))?;
            let stats = segment.stats();
            if stats.min_start.is_none_or(|start| start < descriptor.start)
                || stats.max_end.is_none_or(|end| end > descriptor.end)
                || descriptor.source_points > raw.points
            {
                return Err(Error::Corruption {
                    offset: 0,
                    reason: format!(
                        "active rollup {} does not match its manifest coverage/source",
                        descriptor.file
                    ),
                });
            }
            report.active_rollup_files += 1;
            report.active_rollup_buckets += u64::from(stats.buckets);
            report.active_rollup_bytes += stats.stored_bytes;
        }
        Ok(report)
    }

    /// Creates a self-contained point-in-time backup and publishes the target
    /// directory only after every copied/linkable file and directory is synced.
    pub fn backup_to(&mut self, destination: impl AsRef<Path>) -> Result<BackupReport> {
        self.ensure_healthy()?;
        let destination = destination.as_ref();
        if destination.exists() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "backup destination already exists",
            )));
        }
        let parent = destination.parent().ok_or(Error::InvalidConfig(
            "backup destination must have a parent directory",
        ))?;
        std::fs::create_dir_all(parent)?;
        self.database.flush()?;
        self.check_integrity()?;

        let name = destination
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(Error::InvalidConfig("backup destination must be UTF-8"))?;
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let temporary = parent.join(format!(".{name}.backup-{}-{nonce}", std::process::id()));
        std::fs::create_dir(&temporary)?;
        let result = self.write_backup(&temporary);
        let mut report = match result {
            Ok(report) => report,
            Err(error) => {
                let _ = std::fs::remove_dir_all(&temporary);
                return Err(error);
            }
        };
        if let Err(error) = std::fs::rename(&temporary, destination) {
            let _ = std::fs::remove_dir_all(&temporary);
            return Err(Error::Io(error));
        }
        sync_directory(parent)?;

        // Opening the published copy exercises raw recovery, manifest parsing,
        // and every active segment before the caller treats it as a backup.
        let backup = Self::open(destination)?;
        backup.check_integrity()?;
        report.bytes = directory_bytes(destination)?;
        Ok(report)
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn advance_after_points(&mut self, points: &[Point]) -> Result<()> {
        let stats = self.database.stats()?;
        let mut next = self.manifest.clone();
        let mut changed = false;
        for descriptor in &mut next.rollups {
            if !descriptor.active {
                continue;
            }
            let gap = self
                .database
                .catalog()
                .series(descriptor.series_id)
                .and_then(|series| series.maximum_gap_micros)
                .unwrap_or(0);
            let affected = points.iter().any(|point| {
                point.series_id == descriptor.series_id
                    && point.valid_time >= descriptor.start.saturating_sub(gap)
                    && point.valid_time <= descriptor.end
            });
            if affected {
                descriptor.active = false;
            } else {
                // Advancing provenance proves that an unrelated append was
                // considered, avoiding needless invalidation after restart.
                descriptor.source_commit = stats.commits;
                descriptor.source_points = stats.points;
            }
            changed = true;
        }
        if changed {
            next.generation = next.generation.saturating_add(1);
            self.publish_or_poison(next)?;
        }
        Ok(())
    }

    fn materialize_raw_range(
        &self,
        series_id: u64,
        start: i64,
        end: i64,
        resolution: &RollupResolution,
        max_gap_micros: i64,
    ) -> Result<Vec<GaugeBucket>> {
        let context_start = start.saturating_sub(max_gap_micros);
        let context_end = end.saturating_add(max_gap_micros.max(1));
        let points = self
            .database
            .query_latest(series_id, context_start, context_end);
        let mut buckets = materialize(&points, resolution, max_gap_micros)?;
        buckets.retain(|bucket| bucket.start >= start && bucket.end <= end);
        Ok(buckets)
    }

    fn write_backup(&self, temporary: &Path) -> Result<BackupReport> {
        let manifests = temporary.join(MANIFEST_DIRECTORY);
        let rollups = temporary.join(ROLLUP_DIRECTORY);
        std::fs::create_dir(&manifests)?;
        std::fs::create_dir(&rollups)?;
        let mut report = BackupReport {
            manifest_generation: self.manifest.generation,
            ..BackupReport::default()
        };

        // Never hard-link the active log: future appends to the source inode
        // must not mutate the backup snapshot.
        copy_and_sync(&self.root.join(ACTIVE_LOG), &temporary.join(ACTIVE_LOG))?;
        report.files += 1;
        for descriptor in self.active_rollups() {
            hard_link_or_copy(
                &self.rollup_directory.join(&descriptor.file),
                &rollups.join(&descriptor.file),
            )?;
            report.files += 1;
        }
        if self.manifest.generation > 0 {
            let file = format!("MANIFEST.{:020}", self.manifest.generation);
            hard_link_or_copy(&self.manifest_directory.join(&file), &manifests.join(&file))?;
            report.files += 1;
        }
        sync_directory(&manifests)?;
        sync_directory(&rollups)?;
        sync_directory(temporary)?;
        report.bytes = directory_bytes(temporary)?;
        Ok(report)
    }

    fn verify_and_reconcile_manifest(&mut self) -> Result<()> {
        let stats = self.database.stats()?;
        let mut next = self.manifest.clone();
        let mut changed = false;
        for descriptor in &mut next.rollups {
            if !descriptor.active {
                continue;
            }
            if descriptor.source_points > stats.points {
                return Err(Error::Corruption {
                    offset: 0,
                    reason: format!("rollup {} is ahead of its raw source", descriptor.file),
                });
            }
            let segment = RollupSegment::open(self.rollup_directory.join(&descriptor.file))?;
            let segment_stats = segment.stats();
            if segment_stats
                .min_start
                .is_none_or(|start| start < descriptor.start)
                || segment_stats.max_end.is_none_or(|end| end > descriptor.end)
            {
                return Err(Error::Corruption {
                    offset: 0,
                    reason: format!("rollup {} bounds do not match manifest", descriptor.file),
                });
            }
            if descriptor.source_points < stats.points {
                // A crash may have happened after raw fsync but before manifest
                // invalidation. Conservatively rebuild every stale descriptor.
                descriptor.active = false;
                changed = true;
            } else {
                self.rollup_cache
                    .write()
                    .map_err(|_| Error::Poisoned)?
                    .insert(descriptor.file.clone(), segment);
            }
        }
        if changed {
            next.generation = next.generation.saturating_add(1);
            next.publish(&self.manifest_directory)?;
            self.manifest = next;
        }
        Ok(())
    }

    fn publish_or_poison(&mut self, next: Manifest) -> Result<()> {
        if let Err(error) = next.publish(&self.manifest_directory) {
            self.poisoned = true;
            return Err(error);
        }
        self.manifest = next;
        let active: std::collections::HashSet<_> = self
            .manifest
            .rollups
            .iter()
            .filter(|rollup| rollup.active)
            .map(|rollup| rollup.file.as_str())
            .collect();
        self.rollup_cache
            .write()
            .map_err(|_| Error::Poisoned)?
            .retain(|file, _| active.contains(file.as_str()));
        Ok(())
    }

    fn ensure_healthy(&self) -> Result<()> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
}

fn materialize(
    points: &[Point],
    resolution: &RollupResolution,
    max_gap_micros: i64,
) -> Result<Vec<GaugeBucket>> {
    match resolution {
        RollupResolution::FixedMicros(micros) => {
            if *micros <= 0 {
                return Err(Error::InvalidModel(
                    "fixed rollup resolution must be positive".to_owned(),
                ));
            }
            Ok(FixedGaugeRollup::build(points, *micros, max_gap_micros)
                .buckets()
                .copied()
                .collect())
        }
        RollupResolution::Calendar {
            unit,
            iana_timezone,
        } => Ok(
            CalendarGaugeRollup::build(points, *unit, iana_timezone, max_gap_micros)?
                .buckets()
                .copied()
                .collect(),
        ),
    }
}

struct RollupShard {
    start: i64,
    end: i64,
    buckets: Vec<GaugeBucket>,
}

fn rollup_shards(
    buckets: &[GaugeBucket],
    resolution: &RollupResolution,
    now_micros: i64,
) -> Result<Vec<RollupShard>> {
    match resolution {
        RollupResolution::FixedMicros(micros) if *micros > 0 => {
            // The common 5m/30m/hour tiers get stable UTC-day files. An
            // unusual resolution that does not divide a day gets one bucket
            // per file rather than a moving, rewrite-heavy tail chunk.
            let width = if *micros <= UTC_DAY_MICROS && UTC_DAY_MICROS % *micros == 0 {
                UTC_DAY_MICROS
            } else {
                *micros
            };
            let mut grouped = BTreeMap::<i64, Vec<GaugeBucket>>::new();
            for bucket in buckets {
                let start = bucket.start.div_euclid(width) * width;
                let end = start.saturating_add(width);
                if end <= now_micros {
                    grouped.entry(start).or_default().push(*bucket);
                }
            }
            Ok(grouped
                .into_iter()
                .map(|(start, buckets)| RollupShard {
                    start,
                    end: start.saturating_add(width),
                    buckets,
                })
                .collect())
        }
        RollupResolution::FixedMicros(_) => Err(Error::InvalidModel(
            "fixed rollup resolution must be positive".to_owned(),
        )),
        RollupResolution::Calendar { .. } => Ok(buckets
            .iter()
            .filter(|bucket| bucket.end <= now_micros)
            .map(|bucket| RollupShard {
                start: bucket.start,
                end: bucket.end,
                buckets: vec![*bucket],
            })
            .collect()),
    }
}

struct CoveragePlan<'a> {
    descriptors: Vec<&'a RollupDescriptor>,
    gaps: Vec<(i64, i64)>,
}

fn coverage_plan<'a>(
    mut candidates: Vec<&'a RollupDescriptor>,
    start: i64,
    end: i64,
) -> CoveragePlan<'a> {
    if end <= start {
        return CoveragePlan {
            descriptors: Vec::new(),
            gaps: Vec::new(),
        };
    }
    candidates.sort_by_key(|descriptor| (descriptor.start, descriptor.end));
    let mut selected = Vec::new();
    let mut gaps = Vec::new();
    let mut cursor = start;
    while cursor < end {
        let best = candidates
            .iter()
            .copied()
            .filter(|descriptor| descriptor.start <= cursor && descriptor.end > cursor)
            .max_by_key(|descriptor| descriptor.end);
        if let Some(descriptor) = best {
            cursor = descriptor.end.min(end);
            selected.push(descriptor);
            continue;
        }
        let gap_end = candidates
            .iter()
            .filter(|descriptor| descriptor.start > cursor)
            .map(|descriptor| descriptor.start)
            .min()
            .unwrap_or(end)
            .min(end);
        gaps.push((cursor, gap_end));
        cursor = gap_end;
    }
    CoveragePlan {
        descriptors: selected,
        gaps,
    }
}

fn covering_descriptors(
    candidates: Vec<&RollupDescriptor>,
    start: i64,
    end: i64,
) -> Option<Vec<&RollupDescriptor>> {
    let plan = coverage_plan(candidates, start, end);
    plan.gaps.is_empty().then_some(plan.descriptors)
}

fn ranges_overlap(left_start: i64, left_end: i64, right_start: i64, right_end: i64) -> bool {
    left_start < right_end && right_start < left_end
}

fn query_envelope(start: i64, end: i64, resolution: &RollupResolution) -> Result<(i64, i64)> {
    match resolution {
        RollupResolution::FixedMicros(micros) if *micros > 0 => {
            let first = start.div_euclid(*micros) * *micros;
            let last_timestamp = end.saturating_sub(1);
            let last_start = last_timestamp.div_euclid(*micros) * *micros;
            Ok((first, last_start.saturating_add(*micros)))
        }
        RollupResolution::FixedMicros(_) => Err(Error::InvalidModel(
            "fixed rollup resolution must be positive".to_owned(),
        )),
        RollupResolution::Calendar {
            unit,
            iana_timezone,
        } => {
            let (first, _) = calendar_bucket_bounds(start, *unit, iana_timezone)?;
            let (_, last) = calendar_bucket_bounds(end.saturating_sub(1), *unit, iana_timezone)?;
            Ok((first, last))
        }
    }
}

fn rollup_file_name(
    generation: u64,
    series_id: u64,
    ordinal: usize,
    resolution: &RollupResolution,
) -> String {
    let tag = match resolution {
        RollupResolution::FixedMicros(micros) => format!("f{micros}"),
        RollupResolution::Calendar { unit, .. } => format!("c{unit:?}"),
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("g{generation}-s{series_id}-{tag}-{ordinal}-{nonce}.rseg")
}

fn sync_directory(path: &Path) -> Result<()> {
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

fn copy_and_sync(source: &Path, destination: &Path) -> Result<u64> {
    let bytes = std::fs::copy(source, destination)?;
    std::fs::File::open(destination)?.sync_all()?;
    Ok(bytes)
}

fn hard_link_or_copy(source: &Path, destination: &Path) -> Result<()> {
    if std::fs::hard_link(source, destination).is_err() {
        copy_and_sync(source, destination)?;
    }
    Ok(())
}

fn directory_bytes(path: &Path) -> Result<u64> {
    let mut total = 0_u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        total = total.saturating_add(if metadata.is_dir() {
            directory_bytes(&entry.path())?
        } else {
            metadata.len()
        });
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::{RollupSource, Store};
    use crate::{
        CalendarUnit, Entity, EntityId, Point, RollupPolicy, RollupResolution, RollupTier,
        SeriesDefinition, SeriesSemantics, Transaction,
    };
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    const SECOND: i64 = 1_000_000;
    const DAY: i64 = 86_400 * SECOND;

    fn initialize(store: &mut Store, tiers: Vec<RollupTier>, raw_retention: Option<i64>) {
        let mut transaction = Transaction::new();
        transaction
            .upsert_entity(Entity {
                id: EntityId(1),
                kind: "site".to_owned(),
                name: "test".to_owned(),
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
                maximum_gap_micros: Some(2 * SECOND),
                rollup_policy: RollupPolicy {
                    raw_retain_for_micros: raw_retention,
                    tiers,
                },
            });
        store.commit(transaction).unwrap();
    }

    fn points() -> Vec<Point> {
        (0..=20)
            .map(|second| Point::actual(1, second * SECOND, second as f64))
            .collect()
    }

    #[test]
    fn materialized_query_survives_reopen() {
        let directory = tempdir().unwrap();
        let resolution = RollupResolution::FixedMicros(5 * SECOND);
        {
            let mut store = Store::open(directory.path()).unwrap();
            initialize(
                &mut store,
                vec![RollupTier {
                    resolution: resolution.clone(),
                    retain_for_micros: None,
                }],
                None,
            );
            let mut transaction = Transaction::new();
            transaction.append_points(points());
            store.commit(transaction).unwrap();
            let report = store.maintain(DAY).unwrap();
            assert_eq!(report.rollup_files_written, 1);
        }
        let store = Store::open(directory.path()).unwrap();
        let query = store.query_gauge(1, 0, 20 * SECOND, &resolution).unwrap();
        assert_eq!(query.source, RollupSource::Materialized);
        assert_eq!(query.buckets.len(), 4);
    }

    #[test]
    fn late_revision_invalidates_then_rebuilds_rollup() {
        let directory = tempdir().unwrap();
        let resolution = RollupResolution::FixedMicros(5 * SECOND);
        let mut store = Store::open(directory.path()).unwrap();
        initialize(
            &mut store,
            vec![RollupTier {
                resolution: resolution.clone(),
                retain_for_micros: None,
            }],
            None,
        );
        let mut transaction = Transaction::new();
        transaction.append_points(points());
        store.commit(transaction).unwrap();
        store.maintain(DAY).unwrap();

        let mut correction = Point::actual(1, 6 * SECOND, 100.0);
        correction.change_time = 30 * SECOND;
        let mut transaction = Transaction::new();
        transaction.append_points(vec![correction]);
        store.commit(transaction).unwrap();
        assert_eq!(
            store
                .query_gauge(1, 0, 20 * SECOND, &resolution)
                .unwrap()
                .source,
            RollupSource::Raw
        );

        store.maintain(DAY).unwrap();
        assert_eq!(
            store
                .query_gauge(1, 0, 20 * SECOND, &resolution)
                .unwrap()
                .source,
            RollupSource::Materialized
        );
    }

    #[test]
    fn raw_retention_is_gated_on_every_configured_tier() {
        let directory = tempdir().unwrap();
        let mut store = Store::open(directory.path()).unwrap();
        initialize(
            &mut store,
            vec![
                RollupTier {
                    resolution: RollupResolution::FixedMicros(5 * SECOND),
                    retain_for_micros: None,
                },
                RollupTier {
                    resolution: RollupResolution::Calendar {
                        unit: CalendarUnit::Day,
                        iana_timezone: "Europe/Stockholm".to_owned(),
                    },
                    retain_for_micros: None,
                },
            ],
            Some(5 * SECOND),
        );
        let mut transaction = Transaction::new();
        transaction.append_points(points());
        store.commit(transaction).unwrap();
        assert!(!store.retention_gates(20 * SECOND).unwrap()[0].eligible);

        // The day is not closed yet, so the calendar tier correctly keeps the
        // raw deletion gate shut even though fixed buckets can be materialized.
        store.maintain(20 * SECOND).unwrap();
        assert!(!store.retention_gates(20 * SECOND).unwrap()[0].eligible);
    }

    #[test]
    fn reopen_conservatively_invalidates_a_manifest_missed_by_crash() {
        let directory = tempdir().unwrap();
        let resolution = RollupResolution::FixedMicros(5 * SECOND);
        {
            let mut store = Store::open(directory.path()).unwrap();
            initialize(
                &mut store,
                vec![RollupTier {
                    resolution: resolution.clone(),
                    retain_for_micros: None,
                }],
                None,
            );
            let mut transaction = Transaction::new();
            transaction.append_points(points());
            store.commit(transaction).unwrap();
            store.maintain(DAY).unwrap();
        }
        // Simulate power loss after the raw frame became durable and before a
        // new manifest generation could invalidate the old rollup.
        {
            let mut database = crate::Database::open(directory.path().join("active.wlog")).unwrap();
            let mut correction = Point::actual(1, 6 * SECOND, 100.0);
            correction.change_time = 30 * SECOND;
            let mut transaction = Transaction::new();
            transaction.append_points(vec![correction]);
            database.commit(transaction).unwrap();
            database.close().unwrap();
        }
        let store = Store::open(directory.path()).unwrap();
        assert_eq!(
            store
                .query_gauge(1, 0, 20 * SECOND, &resolution)
                .unwrap()
                .source,
            RollupSource::Raw
        );
    }

    #[test]
    fn new_days_append_rollups_without_rewriting_history() {
        let directory = tempdir().unwrap();
        let resolution = RollupResolution::FixedMicros(5 * SECOND);
        let mut store = Store::open(directory.path()).unwrap();
        initialize(
            &mut store,
            vec![RollupTier {
                resolution: resolution.clone(),
                retain_for_micros: None,
            }],
            None,
        );
        let mut transaction = Transaction::new();
        transaction.append_points(vec![
            Point::actual(1, 0, 1.0),
            Point::actual(1, 5 * SECOND, 2.0),
            Point::actual(1, DAY, 3.0),
            Point::actual(1, DAY + 5 * SECOND, 4.0),
        ]);
        store.commit(transaction).unwrap();
        assert_eq!(store.maintain(2 * DAY).unwrap().rollup_files_written, 2);
        let historical_files: Vec<_> = store
            .active_rollups()
            .map(|rollup| rollup.file.clone())
            .collect();

        let mut transaction = Transaction::new();
        transaction.append_points(vec![
            Point::actual(1, 2 * DAY + 5 * SECOND, 5.0),
            Point::actual(1, 2 * DAY + 10 * SECOND, 6.0),
        ]);
        store.commit(transaction).unwrap();
        assert_eq!(
            store
                .query_gauge(1, 0, 2 * DAY + 15 * SECOND, &resolution)
                .unwrap()
                .source,
            RollupSource::Hybrid
        );
        let report = store.maintain(3 * DAY).unwrap();
        assert_eq!(report.rollup_files_written, 1);
        assert!(
            historical_files
                .iter()
                .all(|file| { store.active_rollups().any(|rollup| &rollup.file == file) })
        );
        assert_eq!(
            store
                .query_gauge(1, 0, 3 * DAY, &resolution)
                .unwrap()
                .source,
            RollupSource::Materialized
        );
    }

    #[test]
    fn backup_is_verified_and_raw_log_is_independent() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("source");
        let destination = directory.path().join("backup");
        let resolution = RollupResolution::FixedMicros(5 * SECOND);
        let mut store = Store::open(&source).unwrap();
        initialize(
            &mut store,
            vec![RollupTier {
                resolution: resolution.clone(),
                retain_for_micros: None,
            }],
            None,
        );
        let mut transaction = Transaction::new();
        transaction.append_points(points());
        store.commit(transaction).unwrap();
        store.maintain(DAY).unwrap();
        let report = store.backup_to(&destination).unwrap();
        assert!(report.files >= 3);
        assert!(report.bytes > 0);

        let backup = Store::open(&destination).unwrap();
        assert_eq!(
            backup
                .query_gauge(1, 0, 20 * SECOND, &resolution)
                .unwrap()
                .source,
            RollupSource::Materialized
        );
        let backup_points = backup.database().stats().unwrap().points;

        let mut transaction = Transaction::new();
        transaction.append_points(vec![Point::actual(1, 30 * SECOND, 30.0)]);
        store.commit(transaction).unwrap();
        assert_eq!(backup.database().stats().unwrap().points, backup_points);
        assert_eq!(store.database().stats().unwrap().points, backup_points + 1);
    }
}
