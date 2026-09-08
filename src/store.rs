use crate::manifest::{self, Manifest, RawSegmentDescriptor, RollupDescriptor};
use crate::rollup::calendar_bucket_bounds;
use crate::snapshot::{
    PublicationStep, StagedDirectory, inject_checksum_mismatch, publication_checkpoint,
    snapshot_digest, snapshot_digest_with_open_prefix, snapshot_file_prefix_digest,
};
use crate::storage::{SalvageSource, sync_directory, sync_parent_directory};
use crate::transaction::{IngressIdentity, Record};
use crate::{
    CalendarGaugeRollup, Commit, Config, Database, Error, FixedGaugeRollup, GaugeBucket,
    IngressWatermarks, Point, Result, RollupResolution, RollupSegment, Segment, SeriesDefinition,
    SeriesSemantics, Transaction,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

const ACTIVE_LOG: &str = "active.wlog";
const MANIFEST_DIRECTORY: &str = "manifests";
const ROLLUP_DIRECTORY: &str = "rollups";
const SEGMENT_DIRECTORY: &str = "segments";
const SEAL_BLOCK_POINTS: usize = 16_384;
const UTC_DAY_MICROS: i64 = 86_400_000_000;
/// Process-local verified rollup files. Queries may temporarily hold more than
/// this when a single range covers a larger working set; idle cache is trimmed
/// back so open no longer preloads every generation into RAM.
const MAX_CACHED_ROLLUP_SEGMENTS: usize = 1_024;

#[cfg(test)]
std::thread_local! {
    static FAIL_AFTER_SEAL_PUBLISH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(crate) fn fail_next_seal_reclaim() {
    FAIL_AFTER_SEAL_PUBLISH.with(|flag| flag.set(true));
}

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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SealReport {
    pub manifest_generation: u64,
    pub segment_file: String,
    pub sealed_points: u64,
    pub live_points: u64,
    pub segment_bytes: u64,
    pub log_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IntegrityReport {
    pub manifest_generation: u64,
    pub raw_points: u64,
    pub raw_commits: u64,
    pub raw_recovered_tail_bytes: u64,
    pub raw_recovered_tail: crate::RecoveredTail,
    pub active_rollup_files: usize,
    pub active_rollup_buckets: u64,
    pub active_rollup_bytes: u64,
    /// Active rollups whose provenance trails the raw log. A writable open
    /// reconciles these by publishing an invalidating manifest generation; a
    /// read-only open leaves the store untouched and only reports them here.
    pub stale_rollup_files: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BackupReport {
    pub files: usize,
    pub bytes: u64,
    pub manifest_generation: u64,
    pub linked_files: usize,
    pub copied_files: usize,
    pub hard_link_fallbacks: usize,
    pub hard_link_fallback_error_kinds: Vec<std::io::ErrorKind>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RestoreReport {
    pub files: usize,
    pub bytes: u64,
    pub manifest_generation: u64,
    pub raw_commits: u64,
    pub raw_points: u64,
    pub source_snapshot_crc32: u32,
    pub destination_snapshot_crc32: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SalvageOptions {
    /// When true, orphan `.wseg` files not named by the recovered manifest
    /// are ignored instead of failing salvage.
    pub drop_orphan_segments: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SalvageStatus {
    Clean,
    Partial,
}

impl fmt::Display for SalvageStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Clean => "clean",
            Self::Partial => "partial",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SalvageReport {
    pub status: SalvageStatus,
    pub source_bytes: u64,
    pub recovered_prefix_bytes: u64,
    pub discarded_bytes: u64,
    pub stop_offset: u64,
    pub stop_reason: crate::SalvageStopReason,
    pub recovered_commits: u64,
    pub recovered_points: u64,
    pub source_prefix_crc32: u32,
    pub destination_snapshot_crc32: u32,
}

impl BackupReport {
    fn record_copy(&mut self) {
        self.files += 1;
        self.copied_files += 1;
    }

    fn record_link_or_copy(&mut self, outcome: LinkOrCopy) {
        self.files += 1;
        match outcome {
            LinkOrCopy::Linked => self.linked_files += 1,
            LinkOrCopy::Copied { link_error } => {
                self.copied_files += 1;
                self.hard_link_fallbacks += 1;
                self.hard_link_fallback_error_kinds.push(link_error.kind());
            }
        }
    }
}

/// A directory-level FTWDB store with a commit log and durable rollup
/// generations. Maintenance is explicit so an embedded caller can schedule it
/// around flash, CPU, and power constraints.
pub struct Store {
    root: PathBuf,
    rollup_directory: PathBuf,
    manifest_directory: PathBuf,
    segment_directory: PathBuf,
    database: Database,
    manifest: Manifest,
    rollup_cache: RwLock<HashMap<String, RollupSegment>>,
    poisoned: bool,
    read_only: bool,
    stale_rollup_files: usize,
    /// Last materialized `series_points(id).len()` for each gauge series.
    /// Empty after open, so the first maintain may still scan; later calls
    /// skip `query_latest` for series whose revision vector is unchanged.
    materialized_series_revisions: HashMap<u64, usize>,
    last_maintain_now_micros: Option<i64>,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, Config::default())
    }

    pub fn open_with(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        let root_created = match std::fs::symlink_metadata(&root) {
            Ok(_) => false,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir_all(&root)?;
                true
            }
            Err(error) => return Err(Error::Io(error)),
        };
        require_real_directory(&root)?;
        if root_created {
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
        }
        let manifest_directory = root.join(MANIFEST_DIRECTORY);
        let rollup_directory = root.join(ROLLUP_DIRECTORY);
        let segment_directory = root.join(SEGMENT_DIRECTORY);
        create_or_require_real_directory(&manifest_directory)?;
        create_or_require_real_directory(&rollup_directory)?;
        create_or_require_real_directory(&segment_directory)?;
        // Make the manifests/ and rollups/ entries durable in the root, then
        // make the root's own entry durable in its parent — the same order
        // segment publication uses: contents first, then the directory entry
        // that names them. Always sync the parent, even when the root already
        // exists: the sidecar creates the directory before open, and a prior
        // open can crash after mkdir but before this fsync. Skipping it would
        // let later Always commits acknowledge durability for a store whose
        // parent dirent can vanish on power loss. `Database::open_with` below
        // syncs the root again after it creates the active log, so the log's
        // entry is covered even though the file does not exist yet here.
        sync_directory(&root)?;
        sync_parent_directory(&root)?;

        // The exclusive advisory lock that `Database::open_with` takes on the
        // active log also guards the whole store directory: every mutation —
        // commits, maintenance, manifest publication, and backups — flows
        // through an open `Database`, so a second `Store` opener fails with
        // `Error::Locked` before it can republish manifests or rewrite
        // rollups. Backups copy (never hard-link) the active log, so opening
        // a published backup does not contend with the source's lock.
        let manifest = Manifest::load(&manifest_directory)?;
        let published_seals = published_seal_generations(&manifest);
        let database =
            Database::open_with_published_seals(root.join(ACTIVE_LOG), config, &published_seals)?;
        let mut store = Self {
            root,
            rollup_directory,
            manifest_directory,
            segment_directory,
            database,
            manifest,
            rollup_cache: RwLock::new(HashMap::new()),
            poisoned: false,
            read_only: false,
            stale_rollup_files: 0,
            materialized_series_revisions: HashMap::new(),
            last_maintain_now_micros: None,
        };
        store.attach_published_segments()?;
        store.verify_and_reconcile_manifest()?;
        // Reclaims superseded manifests/segments and any segment orphaned by
        // a crash between `RollupSegment::create` and manifest publication.
        store.remove_unreferenced_files();
        if store.database.pending_reclaim() {
            store.database.reclaim_live_log()?;
        }
        Ok(store)
    }

    /// Opens an existing store without mutating anything on disk.
    ///
    /// No directory is created, the active log is opened read-only with
    /// simulated torn-tail recovery, no manifest generation is published, and
    /// neither manifest pruning nor the orphaned-rollup sweep runs. Rollups
    /// whose provenance trails the raw log — which a writable open would
    /// invalidate and rebuild — are surfaced through
    /// [`IntegrityReport::stale_rollup_files`] instead. Writer APIs (`commit`,
    /// `append`, `maintain`, `flush`) fail with [`Error::ReadOnly`], while
    /// queries, `check_integrity`, and `backup_to` remain available. The
    /// shared lock on the active log admits concurrent read-only openers but
    /// excludes any exclusive writer.
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        let manifest_directory = root.join(MANIFEST_DIRECTORY);
        let rollup_directory = root.join(ROLLUP_DIRECTORY);
        let segment_directory = root.join(SEGMENT_DIRECTORY);
        require_real_directory(&root)?;
        require_real_directory(&manifest_directory)?;
        require_real_directory(&rollup_directory)?;
        let manifest = Manifest::load(&manifest_directory)?;
        if !manifest.segments.is_empty() {
            require_real_directory(&segment_directory)?;
        }
        let published_seals = published_seal_generations(&manifest);
        let database =
            Database::open_read_only_with_published_seals(root.join(ACTIVE_LOG), &published_seals)?;
        let mut store = Self {
            root,
            rollup_directory,
            manifest_directory,
            segment_directory,
            database,
            manifest,
            rollup_cache: RwLock::new(HashMap::new()),
            poisoned: false,
            read_only: true,
            stale_rollup_files: 0,
            materialized_series_revisions: HashMap::new(),
            last_maintain_now_micros: None,
        };
        store.attach_published_segments()?;
        store.verify_manifest_read_only()?;
        Ok(store)
    }

    #[must_use]
    pub const fn database(&self) -> &Database {
        &self.database
    }

    pub fn stored_bytes(&self) -> Result<u64> {
        directory_bytes(&self.root)
    }

    #[must_use]
    pub const fn manifest_generation(&self) -> u64 {
        self.manifest.generation
    }

    #[must_use]
    pub const fn is_read_only(&self) -> bool {
        self.read_only
    }

    pub fn active_rollups(&self) -> impl Iterator<Item = &RollupDescriptor> {
        self.manifest.rollups.iter().filter(|rollup| rollup.active)
    }

    /// Commits catalog and data records atomically, then durably advances the
    /// rollup manifest if new points affect or supersede materialized state.
    ///
    /// A transaction tagged with [`Transaction::with_commit_id`] makes an
    /// exact retry of this multi-step sequence safe. The identifier and
    /// payload are checked inside [`Database::commit`] before the raw frame
    /// is written, so a matching replay stores nothing and reports
    /// [`Commit::deduplicated`]. Prefer [`Self::commit_ingress`] for
    /// production writers. A reused identifier with different records
    /// conflicts instead of silently dropping the mutation.
    /// The failure mode this closes: the raw commit becomes durable, then
    /// manifest advancement fails and poisons this store (or the process
    /// crashes), so the caller saw an error for data that is permanently in
    /// the log. After reopening the store, recovery has rebuilt the
    /// identifier set from the log and `verify_and_reconcile_manifest` has
    /// already invalidated any rollup whose provenance trails the raw log —
    /// exactly the invalidation the failed advancement would have published.
    /// The retried commit therefore returns `Ok` with `deduplicated: true`,
    /// and this method skips manifest advancement for it: the points were
    /// not rewritten, and rollup provenance is already reconciled.
    pub fn commit(&mut self, transaction: Transaction) -> Result<Commit> {
        self.ensure_writable()?;
        let has_active_rollups = self.manifest.rollups.iter().any(|rollup| rollup.active);
        let committed_points: Vec<Point> = if has_active_rollups {
            transaction
                .records
                .iter()
                .filter_map(|record| match record {
                    Record::Points(points) => Some(points.as_slice()),
                    _ => None,
                })
                .flatten()
                .copied()
                .collect()
        } else {
            Vec::new()
        };
        let mut commit = self.database.commit(transaction)?;
        if commit.deduplicated {
            return Ok(commit);
        }
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

    /// Commits one ordered producer transaction through the raw log and the
    /// same rollup invalidation path as [`Store::commit`]. Exact retries keep
    /// the original raw frame receipt and skip manifest work.
    pub fn commit_ingress(
        &mut self,
        identity: IngressIdentity,
        mut transaction: Transaction,
    ) -> Result<Commit> {
        transaction.with_ingress_identity(identity);
        self.commit(transaction)
    }

    /// Returns accepted and durable progress for one ordered ingress source.
    #[must_use]
    pub fn ingress_watermarks(&self, source_id: u128) -> IngressWatermarks {
        self.database.ingress_watermarks(source_id)
    }

    /// Returns the read-only frame receipt for one ordered source sequence.
    #[must_use]
    pub fn ingress_receipt(&self, source_id: u128, sequence: u64) -> Option<crate::IngressReceipt> {
        self.database.ingress_receipt(source_id, sequence)
    }

    /// Compares source-side shadow batches with this store without writing.
    ///
    /// A read-only store can prove content but cannot prove that a prior
    /// writer synced a recovered receipt. Pair this report with the live
    /// sidecar's durable watermark when deciding whether the source copy may
    /// be released.
    pub fn reconcile_shadow_batches(
        &self,
        expected: &[crate::shadow_protocol::CommitBatchRequest],
        limits: crate::shadow_reconcile::ShadowReconcileLimits,
    ) -> std::result::Result<
        crate::shadow_reconcile::ShadowReconciliationReport,
        crate::shadow_reconcile::ShadowReconcileError,
    > {
        crate::shadow_reconcile::reconcile_shadow_batches(&self.database, expected, limits)
    }

    /// Returns every known ingress source in stable source-ID order.
    #[must_use]
    pub fn all_ingress_watermarks(&self) -> std::collections::BTreeMap<u128, IngressWatermarks> {
        self.database.all_ingress_watermarks()
    }

    /// Compatibility append for a previously initialized catalog. New code
    /// should prefer a mixed `Transaction` so metadata and values are atomic.
    /// Like [`Database::append`], only catalog-independent point invariants
    /// are enforced; series existence and run provenance are checked
    /// exclusively by [`Store::commit`].
    pub fn append(&mut self, points: &[Point]) -> Result<Commit> {
        self.ensure_writable()?;
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

    /// Materializes completed configured gauge buckets that are dirty or newly
    /// closable, then atomically publishes one manifest generation when
    /// descriptors or segment files change.
    pub fn maintain(&mut self, now_micros: i64) -> Result<MaintenanceReport> {
        self.ensure_writable()?;
        // A durable rollup may never get ahead of the raw source it summarizes.
        self.database.flush()?;
        let stats = self.database.stats()?;
        let definitions: Vec<_> = self
            .database
            .catalog()
            .series_definitions()
            .cloned()
            .collect();
        if self.can_skip_maintain_scan(now_micros, stats.points, &definitions)? {
            self.remember_materialized_revisions(&definitions);
            self.last_maintain_now_micros = Some(now_micros);
            return Ok(MaintenanceReport {
                manifest_generation: self.manifest.generation,
                rollup_files_written: 0,
                rollup_buckets_written: 0,
                rollup_bytes_written: 0,
                retention_gates: self.retention_gates(now_micros)?,
            });
        }
        let next_generation = self.manifest.generation.saturating_add(1);
        let mut next = self.manifest.clone();
        let mut files_written = 0_usize;
        let mut buckets_written = 0_u64;
        let mut bytes_written = 0_u64;
        let mut changed = false;

        for definition in &definitions {
            if definition.semantics != SeriesSemantics::Gauge {
                continue;
            }
            if deactivate_expired_rollups(&mut next, definition, now_micros) {
                changed = true;
            }
            if self.can_skip_series_latest_query(definition, now_micros, &next.rollups)? {
                if stamp_active_series_source(&mut next, definition.id, stats.commits, stats.points)
                {
                    changed = true;
                }
                continue;
            }
            let max_gap = definition.maximum_gap_micros.unwrap_or(0);
            let Some((earliest, latest)) = self.database.series_valid_bounds(definition.id) else {
                continue;
            };
            for tier in &definition.rollup_policy.tiers {
                let retention_cutoff = tier
                    .retain_for_micros
                    .map(|retention| now_micros.saturating_sub(retention));
                let needed = needed_completed_shards(
                    definition.id,
                    &tier.resolution,
                    earliest,
                    latest,
                    now_micros,
                    &next.rollups,
                    stats.points,
                )?;
                let needed: Vec<_> = needed
                    .into_iter()
                    .filter(|shard| retention_cutoff.is_none_or(|cutoff| shard.end >= cutoff))
                    .collect();
                if needed.is_empty() {
                    continue;
                }
                let query_start = needed
                    .iter()
                    .map(|shard| shard.start)
                    .min()
                    .unwrap_or(earliest)
                    .saturating_sub(max_gap);
                let query_end = needed
                    .iter()
                    .map(|shard| shard.end)
                    .max()
                    .unwrap_or(latest.saturating_add(1))
                    .saturating_add(max_gap.max(1));
                let points = self
                    .database
                    .query_latest(definition.id, query_start, query_end)?;
                let mut buckets = materialize(&points, &tier.resolution, max_gap)?;
                buckets.retain(|bucket| bucket.end <= now_micros);
                let shards = rollup_shards(&buckets, &tier.resolution, now_micros)?;
                for shard in shards.into_iter().filter(|shard| {
                    needed
                        .iter()
                        .any(|want| want.start == shard.start && want.end == shard.end)
                }) {
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
        self.remember_materialized_revisions(&definitions);
        self.last_maintain_now_micros = Some(now_micros);
        let retention_gates = self.retention_gates(now_micros)?;
        Ok(MaintenanceReport {
            manifest_generation: self.manifest.generation,
            rollup_files_written: files_written,
            rollup_buckets_written: buckets_written,
            rollup_bytes_written: bytes_written,
            retention_gates,
        })
    }

    /// Seals the live raw index into an immutable segment, publishes it, and
    /// rewrites `active.wlog` to catalog + identity receipts.
    ///
    /// After this returns, open/recovery replays only the unsealed tail.
    /// Sealed points stay queryable from the segment file.
    pub fn seal_and_reclaim(&mut self) -> Result<SealReport> {
        self.ensure_writable()?;
        self.database.flush()?;
        let live = self.database.live_points_snapshot();
        if live.is_empty() {
            return Ok(SealReport {
                manifest_generation: self.manifest.generation,
                log_bytes: self.database.stats()?.file_bytes,
                ..SealReport::default()
            });
        }
        let stats = self.database.stats()?;
        let next_generation = self.manifest.generation.saturating_add(1);
        let file = raw_segment_file_name(next_generation);
        let min_valid_time = live.iter().map(|point| point.valid_time).min().unwrap();
        let max_valid_time = live.iter().map(|point| point.valid_time).max().unwrap();
        let segment_stats =
            Segment::create(self.segment_directory.join(&file), &live, SEAL_BLOCK_POINTS)?;
        self.database.write_seal_checkpoint(
            next_generation,
            u64::try_from(live.len()).unwrap_or(u64::MAX),
        )?;

        let mut next = self.manifest.clone();
        next.generation = next_generation;
        next.segments.push(RawSegmentDescriptor {
            file: file.clone(),
            generation: next_generation,
            points: segment_stats.points,
            source_commit: stats.commits,
            source_points: stats.points,
            min_valid_time,
            max_valid_time,
        });
        self.publish_or_poison(next)?;
        self.attach_published_segments()?;

        #[cfg(test)]
        if FAIL_AFTER_SEAL_PUBLISH.with(std::cell::Cell::take) {
            return Err(Error::Io(std::io::Error::other(
                "injected seal reclaim failure",
            )));
        }

        self.database.clear_live_index();
        self.database.reclaim_live_log()?;
        Ok(SealReport {
            manifest_generation: self.manifest.generation,
            segment_file: file,
            sealed_points: segment_stats.points,
            live_points: self.database.live_index_len() as u64,
            segment_bytes: segment_stats.stored_bytes,
            log_bytes: self.database.stats()?.file_bytes,
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
            let mut buckets = self.cached_rollup_buckets(&coverage.descriptors, start, end)?;
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
            let Some((oldest, _)) = self.database.series_valid_bounds(definition.id) else {
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
        self.ensure_writable()?;
        self.database.flush()
    }

    /// Flushes and closes the store. A read-only store has nothing to flush
    /// and simply releases its shared lock.
    pub fn close(mut self) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
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
            raw_recovered_tail_bytes: raw.recovered_tail_bytes,
            raw_recovered_tail: raw.recovered_tail,
            stale_rollup_files: self.stale_rollup_files,
            ..IntegrityReport::default()
        };
        for descriptor in &self.manifest.segments {
            let segment = Segment::open(self.segment_directory.join(&descriptor.file))?;
            segment.verify_blocks()?;
            if segment.stats().points != descriptor.points {
                return Err(Error::Corruption {
                    offset: 0,
                    reason: format!(
                        "raw segment {} does not match its manifest point count",
                        descriptor.file
                    ),
                });
            }
        }
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
        // Flushing is the only mutation `backup_to` performs on the source,
        // and a read-only handle has nothing buffered, so backups from a
        // read-only store are allowed and provably leave the source intact.
        if !self.read_only {
            self.database.flush()?;
        }
        self.check_integrity()?;

        let staged = StagedDirectory::create(destination, "backup")?;
        let mut report = self.write_snapshot(staged.path())?;
        // Hold the stage lock through publication and the destination check.
        // A writer therefore cannot change the active log between them.
        let stage = Self::open_read_only(staged.path())?;
        stage.check_integrity()?;
        let publication = staged.publish(destination)?;
        let checked = (|| {
            publication_checkpoint(PublicationStep::PostCheck)?;
            // The backup check must not reconcile a stale manifest or prune
            // files from the copy it is meant to verify.
            let backup = Self::open_read_only(destination)?;
            backup.check_integrity()?;
            report.bytes = directory_bytes(destination)?;
            Ok(report)
        })();
        match checked {
            Ok(report) => {
                drop(stage);
                publication.commit();
                Ok(report)
            }
            Err(error) => {
                let error = publication.rollback_after(error);
                drop(stage);
                Err(error)
            }
        }
    }

    /// Restores one fully valid, recovery-free snapshot into an absent target.
    ///
    /// The source stays open read-only under a shared lock. The target is built
    /// and checked in a hidden sibling directory, compared with the source by
    /// the versioned snapshot CRC32, and published with an atomic no-clobber
    /// rename. No restore path overwrites an existing target.
    pub fn restore_from(
        backup: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<RestoreReport> {
        let backup = Self::open_read_only(backup)?;
        backup.restore_to(destination.as_ref())
    }

    fn restore_to(&self, destination: &Path) -> Result<RestoreReport> {
        self.ensure_healthy()?;
        let source_integrity = self.check_integrity()?;
        self.require_clean_restore_source(&source_integrity)?;
        let relative_paths = self.selected_snapshot_paths();
        let source_digest = snapshot_digest(&self.root, &relative_paths)?;

        let staged = StagedDirectory::create(destination, "restore")?;
        self.write_snapshot(staged.path())?;
        let stage_digest =
            inject_checksum_mismatch(snapshot_digest(staged.path(), &relative_paths)?);
        if stage_digest != source_digest {
            return Err(snapshot_mismatch(
                "source",
                source_digest,
                "stage",
                stage_digest,
            ));
        }

        // Exercise raw replay, manifest selection, active rollup decoding, and
        // descriptor checks before the directory gets a public name.
        let stage = Self::open_read_only(staged.path())?;
        let stage_integrity = stage.check_integrity()?;
        stage.require_clean_restore_source(&stage_integrity)?;

        let publication = staged.publish(destination)?;
        let checked = (|| {
            publication_checkpoint(PublicationStep::PostCheck)?;
            let restored = Self::open_read_only(destination)?;
            let destination_integrity = restored.check_integrity()?;
            restored.require_clean_restore_source(&destination_integrity)?;
            let destination_digest = snapshot_digest(destination, &relative_paths)?;
            if destination_digest != source_digest {
                return Err(snapshot_mismatch(
                    "source",
                    source_digest,
                    "destination",
                    destination_digest,
                ));
            }
            if destination_integrity.raw_commits != source_integrity.raw_commits
                || destination_integrity.raw_points != source_integrity.raw_points
                || destination_integrity.manifest_generation != source_integrity.manifest_generation
            {
                return Err(Error::Corruption {
                    offset: 0,
                    reason: "restored store counts differ from the checked backup".to_owned(),
                });
            }

            Ok(RestoreReport {
                files: destination_digest.files,
                bytes: destination_digest.bytes,
                manifest_generation: destination_integrity.manifest_generation,
                raw_commits: destination_integrity.raw_commits,
                raw_points: destination_integrity.raw_points,
                source_snapshot_crc32: source_digest.crc32,
                destination_snapshot_crc32: destination_digest.crc32,
            })
        })();
        match checked {
            Ok(report) => {
                drop(stage);
                publication.commit();
                Ok(report)
            }
            Err(error) => {
                let error = publication.rollback_after(error);
                drop(stage);
                Err(error)
            }
        }
    }

    /// Copies the longest raw-log prefix that validates from the first frame
    /// into a new store, together with sealed raw segments the recovered
    /// manifest still names. Derived rollups are never opened.
    pub fn salvage_from(
        damaged_store: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<SalvageReport> {
        Self::salvage_from_with_options(damaged_store, destination, SalvageOptions::default())
    }

    /// Like [`Self::salvage_from`], with optional recovery policy controls.
    pub fn salvage_from_with_options(
        damaged_store: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        options: SalvageOptions,
    ) -> Result<SalvageReport> {
        let damaged_store = damaged_store.as_ref();
        let sealed = plan_sealed_salvage(damaged_store, options)?;
        let published_seals = published_seal_generations(&sealed.manifest);
        let mut source = SalvageSource::open(damaged_store, ACTIVE_LOG, &published_seals)?;
        let destination = destination.as_ref();
        let relative_paths = salvage_snapshot_paths(&sealed);
        let source_digest = if sealed.manifest.segments.is_empty() {
            snapshot_file_prefix_digest(
                &mut source.file,
                ACTIVE_LOG,
                source.recovered_prefix_bytes,
            )?
        } else {
            snapshot_digest_with_open_prefix(
                damaged_store,
                &relative_paths,
                ACTIVE_LOG,
                &mut source.file,
                source.recovered_prefix_bytes,
            )?
        };
        source.ensure_unchanged()?;

        let staged = StagedDirectory::create(destination, "salvage")?;
        write_salvage_stage(&mut source, damaged_store, staged.path(), &sealed)?;
        let stage_digest =
            inject_checksum_mismatch(snapshot_digest(staged.path(), &relative_paths)?);
        if stage_digest != source_digest {
            return Err(snapshot_mismatch(
                "source prefix",
                source_digest,
                "stage",
                stage_digest,
            ));
        }

        let stage = Self::open_read_only(staged.path())?;
        let stage_integrity = stage.check_integrity()?;
        stage.require_clean_restore_source(&stage_integrity)?;
        let expected_points = source
            .recovered_points
            .saturating_add(sealed.sealed_points());
        if stage_integrity.raw_commits != source.recovered_commits
            || stage_integrity.raw_points != expected_points
        {
            return Err(Error::Corruption {
                offset: 0,
                reason: "salvage stage counts differ from the validated source prefix".to_owned(),
            });
        }

        let publication = staged.publish(destination)?;
        let checked = (|| {
            publication_checkpoint(PublicationStep::PostCheck)?;
            let salvaged = Self::open_read_only(destination)?;
            let destination_integrity = salvaged.check_integrity()?;
            salvaged.require_clean_restore_source(&destination_integrity)?;
            let destination_digest = snapshot_digest(destination, &relative_paths)?;
            if destination_digest != source_digest {
                return Err(snapshot_mismatch(
                    "source prefix",
                    source_digest,
                    "destination",
                    destination_digest,
                ));
            }
            if destination_integrity.raw_commits != source.recovered_commits
                || destination_integrity.raw_points != expected_points
            {
                return Err(Error::Corruption {
                    offset: 0,
                    reason: "salvaged store counts differ from the validated source prefix"
                        .to_owned(),
                });
            }
            source.ensure_unchanged()?;
            let discarded_bytes = source
                .source_bytes
                .checked_sub(source.recovered_prefix_bytes)
                .ok_or(Error::SourceChanged {
                    path: source.path.clone(),
                })?;
            Ok(SalvageReport {
                status: if source.stop_reason == crate::SalvageStopReason::CleanEof {
                    SalvageStatus::Clean
                } else {
                    SalvageStatus::Partial
                },
                source_bytes: source.source_bytes,
                recovered_prefix_bytes: source.recovered_prefix_bytes,
                discarded_bytes,
                stop_offset: source.recovered_prefix_bytes,
                stop_reason: source.stop_reason,
                recovered_commits: destination_integrity.raw_commits,
                recovered_points: destination_integrity.raw_points,
                source_prefix_crc32: source_digest.crc32,
                destination_snapshot_crc32: destination_digest.crc32,
            })
        })();
        match checked {
            Ok(report) => {
                drop(stage);
                publication.commit();
                Ok(report)
            }
            Err(error) => {
                let error = publication.rollback_after(error);
                drop(stage);
                Err(error)
            }
        }
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn can_skip_maintain_scan(
        &self,
        now_micros: i64,
        source_points: u64,
        definitions: &[SeriesDefinition],
    ) -> Result<bool> {
        if self
            .manifest
            .rollups
            .iter()
            .any(|rollup| rollup.active && rollup.source_points != source_points)
        {
            return Ok(false);
        }
        if has_retention_work(&self.manifest, definitions, now_micros) {
            return Ok(false);
        }
        for definition in definitions {
            if definition.semantics != SeriesSemantics::Gauge {
                continue;
            }
            if !self.can_skip_series_latest_query(definition, now_micros, &self.manifest.rollups)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn can_skip_series_latest_query(
        &self,
        definition: &SeriesDefinition,
        now_micros: i64,
        rollups: &[RollupDescriptor],
    ) -> Result<bool> {
        let known_unchanged = self.materialized_series_revisions.get(&definition.id)
            == Some(&self.database.series_revision_count(definition.id));
        let no_new_completed_shard = match (known_unchanged, self.last_maintain_now_micros) {
            (true, Some(prev_now)) => {
                !series_gained_completed_shard(definition, prev_now, now_micros)?
            }
            _ => false,
        };
        if no_new_completed_shard {
            return Ok(true);
        }
        Ok(!series_has_missing_completed_shard(
            definition,
            self.database.series_valid_bounds(definition.id),
            now_micros,
            rollups,
        )?)
    }

    fn remember_materialized_revisions(&mut self, definitions: &[SeriesDefinition]) {
        self.materialized_series_revisions.clear();
        for definition in definitions {
            if definition.semantics == SeriesSemantics::Gauge {
                self.materialized_series_revisions.insert(
                    definition.id,
                    self.database.series_revision_count(definition.id),
                );
            }
        }
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
            // One generation per point-bearing commit is the write
            // amplification noted in issue #2. Collapsing it needs a
            // provenance redesign; generation pruning at least bounds the
            // on-disk cost to the retained fallback window.
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
            .query_latest(series_id, context_start, context_end)?;
        let mut buckets = materialize(&points, resolution, max_gap_micros)?;
        buckets.retain(|bucket| bucket.start >= start && bucket.end <= end);
        Ok(buckets)
    }

    fn selected_snapshot_paths(&self) -> Vec<String> {
        let mut files = vec![ACTIVE_LOG.to_owned()];
        files.extend(
            self.active_rollups()
                .map(|descriptor| format!("{ROLLUP_DIRECTORY}/{}", descriptor.file)),
        );
        files.extend(
            self.manifest
                .segments
                .iter()
                .map(|descriptor| format!("{SEGMENT_DIRECTORY}/{}", descriptor.file)),
        );
        if self.manifest.generation > 0 {
            files.push(format!(
                "{MANIFEST_DIRECTORY}/MANIFEST.{:020}",
                self.manifest.generation
            ));
        }
        files.sort();
        files.dedup();
        files
    }

    fn attach_published_segments(&mut self) -> Result<()> {
        let mut opened = Vec::with_capacity(self.manifest.segments.len());
        for descriptor in &self.manifest.segments {
            let path = self.segment_directory.join(&descriptor.file);
            let segment = Segment::open(&path).map_err(|error| Error::Corruption {
                offset: 0,
                reason: format!("raw segment {} is unreadable: {error}", descriptor.file),
            })?;
            if segment.stats().points != descriptor.points {
                return Err(Error::Corruption {
                    offset: 0,
                    reason: format!(
                        "raw segment {} point count does not match the manifest",
                        descriptor.file
                    ),
                });
            }
            opened.push(segment);
        }
        self.database.attach_sealed_segments(opened);
        Ok(())
    }

    fn require_clean_restore_source(&self, report: &IntegrityReport) -> Result<()> {
        if report.stale_rollup_files > 0 {
            return Err(Error::Corruption {
                offset: 0,
                reason: format!(
                    "restore requires a clean backup; {} active rollup files trail the raw log",
                    report.stale_rollup_files
                ),
            });
        }
        if report.raw_recovered_tail_bytes == 0
            && report.raw_recovered_tail == crate::RecoveredTail::None
        {
            return Ok(());
        }
        let physical_bytes = std::fs::metadata(self.root.join(ACTIVE_LOG))?.len();
        Err(Error::Corruption {
            offset: physical_bytes.saturating_sub(report.raw_recovered_tail_bytes),
            reason: format!(
                "restore requires a recovery-free backup; raw log has {} bytes of {}",
                report.raw_recovered_tail_bytes, report.raw_recovered_tail
            ),
        })
    }

    fn write_snapshot(&self, temporary: &Path) -> Result<BackupReport> {
        let manifests = temporary.join(MANIFEST_DIRECTORY);
        let rollups = temporary.join(ROLLUP_DIRECTORY);
        let segments = temporary.join(SEGMENT_DIRECTORY);
        std::fs::create_dir(&manifests)?;
        std::fs::create_dir(&rollups)?;
        std::fs::create_dir(&segments)?;
        let mut report = BackupReport {
            manifest_generation: self.manifest.generation,
            ..BackupReport::default()
        };

        // Never hard-link the active log: future appends to the source inode
        // must not mutate the backup snapshot.
        publication_checkpoint(PublicationStep::Copy)?;
        copy_and_sync(&self.root.join(ACTIVE_LOG), &temporary.join(ACTIVE_LOG))?;
        report.record_copy();
        for descriptor in self.active_rollups() {
            let outcome = hard_link_or_copy(
                &self.rollup_directory.join(&descriptor.file),
                &rollups.join(&descriptor.file),
            )?;
            report.record_link_or_copy(outcome);
        }
        for descriptor in &self.manifest.segments {
            let outcome = hard_link_or_copy(
                &self.segment_directory.join(&descriptor.file),
                &segments.join(&descriptor.file),
            )?;
            report.record_link_or_copy(outcome);
        }
        if self.manifest.generation > 0 {
            let file = format!("MANIFEST.{:020}", self.manifest.generation);
            let outcome =
                hard_link_or_copy(&self.manifest_directory.join(&file), &manifests.join(&file))?;
            report.record_link_or_copy(outcome);
        }
        publication_checkpoint(PublicationStep::Sync)?;
        sync_directory(&manifests)?;
        sync_directory(&rollups)?;
        sync_directory(&segments)?;
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
            }
        }
        if changed {
            next.generation = next.generation.saturating_add(1);
            self.publish_or_poison(next)?;
        }
        Ok(())
    }

    /// The read-only sibling of `verify_and_reconcile_manifest`: identical
    /// corruption checks, but a descriptor whose provenance trails the raw
    /// log is counted for [`IntegrityReport::stale_rollup_files`] instead of
    /// being invalidated by publishing a new manifest generation.
    fn verify_manifest_read_only(&mut self) -> Result<()> {
        let stats = self.database.stats()?;
        let mut stale_rollup_files = 0_usize;
        for descriptor in self.manifest.rollups.iter().filter(|rollup| rollup.active) {
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
                stale_rollup_files += 1;
            }
        }
        self.stale_rollup_files = stale_rollup_files;
        Ok(())
    }

    fn publish_or_poison(&mut self, mut next: Manifest) -> Result<()> {
        // Inactive descriptors are pure history: queries, reconciliation,
        // integrity checks, and backups all filter on `active`, so a new
        // generation drops them instead of carrying them forever. Their
        // segment files stay on disk until no retained fallback manifest
        // generation references them (see `remove_unreferenced_files`).
        next.rollups.retain(|rollup| rollup.active);
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
        self.remove_unreferenced_files();
        Ok(())
    }

    /// Best-effort space reclamation after a durable publish, also run once at
    /// open to catch orphans left by an interrupted `maintain`: prunes manifest
    /// generations beyond the retained fallback window, then unlinks rollup
    /// segments that no retained generation references. Nothing here may fail
    /// the store — the current manifest is already durable, and any file that
    /// survives one pass is reconsidered by the next.
    fn remove_unreferenced_files(&self) {
        // Passing the loaded generation keeps the manifest this store is
        // actually running on alive even when `load` fell back past the
        // newest filename window, and keeps its rollup references in the
        // retained set below.
        let Ok(retained) =
            manifest::prune_generations(&self.manifest_directory, self.manifest.generation)
        else {
            return;
        };
        // If any retained generation cannot be read back, a segment cannot be
        // proven unreferenced (`load` may still fall back to that generation),
        // so skip segment deletion entirely for this pass.
        let Ok(mut referenced) = manifest::referenced_rollup_files(&retained) else {
            return;
        };
        let Ok(mut referenced_segments) = manifest::referenced_segment_files(&retained) else {
            return;
        };
        for rollup in &self.manifest.rollups {
            referenced.insert(rollup.file.clone());
        }
        for segment in &self.manifest.segments {
            referenced_segments.insert(segment.file.clone());
        }
        let Ok(entries) = std::fs::read_dir(&self.rollup_directory) else {
            return;
        };
        let mut removed = false;
        for entry in entries.flatten() {
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            // Only published `.rseg` files are swept; in-flight temporaries
            // are owned by their writer's own failure cleanup.
            if name.ends_with(".rseg") && !referenced.contains(&name) {
                removed |= std::fs::remove_file(entry.path()).is_ok();
            }
        }
        if removed {
            let _ = sync_directory(&self.rollup_directory);
        }
        let Ok(entries) = std::fs::read_dir(&self.segment_directory) else {
            return;
        };
        let mut removed_segments = false;
        for entry in entries.flatten() {
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if name.ends_with(".wseg") && !referenced_segments.contains(&name) {
                removed_segments |= std::fs::remove_file(entry.path()).is_ok();
            }
        }
        if removed_segments {
            let _ = sync_directory(&self.segment_directory);
        }
    }

    fn ensure_healthy(&self) -> Result<()> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        self.ensure_healthy()
    }

    fn cached_rollup_buckets(
        &self,
        descriptors: &[&RollupDescriptor],
        start: i64,
        end: i64,
    ) -> Result<Vec<GaugeBucket>> {
        let mut missing = Vec::new();
        {
            let cache = self.rollup_cache.read().map_err(|_| Error::Poisoned)?;
            if descriptors
                .iter()
                .all(|descriptor| cache.contains_key(&descriptor.file))
            {
                return Ok(descriptors
                    .iter()
                    .flat_map(|descriptor| {
                        cache
                            .get(&descriptor.file)
                            .expect("checked")
                            .query(start, end)
                    })
                    .collect());
            }
            for descriptor in descriptors {
                if !cache.contains_key(&descriptor.file) {
                    missing.push(descriptor.file.clone());
                }
            }
        }

        let mut opened = Vec::with_capacity(missing.len());
        for file in missing {
            opened.push((
                file.clone(),
                RollupSegment::open(self.rollup_directory.join(&file))?,
            ));
        }

        let mut cache = self.rollup_cache.write().map_err(|_| Error::Poisoned)?;
        for (file, segment) in opened {
            cache.entry(file).or_insert(segment);
        }
        let mut buckets = Vec::new();
        for descriptor in descriptors {
            if let Some(segment) = cache.get(&descriptor.file) {
                buckets.extend(segment.query(start, end));
                continue;
            }
            let segment = RollupSegment::open(self.rollup_directory.join(&descriptor.file))?;
            buckets.extend(segment.query(start, end));
            cache.insert(descriptor.file.clone(), segment);
        }
        trim_rollup_cache(&mut cache, descriptors);
        Ok(buckets)
    }
}

fn trim_rollup_cache(cache: &mut HashMap<String, RollupSegment>, keep: &[&RollupDescriptor]) {
    if cache.len() <= MAX_CACHED_ROLLUP_SEGMENTS {
        return;
    }
    let keep: HashSet<&str> = keep
        .iter()
        .map(|descriptor| descriptor.file.as_str())
        .collect();
    let overflow = cache.len() - MAX_CACHED_ROLLUP_SEGMENTS;
    let evict: Vec<String> = cache
        .keys()
        .filter(|file| !keep.contains(file.as_str()))
        .take(overflow)
        .cloned()
        .collect();
    for file in evict {
        cache.remove(&file);
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
            Ok(FixedGaugeRollup::build(points, *micros, max_gap_micros)?
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

fn has_retention_work(
    manifest: &Manifest,
    definitions: &[SeriesDefinition],
    now_micros: i64,
) -> bool {
    definitions.iter().any(|definition| {
        definition.semantics == SeriesSemantics::Gauge
            && definition.rollup_policy.tiers.iter().any(|tier| {
                let Some(retention) = tier.retain_for_micros else {
                    return false;
                };
                let cutoff = now_micros.saturating_sub(retention);
                manifest.rollups.iter().any(|rollup| {
                    rollup.active
                        && rollup.series_id == definition.id
                        && rollup.resolution == tier.resolution
                        && rollup.end < cutoff
                })
            })
    })
}

fn deactivate_expired_rollups(
    next: &mut Manifest,
    definition: &SeriesDefinition,
    now_micros: i64,
) -> bool {
    let mut changed = false;
    for tier in &definition.rollup_policy.tiers {
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
    }
    changed
}

fn stamp_active_series_source(
    next: &mut Manifest,
    series_id: u64,
    source_commit: u64,
    source_points: u64,
) -> bool {
    let mut changed = false;
    for rollup in &mut next.rollups {
        if rollup.active
            && rollup.series_id == series_id
            && (rollup.source_commit != source_commit || rollup.source_points != source_points)
        {
            rollup.source_commit = source_commit;
            rollup.source_points = source_points;
            changed = true;
        }
    }
    changed
}

fn series_gained_completed_shard(
    definition: &SeriesDefinition,
    prev_now: i64,
    now_micros: i64,
) -> Result<bool> {
    if now_micros <= prev_now {
        return Ok(false);
    }
    for tier in &definition.rollup_policy.tiers {
        if latest_completed_shard_end(now_micros, &tier.resolution)?
            > latest_completed_shard_end(prev_now, &tier.resolution)?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn series_has_missing_completed_shard(
    definition: &SeriesDefinition,
    bounds: Option<(i64, i64)>,
    now_micros: i64,
    rollups: &[RollupDescriptor],
) -> Result<bool> {
    let Some((earliest, latest)) = bounds else {
        return Ok(false);
    };
    for tier in &definition.rollup_policy.tiers {
        let needed = needed_completed_shards(
            definition.id,
            &tier.resolution,
            earliest,
            latest,
            now_micros,
            rollups,
            u64::MAX,
        )?;
        if !needed.is_empty() {
            return Ok(true);
        }
    }
    Ok(false)
}

struct ShardBounds {
    start: i64,
    end: i64,
}

fn needed_completed_shards(
    series_id: u64,
    resolution: &RollupResolution,
    earliest: i64,
    latest: i64,
    now_micros: i64,
    rollups: &[RollupDescriptor],
    current_points: u64,
) -> Result<Vec<ShardBounds>> {
    let covered: HashSet<(i64, i64)> = rollups
        .iter()
        .filter(|rollup| {
            rollup.active
                && rollup.series_id == series_id
                && &rollup.resolution == resolution
                && (current_points == u64::MAX || rollup.source_points == current_points)
        })
        .map(|rollup| (rollup.start, rollup.end))
        .collect();
    let mut needed = Vec::new();
    match resolution {
        RollupResolution::FixedMicros(micros) => {
            let width = fixed_shard_width(*micros)?;
            let mut start = earliest.div_euclid(width) * width;
            while start <= latest {
                let end = start.saturating_add(width);
                if end <= now_micros && !covered.contains(&(start, end)) {
                    needed.push(ShardBounds { start, end });
                }
                if end <= start {
                    break;
                }
                start = end;
            }
        }
        RollupResolution::Calendar {
            unit,
            iana_timezone,
        } => {
            let mut cursor = earliest;
            while cursor <= latest {
                let (start, end) = calendar_bucket_bounds(cursor, *unit, iana_timezone)?;
                if end <= now_micros && !covered.contains(&(start, end)) {
                    needed.push(ShardBounds { start, end });
                }
                if end <= cursor {
                    break;
                }
                cursor = end;
            }
        }
    }
    Ok(needed)
}

fn latest_completed_shard_end(
    now_micros: i64,
    resolution: &RollupResolution,
) -> Result<Option<i64>> {
    match resolution {
        RollupResolution::FixedMicros(micros) => {
            let width = fixed_shard_width(*micros)?;
            Ok(Some(now_micros.div_euclid(width) * width))
        }
        RollupResolution::Calendar {
            unit,
            iana_timezone,
        } => {
            let (start, _) = calendar_bucket_bounds(now_micros, *unit, iana_timezone)?;
            Ok(Some(start))
        }
    }
}

fn fixed_shard_width(micros: i64) -> Result<i64> {
    if micros <= 0 {
        return Err(Error::InvalidModel(
            "fixed rollup resolution must be positive".to_owned(),
        ));
    }
    Ok(
        if micros <= UTC_DAY_MICROS && UTC_DAY_MICROS % micros == 0 {
            UTC_DAY_MICROS
        } else {
            micros
        },
    )
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

fn published_seal_generations(manifest: &Manifest) -> HashSet<u64> {
    manifest
        .segments
        .iter()
        .map(|segment| segment.generation)
        .collect()
}

fn raw_segment_file_name(generation: u64) -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("g{generation}-{nonce}.wseg")
}

#[derive(Clone, Debug, Default)]
struct SealedSalvagePlan {
    manifest: Manifest,
}

impl SealedSalvagePlan {
    fn sealed_points(&self) -> u64 {
        self.manifest
            .segments
            .iter()
            .map(|segment| segment.points)
            .sum()
    }
}

fn salvage_snapshot_paths(sealed: &SealedSalvagePlan) -> Vec<String> {
    let mut files = vec![ACTIVE_LOG.to_owned()];
    files.extend(
        sealed
            .manifest
            .segments
            .iter()
            .map(|descriptor| format!("{SEGMENT_DIRECTORY}/{}", descriptor.file)),
    );
    files.sort();
    files.dedup();
    files
}

fn list_wseg_names(root: &Path) -> Result<Vec<String>> {
    let segments = root.join(SEGMENT_DIRECTORY);
    let entries = match std::fs::read_dir(&segments) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(Error::Io(error)),
    };
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if name.ends_with(".wseg") {
            names.push(name);
        }
    }
    names.sort();
    Ok(names)
}

/// Recovers sealed `.wseg` coverage from the highest valid manifest. Missing
/// or unreadable sealed files fail closed so salvage never publishes a store
/// that silently dropped historical raw.
fn plan_sealed_salvage(root: &Path, options: SalvageOptions) -> Result<SealedSalvagePlan> {
    let on_disk = list_wseg_names(root)?;
    let manifests = root.join(MANIFEST_DIRECTORY);
    let loaded = if manifests.is_dir() {
        match Manifest::load(&manifests) {
            Ok(manifest) => manifest,
            Err(_) if on_disk.is_empty() => Manifest::default(),
            Err(error) => return Err(error),
        }
    } else if on_disk.is_empty() {
        Manifest::default()
    } else {
        return Err(Error::Corruption {
            offset: 0,
            reason: "salvage found sealed raw segments but no readable manifest descriptors"
                .to_owned(),
        });
    };

    if loaded.segments.is_empty() && !on_disk.is_empty() {
        return Err(Error::Corruption {
            offset: 0,
            reason: "salvage found sealed raw segments but no manifest descriptors".to_owned(),
        });
    }

    let referenced: HashSet<&str> = loaded
        .segments
        .iter()
        .map(|segment| segment.file.as_str())
        .collect();
    for name in &on_disk {
        if !referenced.contains(name.as_str()) {
            if options.drop_orphan_segments {
                continue;
            }
            return Err(Error::Corruption {
                offset: 0,
                reason: format!(
                    "salvage found sealed segment {name} not named by the recovered manifest"
                ),
            });
        }
    }

    let segment_directory = root.join(SEGMENT_DIRECTORY);
    for descriptor in &loaded.segments {
        let path = segment_directory.join(&descriptor.file);
        let segment = Segment::open(&path).map_err(|error| Error::Corruption {
            offset: 0,
            reason: format!(
                "sealed raw segment {} is unreadable: {error}",
                descriptor.file
            ),
        })?;
        segment.verify_blocks().map_err(|error| Error::Corruption {
            offset: 0,
            reason: format!(
                "sealed raw segment {} failed block verification: {error}",
                descriptor.file
            ),
        })?;
        if segment.stats().points != descriptor.points {
            return Err(Error::Corruption {
                offset: 0,
                reason: format!(
                    "sealed raw segment {} does not match its manifest point count",
                    descriptor.file
                ),
            });
        }
    }

    Ok(SealedSalvagePlan {
        manifest: Manifest {
            generation: loaded.generation,
            rollups: Vec::new(),
            segments: loaded.segments,
        },
    })
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

fn copy_and_sync(source: &Path, destination: &Path) -> std::io::Result<u64> {
    let bytes = std::fs::copy(source, destination)?;
    std::fs::set_permissions(destination, std::fs::Permissions::from_mode(0o600))?;
    std::fs::File::open(destination)?.sync_all()?;
    Ok(bytes)
}

fn write_salvage_stage(
    source: &mut SalvageSource,
    source_root: &Path,
    temporary: &Path,
    sealed: &SealedSalvagePlan,
) -> Result<()> {
    let manifests = temporary.join(MANIFEST_DIRECTORY);
    let rollups = temporary.join(ROLLUP_DIRECTORY);
    let segments = temporary.join(SEGMENT_DIRECTORY);
    std::fs::create_dir(&manifests)?;
    std::fs::create_dir(&rollups)?;
    if !sealed.manifest.segments.is_empty() {
        std::fs::create_dir(&segments)?;
    }
    publication_checkpoint(PublicationStep::Copy)?;
    source.file.seek(SeekFrom::Start(0))?;
    let mut active = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(temporary.join(ACTIVE_LOG))?;
    let mut prefix = std::io::Read::by_ref(&mut source.file).take(source.recovered_prefix_bytes);
    let copied = std::io::copy(&mut prefix, &mut active)?;
    if copied != source.recovered_prefix_bytes {
        return Err(Error::SourceChanged {
            path: source.path.clone(),
        });
    }
    active.sync_all()?;
    for descriptor in &sealed.manifest.segments {
        hard_link_or_copy(
            &source_root.join(SEGMENT_DIRECTORY).join(&descriptor.file),
            &segments.join(&descriptor.file),
        )?;
    }
    if !sealed.manifest.segments.is_empty() {
        let mut salvage_manifest = sealed.manifest.clone();
        if salvage_manifest.generation == 0 {
            salvage_manifest.generation = 1;
        }
        salvage_manifest.publish(&manifests)?;
    }
    source.ensure_unchanged()?;
    publication_checkpoint(PublicationStep::Sync)?;
    sync_directory(&manifests)?;
    sync_directory(&rollups)?;
    if !sealed.manifest.segments.is_empty() {
        sync_directory(&segments)?;
    }
    sync_directory(temporary)?;
    Ok(())
}

#[derive(Debug)]
enum LinkOrCopy {
    Linked,
    Copied { link_error: std::io::Error },
}

#[derive(Debug)]
struct HardLinkThenCopyError {
    link_error: std::io::Error,
    copy_error: std::io::Error,
}

impl std::fmt::Display for HardLinkThenCopyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "hard link failed: {}; copy failed: {}",
            self.link_error, self.copy_error
        )
    }
}

impl std::error::Error for HardLinkThenCopyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.link_error)
    }
}

fn hard_link_or_copy_with<L, C>(
    source: &Path,
    destination: &Path,
    link: L,
    copy: C,
) -> std::io::Result<LinkOrCopy>
where
    L: FnOnce(&Path, &Path) -> std::io::Result<()>,
    C: FnOnce(&Path, &Path) -> std::io::Result<u64>,
{
    match link(source, destination) {
        Ok(()) => Ok(LinkOrCopy::Linked),
        Err(link_error) => match copy(source, destination) {
            Ok(_) => Ok(LinkOrCopy::Copied { link_error }),
            Err(copy_error) => {
                let kind = copy_error.kind();
                Err(std::io::Error::new(
                    kind,
                    HardLinkThenCopyError {
                        link_error,
                        copy_error,
                    },
                ))
            }
        },
    }
}

fn hard_link_or_copy(source: &Path, destination: &Path) -> std::io::Result<LinkOrCopy> {
    let mode = std::fs::symlink_metadata(source)?.permissions().mode();
    if mode & 0o077 != 0 {
        // A shared inode keeps the source mode. Copying into a 0755 backup
        // directory must not republish a 0644 rollup or manifest.
        copy_and_sync(source, destination)?;
        return Ok(LinkOrCopy::Copied {
            link_error: std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "refusing to hard-link a group- or world-accessible inode",
            ),
        });
    }
    hard_link_or_copy_with(
        source,
        destination,
        |source, destination| std::fs::hard_link(source, destination),
        copy_and_sync,
    )
}

fn directory_bytes(path: &Path) -> Result<u64> {
    let mut total = 0_u64;
    let mut directories = vec![path.to_path_buf()];
    while let Some(directory) = directories.pop() {
        require_real_directory(&directory)?;
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            let entry_path = entry.path();
            let metadata = std::fs::symlink_metadata(&entry_path)?;
            if metadata.file_type().is_dir() {
                directories.push(entry_path);
            } else if metadata.file_type().is_file() {
                total = total.checked_add(metadata.len()).ok_or_else(|| {
                    Error::Serialization("stored byte count exceeds u64".to_owned())
                })?;
            } else {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "stored path is not a regular file or directory: {}",
                        entry_path.display()
                    ),
                )));
            }
        }
    }
    Ok(total)
}

fn create_or_require_real_directory(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => require_real_directory(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut builder = std::fs::DirBuilder::new();
            builder.mode(0o700).create(path)?;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
            require_real_directory(path)
        }
        Err(error) => Err(Error::Io(error)),
    }
}

fn require_real_directory(path: &Path) -> Result<()> {
    if std::fs::symlink_metadata(path)?.file_type().is_dir() {
        return Ok(());
    }
    Err(Error::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("snapshot path is not a real directory: {}", path.display()),
    )))
}

fn snapshot_mismatch(
    left_name: &str,
    left: crate::snapshot::SnapshotDigest,
    right_name: &str,
    right: crate::snapshot::SnapshotDigest,
) -> Error {
    Error::Corruption {
        offset: 0,
        reason: format!(
            "snapshot mismatch: {left_name} files={} bytes={} crc32={:08x}, {right_name} files={} bytes={} crc32={:08x}",
            left.files, left.bytes, left.crc32, right.files, right.bytes, right.crc32
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BackupReport, LinkOrCopy, RollupSource, SalvageOptions, SalvageStatus, Store,
        fail_next_seal_reclaim, hard_link_or_copy, hard_link_or_copy_with,
    };
    use crate::snapshot::{PublicationStep, StagedDirectory, fail_next_publication_step};
    use crate::storage::mutate_salvage_source_after_identity_checks;
    use crate::{
        CalendarUnit, Entity, EntityId, Point, RollupPolicy, RollupResolution, RollupTier,
        SalvageStopReason, Segment, SeriesDefinition, SeriesSemantics, Transaction,
    };
    use std::collections::BTreeMap;
    use std::error::Error as _;
    use std::io::{Seek, SeekFrom, Write};
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::path::Path;
    use tempfile::tempdir;

    const SECOND: i64 = 1_000_000;
    const DAY: i64 = 86_400 * SECOND;

    #[test]
    fn hard_link_path_does_not_copy() {
        let result = hard_link_or_copy_with(
            Path::new("source"),
            Path::new("destination"),
            |_, _| Ok(()),
            |_, _| panic!("copy must not run after a successful hard link"),
        )
        .unwrap();

        assert!(matches!(result, LinkOrCopy::Linked));
    }

    #[test]
    fn hard_link_copies_group_readable_source_as_private() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("source");
        let destination = directory.path().join("destination");
        std::fs::write(&source, b"rollup-bytes").unwrap();
        std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o644)).unwrap();

        match hard_link_or_copy(&source, &destination).unwrap() {
            LinkOrCopy::Copied { link_error } => {
                assert_eq!(link_error.kind(), std::io::ErrorKind::PermissionDenied);
            }
            LinkOrCopy::Linked => panic!("group-readable source must be copied"),
        }
        assert_eq!(std::fs::read(&destination).unwrap(), b"rollup-bytes");
        assert_eq!(
            std::fs::metadata(&destination)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_ne!(
            std::fs::metadata(&source).unwrap().ino(),
            std::fs::metadata(&destination).unwrap().ino()
        );
    }

    #[test]
    fn hard_link_keeps_a_private_source_inode() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("source");
        let destination = directory.path().join("destination");
        std::fs::write(&source, b"private").unwrap();
        std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o600)).unwrap();

        assert!(matches!(
            hard_link_or_copy(&source, &destination).unwrap(),
            LinkOrCopy::Linked
        ));
        assert_eq!(
            std::fs::metadata(&source).unwrap().ino(),
            std::fs::metadata(&destination).unwrap().ino()
        );
    }

    #[test]
    fn copy_fallback_reports_the_hard_link_cause() {
        let result = hard_link_or_copy_with(
            Path::new("source"),
            Path::new("destination"),
            |_, _| {
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "link denied",
                ))
            },
            |_, _| Ok(12),
        )
        .unwrap();

        match result {
            LinkOrCopy::Copied { link_error } => {
                assert_eq!(link_error.kind(), std::io::ErrorKind::PermissionDenied);
                assert!(link_error.to_string().contains("link denied"));
            }
            LinkOrCopy::Linked => panic!("expected a copy fallback"),
        }
    }

    #[test]
    fn backup_report_counts_links_copies_and_fallback_kinds() {
        let mut report = BackupReport::default();
        report.record_copy();
        report.record_link_or_copy(LinkOrCopy::Linked);
        report.record_link_or_copy(LinkOrCopy::Copied {
            link_error: std::io::Error::new(
                std::io::ErrorKind::CrossesDevices,
                "different filesystems",
            ),
        });

        assert_eq!(report.files, 3);
        assert_eq!(report.linked_files, 1);
        assert_eq!(report.copied_files, 2);
        assert_eq!(report.hard_link_fallbacks, 1);
        assert_eq!(
            report.hard_link_fallback_error_kinds,
            [std::io::ErrorKind::CrossesDevices]
        );
    }

    #[test]
    fn double_failure_keeps_copy_kind_and_both_causes() {
        let error = hard_link_or_copy_with(
            Path::new("source"),
            Path::new("destination"),
            |_, _| {
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "link denied",
                ))
            },
            |_, _| {
                Err(std::io::Error::new(
                    std::io::ErrorKind::StorageFull,
                    "copy disk full",
                ))
            },
        )
        .unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::StorageFull);
        let message = error.to_string();
        assert!(message.contains("link denied"));
        assert!(message.contains("copy disk full"));
        let link = error
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap();
        assert_eq!(link.kind(), std::io::ErrorKind::PermissionDenied);
    }

    /// Every file under `root` (recursively) with its full contents, so a
    /// test can prove an operation changed nothing on disk.
    fn directory_snapshot(root: &std::path::Path) -> BTreeMap<String, Vec<u8>> {
        fn walk(directory: &std::path::Path, prefix: &str, into: &mut BTreeMap<String, Vec<u8>>) {
            for entry in std::fs::read_dir(directory).unwrap() {
                let entry = entry.unwrap();
                let name = format!("{prefix}{}", entry.file_name().to_string_lossy());
                if entry.metadata().unwrap().is_dir() {
                    walk(&entry.path(), &format!("{name}/"), into);
                } else {
                    into.insert(name, std::fs::read(entry.path()).unwrap());
                }
            }
        }
        let mut snapshot = BTreeMap::new();
        walk(root, "", &mut snapshot);
        snapshot
    }

    fn stored_files(directory: &std::path::Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

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

    fn create_verified_backup(root: &Path, with_rollup: bool) -> std::path::PathBuf {
        let source = root.join("source");
        let backup = root.join("backup");
        let mut store = Store::open(&source).unwrap();
        let tiers = with_rollup
            .then_some(vec![RollupTier {
                resolution: RollupResolution::FixedMicros(5 * SECOND),
                retain_for_micros: None,
            }])
            .unwrap_or_default();
        initialize(&mut store, tiers, None);
        let mut transaction = Transaction::new();
        transaction.append_points(points());
        store.commit(transaction).unwrap();
        if with_rollup {
            store.maintain(DAY).unwrap();
        }
        store.backup_to(&backup).unwrap();
        backup
    }

    fn assert_io_kind(error: crate::Error, expected: std::io::ErrorKind) {
        match error {
            crate::Error::Io(error) => assert_eq!(error.kind(), expected),
            other => panic!("expected I/O error {expected:?}, got {other:?}"),
        }
    }

    #[cfg(target_os = "linux")]
    fn create_fifo(path: &Path) {
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            path,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .unwrap();
    }

    #[cfg(target_os = "macos")]
    fn create_fifo(path: &Path) {
        use std::os::unix::ffi::OsStrExt;

        let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: `path` is a live NUL-terminated string and mode has no
        // platform-dependent bits beyond user read/write permissions.
        let result = unsafe { libc::mkfifo(path.as_ptr(), 0o600) };
        if result != 0 {
            panic!("mkfifo failed: {}", std::io::Error::last_os_error());
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn create_fifo(_path: &Path) {
        panic!("FIFO restore tests require Linux or macOS");
    }

    fn restore_with_deadline(
        backup: std::path::PathBuf,
        target: std::path::PathBuf,
        fifo: &Path,
    ) -> crate::Result<crate::RestoreReport> {
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            sender.send(Store::restore_from(backup, target)).unwrap();
        });
        let mut timed_out = false;
        let result = match receiver.recv_timeout(std::time::Duration::from_secs(5)) {
            Ok(result) => result,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                timed_out = true;
                let writer = rustix::fs::open(
                    fifo,
                    rustix::fs::OFlags::WRONLY
                        | rustix::fs::OFlags::CLOEXEC
                        | rustix::fs::OFlags::NONBLOCK,
                    rustix::fs::Mode::empty(),
                )
                .expect("a blocked FIFO reader must admit a nonblocking writer");
                drop(writer);
                receiver
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .expect("restore worker must exit after its FIFO reader is released")
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("restore worker disconnected without a result")
            }
        };
        worker.join().unwrap();
        assert!(!timed_out, "restore blocked while opening a selected FIFO");
        result
    }

    fn salvage_with_deadline(
        source: std::path::PathBuf,
        target: std::path::PathBuf,
        fifo: &Path,
    ) -> crate::Result<crate::SalvageReport> {
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            sender.send(Store::salvage_from(source, target)).unwrap();
        });
        let mut timed_out = false;
        let result = match receiver.recv_timeout(std::time::Duration::from_secs(5)) {
            Ok(result) => result,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                timed_out = true;
                let writer = rustix::fs::open(
                    fifo,
                    rustix::fs::OFlags::WRONLY
                        | rustix::fs::OFlags::CLOEXEC
                        | rustix::fs::OFlags::NONBLOCK,
                    rustix::fs::Mode::empty(),
                )
                .expect("a blocked FIFO reader must admit a nonblocking writer");
                drop(writer);
                receiver
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .expect("salvage worker must exit after its FIFO reader is released")
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("salvage worker disconnected without a result")
            }
        };
        worker.join().unwrap();
        assert!(!timed_out, "salvage blocked while opening active.wlog");
        result
    }

    fn flip_last_byte(path: &Path) {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        file.seek(SeekFrom::End(-1)).unwrap();
        let mut byte = [0_u8; 1];
        std::io::Read::read_exact(&mut file, &mut byte).unwrap();
        file.seek(SeekFrom::End(-1)).unwrap();
        file.write_all(&[byte[0] ^ 0xff]).unwrap();
        file.sync_all().unwrap();
    }

    fn overwrite_byte(path: &Path, offset: u64, byte: u8) {
        let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&[byte]).unwrap();
        file.sync_all().unwrap();
    }

    fn restore_stages(parent: &Path, target_name: &str) -> Vec<String> {
        let prefix = format!(".{target_name}.restore-");
        stored_files(parent)
            .into_iter()
            .filter(|name| name.starts_with(&prefix))
            .collect()
    }

    fn salvage_stages(parent: &Path, target_name: &str) -> Vec<String> {
        let prefix = format!(".{target_name}.salvage-");
        stored_files(parent)
            .into_iter()
            .filter(|name| name.starts_with(&prefix))
            .collect()
    }

    fn active_manifest_path(backup: &Path) -> std::path::PathBuf {
        let store = Store::open_read_only(backup).unwrap();
        let generation = store.manifest_generation();
        drop(store);
        backup
            .join("manifests")
            .join(format!("MANIFEST.{generation:020}"))
    }

    fn active_rollup_path(backup: &Path) -> std::path::PathBuf {
        let store = Store::open_read_only(backup).unwrap();
        let file = store.active_rollups().next().unwrap().file.clone();
        drop(store);
        backup.join("rollups").join(file)
    }

    #[test]
    fn open_creates_and_reopens_a_nested_root() {
        let directory = tempdir().unwrap();
        // A root whose own directory entry did not exist before the open
        // exercises the created-root publication path (root sync plus the
        // parent entry sync) alongside the active log creation sync.
        let root = directory.path().join("nested").join("store");
        {
            let mut store = Store::open(&root).unwrap();
            initialize(&mut store, Vec::new(), None);
            store.close().unwrap();
        }
        let store = Store::open(&root).unwrap();
        assert_eq!(store.database().stats().unwrap().catalog_records, 2);
    }

    #[test]
    fn open_creates_an_owner_only_root_and_active_log() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("private-store");
        Store::open(&root).unwrap().close().unwrap();
        assert_eq!(
            std::fs::symlink_metadata(&root)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(root.join("active.wlog"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        for directory in ["manifests", "rollups", "segments"] {
            assert_eq!(
                std::fs::symlink_metadata(root.join(directory))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
    }

    #[test]
    fn writable_open_rejects_symlinked_store_directories() {
        let directory = tempdir().unwrap();
        let outside = directory.path().join("outside");
        std::fs::create_dir(&outside).unwrap();

        let linked_root = directory.path().join("linked-root");
        std::os::unix::fs::symlink(&outside, &linked_root).unwrap();
        assert!(Store::open(&linked_root).is_err());
        assert!(std::fs::read_dir(&outside).unwrap().next().is_none());

        for child in ["manifests", "rollups", "segments"] {
            let root = directory.path().join(format!("store-{child}"));
            let child_outside = directory.path().join(format!("outside-{child}"));
            std::fs::create_dir(&root).unwrap();
            std::fs::create_dir(&child_outside).unwrap();
            std::os::unix::fs::symlink(&child_outside, root.join(child)).unwrap();

            assert!(Store::open(&root).is_err(), "accepted symlinked {child}");
            assert!(
                std::fs::read_dir(&child_outside).unwrap().next().is_none(),
                "wrote through symlinked {child}"
            );
        }
    }

    #[test]
    fn stored_bytes_rejects_symlinks_instead_of_following_them() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("store");
        let outside = directory.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("large"), vec![0_u8; 4096]).unwrap();
        let store = Store::open(&root).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("linked-outside")).unwrap();

        assert!(store.stored_bytes().is_err());
    }

    #[test]
    fn open_of_a_precreated_root_still_publishes_the_parent_entry() {
        // ftwdb-shadow creates the private store directory before Store::open.
        // The parent fsync must still run; otherwise Always commits can be
        // acknowledged for a directory whose parent dirent is not durable.
        let directory = tempdir().unwrap();
        let root = directory.path().join("precreated");
        std::fs::create_dir(&root).unwrap();
        {
            let mut store = Store::open(&root).unwrap();
            initialize(&mut store, Vec::new(), None);
            store.close().unwrap();
        }
        let store = Store::open_read_only(&root).unwrap();
        assert_eq!(store.database().stats().unwrap().catalog_records, 2);
    }

    #[test]
    fn second_store_opener_fails_until_the_first_closes() {
        let directory = tempdir().unwrap();
        let first = Store::open(directory.path()).unwrap();
        match Store::open(directory.path()) {
            Err(crate::Error::Locked { path }) => {
                assert_eq!(path, directory.path().join("active.wlog"));
            }
            Err(other) => panic!("expected Error::Locked, got {other:?}"),
            Ok(_) => panic!("expected Error::Locked, got a second open store"),
        }
        first.close().unwrap();
        let mut reopened = Store::open(directory.path()).unwrap();
        initialize(&mut reopened, Vec::new(), None);
    }

    #[test]
    fn read_only_open_neither_reconciles_nor_sweeps() {
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
            store.close().unwrap();
        }
        // Rollup provenance now trails the raw log, as after a crash between
        // raw fsync and manifest invalidation: a writable open would publish
        // a reconciling manifest generation.
        {
            let mut database = crate::Database::open(directory.path().join("active.wlog")).unwrap();
            let mut correction = Point::actual(1, 6 * SECOND, 100.0);
            correction.change_time = 30 * SECOND;
            let mut transaction = Transaction::new();
            transaction.append_points(vec![correction]);
            database.commit(transaction).unwrap();
            database.close().unwrap();
        }
        // An orphan that a writable open would sweep.
        std::fs::write(
            directory
                .path()
                .join("rollups")
                .join("g9-s1-f5000000-0-42.rseg"),
            b"junk",
        )
        .unwrap();
        let before = directory_snapshot(directory.path());

        let mut store = Store::open_read_only(directory.path()).unwrap();
        // Staleness is surfaced as information instead of a mutation.
        assert_eq!(store.check_integrity().unwrap().stale_rollup_files, 1);
        // The stale materialization is not served; queries fall back to raw.
        assert_eq!(
            store
                .query_gauge(1, 0, 20 * SECOND, &resolution)
                .unwrap()
                .source,
            RollupSource::Raw
        );
        // Writer APIs fail with a structured error.
        assert!(matches!(
            store.commit(Transaction::new()),
            Err(crate::Error::ReadOnly)
        ));
        assert!(matches!(store.append(&[]), Err(crate::Error::ReadOnly)));
        assert!(matches!(store.maintain(DAY), Err(crate::Error::ReadOnly)));
        assert!(matches!(store.flush(), Err(crate::Error::ReadOnly)));
        store.close().unwrap();

        assert_eq!(directory_snapshot(directory.path()), before);
    }

    #[test]
    fn read_only_store_open_does_not_create_a_missing_store() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("absent-store");
        assert!(Store::open_read_only(&path).is_err());
        assert!(!path.exists());
    }

    #[test]
    fn backup_from_a_read_only_store_leaves_the_source_untouched() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("source");
        let destination = directory.path().join("backup");
        let resolution = RollupResolution::FixedMicros(5 * SECOND);
        {
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
            store.close().unwrap();
        }
        let before = directory_snapshot(&source);

        let mut store = Store::open_read_only(&source).unwrap();
        let report = store.backup_to(&destination).unwrap();
        assert!(report.files >= 3);
        assert_eq!(report.copied_files, 1);
        assert_eq!(report.linked_files + report.copied_files, report.files);
        assert_eq!(report.hard_link_fallbacks, 0);
        assert!(report.hard_link_fallback_error_kinds.is_empty());
        store.close().unwrap();
        assert_eq!(directory_snapshot(&source), before);

        let backup = Store::open(&destination).unwrap();
        backup.check_integrity().unwrap();
        assert_eq!(
            backup
                .query_gauge(1, 0, 20 * SECOND, &resolution)
                .unwrap()
                .source,
            RollupSource::Materialized
        );
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
    fn identified_commit_retries_safely_after_a_crash_before_manifest_advance() {
        let directory = tempdir().unwrap();
        let resolution = RollupResolution::FixedMicros(5 * SECOND);
        let mut correction = Point::actual(1, 6 * SECOND, 100.0);
        correction.change_time = 30 * SECOND;
        let points_before;
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
            points_before = store.database().stats().unwrap().points;
        }
        // Simulate the issue-#8 failure: the raw frame became durable, then
        // the store failed (crashed) before manifest advancement, so the
        // caller never got an acknowledgement for permanently stored points.
        {
            let mut database = crate::Database::open(directory.path().join("active.wlog")).unwrap();
            let mut transaction = Transaction::new();
            transaction
                .append_points(vec![correction])
                .with_commit_id(42);
            database.commit(transaction).unwrap();
            database.close().unwrap();
        }

        // Opening the store reconciles rollup provenance with the raw log —
        // the invalidation the failed advancement never published.
        let mut store = Store::open(directory.path()).unwrap();
        assert_eq!(
            store
                .query_gauge(1, 0, 20 * SECOND, &resolution)
                .unwrap()
                .source,
            RollupSource::Raw
        );
        // The retried commit is answered from the log without writing.
        let mut retry = Transaction::new();
        retry.append_points(vec![correction]).with_commit_id(42);
        let generation_before = store.manifest_generation();
        let commit = store.commit(retry).unwrap();
        assert!(commit.deduplicated);
        assert_eq!(commit.points, 0);
        assert_eq!(store.manifest_generation(), generation_before);
        assert_eq!(store.database().stats().unwrap().points, points_before + 1);
        assert_eq!(
            store
                .database()
                .query_history(1, 6 * SECOND, 6 * SECOND + 1)
                .unwrap()
                .len(),
            2 // the original sixth-second sample plus exactly one correction
        );
        // Maintenance then rebuilds the invalidated rollup as usual.
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
    fn identified_store_commit_deduplicates_across_reopen() {
        let directory = tempdir().unwrap();
        let sample = Point::actual(1, 6 * SECOND, 6.0);
        {
            let mut store = Store::open(directory.path()).unwrap();
            initialize(&mut store, Vec::new(), None);
            let mut transaction = Transaction::new();
            transaction.append_points(vec![sample]).with_commit_id(7);
            assert!(!store.commit(transaction).unwrap().deduplicated);
            store.close().unwrap();
        }
        let mut store = Store::open(directory.path()).unwrap();
        let mut retry = Transaction::new();
        retry.append_points(vec![sample]).with_commit_id(7);
        assert!(store.commit(retry).unwrap().deduplicated);
        // A different identifier commits independently.
        let mut other = Transaction::new();
        other
            .append_points(vec![Point::actual(1, 7 * SECOND, 7.0)])
            .with_commit_id(8);
        assert!(!store.commit(other).unwrap().deduplicated);
        assert_eq!(store.database().stats().unwrap().points, 2);
        assert_eq!(
            store.database().query_history(1, 0, DAY).unwrap().len(),
            2 // each identified commit's point appears exactly once
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
    fn second_maintain_without_new_points_writes_no_rollup_files() {
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
        let first = store.maintain(DAY).unwrap();
        assert_eq!(first.rollup_files_written, 1);
        let generation = store.manifest_generation();

        let second = store.maintain(DAY).unwrap();
        assert_eq!(second.rollup_files_written, 0);
        assert_eq!(second.manifest_generation, generation);
        assert_eq!(store.manifest_generation(), generation);
        assert_eq!(
            store
                .query_gauge(1, 0, 20 * SECOND, &resolution)
                .unwrap()
                .source,
            RollupSource::Materialized
        );
    }

    #[test]
    fn maintain_does_not_rewrite_unchanged_series_when_another_ingests() {
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
        transaction.define_series(SeriesDefinition {
            id: 2,
            owner_entity: Some(EntityId(1)),
            owner_relation: None,
            name: "site_power".to_owned(),
            physical_quantity: "power".to_owned(),
            canonical_unit: "W".to_owned(),
            semantics: SeriesSemantics::Gauge,
            maximum_gap_micros: Some(2 * SECOND),
            rollup_policy: RollupPolicy {
                raw_retain_for_micros: None,
                tiers: vec![RollupTier {
                    resolution: resolution.clone(),
                    retain_for_micros: None,
                }],
            },
        });
        store.commit(transaction).unwrap();

        let mut transaction = Transaction::new();
        transaction.append_points(points());
        store.commit(transaction).unwrap();
        store.maintain(DAY).unwrap();
        let series_a_files: Vec<_> = store
            .active_rollups()
            .filter(|rollup| rollup.series_id == 1)
            .map(|rollup| rollup.file.clone())
            .collect();
        assert_eq!(series_a_files.len(), 1);

        let mut transaction = Transaction::new();
        transaction.append_points(
            (0..=20)
                .map(|second| Point::actual(2, second * SECOND, second as f64))
                .collect::<Vec<_>>(),
        );
        store.commit(transaction).unwrap();
        assert_eq!(
            store
                .query_gauge(1, 0, 20 * SECOND, &resolution)
                .unwrap()
                .source,
            RollupSource::Materialized
        );

        let report = store.maintain(DAY).unwrap();
        assert_eq!(report.rollup_files_written, 1);
        let series_a_after: Vec<_> = store
            .active_rollups()
            .filter(|rollup| rollup.series_id == 1)
            .map(|rollup| rollup.file.clone())
            .collect();
        assert_eq!(series_a_after, series_a_files);
        let current_points = store.database().stats().unwrap().points;
        assert!(
            store
                .active_rollups()
                .filter(|rollup| rollup.series_id == 1)
                .all(|rollup| rollup.source_points == current_points)
        );
        assert_eq!(
            store
                .query_gauge(1, 0, 20 * SECOND, &resolution)
                .unwrap()
                .source,
            RollupSource::Materialized
        );
        assert_eq!(
            store
                .query_gauge(2, 0, 20 * SECOND, &resolution)
                .unwrap()
                .source,
            RollupSource::Materialized
        );
    }

    #[test]
    fn later_maintain_closes_completed_shards_without_new_points() {
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
        assert_eq!(store.maintain(20 * SECOND).unwrap().rollup_files_written, 0);
        assert_eq!(
            store
                .query_gauge(1, 0, 20 * SECOND, &resolution)
                .unwrap()
                .source,
            RollupSource::Raw
        );

        let report = store.maintain(DAY).unwrap();
        assert_eq!(report.rollup_files_written, 1);
        assert_eq!(
            store
                .query_gauge(1, 0, 20 * SECOND, &resolution)
                .unwrap()
                .source,
            RollupSource::Materialized
        );
    }

    #[test]
    fn manifest_generations_stay_bounded_across_commits() {
        let directory = tempdir().unwrap();
        let resolution = RollupResolution::FixedMicros(5 * SECOND);
        let mut store = Store::open(directory.path()).unwrap();
        initialize(
            &mut store,
            vec![RollupTier {
                resolution,
                retain_for_micros: None,
            }],
            None,
        );
        let mut transaction = Transaction::new();
        transaction.append_points(points());
        store.commit(transaction).unwrap();
        store.maintain(DAY).unwrap();
        // Every one of these commits advances rollup provenance and publishes
        // a new manifest generation, so growth must be capped by pruning.
        for second in 1..=10 {
            let mut transaction = Transaction::new();
            transaction.append_points(vec![Point::actual(1, DAY + second * SECOND, 1.0)]);
            store.commit(transaction).unwrap();
        }
        assert_eq!(store.manifest_generation(), 11);
        let manifests = stored_files(&directory.path().join("manifests"));
        assert!(manifests.len() <= 3, "unpruned manifests: {manifests:?}");
    }

    #[test]
    fn corrupt_newest_manifest_falls_back_after_pruning() {
        let directory = tempdir().unwrap();
        let resolution = RollupResolution::FixedMicros(5 * SECOND);
        let fallback_generation;
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
            for day in 1..=2 {
                let mut transaction = Transaction::new();
                transaction.append_points(vec![Point::actual(1, day * DAY + SECOND, 1.0)]);
                store.commit(transaction).unwrap();
                store.maintain((day + 1) * DAY).unwrap();
            }
            fallback_generation = store.manifest_generation() - 1;
            store.close().unwrap();
        }
        let manifest_directory = directory.path().join("manifests");
        let manifests = stored_files(&manifest_directory);
        assert_eq!(manifests.len(), 3);
        let rollups_before = stored_files(&directory.path().join("rollups"));
        let newest = manifest_directory.join(manifests.last().unwrap());
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(newest)
            .unwrap();
        file.seek(SeekFrom::Start(30)).unwrap();
        file.write_all(&[0xFF]).unwrap();
        file.sync_all().unwrap();

        let store = Store::open(directory.path()).unwrap();
        assert_eq!(store.manifest_generation(), fallback_generation);
        // While a retained generation cannot be read back, the sweep must
        // abort rather than treat that generation's references as absent.
        assert_eq!(
            stored_files(&directory.path().join("rollups")),
            rollups_before
        );
        assert_eq!(
            store
                .query_gauge(1, 0, 20 * SECOND, &resolution)
                .unwrap()
                .source,
            RollupSource::Materialized
        );
    }

    #[test]
    fn fallback_generation_past_the_retained_window_survives_pruning() {
        let directory = tempdir().unwrap();
        let resolution = RollupResolution::FixedMicros(5 * SECOND);
        let loaded_generation;
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
            loaded_generation = store.manifest_generation();
            store.close().unwrap();
        }
        // More corrupt newer generations than the retained filename window
        // holds, as repeated torn publishes could leave behind: `load` must
        // fall back past all of them, and the prune that open runs must not
        // delete the only generation the store could actually read.
        let manifest_directory = directory.path().join("manifests");
        for corrupt in loaded_generation + 1..=loaded_generation + 3 {
            std::fs::write(
                manifest_directory.join(format!("MANIFEST.{corrupt:020}")),
                b"junk",
            )
            .unwrap();
        }
        let valid = manifest_directory.join(format!("MANIFEST.{loaded_generation:020}"));

        let store = Store::open(directory.path()).unwrap();
        assert_eq!(store.manifest_generation(), loaded_generation);
        assert!(valid.exists());
        store.close().unwrap();

        // Without that file a second open would find only corrupt manifests.
        let store = Store::open(directory.path()).unwrap();
        assert_eq!(store.manifest_generation(), loaded_generation);
        assert_eq!(
            store
                .query_gauge(1, 0, 20 * SECOND, &resolution)
                .unwrap()
                .source,
            RollupSource::Materialized
        );
    }

    #[test]
    fn superseded_rollup_files_are_removed_once_unreferenced() {
        let directory = tempdir().unwrap();
        let resolution = RollupResolution::FixedMicros(5 * SECOND);
        let rollups = directory.path().join("rollups");
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
        let old_file = store.active_rollups().next().unwrap().file.clone();

        let mut correction = Point::actual(1, 6 * SECOND, 100.0);
        correction.change_time = 30 * SECOND;
        let mut transaction = Transaction::new();
        transaction.append_points(vec![correction]);
        store.commit(transaction).unwrap();
        // The invalidated descriptor is dropped, not carried forever.
        assert!(store.manifest.rollups.is_empty());
        store.maintain(DAY).unwrap();
        let new_file = store.active_rollups().next().unwrap().file.clone();
        assert_ne!(old_file, new_file);
        // The superseded file is still referenced by a retained fallback
        // generation, so it must survive this publish.
        assert!(rollups.join(&old_file).exists());

        // One more generation pushes the superseding manifest's predecessors
        // out of the retained window, making the old segment unreferenced.
        let mut transaction = Transaction::new();
        transaction.append_points(vec![Point::actual(1, DAY + SECOND, 1.0)]);
        store.commit(transaction).unwrap();
        assert!(!rollups.join(&old_file).exists());
        assert!(rollups.join(&new_file).exists());
        assert_eq!(
            store
                .query_gauge(1, 0, 20 * SECOND, &resolution)
                .unwrap()
                .source,
            RollupSource::Materialized
        );
    }

    #[test]
    fn open_sweeps_orphaned_rollup_files() {
        let directory = tempdir().unwrap();
        let resolution = RollupResolution::FixedMicros(5 * SECOND);
        let referenced = {
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
            let file = store.active_rollups().next().unwrap().file.clone();
            store.close().unwrap();
            file
        };
        let rollups = directory.path().join("rollups");
        // A stray name and a validly-named file no manifest references, as a
        // maintain that crashed before publication would leave behind.
        std::fs::write(rollups.join("stray.rseg"), b"junk").unwrap();
        std::fs::write(rollups.join("g9-s1-f5000000-0-42.rseg"), b"junk").unwrap();

        let store = Store::open(directory.path()).unwrap();
        assert_eq!(stored_files(&rollups), vec![referenced]);
        assert_eq!(
            store
                .query_gauge(1, 0, 20 * SECOND, &resolution)
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
        assert_eq!(report.copied_files, 1);
        assert_eq!(report.linked_files + report.copied_files, report.files);
        assert_eq!(report.hard_link_fallbacks, 0);
        assert!(report.hard_link_fallback_error_kinds.is_empty());

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

    #[test]
    fn backup_copies_a_group_readable_rollup_instead_of_hard_linking() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("source");
        let destination = directory.path().join("backup");
        let mut store = Store::open(&source).unwrap();
        initialize(
            &mut store,
            vec![RollupTier {
                resolution: RollupResolution::FixedMicros(5 * SECOND),
                retain_for_micros: None,
            }],
            None,
        );
        let mut transaction = Transaction::new();
        transaction.append_points(points());
        store.commit(transaction).unwrap();
        store.maintain(DAY).unwrap();
        let rollup = store
            .active_rollups()
            .next()
            .expect("maintain wrote a rollup")
            .file
            .clone();
        let source_rollup = source.join("rollups").join(&rollup);
        std::fs::set_permissions(&source_rollup, std::fs::Permissions::from_mode(0o644)).unwrap();

        let report = store.backup_to(&destination).unwrap();
        assert!(report.hard_link_fallbacks >= 1);
        assert!(
            report
                .hard_link_fallback_error_kinds
                .contains(&std::io::ErrorKind::PermissionDenied)
        );

        let destination_rollup = destination.join("rollups").join(&rollup);
        assert_eq!(
            std::fs::metadata(&destination_rollup)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_ne!(
            std::fs::metadata(&source_rollup).unwrap().ino(),
            std::fs::metadata(&destination_rollup).unwrap().ino()
        );
    }

    #[test]
    fn restore_preserves_the_selected_snapshot_and_is_independent() {
        let directory = tempdir().unwrap();
        let backup = create_verified_backup(directory.path(), true);
        let target = directory.path().join("restored");
        let backup_before = directory_snapshot(&backup);

        let report = Store::restore_from(&backup, &target).unwrap();
        assert_eq!(report.files, 3);
        assert!(report.bytes > 0);
        assert_eq!(report.raw_commits, 2);
        assert_eq!(report.raw_points, 21);
        assert_eq!(
            report.source_snapshot_crc32,
            report.destination_snapshot_crc32
        );
        let restored = Store::open_read_only(&target).unwrap();
        let integrity = restored.check_integrity().unwrap();
        assert_eq!(integrity.raw_commits, report.raw_commits);
        assert_eq!(integrity.raw_points, report.raw_points);
        assert_eq!(integrity.manifest_generation, report.manifest_generation);
        drop(restored);

        let mut writable = Store::open(&target).unwrap();
        let mut transaction = Transaction::new();
        transaction.append_points(vec![Point::actual(1, 30 * SECOND, 30.0)]);
        writable.commit(transaction).unwrap();
        writable.close().unwrap();
        assert_eq!(directory_snapshot(&backup), backup_before);
        assert_eq!(
            Store::open_read_only(&backup)
                .unwrap()
                .check_integrity()
                .unwrap()
                .raw_points,
            21
        );
    }

    #[test]
    fn restore_never_changes_an_existing_empty_or_working_target() {
        let directory = tempdir().unwrap();
        let backup = create_verified_backup(directory.path(), false);

        let empty = directory.path().join("empty");
        std::fs::create_dir(&empty).unwrap();
        let empty_before = directory_snapshot(&empty);
        assert_io_kind(
            Store::restore_from(&backup, &empty).unwrap_err(),
            std::io::ErrorKind::AlreadyExists,
        );
        assert_eq!(directory_snapshot(&empty), empty_before);

        let working = directory.path().join("working");
        let mut store = Store::open(&working).unwrap();
        initialize(&mut store, Vec::new(), None);
        store.close().unwrap();
        let working_before = directory_snapshot(&working);
        assert_io_kind(
            Store::restore_from(&backup, &working).unwrap_err(),
            std::io::ErrorKind::AlreadyExists,
        );
        assert_eq!(directory_snapshot(&working), working_before);

        let dangling = directory.path().join("dangling");
        std::os::unix::fs::symlink("missing-target", &dangling).unwrap();
        assert_io_kind(
            Store::restore_from(&backup, &dangling).unwrap_err(),
            std::io::ErrorKind::AlreadyExists,
        );
        assert_eq!(
            std::fs::read_link(&dangling).unwrap(),
            Path::new("missing-target")
        );
    }

    #[test]
    fn simultaneous_restores_publish_exactly_one_complete_target() {
        let directory = tempdir().unwrap();
        let backup = create_verified_backup(directory.path(), true);
        let target = directory.path().join("race-target");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));

        let left_backup = backup.clone();
        let left_target = target.clone();
        let left_barrier = std::sync::Arc::clone(&barrier);
        let left = std::thread::spawn(move || {
            left_barrier.wait();
            Store::restore_from(left_backup, left_target)
        });
        let right_backup = backup.clone();
        let right_target = target.clone();
        let right_barrier = std::sync::Arc::clone(&barrier);
        let right = std::thread::spawn(move || {
            right_barrier.wait();
            Store::restore_from(right_backup, right_target)
        });
        barrier.wait();

        let results = [left.join().unwrap(), right.join().unwrap()];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        let loser = results
            .into_iter()
            .find_map(std::result::Result::err)
            .unwrap();
        assert_io_kind(loser, std::io::ErrorKind::AlreadyExists);
        let restored = Store::open_read_only(&target).unwrap();
        let integrity = restored.check_integrity().unwrap();
        assert_eq!(integrity.raw_commits, 2);
        assert_eq!(integrity.raw_points, 21);
        assert_eq!(integrity.active_rollup_files, 1);
        assert!(restore_stages(directory.path(), "race-target").is_empty());
    }

    #[test]
    fn stage_shared_lock_survives_rename_until_the_post_check_finishes() {
        let directory = tempdir().unwrap();
        let backup = create_verified_backup(directory.path(), true);
        let target = directory.path().join("target");
        let source = Store::open_read_only(&backup).unwrap();
        let staged = StagedDirectory::create(&target, "restore").unwrap();
        source.write_snapshot(staged.path()).unwrap();
        let stage = Store::open_read_only(staged.path()).unwrap();

        let publication = staged.publish(&target).unwrap();
        match Store::open(&target) {
            Err(crate::Error::Locked { path }) => {
                assert_eq!(path, target.join("active.wlog"));
            }
            Err(other) => panic!("expected writer lock refusal, got {other:?}"),
            Ok(_) => panic!("writer opened before the post-check finished"),
        }
        Store::open_read_only(&target)
            .unwrap()
            .check_integrity()
            .unwrap();
        drop(stage);
        publication.commit();

        Store::open(&target).unwrap().close().unwrap();
    }

    #[test]
    fn restore_rejects_corrupt_and_incomplete_selected_files() {
        for case in ["raw-crc", "raw-format", "raw-short", "manifest", "rollup"] {
            let directory = tempdir().unwrap();
            let backup = create_verified_backup(directory.path(), true);
            let target = directory.path().join("target");
            match case {
                "raw-crc" => flip_last_byte(&backup.join("active.wlog")),
                "raw-format" => overwrite_byte(&backup.join("active.wlog"), 0, 0),
                "raw-short" => {
                    let mut file = std::fs::OpenOptions::new()
                        .append(true)
                        .open(backup.join("active.wlog"))
                        .unwrap();
                    file.write_all(b"partial").unwrap();
                    file.sync_all().unwrap();
                }
                "manifest" => flip_last_byte(&active_manifest_path(&backup)),
                "rollup" => flip_last_byte(&active_rollup_path(&backup)),
                _ => unreachable!(),
            }

            assert!(
                Store::restore_from(&backup, &target).is_err(),
                "restore accepted {case} corruption"
            );
            assert!(!target.exists(), "restore published {case} corruption");
            assert!(restore_stages(directory.path(), "target").is_empty());
        }
    }

    #[test]
    fn restore_rejects_stale_active_rollup_provenance() {
        let directory = tempdir().unwrap();
        let backup = create_verified_backup(directory.path(), true);
        {
            let mut database = crate::Database::open(backup.join("active.wlog")).unwrap();
            let mut transaction = Transaction::new();
            transaction.append_points(vec![Point::actual(1, 30 * SECOND, 30.0)]);
            database.commit(transaction).unwrap();
            database.close().unwrap();
        }
        assert_eq!(
            Store::open_read_only(&backup)
                .unwrap()
                .check_integrity()
                .unwrap()
                .stale_rollup_files,
            1
        );

        let target = directory.path().join("target");
        let error = Store::restore_from(&backup, &target).unwrap_err();
        assert!(error.to_string().contains("active rollup files trail"));
        assert!(!target.exists());
        assert!(restore_stages(directory.path(), "target").is_empty());
    }

    #[test]
    fn restore_ignores_orphan_rollups_and_inactive_manifest_generations() {
        let directory = tempdir().unwrap();
        let backup = create_verified_backup(directory.path(), true);
        let first = directory.path().join("first");
        let baseline = Store::restore_from(&backup, &first).unwrap();

        std::fs::write(backup.join("rollups/orphan.rseg"), b"not selected").unwrap();
        std::fs::write(
            backup.join("manifests/MANIFEST.00000000000000000000"),
            b"inactive generation",
        )
        .unwrap();
        let second = directory.path().join("second");
        let restored = Store::restore_from(&backup, &second).unwrap();

        assert_eq!(
            restored.source_snapshot_crc32,
            baseline.source_snapshot_crc32
        );
        assert_eq!(
            restored.destination_snapshot_crc32,
            baseline.destination_snapshot_crc32
        );
        assert!(!second.join("rollups/orphan.rseg").exists());
        assert!(
            !second
                .join("manifests/MANIFEST.00000000000000000000")
                .exists()
        );
    }

    #[test]
    fn restore_rejects_a_selected_symlink() {
        let directory = tempdir().unwrap();
        let backup = create_verified_backup(directory.path(), false);
        let external = directory.path().join("external.wlog");
        std::fs::rename(backup.join("active.wlog"), &external).unwrap();
        std::os::unix::fs::symlink(&external, backup.join("active.wlog")).unwrap();

        let target = directory.path().join("target");
        let error = Store::restore_from(&backup, &target).unwrap_err();
        assert!(error.to_string().contains("not a regular file"));
        assert!(!target.exists());
        assert!(restore_stages(directory.path(), "target").is_empty());
    }

    #[test]
    fn restore_rejects_selected_special_files_without_blocking() {
        for selected in ["active", "manifest", "rollup"] {
            let directory = tempdir().unwrap();
            let backup = create_verified_backup(directory.path(), true);
            let path = match selected {
                "active" => backup.join("active.wlog"),
                "manifest" => active_manifest_path(&backup),
                "rollup" => active_rollup_path(&backup),
                _ => unreachable!(),
            };
            std::fs::remove_file(&path).unwrap();
            create_fifo(&path);

            let target = directory.path().join("target");
            let error = restore_with_deadline(backup, target.clone(), &path).unwrap_err();
            assert!(
                error.to_string().contains("not a regular file"),
                "wrong {selected} special-file error: {error}"
            );
            assert!(!target.exists());
            assert!(restore_stages(directory.path(), "target").is_empty());
        }
    }

    #[test]
    fn restore_failures_clean_the_stage_and_leave_no_target() {
        for step in [
            PublicationStep::Copy,
            PublicationStep::Sync,
            PublicationStep::ChecksumMismatch,
            PublicationStep::Publish,
            PublicationStep::ParentSync,
            PublicationStep::PostCheck,
        ] {
            let directory = tempdir().unwrap();
            let backup = create_verified_backup(directory.path(), true);
            let target = directory.path().join("target");
            fail_next_publication_step(step);

            assert!(
                Store::restore_from(&backup, &target).is_err(),
                "injected {step:?} failure did not fail"
            );
            assert!(!target.exists(), "{step:?} failure left a target");
            assert!(
                restore_stages(directory.path(), "target").is_empty(),
                "{step:?} failure left a stage"
            );
        }
    }

    #[test]
    fn backup_rolls_back_final_parent_sync_and_post_check_failures() {
        for step in [PublicationStep::ParentSync, PublicationStep::PostCheck] {
            let directory = tempdir().unwrap();
            let source = directory.path().join("source");
            let target = directory.path().join("backup");
            let mut store = Store::open(&source).unwrap();
            initialize(&mut store, Vec::new(), None);
            fail_next_publication_step(step);

            assert!(store.backup_to(&target).is_err());
            assert!(!target.exists(), "{step:?} failure left a backup");
            let prefix = ".backup.backup-";
            assert!(
                stored_files(directory.path())
                    .iter()
                    .all(|name| !name.starts_with(prefix)),
                "{step:?} failure left a backup stage"
            );
        }
    }

    #[test]
    fn clean_salvage_ignores_all_derived_files_and_preserves_the_source() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("source");
        let target = directory.path().join("salvaged");
        {
            let mut store = Store::open(&source).unwrap();
            initialize(
                &mut store,
                vec![RollupTier {
                    resolution: RollupResolution::FixedMicros(5 * SECOND),
                    retain_for_micros: None,
                }],
                None,
            );
            let mut transaction = Transaction::new();
            transaction.append_points(points());
            store.commit(transaction).unwrap();
            store.maintain(DAY).unwrap();
            store.close().unwrap();
        }
        let manifest = active_manifest_path(&source);
        let rollup = active_rollup_path(&source);
        flip_last_byte(&manifest);
        flip_last_byte(&rollup);
        std::fs::write(source.join("rollups/orphan.rseg"), b"broken orphan").unwrap();
        let source_before = directory_snapshot(&source);

        let report = Store::salvage_from(&source, &target).unwrap();
        assert_eq!(report.status, SalvageStatus::Clean);
        assert_eq!(report.stop_reason, SalvageStopReason::CleanEof);
        assert_eq!(report.source_bytes, report.recovered_prefix_bytes);
        assert_eq!(report.discarded_bytes, 0);
        assert_eq!(report.stop_offset, report.source_bytes);
        assert_eq!(report.recovered_commits, 2);
        assert_eq!(report.recovered_points, 21);
        assert_eq!(
            report.source_prefix_crc32,
            report.destination_snapshot_crc32
        );
        let integrity = Store::open_read_only(&target)
            .unwrap()
            .check_integrity()
            .unwrap();
        assert_eq!(integrity.manifest_generation, 0);
        assert_eq!(integrity.active_rollup_files, 0);
        assert_eq!(integrity.raw_commits, 2);
        assert_eq!(integrity.raw_points, 21);
        let mut appended = Store::open(&target).unwrap();
        appended.append(&[points()[0]]).unwrap();
        appended.close().unwrap();
        assert_eq!(directory_snapshot(&source), source_before);
    }

    #[test]
    fn short_tail_salvages_a_verified_partial_prefix_without_mutating_source() {
        let directory = tempdir().unwrap();
        let source = create_verified_backup(directory.path(), false);
        let active = source.join("active.wlog");
        let clean_bytes = std::fs::metadata(&active).unwrap().len();
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&active)
            .unwrap();
        file.write_all(b"partial").unwrap();
        file.sync_all().unwrap();
        drop(file);
        let source_before = directory_snapshot(&source);
        let target = directory.path().join("salvaged");

        let report = Store::salvage_from(&source, &target).unwrap();
        assert_eq!(report.status, SalvageStatus::Partial);
        assert_eq!(report.stop_reason, SalvageStopReason::IncompleteFrameHeader);
        assert_eq!(report.recovered_prefix_bytes, clean_bytes);
        assert_eq!(report.discarded_bytes, 7);
        assert_eq!(report.stop_offset, clean_bytes);
        assert_eq!(report.recovered_commits, 2);
        assert_eq!(report.recovered_points, 21);
        assert_eq!(
            report.source_prefix_crc32,
            report.destination_snapshot_crc32
        );
        assert_eq!(
            std::fs::metadata(target.join("active.wlog")).unwrap().len(),
            clean_bytes
        );
        assert_eq!(directory_snapshot(&source), source_before);
    }

    #[test]
    fn header_only_and_first_frame_damage_publish_checked_zero_count_stores() {
        let directory = tempdir().unwrap();

        let clean_source = directory.path().join("clean-source");
        std::fs::create_dir_all(clean_source.join("manifests")).unwrap();
        std::fs::create_dir(clean_source.join("rollups")).unwrap();
        crate::Database::open(clean_source.join("active.wlog"))
            .unwrap()
            .close()
            .unwrap();
        let clean_target = directory.path().join("clean-target");
        let clean = Store::salvage_from(&clean_source, &clean_target).unwrap();
        assert_eq!(clean.status, SalvageStatus::Clean);
        assert_eq!(clean.stop_reason, SalvageStopReason::CleanEof);
        assert_eq!((clean.recovered_commits, clean.recovered_points), (0, 0));

        let damaged_source = directory.path().join("damaged-source");
        std::fs::create_dir_all(damaged_source.join("manifests")).unwrap();
        std::fs::create_dir(damaged_source.join("rollups")).unwrap();
        let active = damaged_source.join("active.wlog");
        crate::Database::open(&active).unwrap().close().unwrap();
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&active)
            .unwrap();
        file.write_all(b"partial").unwrap();
        file.sync_all().unwrap();
        drop(file);
        let damaged_before = directory_snapshot(&damaged_source);
        let damaged_target = directory.path().join("damaged-target");
        let partial = Store::salvage_from(&damaged_source, &damaged_target).unwrap();
        assert_eq!(partial.status, SalvageStatus::Partial);
        assert_eq!(
            partial.stop_reason,
            SalvageStopReason::IncompleteFrameHeader
        );
        assert_eq!(partial.discarded_bytes, 7);
        assert_eq!(
            (partial.recovered_commits, partial.recovered_points),
            (0, 0)
        );
        let checked = Store::open_read_only(&damaged_target)
            .unwrap()
            .check_integrity()
            .unwrap();
        assert_eq!((checked.raw_commits, checked.raw_points), (0, 0));
        assert_eq!(directory_snapshot(&damaged_source), damaged_before);
    }

    #[test]
    fn invalid_database_header_is_fatal_and_publishes_nothing() {
        let directory = tempdir().unwrap();
        let source = create_verified_backup(directory.path(), false);
        overwrite_byte(&source.join("active.wlog"), 0, 0);
        let source_before = directory_snapshot(&source);
        let target = directory.path().join("target");

        assert!(matches!(
            Store::salvage_from(&source, &target),
            Err(crate::Error::InvalidHeader)
        ));
        assert!(!target.exists());
        assert!(salvage_stages(directory.path(), "target").is_empty());
        assert_eq!(directory_snapshot(&source), source_before);
    }

    #[test]
    fn unsupported_database_version_and_header_crc_are_fatal_without_publication() {
        for (kind, offset) in [("version", 8_u64), ("header-crc", 12_u64)] {
            let directory = tempdir().unwrap();
            let source = create_verified_backup(directory.path(), false);
            overwrite_byte(&source.join("active.wlog"), offset, 2);
            let source_before = directory_snapshot(&source);
            let target = directory.path().join("target");

            let error = Store::salvage_from(&source, &target).unwrap_err();
            match kind {
                "version" => assert!(matches!(error, crate::Error::UnsupportedVersion(2))),
                "header-crc" => assert!(matches!(error, crate::Error::InvalidHeader)),
                _ => unreachable!(),
            }
            assert!(!target.exists());
            assert!(salvage_stages(directory.path(), "target").is_empty());
            assert_eq!(directory_snapshot(&source), source_before);
        }
    }

    #[test]
    fn salvage_source_change_after_staging_is_fatal_and_cleans_everything() {
        let directory = tempdir().unwrap();
        let source = create_verified_backup(directory.path(), false);
        let source_before = directory_snapshot(&source);
        let target = directory.path().join("target");

        // Pass the first check after scanning and digesting. Mutate the open
        // source at the second check, after the stage copy has been synced.
        mutate_salvage_source_after_identity_checks(1);
        assert!(matches!(
            Store::salvage_from(&source, &target),
            Err(crate::Error::SourceChanged { path }) if path == source.join("active.wlog")
        ));
        assert_ne!(directory_snapshot(&source), source_before);
        assert!(!target.exists());
        assert!(salvage_stages(directory.path(), "target").is_empty());
    }

    #[test]
    fn salvage_source_change_during_post_check_rolls_back_the_target() {
        let directory = tempdir().unwrap();
        let source = create_verified_backup(directory.path(), false);
        let source_before = directory_snapshot(&source);
        let target = directory.path().join("target");

        // Pass the checks after the source digest and stage copy, then mutate
        // at the final source check after the published target was checked.
        mutate_salvage_source_after_identity_checks(2);
        assert!(matches!(
            Store::salvage_from(&source, &target),
            Err(crate::Error::SourceChanged { path }) if path == source.join("active.wlog")
        ));
        assert_ne!(directory_snapshot(&source), source_before);
        assert!(!target.exists());
        assert!(salvage_stages(directory.path(), "target").is_empty());
    }

    #[test]
    fn salvage_respects_an_exclusive_writer_lock_then_succeeds_after_close() {
        let directory = tempdir().unwrap();
        let source = create_verified_backup(directory.path(), false);
        let target = directory.path().join("target");
        let writer = Store::open(&source).unwrap();

        assert!(matches!(
            Store::salvage_from(&source, &target),
            Err(crate::Error::Locked { path }) if path == source.join("active.wlog")
        ));
        assert!(!target.exists());
        assert!(salvage_stages(directory.path(), "target").is_empty());

        writer.close().unwrap();
        let report = Store::salvage_from(&source, &target).unwrap();
        assert_eq!(report.status, SalvageStatus::Clean);
        assert_eq!((report.recovered_commits, report.recovered_points), (2, 21));
    }

    #[test]
    fn salvage_never_changes_existing_targets() {
        let directory = tempdir().unwrap();
        let source = create_verified_backup(directory.path(), false);
        let empty = directory.path().join("empty");
        std::fs::create_dir(&empty).unwrap();
        let empty_before = directory_snapshot(&empty);
        assert_io_kind(
            Store::salvage_from(&source, &empty).unwrap_err(),
            std::io::ErrorKind::AlreadyExists,
        );
        assert_eq!(directory_snapshot(&empty), empty_before);

        let working = directory.path().join("working");
        let mut store = Store::open(&working).unwrap();
        initialize(&mut store, Vec::new(), None);
        store.close().unwrap();
        let working_before = directory_snapshot(&working);
        assert_io_kind(
            Store::salvage_from(&source, &working).unwrap_err(),
            std::io::ErrorKind::AlreadyExists,
        );
        assert_eq!(directory_snapshot(&working), working_before);

        let dangling = directory.path().join("dangling");
        std::os::unix::fs::symlink("missing-target", &dangling).unwrap();
        assert_io_kind(
            Store::salvage_from(&source, &dangling).unwrap_err(),
            std::io::ErrorKind::AlreadyExists,
        );
        assert_eq!(
            std::fs::read_link(&dangling).unwrap(),
            Path::new("missing-target")
        );
    }

    #[test]
    fn simultaneous_salvages_publish_exactly_one_complete_target() {
        let directory = tempdir().unwrap();
        let source = create_verified_backup(directory.path(), false);
        let target = directory.path().join("race-target");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let mut threads = Vec::new();
        for _ in 0..2 {
            let source = source.clone();
            let target = target.clone();
            let barrier = std::sync::Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                Store::salvage_from(source, target)
            }));
        }
        barrier.wait();
        let results: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        let loser = results.into_iter().find_map(Result::err).unwrap();
        assert_io_kind(loser, std::io::ErrorKind::AlreadyExists);
        let integrity = Store::open_read_only(&target)
            .unwrap()
            .check_integrity()
            .unwrap();
        assert_eq!(integrity.raw_commits, 2);
        assert_eq!(integrity.raw_points, 21);
        assert!(salvage_stages(directory.path(), "race-target").is_empty());
    }

    #[test]
    fn salvage_rejects_symlink_fifo_and_non_file_active_log_without_blocking() {
        for kind in ["symlink", "fifo", "directory"] {
            let directory = tempdir().unwrap();
            let source = directory.path().join("source");
            std::fs::create_dir(&source).unwrap();
            let active = source.join("active.wlog");
            match kind {
                "symlink" => std::os::unix::fs::symlink("outside", &active).unwrap(),
                "fifo" => create_fifo(&active),
                "directory" => std::fs::create_dir(&active).unwrap(),
                _ => unreachable!(),
            }
            let target = directory.path().join("target");
            let error = salvage_with_deadline(source, target.clone(), &active).unwrap_err();
            assert!(error.to_string().contains("not a regular file"));
            assert!(!target.exists());
            assert!(salvage_stages(directory.path(), "target").is_empty());
        }
    }

    #[test]
    fn salvage_rejects_a_symlink_or_non_directory_source_root() {
        let directory = tempdir().unwrap();
        let real_source = create_verified_backup(directory.path(), false);
        let source_before = directory_snapshot(&real_source);

        let linked_source = directory.path().join("linked-source");
        std::os::unix::fs::symlink(&real_source, &linked_source).unwrap();
        let linked_target = directory.path().join("linked-target");
        assert!(Store::salvage_from(&linked_source, &linked_target).is_err());
        assert!(!linked_target.exists());
        assert_eq!(directory_snapshot(&real_source), source_before);

        let file_source = directory.path().join("file-source");
        std::fs::write(&file_source, b"not a directory").unwrap();
        let file_before = std::fs::read(&file_source).unwrap();
        let file_target = directory.path().join("file-target");
        assert!(Store::salvage_from(&file_source, &file_target).is_err());
        assert!(!file_target.exists());
        assert_eq!(std::fs::read(&file_source).unwrap(), file_before);
    }

    #[test]
    fn salvage_failures_clean_the_stage_and_leave_no_target() {
        for step in [
            PublicationStep::Copy,
            PublicationStep::Sync,
            PublicationStep::ChecksumMismatch,
            PublicationStep::Publish,
            PublicationStep::ParentSync,
            PublicationStep::PostCheck,
        ] {
            let directory = tempdir().unwrap();
            let source = create_verified_backup(directory.path(), false);
            let target = directory.path().join("target");
            fail_next_publication_step(step);
            assert!(Store::salvage_from(&source, &target).is_err());
            assert!(!target.exists(), "{step:?} failure left a target");
            assert!(
                salvage_stages(directory.path(), "target").is_empty(),
                "{step:?} failure left a stage"
            );
        }
    }

    #[test]
    fn seal_reclaim_reopen_reads_a_point_only_from_the_sealed_segment() {
        let directory = tempdir().unwrap();
        let sealed = Point::actual(1, 5 * SECOND, 5.0);
        let log_bytes_before;
        {
            let mut store = Store::open(directory.path()).unwrap();
            initialize(&mut store, Vec::new(), None);
            let mut transaction = Transaction::new();
            transaction.append_points(vec![sealed]);
            store.commit(transaction).unwrap();
            log_bytes_before = store.database().stats().unwrap().file_bytes;
            let report = store.seal_and_reclaim().unwrap();
            assert_eq!(report.sealed_points, 1);
            assert_eq!(report.live_points, 0);
            assert!(report.log_bytes < log_bytes_before);
            assert_eq!(store.database().live_index_len(), 0);
            assert_eq!(store.database().sealed_point_count(), 1);
            assert_eq!(
                store
                    .database()
                    .query_latest(1, i64::MIN, i64::MAX)
                    .unwrap(),
                vec![sealed]
            );
            store.close().unwrap();
        }

        let store = Store::open(directory.path()).unwrap();
        assert_eq!(store.database().live_index_len(), 0);
        assert_eq!(store.database().sealed_point_count(), 1);
        assert_eq!(
            store
                .database()
                .query_latest(1, i64::MIN, i64::MAX)
                .unwrap(),
            vec![sealed]
        );
        assert_eq!(
            store
                .database()
                .query_history(1, i64::MIN, i64::MAX)
                .unwrap(),
            vec![sealed]
        );
        assert!(store.database().stats().unwrap().file_bytes < log_bytes_before);
    }

    #[test]
    fn crash_after_seal_publish_before_reclaim_keeps_winners() {
        let directory = tempdir().unwrap();
        let first = Point::actual(1, 5 * SECOND, 5.0);
        let correction = Point {
            series_id: 1,
            valid_time: 5 * SECOND,
            valid_time_end: 5 * SECOND,
            knowledge_time: 6 * SECOND,
            change_time: 6 * SECOND,
            run_id: 0,
            value: 9.0,
            quality: 0,
            flags: 0,
        };
        {
            let mut store = Store::open(directory.path()).unwrap();
            initialize(&mut store, Vec::new(), None);
            let mut transaction = Transaction::new();
            transaction.append_points(vec![first, correction]);
            store.commit(transaction).unwrap();
            fail_next_seal_reclaim();
            assert!(store.seal_and_reclaim().is_err());
            store.close().unwrap();
        }

        let store = Store::open(directory.path()).unwrap();
        assert_eq!(store.database().live_index_len(), 0);
        assert_eq!(
            store
                .database()
                .query_latest(1, i64::MIN, i64::MAX)
                .unwrap(),
            vec![correction]
        );
        assert_eq!(
            store
                .database()
                .query_history(1, i64::MIN, i64::MAX)
                .unwrap(),
            vec![first, correction]
        );
        assert_eq!(store.database().stats().unwrap().points, 2);
    }

    #[test]
    fn range_query_spans_sealed_history_and_the_live_tail() {
        let directory = tempdir().unwrap();
        let historical = Point::actual(1, 5 * SECOND, 5.0);
        let tail = Point::actual(1, 15 * SECOND, 15.0);
        let mut store = Store::open(directory.path()).unwrap();
        initialize(&mut store, Vec::new(), None);
        let mut transaction = Transaction::new();
        transaction.append_points(vec![historical]);
        store.commit(transaction).unwrap();
        store.seal_and_reclaim().unwrap();
        assert_eq!(store.database().live_index_len(), 0);

        let mut transaction = Transaction::new();
        transaction.append_points(vec![tail]);
        store.commit(transaction).unwrap();
        assert_eq!(store.database().live_index_len(), 1);
        assert_eq!(
            store.database().query_latest(1, 0, 10 * SECOND).unwrap(),
            vec![historical]
        );
        assert_eq!(
            store
                .database()
                .query_latest(1, 10 * SECOND, 20 * SECOND)
                .unwrap(),
            vec![tail]
        );
        assert_eq!(
            store.database().query_latest(1, 0, 20 * SECOND).unwrap(),
            vec![historical, tail]
        );
    }

    #[test]
    fn live_tail_wins_an_equal_bitemporal_tie_after_seal() {
        let directory = tempdir().unwrap();
        let first = Point::actual(1, 5 * SECOND, 1.0);
        let correction = Point::actual(1, 5 * SECOND, 2.0);

        {
            let mut store = Store::open(directory.path()).unwrap();
            initialize(&mut store, Vec::new(), None);
            let mut transaction = Transaction::new();
            transaction.append_points(vec![first]);
            store.commit(transaction).unwrap();
            store.seal_and_reclaim().unwrap();

            let mut transaction = Transaction::new();
            transaction.append_points(vec![correction]);
            store.commit(transaction).unwrap();
            assert_eq!(
                store.database().query_history(1, 0, 10 * SECOND).unwrap(),
                vec![first, correction]
            );
            assert_eq!(
                store.database().query_latest(1, 0, 10 * SECOND).unwrap(),
                vec![correction]
            );
            store.seal_and_reclaim().unwrap();
            assert_eq!(
                store.database().query_latest(1, 0, 10 * SECOND).unwrap(),
                vec![correction]
            );
            store.close().unwrap();
        }

        let store = Store::open_read_only(directory.path()).unwrap();
        assert_eq!(
            store.database().query_history(1, 0, 10 * SECOND).unwrap(),
            vec![first, correction]
        );
        assert_eq!(
            store.database().query_latest(1, 0, 10 * SECOND).unwrap(),
            vec![correction]
        );
    }

    #[test]
    fn maintain_after_seal_materializes_from_segments_not_the_live_index() {
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
        store.seal_and_reclaim().unwrap();
        assert_eq!(store.database().live_index_len(), 0);
        assert_eq!(store.database().sealed_point_count(), 21);

        let report = store.maintain(DAY).unwrap();
        assert_eq!(report.rollup_files_written, 1);
        assert_eq!(store.database().live_index_len(), 0);
        let persisted = store.query_gauge(1, 0, 20 * SECOND, &resolution).unwrap();
        assert_eq!(persisted.source, RollupSource::Materialized);
        let raw = store
            .database()
            .rollup_gauge(1, 0, 20 * SECOND + 1, 5 * SECOND, 2 * SECOND)
            .unwrap()
            .range(0, 20 * SECOND);
        assert_eq!(persisted.buckets, raw);
    }

    #[test]
    fn dirty_maintain_after_seal_queries_only_the_live_window() {
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
        let historical: Vec<_> = store
            .active_rollups()
            .map(|rollup| rollup.file.clone())
            .collect();
        store.seal_and_reclaim().unwrap();
        assert_eq!(store.database().live_index_len(), 0);

        let mut transaction = Transaction::new();
        transaction.append_points(vec![Point::actual(1, DAY + 5 * SECOND, 21.0)]);
        store.commit(transaction).unwrap();
        assert_eq!(store.database().live_index_len(), 1);
        let report = store.maintain(2 * DAY).unwrap();
        assert_eq!(report.rollup_files_written, 1);
        assert!(
            historical
                .iter()
                .all(|file| store.active_rollups().any(|rollup| &rollup.file == file))
        );
        assert_eq!(store.database().live_index_len(), 1);
        assert_eq!(
            store
                .query_gauge(1, 0, 2 * DAY, &resolution)
                .unwrap()
                .source,
            RollupSource::Materialized
        );
    }

    #[test]
    fn identified_commit_still_deduplicates_after_seal_and_reclaim() {
        let directory = tempdir().unwrap();
        let sample = Point::actual(1, 6 * SECOND, 6.0);
        let mut store = Store::open(directory.path()).unwrap();
        initialize(&mut store, Vec::new(), None);
        let mut transaction = Transaction::new();
        transaction.append_points(vec![sample]).with_commit_id(7);
        assert!(!store.commit(transaction).unwrap().deduplicated);
        store.seal_and_reclaim().unwrap();
        assert_eq!(store.database().live_index_len(), 0);

        let mut retry = Transaction::new();
        retry.append_points(vec![sample]).with_commit_id(7);
        assert!(store.commit(retry).unwrap().deduplicated);
        assert_eq!(store.database().live_index_len(), 0);
        assert_eq!(
            store.database().query_history(1, 0, DAY).unwrap(),
            vec![sample]
        );
    }

    fn sealed_wseg_paths(root: &Path) -> Vec<std::path::PathBuf> {
        let mut paths: Vec<_> = std::fs::read_dir(root.join("segments"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("wseg"))
            .collect();
        paths.sort();
        paths
    }

    fn corrupt_first_sealed_block_payload(root: &Path) {
        let wseg = sealed_wseg_paths(root)[0].clone();
        let segment = Segment::open(&wseg).unwrap();
        let payload_offset = segment
            .first_block_payload_offset()
            .expect("sealed segment must expose a block payload offset");
        drop(segment);
        overwrite_byte(&wseg, payload_offset, 0xAA);
    }

    #[test]
    fn query_latest_returns_corruption_for_corrupt_sealed_block_payload() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("store");
        {
            let mut store = Store::open(&root).unwrap();
            initialize(&mut store, Vec::new(), None);
            let mut transaction = Transaction::new();
            transaction.append_points(vec![Point::actual(1, SECOND, 1.0)]);
            store.commit(transaction).unwrap();
            store.seal_and_reclaim().unwrap();
            store.close().unwrap();
        }
        corrupt_first_sealed_block_payload(&root);
        let store = Store::open_read_only(&root).unwrap();
        assert!(matches!(
            store.database().query_latest(1, 0, DAY),
            Err(crate::Error::Corruption { .. })
        ));
    }

    #[test]
    fn check_integrity_fails_on_corrupt_sealed_block_payload() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("store");
        {
            let mut store = Store::open(&root).unwrap();
            initialize(&mut store, Vec::new(), None);
            let mut transaction = Transaction::new();
            transaction.append_points(vec![Point::actual(1, SECOND, 1.0)]);
            store.commit(transaction).unwrap();
            store.seal_and_reclaim().unwrap();
            store.close().unwrap();
        }
        corrupt_first_sealed_block_payload(&root);
        let store = Store::open_read_only(&root).unwrap();
        assert!(matches!(
            store.check_integrity(),
            Err(crate::Error::Corruption { .. })
        ));
    }

    #[test]
    fn salvage_with_drop_orphan_segments_ignores_unreferenced_wseg() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("source");
        let target = directory.path().join("salvaged");
        {
            let mut store = Store::open(&source).unwrap();
            initialize(&mut store, Vec::new(), None);
            let mut transaction = Transaction::new();
            transaction.append_points(vec![Point::actual(1, SECOND, 1.0)]);
            store.commit(transaction).unwrap();
            store.seal_and_reclaim().unwrap();
            store.close().unwrap();
        }
        let wseg = sealed_wseg_paths(&source)[0].clone();
        std::fs::copy(&wseg, source.join("segments").join("orphan.wseg")).unwrap();
        assert!(Store::salvage_from(&source, &target).is_err());

        let target2 = directory.path().join("salvaged2");
        let report = Store::salvage_from_with_options(
            &source,
            &target2,
            SalvageOptions {
                drop_orphan_segments: true,
            },
        )
        .unwrap();
        assert_eq!(report.recovered_points, 1);
        let salvaged = Store::open_read_only(&target2).unwrap();
        assert_eq!(
            salvaged.database().query_history(1, 0, DAY).unwrap(),
            vec![Point::actual(1, SECOND, 1.0)]
        );
    }

    #[test]
    fn salvage_recovers_a_point_that_exists_only_in_a_sealed_segment() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("source");
        let target = directory.path().join("salvaged");
        let sealed = Point::actual(1, SECOND, 1.0);
        let live = Point::actual(1, 2 * SECOND, 2.0);
        {
            let mut store = Store::open(&source).unwrap();
            initialize(&mut store, Vec::new(), None);
            let mut transaction = Transaction::new();
            transaction.append_points(vec![sealed]);
            store.commit(transaction).unwrap();
            store.seal_and_reclaim().unwrap();
            assert_eq!(store.database().live_index_len(), 0);
            let mut transaction = Transaction::new();
            transaction.append_points(vec![live]);
            store.commit(transaction).unwrap();
            store.close().unwrap();
        }
        let source_before = directory_snapshot(&source);

        let report = Store::salvage_from(&source, &target).unwrap();
        assert_eq!(report.status, SalvageStatus::Clean);
        assert_eq!(report.stop_reason, SalvageStopReason::CleanEof);
        assert_eq!(report.recovered_points, 2);
        assert_eq!(
            report.source_prefix_crc32,
            report.destination_snapshot_crc32
        );

        let salvaged = Store::open_read_only(&target).unwrap();
        assert_eq!(salvaged.database().sealed_point_count(), 1);
        assert_eq!(salvaged.database().live_index_len(), 1);
        assert_eq!(
            salvaged.database().query_history(1, 0, DAY).unwrap(),
            vec![sealed, live]
        );
        assert_eq!(
            salvaged.database().query_history(1, 0, SECOND + 1).unwrap(),
            vec![sealed]
        );
        drop(salvaged);

        let reopened = Store::open(&target).unwrap();
        assert_eq!(
            reopened.database().query_history(1, 0, SECOND + 1).unwrap(),
            vec![sealed]
        );
        assert_eq!(directory_snapshot(&source), source_before);
    }

    #[test]
    fn salvage_recovers_a_torn_live_tail_with_sealed_history_intact() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("source");
        let target = directory.path().join("salvaged");
        let sealed = Point::actual(1, SECOND, 1.0);
        let live = Point::actual(1, 3 * SECOND, 3.0);
        {
            let mut store = Store::open(&source).unwrap();
            initialize(&mut store, Vec::new(), None);
            let mut transaction = Transaction::new();
            transaction.append_points(vec![sealed]);
            store.commit(transaction).unwrap();
            store.seal_and_reclaim().unwrap();
            let mut transaction = Transaction::new();
            transaction.append_points(vec![live]);
            store.commit(transaction).unwrap();
            store.close().unwrap();
        }
        let active = source.join("active.wlog");
        let clean_bytes = std::fs::metadata(&active).unwrap().len();
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&active)
            .unwrap();
        file.write_all(b"partial").unwrap();
        file.sync_all().unwrap();
        drop(file);
        let source_before = directory_snapshot(&source);

        let report = Store::salvage_from(&source, &target).unwrap();
        assert_eq!(report.status, SalvageStatus::Partial);
        assert_eq!(report.stop_reason, SalvageStopReason::IncompleteFrameHeader);
        assert_eq!(report.recovered_prefix_bytes, clean_bytes);
        assert_eq!(report.discarded_bytes, 7);
        assert_eq!(report.recovered_points, 2);
        assert_eq!(
            std::fs::metadata(target.join("active.wlog")).unwrap().len(),
            clean_bytes
        );

        let salvaged = Store::open_read_only(&target).unwrap();
        assert_eq!(
            salvaged.database().query_history(1, 0, DAY).unwrap(),
            vec![sealed, live]
        );
        assert_eq!(salvaged.database().sealed_point_count(), 1);
        assert_eq!(directory_snapshot(&source), source_before);
    }

    #[test]
    fn salvage_then_maintain_query_gauge_matches_raw_winners() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("source");
        let salvaged_path = directory.path().join("salvaged");
        let resolution = RollupResolution::FixedMicros(5 * SECOND);
        {
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
            store.seal_and_reclaim().unwrap();
            assert_eq!(store.database().live_index_len(), 0);
            store.close().unwrap();
        }

        Store::salvage_from(&source, &salvaged_path).unwrap();
        let mut salvaged = Store::open(&salvaged_path).unwrap();
        assert!(
            salvaged.active_rollups().next().is_none(),
            "salvage must drop rollups so maintain has to rebuild them"
        );
        let report = salvaged.maintain(DAY).unwrap();
        assert_eq!(report.rollup_files_written, 1);
        let persisted = salvaged
            .query_gauge(1, 0, 20 * SECOND, &resolution)
            .unwrap();
        assert_eq!(persisted.source, RollupSource::Materialized);
        let raw = salvaged
            .database()
            .rollup_gauge(1, 0, 20 * SECOND + 1, 5 * SECOND, 2 * SECOND)
            .unwrap()
            .range(0, 20 * SECOND);
        assert_eq!(persisted.buckets, raw);
    }

    #[test]
    fn salvage_fails_closed_on_an_unreadable_or_unreferenced_sealed_segment() {
        for kind in ["corrupt", "missing", "no-manifest"] {
            let directory = tempdir().unwrap();
            let source = directory.path().join("source");
            let target = directory.path().join("salvaged");
            {
                let mut store = Store::open(&source).unwrap();
                initialize(&mut store, Vec::new(), None);
                let mut transaction = Transaction::new();
                transaction.append_points(vec![Point::actual(1, SECOND, 1.0)]);
                store.commit(transaction).unwrap();
                store.seal_and_reclaim().unwrap();
                store.close().unwrap();
            }
            match kind {
                "corrupt" => flip_last_byte(&sealed_wseg_paths(&source)[0]),
                "missing" => std::fs::remove_file(&sealed_wseg_paths(&source)[0]).unwrap(),
                "no-manifest" => {
                    for entry in std::fs::read_dir(source.join("manifests")).unwrap() {
                        std::fs::remove_file(entry.unwrap().path()).unwrap();
                    }
                }
                _ => unreachable!(),
            }
            let source_before = directory_snapshot(&source);
            let error = Store::salvage_from(&source, &target).unwrap_err();
            assert!(
                matches!(error, crate::Error::Corruption { .. }),
                "{kind} must fail closed, got {error}"
            );
            assert!(!target.exists(), "{kind} published a target");
            assert!(
                salvage_stages(directory.path(), "salvaged").is_empty(),
                "{kind} left a stage"
            );
            assert_eq!(directory_snapshot(&source), source_before);
        }
    }

    #[test]
    fn backup_and_restore_preserve_sealed_segment_history() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("source");
        let backup = directory.path().join("backup");
        let restored = directory.path().join("restored");
        let sealed = Point::actual(1, SECOND, 1.0);
        let live = Point::actual(1, 4 * SECOND, 4.0);
        {
            let mut store = Store::open(&source).unwrap();
            initialize(&mut store, Vec::new(), None);
            let mut transaction = Transaction::new();
            transaction.append_points(vec![sealed]);
            store.commit(transaction).unwrap();
            store.seal_and_reclaim().unwrap();
            let mut transaction = Transaction::new();
            transaction.append_points(vec![live]);
            store.commit(transaction).unwrap();
            store.backup_to(&backup).unwrap();
            store.close().unwrap();
        }

        let report = Store::restore_from(&backup, &restored).unwrap();
        assert!(report.raw_points >= 2);
        assert_eq!(
            report.source_snapshot_crc32,
            report.destination_snapshot_crc32
        );

        let store = Store::open_read_only(&restored).unwrap();
        assert_eq!(store.database().sealed_point_count(), 1);
        assert_eq!(
            store.database().query_history(1, 0, DAY).unwrap(),
            vec![sealed, live]
        );
        assert_eq!(
            store.database().query_history(1, 0, SECOND + 1).unwrap(),
            vec![sealed]
        );
        assert!(!sealed_wseg_paths(&restored).is_empty());
        assert!(!sealed_wseg_paths(&backup).is_empty());
    }
}
