//! Bounded, read-only comparison of source batches with an FTWDB shadow store.
//!
//! The caller supplies the source batches for one comparison window in their
//! intended cross-source commit order. The report checks every ordered-ingress
//! receipt, the last supplied form of each catalog object, and exact point
//! multiplicities. Point comparison covers the smallest timestamp span that
//! contains the supplied points for each series. Catalog comparison is
//! one-way because catalog records do not retain an ingress source ID.
//!
//! This module does not decode another export format and does not write to the
//! database. It reads the live index and bounded blocks from sealed segments.

use crate::shadow_protocol::CommitBatchRequest;
use crate::{Database, IngressIdentity, Point};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

pub const DEFAULT_MAX_RECONCILE_BATCHES: usize = 65_536;
pub const DEFAULT_MAX_RECONCILE_METADATA: usize = 1_000_000;
pub const DEFAULT_MAX_RECONCILE_POINTS: usize = 1_000_000;
pub const DEFAULT_MAX_RECONCILE_SCANNED_POINTS: usize = 1_000_000;
pub const DEFAULT_MAX_RECONCILE_DETAILS: usize = 256;

/// Work and output limits for one reconciliation window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShadowReconcileLimits {
    pub max_batches: usize,
    pub max_metadata_records: usize,
    pub max_expected_points: usize,
    pub max_observed_points: usize,
    /// Maximum raw series entries visited before timestamp filtering.
    pub max_scanned_points: usize,
    pub max_mismatch_details: usize,
}

impl Default for ShadowReconcileLimits {
    fn default() -> Self {
        Self {
            max_batches: DEFAULT_MAX_RECONCILE_BATCHES,
            max_metadata_records: DEFAULT_MAX_RECONCILE_METADATA,
            max_expected_points: DEFAULT_MAX_RECONCILE_POINTS,
            max_observed_points: DEFAULT_MAX_RECONCILE_POINTS,
            max_scanned_points: DEFAULT_MAX_RECONCILE_SCANNED_POINTS,
            max_mismatch_details: DEFAULT_MAX_RECONCILE_DETAILS,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileLimit {
    Batches,
    MetadataRecords,
    ExpectedPoints,
    ObservedPoints,
    ScannedPoints,
}

#[derive(Debug)]
pub enum ShadowReconcileError {
    LimitExceeded {
        limit: ReconcileLimit,
        maximum: usize,
    },
    InvalidIdentity(IngressIdentity),
    DuplicateSourceSequence {
        source_id: u128,
        sequence: u64,
    },
    DuplicateCommitId(u128),
    MaximumTimestamp {
        series_id: u64,
    },
    Store(crate::Error),
}

impl fmt::Display for ShadowReconcileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LimitExceeded { limit, maximum } => {
                write!(
                    formatter,
                    "reconciliation {limit:?} limit exceeds {maximum}"
                )
            }
            Self::InvalidIdentity(identity) => write!(
                formatter,
                "invalid reconciliation identity for source {} sequence {}",
                identity.source_id, identity.sequence
            ),
            Self::DuplicateSourceSequence {
                source_id,
                sequence,
            } => write!(
                formatter,
                "duplicate reconciliation source {source_id} sequence {sequence}"
            ),
            Self::DuplicateCommitId(commit_id) => {
                write!(formatter, "duplicate reconciliation commit id {commit_id}")
            }
            Self::MaximumTimestamp { series_id } => write!(
                formatter,
                "series {series_id} contains i64::MAX, which has no exclusive query end"
            ),
            Self::Store(error) => {
                write!(formatter, "could not verify stored ingress bytes: {error}")
            }
        }
    }
}

impl Error for ShadowReconcileError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Store(error) => Some(error),
            _ => None,
        }
    }
}

impl From<crate::Error> for ShadowReconcileError {
    fn from(value: crate::Error) -> Self {
        Self::Store(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogObjectKind {
    Entity,
    Relation,
    Series,
    Run,
    Plan,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogMismatch {
    Missing,
    Different,
}

/// Exact point identity. Floating-point values use their wire bits, so `-0.0`
/// and `0.0` do not compare as the same source row.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ShadowPointKey {
    pub series_id: u64,
    pub valid_time: i64,
    pub valid_time_end: i64,
    pub knowledge_time: i64,
    pub change_time: i64,
    pub run_id: u128,
    pub value_bits: u64,
    pub quality: u32,
    pub flags: u32,
}

impl From<Point> for ShadowPointKey {
    fn from(point: Point) -> Self {
        Self {
            series_id: point.series_id,
            valid_time: point.valid_time,
            valid_time_end: point.valid_time_end,
            knowledge_time: point.knowledge_time,
            change_time: point.change_time,
            run_id: point.run_id,
            value_bits: point.value.to_bits(),
            quality: point.quality,
            flags: point.flags,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShadowReconcileDetail {
    MissingReceipt {
        identity: IngressIdentity,
    },
    ReceiptCommitConflict {
        expected: IngressIdentity,
        actual_commit_id: u128,
    },
    ReceiptShapeMismatch {
        identity: IngressIdentity,
        expected_records: usize,
        actual_records: usize,
        expected_points: usize,
        actual_points: usize,
    },
    ReceiptPayloadMismatch {
        identity: IngressIdentity,
    },
    Catalog {
        kind: CatalogObjectKind,
        id: u128,
        mismatch: CatalogMismatch,
    },
    PointCount {
        point: ShadowPointKey,
        expected: usize,
        actual: usize,
    },
}

/// Stable counts plus a bounded sample of mismatches.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ShadowReconciliationReport {
    pub expected_batches: usize,
    pub matching_receipts: usize,
    pub missing_receipts: usize,
    pub conflicting_receipts: usize,
    pub receipt_shape_mismatches: usize,
    pub receipt_payload_mismatches: usize,
    pub nondurable_receipts: usize,
    pub expected_catalog_objects: usize,
    pub matching_catalog_objects: usize,
    pub missing_catalog_objects: usize,
    pub different_catalog_objects: usize,
    pub expected_points: usize,
    /// Raw entries visited before timestamp filtering.
    pub scanned_points: usize,
    pub observed_points: usize,
    pub matching_points: usize,
    pub missing_points: usize,
    pub unexpected_points: usize,
    pub mismatch_groups: usize,
    pub mismatch_details: Vec<ShadowReconcileDetail>,
    pub details_truncated: bool,
}

impl ShadowReconciliationReport {
    /// True when all expected receipts and data match. Durability is separate.
    #[must_use]
    pub const fn content_matches(&self) -> bool {
        self.missing_receipts == 0
            && self.conflicting_receipts == 0
            && self.receipt_shape_mismatches == 0
            && self.receipt_payload_mismatches == 0
            && self.missing_catalog_objects == 0
            && self.different_catalog_objects == 0
            && self.missing_points == 0
            && self.unexpected_points == 0
    }

    /// True when content matches and every found receipt has a sync proof.
    #[must_use]
    pub const fn ready_to_release_source_copy(&self) -> bool {
        self.content_matches() && self.nondurable_receipts == 0
    }
}

#[derive(Clone, Copy)]
struct SeriesSpan {
    start: i64,
    end: i64,
}

/// Compares a bounded source window with checked FTWDB state without writes.
pub fn reconcile_shadow_batches(
    database: &Database,
    expected: &[CommitBatchRequest],
    limits: ShadowReconcileLimits,
) -> Result<ShadowReconciliationReport, ShadowReconcileError> {
    check_limit(expected.len(), limits.max_batches, ReconcileLimit::Batches)?;

    let mut source_sequences = BTreeSet::new();
    let mut commit_ids = BTreeSet::new();
    let mut metadata_records = 0_usize;
    let mut expected_points = 0_usize;
    for batch in expected {
        let identity = IngressIdentity::new(batch.source_id, batch.sequence, batch.commit_id);
        if batch.source_id == 0 {
            return Err(ShadowReconcileError::InvalidIdentity(identity));
        }
        if !source_sequences.insert((batch.source_id, batch.sequence)) {
            return Err(ShadowReconcileError::DuplicateSourceSequence {
                source_id: batch.source_id,
                sequence: batch.sequence,
            });
        }
        if !commit_ids.insert(batch.commit_id) {
            return Err(ShadowReconcileError::DuplicateCommitId(batch.commit_id));
        }
        metadata_records = metadata_records.checked_add(metadata_count(batch)).ok_or(
            ShadowReconcileError::LimitExceeded {
                limit: ReconcileLimit::MetadataRecords,
                maximum: limits.max_metadata_records,
            },
        )?;
        expected_points = expected_points.checked_add(batch.points.len()).ok_or(
            ShadowReconcileError::LimitExceeded {
                limit: ReconcileLimit::ExpectedPoints,
                maximum: limits.max_expected_points,
            },
        )?;
    }
    check_limit(
        metadata_records,
        limits.max_metadata_records,
        ReconcileLimit::MetadataRecords,
    )?;
    check_limit(
        expected_points,
        limits.max_expected_points,
        ReconcileLimit::ExpectedPoints,
    )?;

    let mut report = ShadowReconciliationReport {
        expected_batches: expected.len(),
        expected_points,
        ..ShadowReconciliationReport::default()
    };
    reconcile_receipts(database, expected, limits, &mut report)?;
    reconcile_catalog(database, expected, limits, &mut report);
    reconcile_points(database, expected, limits, &mut report)?;
    report.details_truncated = report.mismatch_details.len() < report.mismatch_groups;
    Ok(report)
}

fn reconcile_receipts(
    database: &Database,
    expected: &[CommitBatchRequest],
    limits: ShadowReconcileLimits,
    report: &mut ShadowReconciliationReport,
) -> Result<(), ShadowReconcileError> {
    for batch in expected {
        let identity = IngressIdentity::new(batch.source_id, batch.sequence, batch.commit_id);
        let expected_records = metadata_count(batch) + usize::from(!batch.points.is_empty());
        let Some(receipt) = database.ingress_receipt(batch.source_id, batch.sequence) else {
            report.missing_receipts += 1;
            record_detail(
                report,
                limits,
                ShadowReconcileDetail::MissingReceipt { identity },
            );
            continue;
        };
        if !receipt.durable {
            report.nondurable_receipts += 1;
        }
        let mut matches = true;
        let commit_id_matches = receipt.identity.commit_id == batch.commit_id;
        if !commit_id_matches {
            matches = false;
            report.conflicting_receipts += 1;
            record_detail(
                report,
                limits,
                ShadowReconcileDetail::ReceiptCommitConflict {
                    expected: identity,
                    actual_commit_id: receipt.identity.commit_id,
                },
            );
        }
        if receipt.records != expected_records || receipt.points != batch.points.len() {
            matches = false;
            report.receipt_shape_mismatches += 1;
            record_detail(
                report,
                limits,
                ShadowReconcileDetail::ReceiptShapeMismatch {
                    identity,
                    expected_records,
                    actual_records: receipt.records,
                    expected_points: batch.points.len(),
                    actual_points: receipt.points,
                },
            );
        }
        if commit_id_matches {
            let transaction = crate::shadow_protocol::transaction_from_batch(batch.clone());
            let payload_matches = database
                .verify_ingress_payload(identity, &transaction)?
                .unwrap_or(false);
            if !payload_matches {
                matches = false;
                report.receipt_payload_mismatches += 1;
                record_detail(
                    report,
                    limits,
                    ShadowReconcileDetail::ReceiptPayloadMismatch { identity },
                );
            }
        }
        if matches {
            report.matching_receipts += 1;
        }
    }
    Ok(())
}

fn reconcile_catalog(
    database: &Database,
    expected: &[CommitBatchRequest],
    limits: ShadowReconcileLimits,
    report: &mut ShadowReconciliationReport,
) {
    let mut entities = BTreeMap::new();
    let mut relations = BTreeMap::new();
    let mut series = BTreeMap::new();
    let mut runs = BTreeMap::new();
    let mut plans = BTreeMap::new();
    for batch in expected {
        entities.extend(batch.entities.iter().map(|value| (value.id, value)));
        relations.extend(batch.relations.iter().map(|value| (value.id, value)));
        series.extend(batch.series.iter().map(|value| (value.id, value)));
        runs.extend(batch.runs.iter().map(|value| (value.id, value)));
        plans.extend(batch.plans.iter().map(|value| (value.id, value)));
    }
    report.expected_catalog_objects =
        entities.len() + relations.len() + series.len() + runs.len() + plans.len();

    macro_rules! compare_catalog {
        ($values:expr, $lookup:ident, $kind:expr, $id:expr) => {
            for (id, expected) in $values {
                match database.catalog().$lookup(id) {
                    Some(actual) if actual == expected => report.matching_catalog_objects += 1,
                    Some(_) => {
                        report.different_catalog_objects += 1;
                        record_detail(
                            report,
                            limits,
                            ShadowReconcileDetail::Catalog {
                                kind: $kind,
                                id: $id(id),
                                mismatch: CatalogMismatch::Different,
                            },
                        );
                    }
                    None => {
                        report.missing_catalog_objects += 1;
                        record_detail(
                            report,
                            limits,
                            ShadowReconcileDetail::Catalog {
                                kind: $kind,
                                id: $id(id),
                                mismatch: CatalogMismatch::Missing,
                            },
                        );
                    }
                }
            }
        };
    }
    compare_catalog!(
        entities,
        entity,
        CatalogObjectKind::Entity,
        |id: crate::EntityId| id.0
    );
    compare_catalog!(
        relations,
        relation,
        CatalogObjectKind::Relation,
        |id: crate::RelationId| id.0
    );
    compare_catalog!(series, series, CatalogObjectKind::Series, |id: u64| {
        u128::from(id)
    });
    compare_catalog!(runs, run, CatalogObjectKind::Run, |id: crate::RunId| id.0);
    compare_catalog!(plans, plan, CatalogObjectKind::Plan, |id: u128| id);
}

fn reconcile_points(
    database: &Database,
    expected: &[CommitBatchRequest],
    limits: ShadowReconcileLimits,
    report: &mut ShadowReconciliationReport,
) -> Result<(), ShadowReconcileError> {
    let mut expected_counts = BTreeMap::<ShadowPointKey, usize>::new();
    let mut spans = BTreeMap::<u64, SeriesSpan>::new();
    for point in expected.iter().flat_map(|batch| &batch.points) {
        let end =
            point
                .valid_time
                .checked_add(1)
                .ok_or(ShadowReconcileError::MaximumTimestamp {
                    series_id: point.series_id,
                })?;
        expected_counts
            .entry((*point).into())
            .and_modify(|count| *count += 1)
            .or_insert(1);
        spans
            .entry(point.series_id)
            .and_modify(|span| {
                span.start = span.start.min(point.valid_time);
                span.end = span.end.max(end);
            })
            .or_insert(SeriesSpan {
                start: point.valid_time,
                end,
            });
    }

    let mut observed_counts = BTreeMap::<ShadowPointKey, usize>::new();
    for (series_id, span) in spans {
        database.visit_history(
            series_id,
            span.start,
            span.end,
            |count| {
                report.scanned_points = report.scanned_points.checked_add(count).ok_or(
                    ShadowReconcileError::LimitExceeded {
                        limit: ReconcileLimit::ScannedPoints,
                        maximum: limits.max_scanned_points,
                    },
                )?;
                check_limit(
                    report.scanned_points,
                    limits.max_scanned_points,
                    ReconcileLimit::ScannedPoints,
                )
            },
            |point| {
                report.observed_points = report.observed_points.checked_add(1).ok_or(
                    ShadowReconcileError::LimitExceeded {
                        limit: ReconcileLimit::ObservedPoints,
                        maximum: limits.max_observed_points,
                    },
                )?;
                check_limit(
                    report.observed_points,
                    limits.max_observed_points,
                    ReconcileLimit::ObservedPoints,
                )?;
                observed_counts
                    .entry(point.into())
                    .and_modify(|count| *count += 1)
                    .or_insert(1);
                Ok(())
            },
        )?;
    }

    let keys: BTreeSet<_> = expected_counts
        .keys()
        .chain(observed_counts.keys())
        .copied()
        .collect();
    for point in keys {
        let expected = expected_counts.get(&point).copied().unwrap_or(0);
        let actual = observed_counts.get(&point).copied().unwrap_or(0);
        report.matching_points += expected.min(actual);
        report.missing_points += expected.saturating_sub(actual);
        report.unexpected_points += actual.saturating_sub(expected);
        if expected != actual {
            record_detail(
                report,
                limits,
                ShadowReconcileDetail::PointCount {
                    point,
                    expected,
                    actual,
                },
            );
        }
    }
    Ok(())
}

fn metadata_count(batch: &CommitBatchRequest) -> usize {
    batch.entities.len()
        + batch.relations.len()
        + batch.series.len()
        + batch.runs.len()
        + batch.plans.len()
}

fn check_limit(
    actual: usize,
    maximum: usize,
    limit: ReconcileLimit,
) -> Result<(), ShadowReconcileError> {
    if actual > maximum {
        Err(ShadowReconcileError::LimitExceeded { limit, maximum })
    } else {
        Ok(())
    }
}

fn record_detail(
    report: &mut ShadowReconciliationReport,
    limits: ShadowReconcileLimits,
    detail: ShadowReconcileDetail,
) {
    report.mismatch_groups += 1;
    if report.mismatch_details.len() < limits.max_mismatch_details {
        report.mismatch_details.push(detail);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shadow_protocol::CommitBatchRequest;
    use crate::{
        Config, Durability, Entity, EntityId, RollupPolicy, Run, RunId, RunKind, RunStatus,
        SeriesDefinition, SeriesSemantics,
    };
    use tempfile::tempdir;

    fn batch(value: f64) -> CommitBatchRequest {
        CommitBatchRequest {
            source_id: 7,
            sequence: 10,
            commit_id: 99,
            entities: vec![Entity {
                id: EntityId(1),
                kind: "site".into(),
                name: "alpha".into(),
                parent: None,
                valid_from: 1,
                valid_to: None,
                properties: BTreeMap::new(),
            }],
            relations: Vec::new(),
            series: vec![SeriesDefinition {
                id: 2,
                owner_entity: Some(EntityId(1)),
                owner_relation: None,
                name: "grid_power".into(),
                physical_quantity: "power".into(),
                canonical_unit: "W".into(),
                semantics: SeriesSemantics::Gauge,
                maximum_gap_micros: Some(5_000_000),
                rollup_policy: RollupPolicy {
                    raw_retain_for_micros: None,
                    tiers: Vec::new(),
                },
            }],
            runs: vec![Run {
                id: RunId(3),
                kind: RunKind::Import,
                status: RunStatus::Succeeded,
                created_at: 2,
                knowledge_time: 2,
                workflow: "shadow".into(),
                model: "source".into(),
                model_version: "1".into(),
                parent_run: None,
                input_snapshot: None,
                attributes: BTreeMap::new(),
            }],
            plans: Vec::new(),
            points: vec![Point {
                series_id: 2,
                valid_time: 4,
                valid_time_end: 4,
                knowledge_time: 5,
                change_time: 6,
                run_id: 3,
                value,
                quality: 7,
                flags: 8,
            }],
        }
    }

    fn transaction(batch: &CommitBatchRequest) -> crate::Transaction {
        crate::shadow_protocol::transaction_from_batch(batch.clone())
    }

    #[test]
    fn exact_window_matches_receipt_catalog_and_point_bits() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("shadow.ftwdb");
        let expected = batch(-0.0);
        let mut database = Database::open(&path).unwrap();
        database
            .commit_ingress(
                IngressIdentity::new(expected.source_id, expected.sequence, expected.commit_id),
                transaction(&expected),
            )
            .unwrap();

        let report = reconcile_shadow_batches(
            &database,
            std::slice::from_ref(&expected),
            ShadowReconcileLimits::default(),
        )
        .unwrap();
        assert!(report.content_matches());
        assert!(report.ready_to_release_source_copy());
        assert_eq!((report.matching_receipts, report.matching_points), (1, 1));
        assert_eq!(report.expected_catalog_objects, 3);
        assert_eq!(report.matching_catalog_objects, 3);
        assert_eq!(report.scanned_points, 1);
        assert!(report.mismatch_details.is_empty());
    }

    #[test]
    fn exact_receipt_payload_detects_batches_with_swapped_points() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("shadow.ftwdb");
        let first = batch(10.0);
        let mut second = batch(20.0);
        second.sequence = 11;
        second.commit_id = 100;
        second.points[0].valid_time = 100;
        second.points[0].valid_time_end = 100;

        let mut stored_first = second.clone();
        stored_first.sequence = first.sequence;
        stored_first.commit_id = first.commit_id;
        let mut stored_second = first.clone();
        stored_second.sequence = second.sequence;
        stored_second.commit_id = second.commit_id;

        let mut database = Database::open(&path).unwrap();
        database
            .commit_ingress(
                IngressIdentity::new(
                    stored_first.source_id,
                    stored_first.sequence,
                    stored_first.commit_id,
                ),
                transaction(&stored_first),
            )
            .unwrap();
        database
            .commit_ingress(
                IngressIdentity::new(
                    stored_second.source_id,
                    stored_second.sequence,
                    stored_second.commit_id,
                ),
                transaction(&stored_second),
            )
            .unwrap();

        let report = reconcile_shadow_batches(
            &database,
            &[first, second],
            ShadowReconcileLimits::default(),
        )
        .unwrap();
        assert!(!report.content_matches());
        assert_eq!(report.receipt_payload_mismatches, 2);
        assert_eq!((report.missing_points, report.unexpected_points), (0, 0));
    }

    #[test]
    fn reports_changed_identity_metadata_and_exact_point_bits() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("shadow.ftwdb");
        let stored = batch(0.0);
        let mut database = Database::open(&path).unwrap();
        database
            .commit_ingress(
                IngressIdentity::new(stored.source_id, stored.sequence, stored.commit_id),
                transaction(&stored),
            )
            .unwrap();

        let mut expected = batch(-0.0);
        expected.commit_id += 1;
        expected.entities[0].name = "changed".into();
        let report = reconcile_shadow_batches(
            &database,
            &[expected],
            ShadowReconcileLimits {
                max_mismatch_details: 2,
                ..ShadowReconcileLimits::default()
            },
        )
        .unwrap();
        assert!(!report.content_matches());
        assert_eq!(report.conflicting_receipts, 1);
        assert_eq!(report.different_catalog_objects, 1);
        assert_eq!((report.missing_points, report.unexpected_points), (1, 1));
        assert_eq!(report.mismatch_details.len(), 2);
        assert!(report.details_truncated);
    }

    #[test]
    fn read_only_recovery_does_not_claim_receipt_durability() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("shadow.ftwdb");
        let expected = batch(1.0);
        {
            let mut database = Database::open_with(
                &path,
                Config {
                    durability: Durability::Always,
                    ..Config::default()
                },
            )
            .unwrap();
            database
                .commit_ingress(
                    IngressIdentity::new(expected.source_id, expected.sequence, expected.commit_id),
                    transaction(&expected),
                )
                .unwrap();
        }
        let database = Database::open_read_only(&path).unwrap();
        let report =
            reconcile_shadow_batches(&database, &[expected], ShadowReconcileLimits::default())
                .unwrap();
        assert!(report.content_matches());
        assert_eq!(report.nondurable_receipts, 1);
        assert!(!report.ready_to_release_source_copy());
    }

    #[test]
    fn scan_limit_counts_filtered_block_points_and_spans_seals_and_live_tail() {
        let directory = tempdir().unwrap();
        let mut store = crate::Store::open(directory.path()).unwrap();
        let expected = batch(1.0);
        store
            .commit_ingress(
                IngressIdentity::new(expected.source_id, expected.sequence, expected.commit_id),
                transaction(&expected),
            )
            .unwrap();
        let outside = [Point::actual(2, 0, 0.0), Point::actual(2, 8, 8.0)];
        let mut transaction = crate::Transaction::new();
        transaction.append_points(outside.to_vec());
        store.commit(transaction).unwrap();
        store.seal_and_reclaim().unwrap();
        // Only the expected point falls in the comparison window. All three
        // block entries must still count against the decode budget.
        let check = |store: &crate::Store, maximum| {
            reconcile_shadow_batches(
                store.database(),
                std::slice::from_ref(&expected),
                ShadowReconcileLimits {
                    max_scanned_points: maximum,
                    ..ShadowReconcileLimits::default()
                },
            )
        };
        assert!(matches!(
            check(&store, 2),
            Err(ShadowReconcileError::LimitExceeded {
                limit: ReconcileLimit::ScannedPoints,
                maximum: 2,
            })
        ));
        let report = check(&store, 3).unwrap();
        assert_eq!(report.scanned_points, 3);
        assert_eq!(report.observed_points, 1);
        assert!(report.content_matches());

        for seal in [true, false] {
            let mut extra = crate::Transaction::new();
            extra.append_points(vec![expected.points[0]]);
            store.commit(extra).unwrap();
            if seal {
                store.seal_and_reclaim().unwrap();
            }
        }
        assert!(matches!(
            check(&store, 4),
            Err(ShadowReconcileError::LimitExceeded {
                limit: ReconcileLimit::ScannedPoints,
                maximum: 4,
            })
        ));
        let report = check(&store, 5).unwrap();
        assert_eq!(report.scanned_points, 5);
        assert_eq!(report.observed_points, 3);
        assert_eq!(report.unexpected_points, 2);
        assert!(matches!(
            reconcile_shadow_batches(
                store.database(),
                &[expected],
                ShadowReconcileLimits {
                    max_observed_points: 1,
                    ..ShadowReconcileLimits::default()
                }
            ),
            Err(ShadowReconcileError::LimitExceeded {
                limit: ReconcileLimit::ObservedPoints,
                maximum: 1,
            })
        ));
    }

    #[test]
    fn scan_limit_rejects_before_reading_an_oversize_block() {
        use std::os::unix::fs::FileExt;
        let directory = tempdir().unwrap();
        let mut database = Database::open(directory.path().join("raw.wlog")).unwrap();
        let expected = batch(1.0);
        database
            .commit_ingress(
                IngressIdentity::new(expected.source_id, expected.sequence, expected.commit_id),
                transaction(&expected),
            )
            .unwrap();
        let segment_path = directory.path().join("raw.wseg");
        crate::Segment::create(
            &segment_path,
            &[expected.points[0], Point::actual(2, 8, 8.0)],
            2,
        )
        .unwrap();
        let segment = crate::Segment::open(&segment_path).unwrap();
        let offset = segment.first_block_payload_offset().unwrap();
        database.attach_sealed_segments(vec![segment]);
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&segment_path)
            .unwrap();
        file.write_all_at(&[0xff], offset).unwrap();
        assert!(matches!(
            reconcile_shadow_batches(
                &database,
                std::slice::from_ref(&expected),
                ShadowReconcileLimits {
                    max_scanned_points: 1,
                    ..ShadowReconcileLimits::default()
                }
            ),
            Err(ShadowReconcileError::LimitExceeded {
                limit: ReconcileLimit::ScannedPoints,
                maximum: 1
            })
        ));
        assert!(matches!(
            reconcile_shadow_batches(&database, &[expected], ShadowReconcileLimits::default()),
            Err(ShadowReconcileError::Store(crate::Error::Corruption { .. }))
        ));
    }

    #[test]
    fn rejects_duplicate_keys_and_work_above_limits() {
        let directory = tempdir().unwrap();
        let mut database = Database::open(directory.path().join("shadow.ftwdb")).unwrap();
        let expected = batch(1.0);
        assert!(matches!(
            reconcile_shadow_batches(
                &database,
                &[expected.clone(), expected.clone()],
                ShadowReconcileLimits::default()
            ),
            Err(ShadowReconcileError::DuplicateSourceSequence { .. })
        ));
        assert!(matches!(
            reconcile_shadow_batches(
                &database,
                std::slice::from_ref(&expected),
                ShadowReconcileLimits {
                    max_expected_points: 0,
                    ..ShadowReconcileLimits::default()
                }
            ),
            Err(ShadowReconcileError::LimitExceeded {
                limit: ReconcileLimit::ExpectedPoints,
                maximum: 0,
            })
        ));

        database
            .commit_ingress(
                IngressIdentity::new(expected.source_id, expected.sequence, expected.commit_id),
                transaction(&expected),
            )
            .unwrap();
        assert!(matches!(
            reconcile_shadow_batches(
                &database,
                &[expected],
                ShadowReconcileLimits {
                    max_scanned_points: 0,
                    ..ShadowReconcileLimits::default()
                }
            ),
            Err(ShadowReconcileError::LimitExceeded {
                limit: ReconcileLimit::ScannedPoints,
                maximum: 0,
            })
        ));
    }
}
