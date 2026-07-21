//! WattDB is an experimental embedded time-series storage engine designed for
//! energy data and write-constrained edge storage.
//!
//! The current crate is a deliberately small vertical slice: an append-only,
//! checksummed commit log, crash-tail recovery, bitemporal queries, and
//! mergeable energy aggregates. It is not yet production-ready.

mod aggregate;
mod catalog;
mod error;
mod manifest;
mod model;
mod rollup;
mod rollup_segment;
mod segment;
mod storage;
mod store;
mod transaction;

pub use aggregate::{CounterAggregate, GaugeAggregate, Sample};
pub use catalog::{Catalog, CatalogStats};
pub use error::{Error, Result};
pub use manifest::RollupDescriptor;
pub use model::{
    CalendarUnit, Entity, EntityId, Plan, PlanStatus, Properties, PropertyValue, Relation,
    RelationId, RollupPolicy, RollupResolution, RollupTier, Run, RunId, RunKind, RunStatus,
    SeriesDefinition, SeriesSemantics,
};
pub use rollup::{CalendarGaugeRollup, FixedGaugeRollup, GaugeBucket};
pub use rollup_segment::{RollupSegment, RollupSegmentStats};
pub use segment::{Segment, SegmentStats};
pub use storage::{Commit, Config, Database, Durability, PlanOutcome, Point, Stats};
pub use store::{MaintenanceReport, RetentionGate, RollupQuery, RollupSource, Store};
pub use transaction::Transaction;
