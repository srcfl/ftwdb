//! WattDB is an experimental embedded time-series storage engine designed for
//! energy data and write-constrained edge storage.
//!
//! The current crate is a deliberately small vertical slice: an append-only,
//! checksummed commit log, crash-tail recovery, bitemporal queries, and
//! mergeable energy aggregates. It is not yet production-ready.

mod aggregate;
mod catalog;
mod error;
mod model;
mod rollup;
mod storage;
mod transaction;

pub use aggregate::{CounterAggregate, GaugeAggregate, Sample};
pub use catalog::{Catalog, CatalogStats};
pub use error::{Error, Result};
pub use model::{
    CalendarUnit, Entity, EntityId, Plan, PlanStatus, Properties, PropertyValue, Relation,
    RelationId, RollupPolicy, RollupResolution, RollupTier, Run, RunId, RunKind, RunStatus,
    SeriesDefinition, SeriesSemantics,
};
pub use rollup::{FixedGaugeRollup, GaugeBucket};
pub use storage::{Commit, Config, Database, Durability, PlanOutcome, Point, Stats};
pub use transaction::Transaction;
