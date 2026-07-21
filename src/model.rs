use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EntityId(pub u128);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RelationId(pub u128);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RunId(pub u128);

#[derive(Clone, Debug, PartialEq)]
pub enum PropertyValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    Text(String),
}

pub type Properties = BTreeMap<String, PropertyValue>;

/// An asset, site, portfolio, meter, market, or other stable domain object.
#[derive(Clone, Debug, PartialEq)]
pub struct Entity {
    pub id: EntityId,
    pub kind: String,
    pub name: String,
    pub parent: Option<EntityId>,
    pub valid_from: i64,
    pub valid_to: Option<i64>,
    pub properties: Properties,
}

/// A typed topological or logical edge between entities.
#[derive(Clone, Debug, PartialEq)]
pub struct Relation {
    pub id: RelationId,
    pub kind: String,
    pub source: EntityId,
    pub target: EntityId,
    pub valid_from: i64,
    pub valid_to: Option<i64>,
    pub properties: Properties,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SeriesSemantics {
    Gauge,
    IntervalTotal,
    Counter,
    State,
    Event,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CalendarUnit {
    Day,
    Month,
    Year,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RollupResolution {
    FixedMicros(i64),
    Calendar {
        unit: CalendarUnit,
        iana_timezone: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RollupTier {
    pub resolution: RollupResolution,
    /// `None` retains this tier forever.
    pub retain_for_micros: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RollupPolicy {
    pub raw_retain_for_micros: Option<i64>,
    pub tiers: Vec<RollupTier>,
}

/// Catalog metadata that determines how values are interpreted and rolled up.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SeriesDefinition {
    pub id: u64,
    pub owner_entity: Option<EntityId>,
    pub owner_relation: Option<RelationId>,
    pub name: String,
    pub physical_quantity: String,
    pub canonical_unit: String,
    pub semantics: SeriesSemantics,
    pub maximum_gap_micros: Option<i64>,
    pub rollup_policy: RollupPolicy,
}

impl SeriesDefinition {
    pub fn validate(&self) -> std::result::Result<(), &'static str> {
        if self.id == 0 {
            return Err("series id zero is reserved");
        }
        if self.name.trim().is_empty() {
            return Err("series name must not be empty");
        }
        if self.physical_quantity.trim().is_empty() || self.canonical_unit.trim().is_empty() {
            return Err("physical quantity and canonical unit are required");
        }
        if self.owner_entity.is_some() == self.owner_relation.is_some() {
            return Err("series must belong to exactly one entity or relation");
        }
        if self.maximum_gap_micros.is_some_and(|gap| gap < 0) {
            return Err("maximum gap must not be negative");
        }
        for tier in &self.rollup_policy.tiers {
            match &tier.resolution {
                RollupResolution::FixedMicros(value) if *value <= 0 => {
                    return Err("fixed rollup resolution must be positive");
                }
                RollupResolution::Calendar { iana_timezone, .. }
                    if iana_timezone.trim().is_empty() =>
                {
                    return Err("calendar rollups require an IANA timezone");
                }
                _ => {}
            }
            if tier
                .retain_for_micros
                .is_some_and(|retention| retention <= 0)
            {
                return Err("rollup retention must be positive or forever");
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunKind {
    Forecast,
    Optimization,
    Import,
    Control,
    Reconciliation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

/// Provenance shared by forecast values, plans, imports, and outcomes.
#[derive(Clone, Debug, PartialEq)]
pub struct Run {
    pub id: RunId,
    pub kind: RunKind,
    pub status: RunStatus,
    pub created_at: i64,
    pub knowledge_time: i64,
    pub workflow: String,
    pub model: String,
    pub model_version: String,
    pub parent_run: Option<RunId>,
    pub input_snapshot: Option<RunId>,
    pub attributes: Properties,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlanStatus {
    Candidate,
    Approved,
    Deployed,
    Superseded,
    Cancelled,
}

/// Optimization-plan metadata. Schedules and setpoints are interval points
/// whose `run_id` refers to this plan's run.
#[derive(Clone, Debug, PartialEq)]
pub struct Plan {
    pub id: u128,
    pub run_id: RunId,
    pub status: PlanStatus,
    pub horizon_start: i64,
    pub horizon_end: i64,
    pub resolution_micros: i64,
    pub scenario: String,
    pub objective_terms: BTreeMap<String, f64>,
    pub objective_value: Option<f64>,
    pub supersedes: Option<u128>,
    pub attributes: Properties,
}

impl Plan {
    pub fn validate(&self) -> std::result::Result<(), &'static str> {
        if self.id == 0 || self.run_id.0 == 0 {
            return Err("plan and run ids must be non-zero");
        }
        if self.horizon_end <= self.horizon_start {
            return Err("plan horizon must have positive duration");
        }
        if self.resolution_micros <= 0 {
            return Err("plan resolution must be positive");
        }
        if self.scenario.trim().is_empty() {
            return Err("plan scenario must not be empty");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EntityId, Plan, PlanStatus, RollupPolicy, RollupResolution, RollupTier, RunId,
        SeriesDefinition, SeriesSemantics,
    };
    use std::collections::BTreeMap;

    #[test]
    fn energy_series_requires_one_owner_and_valid_rollups() {
        let series = SeriesDefinition {
            id: 1,
            owner_entity: Some(EntityId(1)),
            owner_relation: None,
            name: "battery_power".to_owned(),
            physical_quantity: "power".to_owned(),
            canonical_unit: "W".to_owned(),
            semantics: SeriesSemantics::Gauge,
            maximum_gap_micros: Some(10_000_000),
            rollup_policy: RollupPolicy {
                raw_retain_for_micros: Some(14 * 86_400_000_000),
                tiers: vec![RollupTier {
                    resolution: RollupResolution::FixedMicros(300_000_000),
                    retain_for_micros: None,
                }],
            },
        };
        assert_eq!(series.validate(), Ok(()));
    }

    #[test]
    fn plan_requires_a_real_horizon() {
        let plan = Plan {
            id: 1,
            run_id: RunId(2),
            status: PlanStatus::Candidate,
            horizon_start: 100,
            horizon_end: 100,
            resolution_micros: 300_000_000,
            scenario: "base".to_owned(),
            objective_terms: BTreeMap::new(),
            objective_value: None,
            supersedes: None,
            attributes: BTreeMap::new(),
        };
        assert_eq!(
            plan.validate(),
            Err("plan horizon must have positive duration")
        );
    }
}
