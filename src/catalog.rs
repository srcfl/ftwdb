use crate::model::{
    Entity, EntityId, Plan, PlanStatus, Relation, RelationId, Run, RunId, RunKind, RunStatus,
    SeriesDefinition,
};
use crate::transaction::Record;
use crate::{Error, Point, Result};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CatalogStats {
    pub entities: usize,
    pub relations: usize,
    pub series: usize,
    pub runs: usize,
    pub plans: usize,
}

#[derive(Clone, Debug, Default)]
pub struct Catalog {
    entities: BTreeMap<EntityId, Entity>,
    relations: BTreeMap<RelationId, Relation>,
    series: BTreeMap<u64, SeriesDefinition>,
    runs: BTreeMap<RunId, Run>,
    plans: BTreeMap<u128, Plan>,
}

impl Catalog {
    #[must_use]
    pub fn entity(&self, id: EntityId) -> Option<&Entity> {
        self.entities.get(&id)
    }

    #[must_use]
    pub fn relation(&self, id: RelationId) -> Option<&Relation> {
        self.relations.get(&id)
    }

    #[must_use]
    pub fn series(&self, id: u64) -> Option<&SeriesDefinition> {
        self.series.get(&id)
    }

    #[must_use]
    pub fn run(&self, id: RunId) -> Option<&Run> {
        self.runs.get(&id)
    }

    #[must_use]
    pub fn plan(&self, id: u128) -> Option<&Plan> {
        self.plans.get(&id)
    }

    pub fn entities(&self) -> impl ExactSizeIterator<Item = &Entity> {
        self.entities.values()
    }

    pub fn relations(&self) -> impl ExactSizeIterator<Item = &Relation> {
        self.relations.values()
    }

    pub fn series_definitions(&self) -> impl ExactSizeIterator<Item = &SeriesDefinition> {
        self.series.values()
    }

    pub fn runs(&self) -> impl ExactSizeIterator<Item = &Run> {
        self.runs.values()
    }

    pub fn plans(&self) -> impl ExactSizeIterator<Item = &Plan> {
        self.plans.values()
    }

    #[must_use]
    pub fn stats(&self) -> CatalogStats {
        CatalogStats {
            entities: self.entities.len(),
            relations: self.relations.len(),
            series: self.series.len(),
            runs: self.runs.len(),
            plans: self.plans.len(),
        }
    }

    pub(crate) fn validate_and_apply(&self, records: &[Record]) -> Result<Self> {
        let mut candidate = self.clone();
        for record in records {
            candidate.apply(record)?;
        }
        candidate.validate_references()?;
        for record in records {
            if let Record::Points(points) = record {
                candidate.validate_points(points)?;
            }
        }
        Ok(candidate)
    }

    pub(crate) fn apply_recovered(&mut self, records: &[Record], offset: u64) -> Result<()> {
        let candidate = self
            .validate_and_apply(records)
            .map_err(|error| Error::Corruption {
                offset,
                reason: format!("invalid recovered transaction: {error}"),
            })?;
        *self = candidate;
        Ok(())
    }

    fn apply(&mut self, record: &Record) -> Result<()> {
        match record {
            Record::Entity(entity) => {
                validate_entity(entity)?;
                self.entities.insert(entity.id, entity.clone());
            }
            Record::Relation(relation) => {
                validate_relation(relation)?;
                self.relations.insert(relation.id, relation.clone());
            }
            Record::Series(series) => {
                series
                    .validate()
                    .map_err(|reason| Error::InvalidModel(reason.to_owned()))?;
                self.series.insert(series.id, series.clone());
            }
            Record::Run(run) => {
                validate_run(run)?;
                if let Some(previous) = self.runs.get(&run.id) {
                    validate_run_transition(previous, run)?;
                }
                self.runs.insert(run.id, run.clone());
            }
            Record::Plan(plan) => {
                plan.validate()
                    .map_err(|reason| Error::InvalidModel(reason.to_owned()))?;
                if let Some(previous) = self.plans.get(&plan.id) {
                    validate_plan_transition(previous, plan)?;
                }
                self.plans.insert(plan.id, plan.clone());
            }
            Record::Points(_) => {}
        }
        Ok(())
    }

    fn validate_references(&self) -> Result<()> {
        for entity in self.entities.values() {
            if let Some(parent) = entity.parent
                && !self.entities.contains_key(&parent)
            {
                return invalid(format!(
                    "entity {} refers to missing parent {}",
                    entity.id.0, parent.0
                ));
            }
            validate_no_parent_cycle(entity.id, &self.entities)?;
        }
        for relation in self.relations.values() {
            if !self.entities.contains_key(&relation.source)
                || !self.entities.contains_key(&relation.target)
            {
                return invalid(format!("relation {} has a missing endpoint", relation.id.0));
            }
        }
        for series in self.series.values() {
            if series
                .owner_entity
                .is_some_and(|owner| !self.entities.contains_key(&owner))
                || series
                    .owner_relation
                    .is_some_and(|owner| !self.relations.contains_key(&owner))
            {
                return invalid(format!("series {} has a missing owner", series.id));
            }
        }
        for run in self.runs.values() {
            if run
                .parent_run
                .is_some_and(|parent| !self.runs.contains_key(&parent))
                || run
                    .input_snapshot
                    .is_some_and(|input| !self.runs.contains_key(&input))
            {
                return invalid(format!("run {} has missing provenance", run.id.0));
            }
        }
        for plan in self.plans.values() {
            let Some(run) = self.runs.get(&plan.run_id) else {
                return invalid(format!("plan {} refers to a missing run", plan.id));
            };
            if run.kind != RunKind::Optimization {
                return invalid(format!(
                    "plan {} must refer to an optimization run",
                    plan.id
                ));
            }
            if plan
                .supersedes
                .is_some_and(|previous| !self.plans.contains_key(&previous))
            {
                return invalid(format!("plan {} supersedes a missing plan", plan.id));
            }
        }
        Ok(())
    }

    fn validate_points(&self, points: &[Point]) -> Result<()> {
        validate_point_intervals(points)?;
        for point in points {
            if !self.series.contains_key(&point.series_id) {
                return invalid(format!(
                    "point refers to undefined series {}",
                    point.series_id
                ));
            }
            if point.run_id != 0 && !self.runs.contains_key(&RunId(point.run_id)) {
                return invalid(format!("point refers to missing run {}", point.run_id));
            }
        }
        Ok(())
    }
}

/// The catalog-independent point invariants. Transaction commits enforce
/// these through `validate_points`; the legacy catalog-less `append` path
/// enforces exactly this subset so both writers reject a malformed interval
/// with the same error.
pub(crate) fn validate_point_intervals(points: &[Point]) -> Result<()> {
    for point in points {
        if point.valid_time_end < point.valid_time {
            return invalid("point interval ends before it starts".to_owned());
        }
    }
    Ok(())
}

pub(crate) fn validate_entity(entity: &Entity) -> Result<()> {
    if entity.id.0 == 0 {
        return invalid("entity id zero is reserved".to_owned());
    }
    if entity.kind.trim().is_empty() || entity.name.trim().is_empty() {
        return invalid("entity kind and name are required".to_owned());
    }
    if entity.parent == Some(entity.id) {
        return invalid("entity cannot be its own parent".to_owned());
    }
    if entity.valid_to.is_some_and(|end| end <= entity.valid_from) {
        return invalid("entity validity interval must be positive".to_owned());
    }
    Ok(())
}

fn validate_relation(relation: &Relation) -> Result<()> {
    if relation.id.0 == 0 || relation.kind.trim().is_empty() {
        return invalid("relation id and kind are required".to_owned());
    }
    if relation
        .valid_to
        .is_some_and(|end| end <= relation.valid_from)
    {
        return invalid("relation validity interval must be positive".to_owned());
    }
    Ok(())
}

pub(crate) fn validate_run(run: &Run) -> Result<()> {
    if run.id.0 == 0 || run.workflow.trim().is_empty() {
        return invalid("run id and workflow are required".to_owned());
    }
    if run.parent_run == Some(run.id) || run.input_snapshot == Some(run.id) {
        return invalid("run cannot refer to itself".to_owned());
    }
    Ok(())
}

fn validate_run_transition(previous: &Run, next: &Run) -> Result<()> {
    let terminal = matches!(
        previous.status,
        RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
    );
    if terminal && previous != next {
        return invalid(format!("completed run {} is immutable", previous.id.0));
    }
    if previous.kind != next.kind || previous.created_at != next.created_at {
        return invalid(format!(
            "run {} identity fields are immutable",
            previous.id.0
        ));
    }
    Ok(())
}

fn validate_plan_transition(previous: &Plan, next: &Plan) -> Result<()> {
    if previous.run_id != next.run_id
        || previous.horizon_start != next.horizon_start
        || previous.horizon_end != next.horizon_end
        || previous.resolution_micros != next.resolution_micros
    {
        return invalid(format!(
            "plan {} schedule identity fields are immutable",
            previous.id
        ));
    }
    let allowed = previous.status == next.status
        || matches!(
            (previous.status, next.status),
            (PlanStatus::Candidate, PlanStatus::Approved)
                | (PlanStatus::Candidate, PlanStatus::Cancelled)
                | (PlanStatus::Candidate, PlanStatus::Superseded)
                | (PlanStatus::Approved, PlanStatus::Deployed)
                | (PlanStatus::Approved, PlanStatus::Cancelled)
                | (PlanStatus::Approved, PlanStatus::Superseded)
                | (PlanStatus::Deployed, PlanStatus::Superseded)
        );
    if !allowed {
        return invalid(format!("invalid plan {} status transition", previous.id));
    }
    Ok(())
}

fn validate_no_parent_cycle(id: EntityId, entities: &BTreeMap<EntityId, Entity>) -> Result<()> {
    let mut seen = BTreeSet::new();
    let mut current = Some(id);
    while let Some(entity_id) = current {
        if !seen.insert(entity_id) {
            return invalid(format!("entity parent cycle contains {}", entity_id.0));
        }
        current = entities.get(&entity_id).and_then(|entity| entity.parent);
    }
    Ok(())
}

fn invalid<T>(reason: String) -> Result<T> {
    Err(Error::InvalidModel(reason))
}
