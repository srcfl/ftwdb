use crate::{Entity, Plan, Point, Relation, Run, SeriesDefinition};

#[derive(Clone, Debug)]
pub(crate) enum Record {
    Entity(Entity),
    Relation(Relation),
    Series(SeriesDefinition),
    Run(Run),
    Plan(Plan),
    Points(Vec<Point>),
}

/// One atomic catalog-and-data commit.
#[derive(Clone, Debug, Default)]
pub struct Transaction {
    pub(crate) records: Vec<Record>,
}

impl Transaction {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            records: Vec::new(),
        }
    }

    pub fn upsert_entity(&mut self, entity: Entity) -> &mut Self {
        self.records.push(Record::Entity(entity));
        self
    }

    pub fn upsert_relation(&mut self, relation: Relation) -> &mut Self {
        self.records.push(Record::Relation(relation));
        self
    }

    pub fn define_series(&mut self, series: SeriesDefinition) -> &mut Self {
        self.records.push(Record::Series(series));
        self
    }

    pub fn upsert_run(&mut self, run: Run) -> &mut Self {
        self.records.push(Record::Run(run));
        self
    }

    pub fn upsert_plan(&mut self, plan: Plan) -> &mut Self {
        self.records.push(Record::Plan(plan));
        self
    }

    pub fn append_points(&mut self, points: impl Into<Vec<Point>>) -> &mut Self {
        self.records.push(Record::Points(points.into()));
        self
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    #[must_use]
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    #[must_use]
    pub fn point_count(&self) -> usize {
        self.records
            .iter()
            .map(|record| match record {
                Record::Points(points) => points.len(),
                _ => 0,
            })
            .sum()
    }
}
