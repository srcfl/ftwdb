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
    pub(crate) commit_id: Option<u128>,
}

impl Transaction {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            records: Vec::new(),
            commit_id: None,
        }
    }

    /// Tags this transaction with a client-supplied idempotency identifier.
    ///
    /// The identifier is stored inside the same durable frame as the
    /// transaction's records, so it survives crashes exactly when the data
    /// does. Committing a transaction whose identifier has already been
    /// durably committed writes nothing and reports
    /// [`Commit::deduplicated`](crate::Commit::deduplicated), which makes a
    /// retry after "error or crash after the durable write" safe: the points
    /// are stored exactly once. A `u128` fits a UUID; every value, including
    /// zero, is a valid identifier. Transactions without an identifier keep
    /// today's at-least-once behavior and are never deduplicated.
    pub fn with_commit_id(&mut self, commit_id: u128) -> &mut Self {
        self.commit_id = Some(commit_id);
        self
    }

    /// The idempotency identifier set by [`Transaction::with_commit_id`].
    #[must_use]
    pub const fn commit_id(&self) -> Option<u128> {
        self.commit_id
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
