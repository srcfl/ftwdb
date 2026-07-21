use crate::catalog::{validate_entity, validate_point_intervals, validate_run};
use crate::{
    CalendarUnit, Entity, EntityId, Error, GaugeBucket, Plan, PlanStatus, Point, Properties,
    PropertyValue, RelationId, Result, RollupPolicy, RollupResolution, RollupTier, Run, RunId,
    RunKind, RunStatus, SeriesDefinition, SeriesSemantics, Transaction,
};
use crc32fast::Hasher;
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

const SECOND: i64 = 1_000_000;
const MINUTE: i64 = 60 * SECOND;
const HOUR: i64 = 60 * MINUTE;
const DAY: i64 = 24 * HOUR;
const FIVE_MINUTES: i64 = 5 * MINUTE;

/// Workload v1 limits keep reads and writes bounded on small edge systems.
/// One site with 366 days at a 60-second cadence stays below these limits.
pub const MAX_WORKLOAD_BUNDLE_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_WORKLOAD_SITES: u32 = 256;
pub const MAX_WORKLOAD_DAYS: u32 = 366;
pub const MAX_WORKLOAD_POINTS: usize = 4_000_000;

const MAX_WORKLOAD_ENTITIES: usize = MAX_WORKLOAD_SITES as usize;
const MAX_WORKLOAD_SERIES: usize = MAX_WORKLOAD_ENTITIES * 9;
const MAX_WORKLOAD_RUNS: usize = MAX_WORKLOAD_ENTITIES * MAX_WORKLOAD_DAYS as usize * 2;
const MAX_WORKLOAD_PLANS: usize = MAX_WORKLOAD_ENTITIES * MAX_WORKLOAD_DAYS as usize;
const MAX_WORKLOAD_TEXT_BYTES: usize = 1_024;
const MAX_WORKLOAD_PROPERTIES: usize = 64;
const MAX_WORKLOAD_ROLLUP_TIERS: usize = 16;
const MAX_WORKLOAD_OBJECTIVE_TERMS: usize = 64;
const WORKLOAD_READER_BUFFER_BYTES: usize = 8 * 1_024;
const WORKLOAD_SCRATCH_BYTES: usize = MAX_WORKLOAD_TEXT_BYTES;
const MAX_WORKLOAD_SUMMARY_BYTES: u64 = 64 * 1_024;

const GRID_POWER: u64 = 1;
const SOLAR_POWER: u64 = 2;
const BATTERY_POWER: u64 = 3;
const STATE_OF_CHARGE: u64 = 4;
const IMPORT_ENERGY: u64 = 5;
const TEMPERATURE: u64 = 6;
const SPOT_PRICE: u64 = 7;
const LOAD_FORECAST: u64 = 8;
const PLANNED_BATTERY_POWER: u64 = 9;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkloadConfig {
    pub seed: u64,
    pub sites: u32,
    pub days: u32,
    pub cadence_seconds: u32,
    pub start_micros: i64,
}

fn expected_point_limit(config: WorkloadConfig) -> Result<usize> {
    let sites = u64::from(config.sites);
    let days = u64::from(config.days);
    let cadence = u64::from(config.cadence_seconds);
    let telemetry_steps = days
        .checked_mul(86_400)
        .and_then(|seconds| seconds.checked_div(cadence))
        .and_then(|steps| steps.checked_add(1))
        .ok_or(Error::InvalidConfig("workload point limit overflows"))?;
    let per_site = telemetry_steps
        .checked_mul(6)
        .and_then(|telemetry| {
            days.checked_mul(816)
                .and_then(|extra| telemetry.checked_add(extra))
        })
        .ok_or(Error::InvalidConfig("workload point limit overflows"))?;
    let points = sites
        .checked_mul(per_site)
        .ok_or(Error::InvalidConfig("workload point limit overflows"))?;
    usize::try_from(points).map_err(|_| Error::InvalidConfig("workload point limit overflows"))
}

fn check_count(name: &str, actual: usize, expected: usize, maximum: usize) -> Result<()> {
    if actual > maximum {
        return Err(Error::InvalidModel(format!(
            "workload {name} count exceeds its limit"
        )));
    }
    if actual != expected {
        return Err(Error::InvalidModel(format!(
            "workload has {actual} {name}; configuration requires {expected}"
        )));
    }
    Ok(())
}

fn validate_text(value: &str) -> Result<()> {
    if value.len() > MAX_WORKLOAD_TEXT_BYTES {
        return Err(Error::InvalidModel(
            "workload text exceeds the 1024-byte limit".to_owned(),
        ));
    }
    Ok(())
}

fn validate_properties(properties: &Properties) -> Result<()> {
    if properties.len() > MAX_WORKLOAD_PROPERTIES {
        return Err(Error::InvalidModel(
            "workload properties exceed the 64-entry limit".to_owned(),
        ));
    }
    for (name, value) in properties {
        validate_text(name)?;
        if let PropertyValue::Text(text) = value {
            validate_text(text)?;
        }
    }
    Ok(())
}

struct ChecksummedWriter<W> {
    inner: W,
    hasher: Hasher,
    written: u64,
    limit: u64,
}

impl<W> ChecksummedWriter<W> {
    fn new(inner: W, limit: u64) -> Self {
        Self {
            inner,
            hasher: Hasher::new(),
            written: 0,
            limit,
        }
    }

    fn checksum(&self) -> u32 {
        self.hasher.clone().finalize()
    }
}

impl<W: Write> Write for ChecksummedWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let remaining = self.limit.saturating_sub(self.written);
        if buffer.len() as u64 > remaining {
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                "workload bundle exceeds the 256 MiB writer limit",
            ));
        }
        let written = self.inner.write(buffer)?;
        self.hasher.update(&buffer[..written]);
        self.written += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

struct ChecksummedReader<R> {
    inner: R,
    hasher: Hasher,
}

impl<R> ChecksummedReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: Hasher::new(),
        }
    }

    fn checksum(&self) -> u32 {
        self.hasher.clone().finalize()
    }
}

impl<R: Read> Read for ChecksummedReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.hasher.update(&buffer[..read]);
        Ok(read)
    }
}

fn read_workload_from_reader<R: Read>(reader: R, expected_crc32: u32) -> Result<EnergyWorkload> {
    let reader = BufReader::with_capacity(WORKLOAD_READER_BUFFER_BYTES, reader);
    let reader = ChecksummedReader::new(reader);
    let mut scratch = [0_u8; WORKLOAD_SCRATCH_BYTES];
    let (workload, (mut reader, _)): (EnergyWorkload, _) =
        postcard::from_io((reader, &mut scratch))
            .map_err(|error| Error::Serialization(format!("workload decode failed: {error}")))?;
    let actual_crc32 = reader.checksum();
    let mut trailing = [0_u8; 1];
    if reader.read(&mut trailing)? != 0 {
        return Err(Error::Serialization(
            "workload bundle has trailing data".to_owned(),
        ));
    }
    if actual_crc32 != expected_crc32 {
        return Err(Error::Serialization(format!(
            "workload checksum mismatch: expected {expected_crc32:08x}, got {actual_crc32:08x}"
        )));
    }
    Ok(workload)
}

#[derive(Debug)]
struct BundleSummary {
    seed: u64,
    entities: usize,
    series: usize,
    runs: usize,
    plans: usize,
    points: usize,
    crc32: u32,
}

impl BundleSummary {
    fn check(&self, workload: &EnergyWorkload) -> Result<()> {
        let summary = workload.summary_with_crc(self.crc32);
        if self.seed != workload.config.seed
            || self.entities != summary.entities
            || self.series != summary.series
            || self.runs != summary.runs
            || self.plans != summary.plans
            || self.points != summary.points
        {
            return Err(Error::Serialization(
                "workload summary does not match the canonical bundle".to_owned(),
            ));
        }
        Ok(())
    }
}

fn read_bundle_summary(path: impl AsRef<Path>) -> Result<BundleSummary> {
    let path = path.as_ref();
    if path.metadata()?.len() > MAX_WORKLOAD_SUMMARY_BYTES {
        return Err(Error::Serialization(
            "workload summary exceeds 64 KiB".to_owned(),
        ));
    }
    let contents = std::fs::read_to_string(path)?;
    let mut values = BTreeMap::new();
    for line in contents.lines() {
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| Error::Serialization("workload summary line lacks '='".to_owned()))?;
        if values.insert(key, value).is_some() {
            return Err(Error::Serialization(
                "workload summary repeats a field".to_owned(),
            ));
        }
    }
    if values.get("format") != Some(&"ftwdb-energy-workload-v1") {
        return Err(Error::Serialization(
            "workload summary has an unsupported format".to_owned(),
        ));
    }
    fn decimal<T: std::str::FromStr>(
        values: &BTreeMap<&str, &str>,
        name: &'static str,
    ) -> Result<T> {
        values
            .get(name)
            .ok_or_else(|| Error::Serialization(format!("workload summary lacks {name}")))?
            .parse()
            .map_err(|_| Error::Serialization(format!("workload summary has invalid {name}")))
    }
    let crc = values
        .get("crc32")
        .ok_or_else(|| Error::Serialization("workload summary lacks crc32".to_owned()))?;
    let crc32 = u32::from_str_radix(crc, 16)
        .map_err(|_| Error::Serialization("workload summary has invalid crc32".to_owned()))?;
    Ok(BundleSummary {
        seed: decimal(&values, "seed")?,
        entities: decimal(&values, "entities")?,
        series: decimal(&values, "series")?,
        runs: decimal(&values, "runs")?,
        plans: decimal(&values, "plans")?,
        points: decimal(&values, "points")?,
        crc32,
    })
}

struct BoundedVec<T, const N: usize>(Vec<T>);

impl<'de, T: Deserialize<'de>, const N: usize> Deserialize<'de> for BoundedVec<T, N> {
    fn deserialize<D: de::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct BoundedVecVisitor<T, const N: usize>(std::marker::PhantomData<T>);

        impl<'de, T: Deserialize<'de>, const N: usize> Visitor<'de> for BoundedVecVisitor<T, N> {
            type Value = BoundedVec<T, N>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "a sequence with at most {N} entries")
            }

            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let length = sequence.size_hint().unwrap_or(0);
                if length > N {
                    return Err(de::Error::custom(
                        "workload sequence length exceeds its limit",
                    ));
                }
                let mut values = Vec::with_capacity(length);
                while let Some(value) = sequence.next_element()? {
                    if values.len() == N {
                        return Err(de::Error::custom(
                            "workload sequence length exceeds its limit",
                        ));
                    }
                    values.push(value);
                }
                Ok(BoundedVec(values))
            }
        }

        deserializer.deserialize_seq(BoundedVecVisitor(std::marker::PhantomData))
    }
}

struct BoundedMap<K, V, const N: usize>(BTreeMap<K, V>);

impl<'de, K, V, const N: usize> Deserialize<'de> for BoundedMap<K, V, N>
where
    K: Deserialize<'de> + Ord,
    V: Deserialize<'de>,
{
    fn deserialize<D: de::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct BoundedMapVisitor<K, V, const N: usize>(std::marker::PhantomData<(K, V)>);

        impl<'de, K, V, const N: usize> Visitor<'de> for BoundedMapVisitor<K, V, N>
        where
            K: Deserialize<'de> + Ord,
            V: Deserialize<'de>,
        {
            type Value = BoundedMap<K, V, N>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "a map with at most {N} entries")
            }

            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                if map.size_hint().is_some_and(|length| length > N) {
                    return Err(de::Error::custom("workload map length exceeds its limit"));
                }
                let mut values = BTreeMap::new();
                while let Some((key, value)) = map.next_entry()? {
                    if values.len() == N && !values.contains_key(&key) {
                        return Err(de::Error::custom("workload map length exceeds its limit"));
                    }
                    values.insert(key, value);
                }
                Ok(BoundedMap(values))
            }
        }

        deserializer.deserialize_map(BoundedMapVisitor(std::marker::PhantomData))
    }
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct BoundedString<const N: usize>(String);

impl<'de, const N: usize> Deserialize<'de> for BoundedString<N> {
    fn deserialize<D: de::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct BoundedStringVisitor<const N: usize>;

        impl<'de, const N: usize> Visitor<'de> for BoundedStringVisitor<N> {
            type Value = BoundedString<N>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "a string with at most {N} bytes")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
                if value.len() > N {
                    return Err(E::custom("workload string length exceeds its limit"));
                }
                Ok(BoundedString(value.to_owned()))
            }

            fn visit_string<E: de::Error>(
                self,
                value: String,
            ) -> std::result::Result<Self::Value, E> {
                if value.len() > N {
                    return Err(E::custom("workload string length exceeds its limit"));
                }
                Ok(BoundedString(value))
            }
        }

        deserializer.deserialize_string(BoundedStringVisitor)
    }
}

type WorkloadString = BoundedString<MAX_WORKLOAD_TEXT_BYTES>;
type WorkloadProperties = BoundedMap<WorkloadString, WirePropertyValue, MAX_WORKLOAD_PROPERTIES>;

#[derive(Deserialize)]
enum WirePropertyValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    Text(WorkloadString),
}

impl From<WirePropertyValue> for PropertyValue {
    fn from(value: WirePropertyValue) -> Self {
        match value {
            WirePropertyValue::Null => Self::Null,
            WirePropertyValue::Bool(value) => Self::Bool(value),
            WirePropertyValue::Integer(value) => Self::Integer(value),
            WirePropertyValue::Float(value) => Self::Float(value),
            WirePropertyValue::Text(value) => Self::Text(value.0),
        }
    }
}

fn into_properties(properties: WorkloadProperties) -> Properties {
    properties
        .0
        .into_iter()
        .map(|(key, value)| (key.0, value.into()))
        .collect()
}

#[derive(Deserialize)]
struct WireEntity {
    id: EntityId,
    kind: WorkloadString,
    name: WorkloadString,
    parent: Option<EntityId>,
    valid_from: i64,
    valid_to: Option<i64>,
    properties: WorkloadProperties,
}

impl From<WireEntity> for Entity {
    fn from(value: WireEntity) -> Self {
        Self {
            id: value.id,
            kind: value.kind.0,
            name: value.name.0,
            parent: value.parent,
            valid_from: value.valid_from,
            valid_to: value.valid_to,
            properties: into_properties(value.properties),
        }
    }
}

#[derive(Deserialize)]
enum WireRollupResolution {
    FixedMicros(i64),
    Calendar {
        unit: CalendarUnit,
        iana_timezone: WorkloadString,
    },
}

impl From<WireRollupResolution> for RollupResolution {
    fn from(value: WireRollupResolution) -> Self {
        match value {
            WireRollupResolution::FixedMicros(value) => Self::FixedMicros(value),
            WireRollupResolution::Calendar {
                unit,
                iana_timezone,
            } => Self::Calendar {
                unit,
                iana_timezone: iana_timezone.0,
            },
        }
    }
}

#[derive(Deserialize)]
struct WireRollupTier {
    resolution: WireRollupResolution,
    retain_for_micros: Option<i64>,
}

impl From<WireRollupTier> for RollupTier {
    fn from(value: WireRollupTier) -> Self {
        Self {
            resolution: value.resolution.into(),
            retain_for_micros: value.retain_for_micros,
        }
    }
}

#[derive(Deserialize)]
struct WireRollupPolicy {
    raw_retain_for_micros: Option<i64>,
    tiers: BoundedVec<WireRollupTier, MAX_WORKLOAD_ROLLUP_TIERS>,
}

impl From<WireRollupPolicy> for RollupPolicy {
    fn from(value: WireRollupPolicy) -> Self {
        Self {
            raw_retain_for_micros: value.raw_retain_for_micros,
            tiers: value.tiers.0.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Deserialize)]
struct WireSeriesDefinition {
    id: u64,
    owner_entity: Option<EntityId>,
    owner_relation: Option<RelationId>,
    name: WorkloadString,
    physical_quantity: WorkloadString,
    canonical_unit: WorkloadString,
    semantics: SeriesSemantics,
    maximum_gap_micros: Option<i64>,
    rollup_policy: WireRollupPolicy,
}

impl From<WireSeriesDefinition> for SeriesDefinition {
    fn from(value: WireSeriesDefinition) -> Self {
        Self {
            id: value.id,
            owner_entity: value.owner_entity,
            owner_relation: value.owner_relation,
            name: value.name.0,
            physical_quantity: value.physical_quantity.0,
            canonical_unit: value.canonical_unit.0,
            semantics: value.semantics,
            maximum_gap_micros: value.maximum_gap_micros,
            rollup_policy: value.rollup_policy.into(),
        }
    }
}

#[derive(Deserialize)]
struct WireRun {
    id: RunId,
    kind: RunKind,
    status: RunStatus,
    created_at: i64,
    knowledge_time: i64,
    workflow: WorkloadString,
    model: WorkloadString,
    model_version: WorkloadString,
    parent_run: Option<RunId>,
    input_snapshot: Option<RunId>,
    attributes: WorkloadProperties,
}

impl From<WireRun> for Run {
    fn from(value: WireRun) -> Self {
        Self {
            id: value.id,
            kind: value.kind,
            status: value.status,
            created_at: value.created_at,
            knowledge_time: value.knowledge_time,
            workflow: value.workflow.0,
            model: value.model.0,
            model_version: value.model_version.0,
            parent_run: value.parent_run,
            input_snapshot: value.input_snapshot,
            attributes: into_properties(value.attributes),
        }
    }
}

#[derive(Deserialize)]
struct WirePlan {
    id: u128,
    run_id: RunId,
    status: PlanStatus,
    horizon_start: i64,
    horizon_end: i64,
    resolution_micros: i64,
    scenario: WorkloadString,
    objective_terms: BoundedMap<WorkloadString, f64, MAX_WORKLOAD_OBJECTIVE_TERMS>,
    objective_value: Option<f64>,
    supersedes: Option<u128>,
    attributes: WorkloadProperties,
}

impl From<WirePlan> for Plan {
    fn from(value: WirePlan) -> Self {
        Self {
            id: value.id,
            run_id: value.run_id,
            status: value.status,
            horizon_start: value.horizon_start,
            horizon_end: value.horizon_end,
            resolution_micros: value.resolution_micros,
            scenario: value.scenario.0,
            objective_terms: value
                .objective_terms
                .0
                .into_iter()
                .map(|(name, value)| (name.0, value))
                .collect(),
            objective_value: value.objective_value,
            supersedes: value.supersedes,
            attributes: into_properties(value.attributes),
        }
    }
}

#[derive(Deserialize)]
struct WireWorkload {
    config: WorkloadConfig,
    entities: BoundedVec<WireEntity, MAX_WORKLOAD_ENTITIES>,
    series: BoundedVec<WireSeriesDefinition, MAX_WORKLOAD_SERIES>,
    runs: BoundedVec<WireRun, MAX_WORKLOAD_RUNS>,
    plans: BoundedVec<WirePlan, MAX_WORKLOAD_PLANS>,
    points: BoundedVec<Point, MAX_WORKLOAD_POINTS>,
}

impl<'de> Deserialize<'de> for EnergyWorkload {
    fn deserialize<D: de::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let wire = WireWorkload::deserialize(deserializer)?;
        let workload = Self {
            config: wire.config,
            entities: wire.entities.0.into_iter().map(Into::into).collect(),
            series: wire.series.0.into_iter().map(Into::into).collect(),
            runs: wire.runs.0.into_iter().map(Into::into).collect(),
            plans: wire.plans.0.into_iter().map(Into::into).collect(),
            points: wire.points.0,
        };
        workload
            .validate_contract()
            .map_err(|error| de::Error::custom(error.to_string()))?;
        Ok(workload)
    }
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            seed: 0x5741_5454_4442,
            sites: 1,
            days: 7,
            cadence_seconds: 60,
            // 2026-01-01T00:00:00Z. A year-long run crosses both Stockholm
            // DST boundaries while remaining deterministic on every host.
            start_micros: 1_767_225_600 * SECOND,
        }
    }
}

impl WorkloadConfig {
    pub fn validate(self) -> Result<()> {
        if self.sites == 0 || self.days == 0 || self.cadence_seconds == 0 {
            return Err(Error::InvalidConfig(
                "workload sites, days, and cadence must be positive",
            ));
        }
        if i64::from(self.cadence_seconds) * SECOND > DAY {
            return Err(Error::InvalidConfig(
                "workload cadence must not exceed one day",
            ));
        }
        if self.sites > MAX_WORKLOAD_SITES {
            return Err(Error::InvalidConfig("workload exceeds the 256-site limit"));
        }
        if self.days > MAX_WORKLOAD_DAYS {
            return Err(Error::InvalidConfig("workload exceeds the 366-day limit"));
        }
        let _ = self
            .start_micros
            .checked_add(i64::from(self.days) * DAY)
            .ok_or(Error::InvalidConfig("workload timestamp range overflows"))?;
        if expected_point_limit(self)? > MAX_WORKLOAD_POINTS {
            return Err(Error::InvalidConfig(
                "workload exceeds the four-million-point limit",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkloadSummary {
    pub entities: usize,
    pub series: usize,
    pub runs: usize,
    pub plans: usize,
    pub points: usize,
    pub crc32: u32,
}

/// A deterministic, engine-neutral energy workload. It intentionally combines
/// telemetry and non-TSDB records so comparisons cannot silently omit the
/// forecast/optimization product model.
#[derive(Clone, Debug, Serialize)]
pub struct EnergyWorkload {
    pub config: WorkloadConfig,
    pub entities: Vec<Entity>,
    pub series: Vec<SeriesDefinition>,
    pub runs: Vec<Run>,
    pub plans: Vec<Plan>,
    pub points: Vec<Point>,
}

impl EnergyWorkload {
    pub fn generate(config: WorkloadConfig) -> Result<Self> {
        config.validate()?;
        let mut workload = Self {
            config,
            entities: Vec::new(),
            series: Vec::new(),
            runs: Vec::new(),
            plans: Vec::new(),
            points: Vec::new(),
        };
        let mut random = SplitMix64(config.seed);
        for site in 0..config.sites {
            workload.add_site(site, &mut random)?;
        }
        workload.points.sort_by_key(|point| {
            (
                point.valid_time,
                point.series_id,
                point.knowledge_time,
                point.change_time,
            )
        });
        workload.validate_contract()?;
        Ok(workload)
    }

    #[must_use]
    pub fn summary(&self) -> WorkloadSummary {
        WorkloadSummary {
            entities: self.entities.len(),
            series: self.series.len(),
            runs: self.runs.len(),
            plans: self.plans.len(),
            points: self.points.len(),
            crc32: self.checksum(),
        }
    }

    /// Catalog, provenance, and plans in dependency-safe record order.
    #[must_use]
    pub fn metadata_transaction(&self) -> Transaction {
        let mut transaction = Transaction::new();
        for entity in &self.entities {
            transaction.upsert_entity(entity.clone());
        }
        for series in &self.series {
            transaction.define_series(series.clone());
        }
        for run in &self.runs {
            transaction.upsert_run(run.clone());
        }
        for plan in &self.plans {
            transaction.upsert_plan(plan.clone());
        }
        transaction
    }

    pub fn point_transactions(&self, batch_points: usize) -> Result<Vec<Transaction>> {
        if batch_points == 0 {
            return Err(Error::InvalidConfig(
                "workload transaction batch must be positive",
            ));
        }
        Ok(self
            .points
            .chunks(batch_points)
            .map(|points| {
                let mut transaction = Transaction::new();
                transaction.append_points(points.to_vec());
                transaction
            })
            .collect())
    }

    /// Writes a portable CSV bundle for server adapters plus a binary canonical
    /// snapshot whose CRC is the dataset identity.
    pub fn write_bundle(&self, directory: impl AsRef<Path>) -> Result<WorkloadSummary> {
        self.validate_contract()?;
        let directory = directory.as_ref();
        std::fs::create_dir_all(directory)?;
        write_entities(directory.join("entities.csv"), &self.entities)?;
        write_series(directory.join("series.csv"), &self.series)?;
        write_runs(directory.join("runs.csv"), &self.runs)?;
        write_plans(directory.join("plans.csv"), &self.plans)?;
        write_points(directory.join("points.csv"), &self.points)?;
        let file = File::create(directory.join("workload.postcard"))?;
        let writer = BufWriter::new(file);
        let mut writer = ChecksummedWriter::new(writer, MAX_WORKLOAD_BUNDLE_BYTES);
        postcard::to_io(self, &mut writer)
            .map_err(|error| Error::Serialization(format!("workload encode failed: {error}")))?;
        writer.flush()?;
        writer.inner.get_ref().sync_all()?;
        let summary = self.summary_with_crc(writer.checksum());
        let mut summary_file = BufWriter::new(File::create(directory.join("summary.txt"))?);
        writeln!(summary_file, "format=ftwdb-energy-workload-v1")?;
        writeln!(summary_file, "seed={}", self.config.seed)?;
        writeln!(summary_file, "entities={}", summary.entities)?;
        writeln!(summary_file, "series={}", summary.series)?;
        writeln!(summary_file, "runs={}", summary.runs)?;
        writeln!(summary_file, "plans={}", summary.plans)?;
        writeln!(summary_file, "points={}", summary.points)?;
        writeln!(summary_file, "crc32={:08x}", summary.crc32)?;
        summary_file.flush()?;
        Ok(summary)
    }

    pub fn read_bundle(directory: impl AsRef<Path>) -> Result<Self> {
        let directory = directory.as_ref();
        let path = directory.join("workload.postcard");
        let metadata = path.metadata()?;
        if metadata.len() > MAX_WORKLOAD_BUNDLE_BYTES {
            return Err(Error::InvalidConfig(
                "workload bundle exceeds the 256 MiB reader limit",
            ));
        }
        let expected = read_bundle_summary(directory.join("summary.txt"))?;
        let workload = read_workload_from_reader(File::open(path)?, expected.crc32)?;
        expected.check(&workload)?;
        Ok(workload)
    }

    #[must_use]
    pub fn checksum(&self) -> u32 {
        let mut writer = ChecksummedWriter::new(std::io::sink(), MAX_WORKLOAD_BUNDLE_BYTES);
        postcard::to_io(self, &mut writer).expect("valid workload model is serializable");
        writer.checksum()
    }

    fn summary_with_crc(&self, crc32: u32) -> WorkloadSummary {
        WorkloadSummary {
            entities: self.entities.len(),
            series: self.series.len(),
            runs: self.runs.len(),
            plans: self.plans.len(),
            points: self.points.len(),
            crc32,
        }
    }

    fn validate_contract(&self) -> Result<()> {
        self.config.validate()?;
        let sites = self.config.sites as usize;
        let days = self.config.days as usize;
        check_count(
            "entities",
            self.entities.len(),
            sites,
            MAX_WORKLOAD_ENTITIES,
        )?;
        check_count("series", self.series.len(), sites * 9, MAX_WORKLOAD_SERIES)?;
        check_count("runs", self.runs.len(), sites * days * 2, MAX_WORKLOAD_RUNS)?;
        check_count("plans", self.plans.len(), sites * days, MAX_WORKLOAD_PLANS)?;
        if self.points.len() > expected_point_limit(self.config)? {
            return Err(Error::InvalidModel(
                "workload point count exceeds its configuration limit".to_owned(),
            ));
        }

        let mut entity_ids = BTreeSet::new();
        for entity in &self.entities {
            validate_entity(entity)?;
            if !entity_ids.insert(entity.id) {
                return Err(Error::InvalidModel(format!(
                    "workload repeats entity {}",
                    entity.id.0
                )));
            }
            validate_text(&entity.kind)?;
            validate_text(&entity.name)?;
            validate_properties(&entity.properties)?;
        }
        for entity in &self.entities {
            if entity
                .parent
                .is_some_and(|parent| !entity_ids.contains(&parent))
            {
                return Err(Error::InvalidModel(format!(
                    "workload entity {} refers to a missing parent",
                    entity.id.0
                )));
            }
        }

        let mut series_ids = BTreeSet::new();
        for series in &self.series {
            series.validate().map_err(|reason| {
                Error::InvalidModel(format!("invalid workload series: {reason}"))
            })?;
            if !series_ids.insert(series.id) {
                return Err(Error::InvalidModel(format!(
                    "workload repeats series {}",
                    series.id
                )));
            }
            if series
                .owner_entity
                .is_none_or(|owner| !entity_ids.contains(&owner))
                || series.owner_relation.is_some()
            {
                return Err(Error::InvalidModel(format!(
                    "workload series {} has a missing owner",
                    series.id
                )));
            }
            validate_text(&series.name)?;
            validate_text(&series.physical_quantity)?;
            validate_text(&series.canonical_unit)?;
            if series.rollup_policy.tiers.len() > MAX_WORKLOAD_ROLLUP_TIERS {
                return Err(Error::InvalidModel(
                    "workload series has too many rollup tiers".to_owned(),
                ));
            }
            for tier in &series.rollup_policy.tiers {
                if let RollupResolution::Calendar { iana_timezone, .. } = &tier.resolution {
                    validate_text(iana_timezone)?;
                }
            }
        }
        let mut run_kinds = BTreeMap::new();
        for run in &self.runs {
            validate_run(run)?;
            if run_kinds.insert(run.id, run.kind).is_some() {
                return Err(Error::InvalidModel(format!(
                    "workload repeats run {}",
                    run.id.0
                )));
            }
            validate_text(&run.workflow)?;
            validate_text(&run.model)?;
            validate_text(&run.model_version)?;
            validate_properties(&run.attributes)?;
        }
        for run in &self.runs {
            if run
                .parent_run
                .is_some_and(|parent| !run_kinds.contains_key(&parent))
                || run
                    .input_snapshot
                    .is_some_and(|input| !run_kinds.contains_key(&input))
            {
                return Err(Error::InvalidModel(format!(
                    "workload run {} has missing provenance",
                    run.id.0
                )));
            }
        }

        let mut plan_ids = BTreeSet::new();
        for plan in &self.plans {
            plan.validate().map_err(|reason| {
                Error::InvalidModel(format!("invalid workload plan: {reason}"))
            })?;
            if !plan_ids.insert(plan.id) {
                return Err(Error::InvalidModel(format!(
                    "workload repeats plan {}",
                    plan.id
                )));
            }
            if run_kinds.get(&plan.run_id) != Some(&RunKind::Optimization) {
                return Err(Error::InvalidModel(format!(
                    "workload plan {} must refer to an optimization run",
                    plan.id
                )));
            }
            validate_text(&plan.scenario)?;
            if plan.objective_terms.len() > MAX_WORKLOAD_OBJECTIVE_TERMS {
                return Err(Error::InvalidModel(
                    "workload plan has too many objective terms".to_owned(),
                ));
            }
            for name in plan.objective_terms.keys() {
                validate_text(name)?;
            }
            validate_properties(&plan.attributes)?;
        }
        for plan in &self.plans {
            if plan
                .supersedes
                .is_some_and(|previous| !plan_ids.contains(&previous))
            {
                return Err(Error::InvalidModel(format!(
                    "workload plan {} supersedes a missing plan",
                    plan.id
                )));
            }
        }

        validate_point_intervals(&self.points)?;
        for point in &self.points {
            if !series_ids.contains(&point.series_id) {
                return Err(Error::InvalidModel(format!(
                    "workload point refers to undefined series {}",
                    point.series_id
                )));
            }
            if point.run_id != 0 && !run_kinds.contains_key(&RunId(point.run_id)) {
                return Err(Error::InvalidModel(format!(
                    "workload point refers to missing run {}",
                    point.run_id
                )));
            }
        }
        Ok(())
    }

    fn add_site(&mut self, site: u32, random: &mut SplitMix64) -> Result<()> {
        let entity_id = EntityId(u128::from(site) + 1);
        let mut properties = Properties::new();
        properties.insert(
            "timezone".to_owned(),
            PropertyValue::Text("Europe/Stockholm".to_owned()),
        );
        properties.insert(
            "benchmark_site".to_owned(),
            PropertyValue::Integer(i64::from(site)),
        );
        self.entities.push(Entity {
            id: entity_id,
            kind: "site".to_owned(),
            name: format!("site-{site:05}"),
            parent: None,
            valid_from: self.config.start_micros,
            valid_to: None,
            properties,
        });
        self.add_series(site, entity_id);
        self.add_telemetry(site, random)?;
        self.add_forecasts_and_plans(site, random)?;
        Ok(())
    }

    fn add_series(&mut self, site: u32, owner: EntityId) {
        let gauge_policy = || RollupPolicy {
            raw_retain_for_micros: Some(14 * DAY),
            tiers: vec![
                RollupTier {
                    resolution: RollupResolution::FixedMicros(5 * MINUTE),
                    retain_for_micros: Some(180 * DAY),
                },
                RollupTier {
                    resolution: RollupResolution::FixedMicros(30 * MINUTE),
                    retain_for_micros: None,
                },
                RollupTier {
                    resolution: RollupResolution::Calendar {
                        unit: CalendarUnit::Day,
                        iana_timezone: "Europe/Stockholm".to_owned(),
                    },
                    retain_for_micros: None,
                },
                RollupTier {
                    resolution: RollupResolution::Calendar {
                        unit: CalendarUnit::Month,
                        iana_timezone: "Europe/Stockholm".to_owned(),
                    },
                    retain_for_micros: None,
                },
            ],
        };
        let definitions = [
            (
                GRID_POWER,
                "grid_power",
                "power",
                "W",
                SeriesSemantics::Gauge,
            ),
            (
                SOLAR_POWER,
                "solar_power",
                "power",
                "W",
                SeriesSemantics::Gauge,
            ),
            (
                BATTERY_POWER,
                "battery_power",
                "power",
                "W",
                SeriesSemantics::Gauge,
            ),
            (
                STATE_OF_CHARGE,
                "state_of_charge",
                "state_of_charge",
                "%",
                SeriesSemantics::Gauge,
            ),
            (
                IMPORT_ENERGY,
                "import_energy",
                "energy",
                "Wh",
                SeriesSemantics::Counter,
            ),
            (
                TEMPERATURE,
                "outdoor_temperature",
                "temperature",
                "Cel",
                SeriesSemantics::Gauge,
            ),
            (
                SPOT_PRICE,
                "spot_price",
                "energy_price",
                "EUR/MWh",
                SeriesSemantics::Gauge,
            ),
            (
                LOAD_FORECAST,
                "load_forecast",
                "power",
                "W",
                SeriesSemantics::Gauge,
            ),
            (
                PLANNED_BATTERY_POWER,
                "planned_battery_power",
                "power",
                "W",
                SeriesSemantics::Gauge,
            ),
        ];
        for (offset, name, quantity, unit, semantics) in definitions {
            self.series.push(SeriesDefinition {
                id: series_id(site, offset),
                owner_entity: Some(owner),
                owner_relation: None,
                name: name.to_owned(),
                physical_quantity: quantity.to_owned(),
                canonical_unit: unit.to_owned(),
                semantics,
                maximum_gap_micros: Some(3 * i64::from(self.config.cadence_seconds) * SECOND),
                rollup_policy: if semantics == SeriesSemantics::Gauge {
                    gauge_policy()
                } else {
                    RollupPolicy {
                        raw_retain_for_micros: Some(90 * DAY),
                        tiers: Vec::new(),
                    }
                },
            });
        }
    }

    fn add_telemetry(&mut self, site: u32, random: &mut SplitMix64) -> Result<()> {
        let cadence = i64::from(self.config.cadence_seconds) * SECOND;
        let count = i64::from(self.config.days) * DAY / cadence;
        let site_scale = 1.0 + f64::from(site) * 0.03;
        let mut state_of_charge = 52.0 + random.signed() * 5.0;
        let mut import_energy = 100_000.0 + f64::from(site) * 10_000.0;
        for index in 0..=count {
            let valid_time = self
                .config
                .start_micros
                .checked_add(index.saturating_mul(cadence))
                .ok_or_else(|| Error::InvalidModel("workload timestamp overflow".to_owned()))?;
            let phase = (index.saturating_mul(cadence)).rem_euclid(DAY) as f64 / DAY as f64;
            let daily = (phase * std::f64::consts::TAU).sin();
            let solar_shape = (std::f64::consts::PI * ((phase - 0.25) / 0.5))
                .sin()
                .max(0.0);
            let temperature = 7.0 + 8.0 * daily + random.signed() * 0.4;
            let demand = site_scale
                * (4_200.0 + 1_300.0 * (-daily) + (18.0 - temperature).max(0.0) * 90.0)
                + random.signed() * 120.0;
            let solar = site_scale * 5_500.0 * solar_shape * (0.85 + random.unit() * 0.15);
            let hour = phase * 24.0;
            let battery = if (0.0..5.0).contains(&hour) {
                1_600.0
            } else if (17.0..21.0).contains(&hour) {
                -2_000.0
            } else {
                0.0
            };
            let grid = demand - solar + battery;
            state_of_charge = (state_of_charge
                + battery * cadence as f64 / 3_600_000_000.0 / 120.0)
                .clamp(5.0, 95.0);
            import_energy += grid.max(0.0) * cadence as f64 / 3_600_000_000.0;
            if index == count / 2 {
                import_energy = 0.0;
            }

            let gap = index % 997 >= 500 && index % 997 < 504;
            if !gap {
                let quality = u32::from(index % 4_093 == 0);
                let flags = u32::from(index == count / 2);
                for (offset, value) in [
                    (GRID_POWER, grid),
                    (SOLAR_POWER, solar),
                    (BATTERY_POWER, battery),
                    (STATE_OF_CHARGE, state_of_charge),
                    (IMPORT_ENERGY, import_energy),
                    (TEMPERATURE, temperature),
                ] {
                    let mut point = Point::actual(series_id(site, offset), valid_time, value);
                    point.quality = quality;
                    point.flags = flags;
                    self.points.push(point);
                }
            }
            if index < count && index.saturating_mul(cadence).rem_euclid(HOUR) == 0 {
                let price =
                    35.0 + if (17.0..21.0).contains(&hour) {
                        85.0
                    } else {
                        0.0
                    } + random.signed() * 4.0;
                let mut point = Point::actual(series_id(site, SPOT_PRICE), valid_time, price);
                point.valid_time_end = valid_time + HOUR;
                self.points.push(point);
            }
        }
        Ok(())
    }

    fn add_forecasts_and_plans(&mut self, site: u32, random: &mut SplitMix64) -> Result<()> {
        for day in 0..self.config.days {
            let day_start = self.config.start_micros + i64::from(day) * DAY;
            let issue_time = day_start - 12 * HOUR;
            let revision_time = day_start + 6 * HOUR;
            let forecast_run = run_id(site, day, 1);
            let optimization_run = run_id(site, day, 2);
            self.runs.push(Run {
                id: forecast_run,
                kind: RunKind::Forecast,
                status: RunStatus::Succeeded,
                created_at: issue_time,
                knowledge_time: issue_time,
                workflow: "day_ahead_forecast".to_owned(),
                model: "deterministic-benchmark".to_owned(),
                model_version: "1".to_owned(),
                parent_run: None,
                input_snapshot: None,
                attributes: BTreeMap::new(),
            });
            self.runs.push(Run {
                id: optimization_run,
                kind: RunKind::Optimization,
                status: RunStatus::Succeeded,
                created_at: issue_time + MINUTE,
                knowledge_time: issue_time,
                workflow: "battery_dispatch".to_owned(),
                model: "price-peak-arbitrage".to_owned(),
                model_version: "1".to_owned(),
                parent_run: None,
                input_snapshot: Some(forecast_run),
                attributes: BTreeMap::new(),
            });
            let plan_id = plan_id(site, day);
            self.plans.push(Plan {
                id: plan_id,
                run_id: optimization_run,
                status: PlanStatus::Deployed,
                horizon_start: day_start,
                horizon_end: day_start + DAY,
                resolution_micros: FIVE_MINUTES,
                scenario: "base".to_owned(),
                objective_terms: BTreeMap::from([
                    ("energy_cost_eur".to_owned(), 42.0 + random.unit() * 5.0),
                    ("peak_kw".to_owned(), 6.0 + random.unit()),
                ]),
                objective_value: Some(48.0 + random.unit() * 6.0),
                supersedes: None,
                attributes: BTreeMap::new(),
            });

            for step in 0..(DAY / FIVE_MINUTES) {
                let valid_time = day_start + step * FIVE_MINUTES;
                let phase = (step * FIVE_MINUTES) as f64 / DAY as f64;
                let daily = (phase * std::f64::consts::TAU).sin();
                let base = (4_200.0 + 1_300.0 * (-daily)) * (1.0 + f64::from(site) * 0.03);
                let first_value = base + random.signed() * 450.0;
                let revised_value = base + random.signed() * 180.0;
                let mut forecast = Point {
                    series_id: series_id(site, LOAD_FORECAST),
                    valid_time,
                    valid_time_end: valid_time,
                    knowledge_time: issue_time,
                    change_time: issue_time,
                    run_id: forecast_run.0,
                    value: first_value,
                    quality: 0,
                    flags: 0,
                };
                self.points.push(forecast);
                if valid_time >= revision_time {
                    forecast.knowledge_time = revision_time;
                    forecast.change_time = revision_time;
                    forecast.value = revised_value;
                    self.points.push(forecast);
                }

                let hour = phase * 24.0;
                let planned = if (0.0..5.0).contains(&hour) {
                    1_600.0
                } else if (17.0..21.0).contains(&hour) {
                    -2_000.0
                } else {
                    0.0
                };
                self.points.push(Point {
                    series_id: series_id(site, PLANNED_BATTERY_POWER),
                    valid_time,
                    valid_time_end: valid_time + FIVE_MINUTES,
                    knowledge_time: issue_time,
                    change_time: issue_time + MINUTE,
                    run_id: optimization_run.0,
                    value: planned,
                    quality: 0,
                    flags: 0,
                });
            }
        }
        Ok(())
    }
}

/// Stable result identity used to reject benchmark runs whose aggregate values
/// differ even when their latency looks attractive.
#[must_use]
pub fn gauge_bucket_checksum(buckets: &[GaugeBucket]) -> u32 {
    let mut hasher = Hasher::new();
    for bucket in buckets {
        hasher.update(&bucket.start.to_le_bytes());
        hasher.update(&bucket.end.to_le_bytes());
        hasher.update(&bucket.count.to_le_bytes());
        hasher.update(&bucket.sum.to_bits().to_le_bytes());
        hasher.update(
            &bucket
                .min
                .map(f64::to_bits)
                .unwrap_or_default()
                .to_le_bytes(),
        );
        hasher.update(
            &bucket
                .max
                .map(f64::to_bits)
                .unwrap_or_default()
                .to_le_bytes(),
        );
        hasher.update(&bucket.integral_value_micros.to_bits().to_le_bytes());
        hasher.update(&bucket.covered_micros.to_le_bytes());
    }
    hasher.finalize()
}

fn series_id(site: u32, offset: u64) -> u64 {
    u64::from(site) * 100 + offset
}

fn run_id(site: u32, day: u32, kind: u64) -> RunId {
    RunId((u128::from(site) + 1) << 64 | u128::from(day) << 8 | u128::from(kind))
}

fn plan_id(site: u32, day: u32) -> u128 {
    (u128::from(site) + 1) << 64 | u128::from(day) << 8 | 0x80
}

struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 * (1.0 / (1_u64 << 53) as f64)
    }

    fn signed(&mut self) -> f64 {
        self.unit() * 2.0 - 1.0
    }
}

fn write_entities(path: impl AsRef<Path>, entities: &[Entity]) -> Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(writer, "entity_id,kind,name,parent_id,valid_from,valid_to")?;
    for entity in entities {
        writeln!(
            writer,
            "{},{},{},{},{},{}",
            entity.id.0,
            csv(&entity.kind),
            csv(&entity.name),
            entity.parent.map_or(String::new(), |id| id.0.to_string()),
            entity.valid_from,
            entity
                .valid_to
                .map_or(String::new(), |value| value.to_string())
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn write_series(path: impl AsRef<Path>, series: &[SeriesDefinition]) -> Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(
        writer,
        "series_id,owner_entity,name,physical_quantity,canonical_unit,semantics"
    )?;
    for definition in series {
        writeln!(
            writer,
            "{},{},{},{},{},{:?}",
            definition.id,
            definition.owner_entity.map_or(0, |id| id.0),
            csv(&definition.name),
            csv(&definition.physical_quantity),
            csv(&definition.canonical_unit),
            definition.semantics
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn write_runs(path: impl AsRef<Path>, runs: &[Run]) -> Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(
        writer,
        "run_id,kind,status,created_at,knowledge_time,workflow,model,model_version,input_snapshot"
    )?;
    for run in runs {
        writeln!(
            writer,
            "{},{:?},{:?},{},{},{},{},{},{}",
            run.id.0,
            run.kind,
            run.status,
            run.created_at,
            run.knowledge_time,
            csv(&run.workflow),
            csv(&run.model),
            csv(&run.model_version),
            run.input_snapshot
                .map_or(String::new(), |id| id.0.to_string())
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn write_plans(path: impl AsRef<Path>, plans: &[Plan]) -> Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(
        writer,
        "plan_id,run_id,status,horizon_start,horizon_end,resolution_micros,scenario,objective_value"
    )?;
    for plan in plans {
        writeln!(
            writer,
            "{},{},{:?},{},{},{},{},{}",
            plan.id,
            plan.run_id.0,
            plan.status,
            plan.horizon_start,
            plan.horizon_end,
            plan.resolution_micros,
            csv(&plan.scenario),
            plan.objective_value
                .map_or(String::new(), |value| value.to_string())
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn write_points(path: impl AsRef<Path>, points: &[Point]) -> Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(
        writer,
        "series_id,valid_time,valid_time_end,knowledge_time,change_time,run_id,value,quality,flags"
    )?;
    for point in points {
        writeln!(
            writer,
            "{},{},{},{},{},{},{:.17},{},{}",
            point.series_id,
            point.valid_time,
            point.valid_time_end,
            point.knowledge_time,
            point.change_time,
            point.run_id,
            point.value,
            point.quality,
            point.flags
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn csv(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EnergyWorkload, MAX_WORKLOAD_BUNDLE_BYTES, MAX_WORKLOAD_DAYS, MAX_WORKLOAD_ENTITIES,
        WorkloadConfig, read_workload_from_reader,
    };
    use crate::{EntityId, Error, RunKind, SeriesSemantics, Store};
    use std::io::{Read, Seek, SeekFrom, Write};
    use tempfile::tempdir;

    struct ChunkedReader<R> {
        inner: R,
        maximum: usize,
    }

    impl<R: Read> Read for ChunkedReader<R> {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            let length = buffer.len().min(self.maximum);
            self.inner.read(&mut buffer[..length])
        }
    }

    fn append_varint(output: &mut Vec<u8>, mut value: usize) {
        while value >= 0x80 {
            output.push((value as u8 & 0x7f) | 0x80);
            value >>= 7;
        }
        output.push(value as u8);
    }

    #[test]
    fn same_seed_produces_identical_mixed_workload() {
        let config = WorkloadConfig {
            days: 1,
            cadence_seconds: 300,
            ..WorkloadConfig::default()
        };
        let left = EnergyWorkload::generate(config).unwrap();
        let right = EnergyWorkload::generate(config).unwrap();

        assert_eq!(left.checksum(), right.checksum());
        assert!(
            left.series
                .iter()
                .any(|series| series.semantics == SeriesSemantics::Counter)
        );
        assert!(
            left.runs
                .iter()
                .any(|run| run.kind == RunKind::Optimization)
        );
        assert!(!left.plans.is_empty());
        assert!(left.points.iter().any(|point| point.run_id != 0));
        assert!(left.points.windows(2).any(|pair| {
            pair[0].series_id == pair[1].series_id
                && pair[0].valid_time == pair[1].valid_time
                && pair[0].knowledge_time != pair[1].knowledge_time
        }));
    }

    #[test]
    fn workload_round_trips_through_store_in_batches() {
        let workload = EnergyWorkload::generate(WorkloadConfig {
            days: 1,
            cadence_seconds: 900,
            ..WorkloadConfig::default()
        })
        .unwrap();
        let directory = tempdir().unwrap();
        let mut store = Store::open(directory.path()).unwrap();
        store.commit(workload.metadata_transaction()).unwrap();
        for transaction in workload.point_transactions(1_000).unwrap() {
            store.commit(transaction).unwrap();
        }
        assert_eq!(
            store.database().stats().unwrap().points as usize,
            workload.points.len()
        );
        assert_eq!(
            store.database().catalog().plans().len(),
            workload.plans.len()
        );
    }

    #[test]
    fn portable_bundle_has_stable_identity_and_all_tables() {
        let workload = EnergyWorkload::generate(WorkloadConfig {
            days: 1,
            cadence_seconds: 3_600,
            ..WorkloadConfig::default()
        })
        .unwrap();
        let directory = tempdir().unwrap();
        let summary = workload.write_bundle(directory.path()).unwrap();

        assert_eq!(summary.crc32, workload.checksum());
        for file in [
            "entities.csv",
            "series.csv",
            "runs.csv",
            "plans.csv",
            "points.csv",
            "workload.postcard",
            "summary.txt",
        ] {
            assert!(directory.path().join(file).metadata().unwrap().len() > 0);
        }
    }

    #[test]
    fn v1_bundle_streams_through_one_byte_reader_chunks() {
        let workload = EnergyWorkload::generate(WorkloadConfig {
            seed: 95_961_480_299_586,
            days: 1,
            cadence_seconds: 3_600,
            ..WorkloadConfig::default()
        })
        .unwrap();
        // The old writer used this whole-model allocation. Test-only bytes
        // prove that the streaming writer keeps the v1 layout on each host.
        let legacy = postcard::to_stdvec(&workload).unwrap();
        let legacy_crc = crc32fast::hash(&legacy);
        assert_eq!(workload.checksum(), legacy_crc);
        let directory = tempdir().unwrap();
        workload.write_bundle(directory.path()).unwrap();
        let path = directory.path().join("workload.postcard");
        assert_eq!(std::fs::read(&path).unwrap(), legacy);
        let file = std::fs::File::open(path).unwrap();
        let decoded = read_workload_from_reader(
            ChunkedReader {
                inner: file,
                maximum: 1,
            },
            legacy_crc,
        )
        .unwrap();

        assert_eq!(decoded.config, workload.config);
        assert_eq!(decoded.summary(), workload.summary());
    }

    #[test]
    fn sparse_bundle_over_file_limit_is_rejected_before_decode() {
        let directory = tempdir().unwrap();
        let file = std::fs::File::create(directory.path().join("workload.postcard")).unwrap();
        file.set_len(MAX_WORKLOAD_BUNDLE_BYTES + 1).unwrap();
        drop(file);

        assert!(matches!(
            EnergyWorkload::read_bundle(directory.path()),
            Err(Error::InvalidConfig(_))
        ));
    }

    #[test]
    fn malicious_collection_length_and_varint_are_rejected() {
        let mut oversized = Vec::new();
        postcard::to_io(&WorkloadConfig::default(), &mut oversized).unwrap();
        append_varint(&mut oversized, MAX_WORKLOAD_ENTITIES + 1);
        let oversized_crc = crc32fast::hash(&oversized);
        assert!(matches!(
            read_workload_from_reader(std::io::Cursor::new(oversized), oversized_crc),
            Err(Error::Serialization(_))
        ));

        let mut bad_varint = Vec::new();
        postcard::to_io(&WorkloadConfig::default(), &mut bad_varint).unwrap();
        bad_varint.extend_from_slice(&[0x80; 12]);
        let bad_varint_crc = crc32fast::hash(&bad_varint);
        assert!(matches!(
            read_workload_from_reader(std::io::Cursor::new(bad_varint), bad_varint_crc),
            Err(Error::Serialization(_))
        ));

        let mut oversized_points = Vec::new();
        postcard::to_io(&WorkloadConfig::default(), &mut oversized_points).unwrap();
        oversized_points.extend_from_slice(&[0, 0, 0, 0]);
        append_varint(&mut oversized_points, super::MAX_WORKLOAD_POINTS + 1);
        let oversized_points_crc = crc32fast::hash(&oversized_points);
        assert!(matches!(
            read_workload_from_reader(std::io::Cursor::new(oversized_points), oversized_points_crc),
            Err(Error::Serialization(_))
        ));
    }

    #[test]
    fn invalid_model_ids_and_references_are_rejected_on_write_and_read() {
        let workload = EnergyWorkload::generate(WorkloadConfig {
            days: 1,
            cadence_seconds: 3_600,
            ..WorkloadConfig::default()
        })
        .unwrap();

        let mut invalid_entity = workload.clone();
        invalid_entity.entities[0].id = EntityId(0);
        assert!(matches!(
            invalid_entity.write_bundle(tempdir().unwrap().path()),
            Err(Error::InvalidModel(_))
        ));

        let mut invalid_point = workload;
        invalid_point.points[0].series_id = u64::MAX;
        let encoded = postcard::to_stdvec(&invalid_point).unwrap();
        let crc32 = crc32fast::hash(&encoded);
        assert!(matches!(
            read_workload_from_reader(std::io::Cursor::new(encoded), crc32),
            Err(Error::Serialization(_))
        ));
    }

    #[test]
    fn generator_rejects_each_config_limit_and_large_combinations() {
        for config in [
            WorkloadConfig {
                sites: super::MAX_WORKLOAD_SITES + 1,
                ..WorkloadConfig::default()
            },
            WorkloadConfig {
                days: MAX_WORKLOAD_DAYS + 1,
                ..WorkloadConfig::default()
            },
            WorkloadConfig {
                cadence_seconds: 86_401,
                ..WorkloadConfig::default()
            },
            WorkloadConfig {
                sites: 2,
                days: MAX_WORKLOAD_DAYS,
                cadence_seconds: 60,
                ..WorkloadConfig::default()
            },
        ] {
            assert!(matches!(
                EnergyWorkload::generate(config),
                Err(Error::InvalidConfig(_))
            ));
        }
    }

    #[test]
    fn trailing_and_checksum_corrupt_bundle_data_are_rejected() {
        let workload = EnergyWorkload::generate(WorkloadConfig {
            days: 1,
            cadence_seconds: 3_600,
            ..WorkloadConfig::default()
        })
        .unwrap();

        let trailing = tempdir().unwrap();
        workload.write_bundle(trailing.path()).unwrap();
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(trailing.path().join("workload.postcard"))
            .unwrap();
        file.write_all(&[0]).unwrap();
        file.sync_all().unwrap();
        assert!(matches!(
            EnergyWorkload::read_bundle(trailing.path()),
            Err(Error::Serialization(_))
        ));

        let corrupt = tempdir().unwrap();
        workload.write_bundle(corrupt.path()).unwrap();
        let path = corrupt.path().join("workload.postcard");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::End(-1)).unwrap();
        let mut last = [0_u8; 1];
        file.read_exact(&mut last).unwrap();
        file.seek(SeekFrom::End(-1)).unwrap();
        file.write_all(&[last[0] ^ 0xff]).unwrap();
        file.sync_all().unwrap();
        assert!(matches!(
            EnergyWorkload::read_bundle(corrupt.path()),
            Err(Error::Serialization(_))
        ));
    }

    #[test]
    fn maximum_day_contract_generates_writes_and_reads() {
        let workload = EnergyWorkload::generate(WorkloadConfig {
            days: MAX_WORKLOAD_DAYS,
            cadence_seconds: 86_400,
            ..WorkloadConfig::default()
        })
        .unwrap();
        let directory = tempdir().unwrap();
        let written = workload.write_bundle(directory.path()).unwrap();
        let decoded = EnergyWorkload::read_bundle(directory.path()).unwrap();

        assert_eq!(decoded.summary(), written);
    }
}
