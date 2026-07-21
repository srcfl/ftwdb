use crate::{
    CalendarUnit, Entity, EntityId, Error, GaugeBucket, Plan, PlanStatus, Point, Properties,
    PropertyValue, Result, RollupPolicy, RollupResolution, RollupTier, Run, RunId, RunKind,
    RunStatus, SeriesDefinition, SeriesSemantics, Transaction,
};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

const SECOND: i64 = 1_000_000;
const MINUTE: i64 = 60 * SECOND;
const HOUR: i64 = 60 * MINUTE;
const DAY: i64 = 24 * HOUR;
const FIVE_MINUTES: i64 = 5 * MINUTE;

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
        let samples = u64::from(self.sites)
            .saturating_mul(u64::from(self.days))
            .saturating_mul(86_400 / u64::from(self.cadence_seconds));
        if samples > 100_000_000 {
            return Err(Error::InvalidConfig(
                "workload exceeds the 100 million base-sample safety limit",
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
#[derive(Clone, Debug, Deserialize, Serialize)]
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
        let directory = directory.as_ref();
        std::fs::create_dir_all(directory)?;
        write_entities(directory.join("entities.csv"), &self.entities)?;
        write_series(directory.join("series.csv"), &self.series)?;
        write_runs(directory.join("runs.csv"), &self.runs)?;
        write_plans(directory.join("plans.csv"), &self.plans)?;
        write_points(directory.join("points.csv"), &self.points)?;
        let canonical = postcard::to_stdvec(self)
            .map_err(|error| Error::Serialization(format!("workload encode failed: {error}")))?;
        let mut file = File::create(directory.join("workload.postcard"))?;
        file.write_all(&canonical)?;
        file.sync_all()?;
        let summary = self.summary();
        let mut summary_file = BufWriter::new(File::create(directory.join("summary.txt"))?);
        writeln!(summary_file, "format=wattdb-energy-workload-v1")?;
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
        let path = directory.as_ref().join("workload.postcard");
        let metadata = path.metadata()?;
        if metadata.len() > 8 * 1024 * 1024 * 1024 {
            return Err(Error::InvalidConfig(
                "workload bundle exceeds the 8 GiB reader limit",
            ));
        }
        let encoded = std::fs::read(path)?;
        let workload: Self = postcard::from_bytes(&encoded)
            .map_err(|error| Error::Serialization(format!("workload decode failed: {error}")))?;
        workload.config.validate()?;
        Ok(workload)
    }

    #[must_use]
    pub fn checksum(&self) -> u32 {
        let mut hasher = Hasher::new();
        let encoded = postcard::to_stdvec(self).expect("workload model is serializable");
        hasher.update(&encoded);
        hasher.finalize()
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
    use super::{EnergyWorkload, WorkloadConfig};
    use crate::{RunKind, SeriesSemantics, Store};
    use tempfile::tempdir;

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
}
