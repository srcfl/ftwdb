use crate::{
    Entity, EntityId, Error, Point, Properties, PropertyValue, Result, RollupPolicy,
    SeriesDefinition, SeriesSemantics, Store, Transaction,
};
use crc32fast::Hasher;
use std::collections::{HashMap, HashSet};
use std::io::BufRead;

pub const REAL_FIXTURE_START_MICROS: i64 = 1_767_225_600_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RealFixtureLoadReport {
    pub points: u64,
    pub commits: u64,
    pub commits_durable_immediately: u64,
    pub entities: usize,
    pub series: usize,
    pub input_bytes: u64,
    pub input_crc32: u32,
    pub points_crc32: u32,
    pub first_offset_millis: i64,
    pub last_offset_millis: i64,
}

/// Loads the repository's sanitized real-installation CSV fixture.
///
/// The input uses offsets instead of source timestamps. This keeps cadence,
/// jitter, gaps, ordering, and values without retaining the installation's
/// exact dates. The caller must decompress `points.csv.gz` before calling.
pub fn load_real_fixture<R: BufRead>(
    mut reader: R,
    store: &mut Store,
    batch_points: usize,
) -> Result<RealFixtureLoadReport> {
    if batch_points == 0 || batch_points > crate::Config::default().max_batch_points {
        return Err(Error::InvalidModel(format!(
            "real fixture batch points must be between 1 and {}",
            crate::Config::default().max_batch_points
        )));
    }

    let mut input_hasher = Hasher::new();
    let mut points_hasher = Hasher::new();
    let mut input_bytes = 0_u64;
    let mut points_total = 0_u64;
    let mut commits = 0_u64;
    let mut durable_commits = 0_u64;
    let mut line_number = 0_u64;
    let mut line = String::new();
    let mut transaction = Transaction::new();
    let mut batch = Vec::with_capacity(batch_points);
    let mut entities = HashSet::<u64>::new();
    let mut series_owners = HashMap::<u64, u64>::new();
    let mut first_offset = None::<i64>;
    let mut last_offset = None::<i64>;

    loop {
        line.clear();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 {
            break;
        }
        line_number += 1;
        input_bytes = input_bytes.saturating_add(bytes as u64);
        input_hasher.update(line.as_bytes());
        let content = line.trim_end_matches(['\r', '\n']);
        if line_number == 1 {
            if content != "driver_id,series_id,offset_ms,value" {
                return Err(Error::InvalidModel(
                    "real fixture has an unknown CSV header".to_owned(),
                ));
            }
            continue;
        }
        if content.is_empty() {
            continue;
        }

        let (driver_id, series_id, offset_millis, value) =
            parse_row(content).map_err(|reason| {
                Error::InvalidModel(format!("invalid real fixture row {line_number}: {reason}"))
            })?;
        if entities.insert(driver_id) {
            let mut properties = Properties::new();
            properties.insert(
                "source".to_owned(),
                PropertyValue::Text("sanitized-real-installation".to_owned()),
            );
            transaction.upsert_entity(Entity {
                id: EntityId(u128::from(driver_id)),
                kind: "fixture_driver".to_owned(),
                name: format!("driver-{driver_id:04}"),
                parent: None,
                valid_from: REAL_FIXTURE_START_MICROS,
                valid_to: None,
                properties,
            });
        }
        match series_owners.insert(series_id, driver_id) {
            None => {
                transaction.define_series(SeriesDefinition {
                    id: series_id,
                    owner_entity: Some(EntityId(u128::from(driver_id))),
                    owner_relation: None,
                    name: format!("metric-{series_id:04}"),
                    physical_quantity: "sanitized_measurement".to_owned(),
                    canonical_unit: "1".to_owned(),
                    semantics: SeriesSemantics::Gauge,
                    maximum_gap_micros: None,
                    rollup_policy: RollupPolicy {
                        raw_retain_for_micros: None,
                        tiers: Vec::new(),
                    },
                });
            }
            Some(owner) if owner != driver_id => {
                return Err(Error::InvalidModel(format!(
                    "invalid real fixture row {line_number}: series {series_id} changed owner"
                )));
            }
            Some(_) => {}
        }

        let timestamp_micros = REAL_FIXTURE_START_MICROS
            .checked_add(offset_millis.checked_mul(1_000).ok_or_else(|| {
                Error::InvalidModel(format!(
                    "invalid real fixture row {line_number}: timestamp overflow"
                ))
            })?)
            .ok_or_else(|| {
                Error::InvalidModel(format!(
                    "invalid real fixture row {line_number}: timestamp overflow"
                ))
            })?;
        let point = Point::actual(series_id, timestamp_micros, value);
        update_point_checksum(&mut points_hasher, point);
        batch.push(point);
        points_total += 1;
        first_offset = Some(first_offset.map_or(offset_millis, |old| old.min(offset_millis)));
        last_offset = Some(last_offset.map_or(offset_millis, |old| old.max(offset_millis)));

        if batch.len() == batch_points {
            transaction.append_points(std::mem::take(&mut batch));
            let commit = store.commit(std::mem::take(&mut transaction))?;
            commits += 1;
            durable_commits += u64::from(commit.durable);
        }
    }

    if line_number == 0 {
        return Err(Error::InvalidModel("real fixture is empty".to_owned()));
    }
    if !batch.is_empty() || !transaction.is_empty() {
        transaction.append_points(batch);
        let commit = store.commit(transaction)?;
        commits += 1;
        durable_commits += u64::from(commit.durable);
    }
    store.flush()?;

    Ok(RealFixtureLoadReport {
        points: points_total,
        commits,
        commits_durable_immediately: durable_commits,
        entities: entities.len(),
        series: series_owners.len(),
        input_bytes,
        input_crc32: input_hasher.finalize(),
        points_crc32: points_hasher.finalize(),
        first_offset_millis: first_offset.unwrap_or(0),
        last_offset_millis: last_offset.unwrap_or(0),
    })
}

fn parse_row(line: &str) -> std::result::Result<(u64, u64, i64, f64), String> {
    let mut fields = line.split(',');
    let driver_id = parse_positive(fields.next(), "driver id")?;
    let series_id = parse_positive(fields.next(), "series id")?;
    let offset_millis = fields
        .next()
        .ok_or("missing offset")?
        .parse::<i64>()
        .map_err(|_| "invalid offset".to_owned())?;
    if offset_millis < 0 {
        return Err("negative offset".to_owned());
    }
    let value = fields
        .next()
        .ok_or("missing value")?
        .parse::<f64>()
        .map_err(|_| "invalid value".to_owned())?;
    if fields.next().is_some() {
        return Err("too many fields".to_owned());
    }
    if !value.is_finite() {
        return Err("non-finite value".to_owned());
    }
    Ok((driver_id, series_id, offset_millis, value))
}

fn parse_positive(encoded: Option<&str>, name: &str) -> std::result::Result<u64, String> {
    let value = encoded
        .ok_or_else(|| format!("missing {name}"))?
        .parse::<u64>()
        .map_err(|_| format!("invalid {name}"))?;
    if value == 0 {
        return Err(format!("{name} must be positive"));
    }
    Ok(value)
}

fn update_point_checksum(hasher: &mut Hasher, point: Point) {
    hasher.update(&point.series_id.to_le_bytes());
    hasher.update(&point.valid_time.to_le_bytes());
    hasher.update(&point.value.to_bits().to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::{REAL_FIXTURE_START_MICROS, load_real_fixture};
    use crate::{Config, Durability, Store};
    use std::io::Cursor;

    const FIXTURE: &str = concat!(
        "driver_id,series_id,offset_ms,value\n",
        "1,1,0,12.5\n",
        "1,2,5,0\n",
        "2,3,3,-4.25\n",
        "1,1,20,13.5\n",
    );

    #[test]
    fn loads_shifted_real_fixture_and_keeps_input_order() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = Store::open_with(
            directory.path(),
            Config {
                durability: Durability::Manual,
                ..Config::default()
            },
        )
        .unwrap();

        let report = load_real_fixture(Cursor::new(FIXTURE), &mut store, 2).unwrap();
        assert_eq!(report.points, 4);
        assert_eq!(report.entities, 2);
        assert_eq!(report.series, 3);
        assert_eq!(report.commits, 2);
        assert_eq!(report.first_offset_millis, 0);
        assert_eq!(report.last_offset_millis, 20);

        let history = store.database().query_history(
            1,
            REAL_FIXTURE_START_MICROS,
            REAL_FIXTURE_START_MICROS + 21_000,
        );
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].value, 12.5);
        assert_eq!(history[1].value, 13.5);
    }

    #[test]
    fn rejects_changed_series_owner_and_invalid_header() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = Store::open(directory.path()).unwrap();
        let changed_owner = concat!(
            "driver_id,series_id,offset_ms,value\n",
            "1,1,0,1\n",
            "2,1,1,2\n",
        );
        assert!(load_real_fixture(Cursor::new(changed_owner), &mut store, 10).is_err());

        let other = tempfile::tempdir().unwrap();
        let mut store = Store::open(other.path()).unwrap();
        assert!(load_real_fixture(Cursor::new("wrong\n"), &mut store, 10).is_err());
    }
}
