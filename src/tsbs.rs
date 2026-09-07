use crate::{
    Entity, EntityId, Error, Point, Properties, PropertyValue, Result, RollupPolicy,
    SeriesDefinition, SeriesSemantics, Store, Transaction,
};
use crc32fast::Hasher;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::BufRead;

const MAX_FIELDS_PER_ROW: usize = 10;
const SERIES_STRIDE: u64 = 16;
const ENTITY_ID_MODULUS: u64 = (1_u64 << 60) - 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TsbsIotLoadReport {
    pub rows: u64,
    pub points: u64,
    pub commits: u64,
    pub commits_durable_immediately: u64,
    pub entities: usize,
    pub series: usize,
    pub input_bytes: u64,
    pub input_crc32: u32,
    pub points_crc32: u32,
}

struct ParsedRow {
    measurement: String,
    tags: BTreeMap<String, String>,
    timestamp_micros: i64,
    fields: Vec<(String, f64)>,
}

#[derive(Clone, Copy)]
struct FieldDefinition {
    ordinal: u64,
    quantity: &'static str,
    unit: &'static str,
    semantics: SeriesSemantics,
}

/// Loads TSBS IoT data in the official Influx line-protocol format.
///
/// One TSBS row contains up to ten metrics. `batch_rows` therefore follows
/// TSBS loader terminology, while FTWDB stores and reports each metric as one
/// point. Input order is retained, including TSBS's out-of-order rows.
pub fn load_tsbs_iot<R: BufRead>(
    mut reader: R,
    store: &mut Store,
    batch_rows: usize,
) -> Result<TsbsIotLoadReport> {
    let maximum_rows = crate::Config::default().max_batch_points / MAX_FIELDS_PER_ROW;
    if batch_rows == 0 || batch_rows > maximum_rows {
        return Err(Error::InvalidModel(format!(
            "TSBS batch rows must be between 1 and {maximum_rows}"
        )));
    }

    let mut input_hasher = Hasher::new();
    let mut points_hasher = Hasher::new();
    let mut input_bytes = 0_u64;
    let mut rows = 0_u64;
    let mut points_total = 0_u64;
    let mut commits = 0_u64;
    let mut durable_commits = 0_u64;
    let mut rows_in_batch = 0_usize;
    let mut line_number = 0_u64;
    let mut line = String::new();
    let mut transaction = Transaction::new();
    let mut batch_points = Vec::with_capacity(batch_rows.saturating_mul(MAX_FIELDS_PER_ROW));
    let mut entity_tags = HashMap::<u64, BTreeMap<String, String>>::new();
    let mut defined_series = HashSet::<u64>::new();

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
        if content.is_empty() {
            continue;
        }
        let parsed = parse_row(content).map_err(|reason| {
            Error::InvalidModel(format!("invalid TSBS IoT row {line_number}: {reason}"))
        })?;
        let entity_key = entity_key(&parsed.tags);

        match entity_tags.get(&entity_key) {
            Some(existing) if existing != &parsed.tags => {
                return Err(Error::InvalidModel(format!(
                    "invalid TSBS IoT row {line_number}: tag-set id collision"
                )));
            }
            Some(_) => {}
            None => {
                let entity = entity_from_row(&parsed, entity_key);
                transaction.upsert_entity(entity);
                entity_tags.insert(entity_key, parsed.tags.clone());
            }
        }

        for (field, value) in parsed.fields {
            let definition = field_definition(&parsed.measurement, &field).ok_or_else(|| {
                Error::InvalidModel(format!(
                    "invalid TSBS IoT row {line_number}: unknown field {}.{field}",
                    parsed.measurement
                ))
            })?;
            let series_id = series_id(entity_key, definition.ordinal)?;
            if defined_series.insert(series_id) {
                transaction.define_series(SeriesDefinition {
                    id: series_id,
                    owner_entity: Some(entity_id(entity_key)),
                    owner_relation: None,
                    name: format!("{}.{field}", parsed.measurement),
                    physical_quantity: definition.quantity.to_owned(),
                    canonical_unit: definition.unit.to_owned(),
                    semantics: definition.semantics,
                    maximum_gap_micros: None,
                    rollup_policy: RollupPolicy {
                        raw_retain_for_micros: None,
                        tiers: Vec::new(),
                    },
                });
            }
            let point = Point::actual(series_id, parsed.timestamp_micros, value);
            update_point_checksum(&mut points_hasher, point);
            batch_points.push(point);
            points_total += 1;
        }
        rows += 1;
        rows_in_batch += 1;

        if rows_in_batch == batch_rows {
            transaction.append_points(std::mem::take(&mut batch_points));
            let commit = store.commit(std::mem::take(&mut transaction))?;
            commits += 1;
            durable_commits += u64::from(commit.durable);
            rows_in_batch = 0;
        }
    }

    if rows_in_batch != 0 {
        transaction.append_points(batch_points);
        let commit = store.commit(transaction)?;
        commits += 1;
        durable_commits += u64::from(commit.durable);
    }
    store.flush()?;

    Ok(TsbsIotLoadReport {
        rows,
        points: points_total,
        commits,
        commits_durable_immediately: durable_commits,
        entities: entity_tags.len(),
        series: defined_series.len(),
        input_bytes,
        input_crc32: input_hasher.finalize(),
        points_crc32: points_hasher.finalize(),
    })
}

fn parse_row(line: &str) -> std::result::Result<ParsedRow, String> {
    let mut parts = line.split(' ');
    let measurement_and_tags = parts.next().ok_or("missing measurement")?;
    let fields = parts.next().ok_or("missing fields")?;
    let timestamp = parts.next().ok_or("missing timestamp")?;
    if parts.next().is_some() {
        return Err("unexpected spaces".to_owned());
    }

    let mut header = measurement_and_tags.split(',');
    let measurement = header.next().ok_or("missing measurement")?;
    if !matches!(measurement, "readings" | "diagnostics") {
        return Err(format!("unknown measurement {measurement}"));
    }
    let mut tags = BTreeMap::new();
    for tag in header {
        let (key, value) = tag
            .split_once('=')
            .ok_or_else(|| format!("invalid tag {tag}"))?;
        if key.is_empty()
            || value.is_empty()
            || tags.insert(key.to_owned(), value.to_owned()).is_some()
        {
            return Err(format!("invalid or duplicate tag {key}"));
        }
    }
    let timestamp_nanos = timestamp
        .parse::<i64>()
        .map_err(|_| format!("invalid timestamp {timestamp}"))?;
    if timestamp_nanos % 1_000 != 0 {
        return Err(format!(
            "timestamp {timestamp} cannot be represented as whole microseconds"
        ));
    }
    let timestamp_micros = timestamp_nanos / 1_000;

    let mut parsed_fields = Vec::new();
    let mut names = HashSet::new();
    for field in fields.split(',') {
        let (name, encoded) = field
            .split_once('=')
            .ok_or_else(|| format!("invalid field {field}"))?;
        if name.is_empty() || !names.insert(name) {
            return Err(format!("invalid or duplicate field {name}"));
        }
        let number = encoded.strip_suffix('i').unwrap_or(encoded);
        let value = number
            .parse::<f64>()
            .map_err(|_| format!("invalid field value {encoded}"))?;
        if !value.is_finite() {
            return Err(format!("non-finite field value {encoded}"));
        }
        parsed_fields.push((name.to_owned(), value));
    }
    validate_present_tags(&tags)?;

    Ok(ParsedRow {
        measurement: measurement.to_owned(),
        tags,
        timestamp_micros,
        fields: parsed_fields,
    })
}

fn validate_present_tags(tags: &BTreeMap<String, String>) -> std::result::Result<(), String> {
    const ALL_TAGS: [&str; 5] = ["name", "fleet", "driver", "model", "device_version"];
    for key in tags.keys() {
        if !ALL_TAGS.contains(&key.as_str()) {
            return Err(format!("unexpected tag {key} in TSBS IoT row"));
        }
    }
    Ok(())
}

fn entity_from_row(row: &ParsedRow, entity_key: u64) -> Entity {
    let mut properties = Properties::new();
    for (key, value) in &row.tags {
        if key == "name" {
            continue;
        }
        properties.insert(key.clone(), PropertyValue::Text(value.clone()));
    }
    Entity {
        id: entity_id(entity_key),
        kind: "tsbs_truck_tagset".to_owned(),
        name: row
            .tags
            .get("name")
            .cloned()
            .unwrap_or_else(|| format!("missing-name-{entity_key:015x}")),
        parent: None,
        valid_from: 0,
        valid_to: None,
        properties,
    }
}

const fn entity_id(entity_key: u64) -> EntityId {
    EntityId(entity_key as u128 + 1)
}

fn series_id(entity_key: u64, ordinal: u64) -> Result<u64> {
    entity_key
        .checked_mul(SERIES_STRIDE)
        .and_then(|base| base.checked_add(ordinal + 1))
        .ok_or_else(|| Error::InvalidModel("TSBS series id overflow".to_owned()))
}

fn entity_key(tags: &BTreeMap<String, String>) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for (key, value) in tags {
        for byte in key.bytes().chain([0]).chain(value.bytes()).chain([0xff]) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash % ENTITY_ID_MODULUS
}

fn field_definition(measurement: &str, field: &str) -> Option<FieldDefinition> {
    let (ordinal, quantity, unit, semantics) = match (measurement, field) {
        ("readings", "load_capacity") => (0, "load_capacity", "1", SeriesSemantics::Gauge),
        ("readings", "fuel_capacity") => (1, "fuel_capacity", "1", SeriesSemantics::Gauge),
        ("readings", "nominal_fuel_consumption") => {
            (2, "nominal_fuel_consumption", "1", SeriesSemantics::Gauge)
        }
        ("readings", "latitude") => (3, "latitude", "degree", SeriesSemantics::Gauge),
        ("readings", "longitude") => (4, "longitude", "degree", SeriesSemantics::Gauge),
        ("readings", "elevation") => (5, "elevation", "m", SeriesSemantics::Gauge),
        ("readings", "velocity") => (6, "velocity", "km/h", SeriesSemantics::Gauge),
        ("readings", "heading") => (7, "heading", "degree", SeriesSemantics::Gauge),
        ("readings", "grade") => (8, "grade", "%", SeriesSemantics::Gauge),
        ("readings", "fuel_consumption") => (9, "fuel_consumption", "1", SeriesSemantics::Gauge),
        ("diagnostics", "load_capacity") => (10, "load_capacity", "1", SeriesSemantics::Gauge),
        ("diagnostics", "fuel_capacity") => (11, "fuel_capacity", "1", SeriesSemantics::Gauge),
        ("diagnostics", "nominal_fuel_consumption") => {
            (12, "nominal_fuel_consumption", "1", SeriesSemantics::Gauge)
        }
        ("diagnostics", "fuel_state") => (13, "fuel_state", "1", SeriesSemantics::Gauge),
        ("diagnostics", "current_load") => (14, "load", "1", SeriesSemantics::Gauge),
        ("diagnostics", "status") => (15, "status", "1", SeriesSemantics::State),
        _ => return None,
    };
    Some(FieldDefinition {
        ordinal,
        quantity,
        unit,
        semantics,
    })
}

fn update_point_checksum(hasher: &mut Hasher, point: Point) {
    hasher.update(&point.series_id.to_le_bytes());
    hasher.update(&point.valid_time.to_le_bytes());
    hasher.update(&point.value.to_bits().to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::{load_tsbs_iot, parse_row};
    use crate::{Config, Durability, PropertyValue, Store};
    use std::io::Cursor;

    const ROWS: &str = concat!(
        "readings,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 ",
        "load_capacity=1500,fuel_capacity=150,nominal_fuel_consumption=12,latitude=52.31854,longitude=4.72037,elevation=124,velocity=0,heading=221,grade=0,fuel_consumption=25 1451606400000000000\n",
        "diagnostics,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 ",
        "load_capacity=1500,fuel_capacity=150,nominal_fuel_consumption=12,fuel_state=1,current_load=0,status=0i 1451606400000000000\n",
        "readings,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 ",
        "load_capacity=1500,fuel_capacity=150,nominal_fuel_consumption=12,latitude=52.32,elevation=135,velocity=2,heading=218,grade=0,fuel_consumption=30.1 1451606390000000000\n",
    );

    #[test]
    fn parses_missing_fields_integer_fields_and_nanosecond_timestamps() {
        let row = parse_row(ROWS.lines().nth(1).unwrap()).unwrap();
        assert_eq!(row.measurement, "diagnostics");
        assert_eq!(row.timestamp_micros, 1_451_606_400_000_000);
        assert_eq!(row.fields[5], ("status".to_owned(), 0.0));

        let missing = parse_row(ROWS.lines().nth(2).unwrap()).unwrap();
        assert_eq!(missing.fields.len(), 9);
        assert!(missing.fields.iter().all(|(name, _)| name != "longitude"));
    }

    #[test]
    fn loads_catalog_points_and_out_of_order_rows() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = Store::open_with(
            directory.path(),
            Config {
                durability: Durability::Manual,
                ..Config::default()
            },
        )
        .unwrap();
        let report = load_tsbs_iot(Cursor::new(ROWS), &mut store, 2).unwrap();

        assert_eq!(report.rows, 3);
        assert_eq!(report.points, 25);
        assert_eq!(report.commits, 2);
        assert_eq!(report.commits_durable_immediately, 0);
        assert_eq!(report.entities, 1);
        assert_eq!(report.series, 16);
        assert_ne!(report.input_crc32, 0);
        assert_ne!(report.points_crc32, 0);
        assert_eq!(store.database().stats().unwrap().points, 25);
        let latitude = store
            .database()
            .catalog()
            .series_definitions()
            .find(|series| series.name == "readings.latitude")
            .unwrap();
        assert_eq!(
            store
                .database()
                .query_latest(latitude.id, i64::MIN, i64::MAX)
                .unwrap()
                .len(),
            2
        );
        let truck = store
            .database()
            .catalog()
            .entities()
            .find(|entity| entity.name == "truck_0")
            .unwrap();
        assert_eq!(
            truck.properties.get("fleet"),
            Some(&PropertyValue::Text("South".to_owned()))
        );
    }

    #[test]
    fn preserves_missing_or_changed_tag_sets_as_distinct_entities() {
        let changed = ROWS.replacen("fleet=South", "fleet=North", 1);
        let directory = tempfile::tempdir().unwrap();
        let mut store = Store::open(directory.path()).unwrap();
        let report = load_tsbs_iot(Cursor::new(changed), &mut store, 10).unwrap();
        assert_eq!(report.entities, 2);
    }
}
