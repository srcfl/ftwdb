use crate::{CalendarUnit, Error, Point, Result, RollupResolution, Sample};
use jiff::{Timestamp, ToSpan};
use std::collections::BTreeMap;

/// A closed aggregate state for one fixed UTC bucket.
///
/// Unlike a streaming `GaugeAggregate`, its integral is already clipped at the
/// bucket edges. Therefore states can be added to coarser levels without
/// reading points or inventing coverage between buckets.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GaugeBucket {
    pub start: i64,
    pub end: i64,
    pub count: u64,
    pub sum: f64,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub first: Option<Sample>,
    pub last: Option<Sample>,
    pub integral_value_micros: f64,
    pub covered_micros: i64,
}

impl GaugeBucket {
    #[must_use]
    pub const fn empty(start: i64, end: i64) -> Self {
        Self {
            start,
            end,
            count: 0,
            sum: 0.0,
            min: None,
            max: None,
            first: None,
            last: None,
            integral_value_micros: 0.0,
            covered_micros: 0,
        }
    }

    fn add_sample(&mut self, sample: Sample) {
        self.count += 1;
        self.sum += sample.value;
        self.min = Some(
            self.min
                .map_or(sample.value, |value| value.min(sample.value)),
        );
        self.max = Some(
            self.max
                .map_or(sample.value, |value| value.max(sample.value)),
        );
        self.first.get_or_insert(sample);
        self.last = Some(sample);
    }

    fn add_covered_interval(&mut self, value: f64, duration_micros: i64) {
        debug_assert!(duration_micros >= 0);
        self.integral_value_micros += value * duration_micros as f64;
        self.covered_micros += duration_micros;
        self.min = Some(self.min.map_or(value, |current| current.min(value)));
        self.max = Some(self.max.map_or(value, |current| current.max(value)));
    }

    #[must_use]
    pub fn sample_mean(self) -> Option<f64> {
        (self.count > 0).then(|| self.sum / self.count as f64)
    }

    #[must_use]
    pub fn time_weighted_mean(self) -> Option<f64> {
        (self.covered_micros > 0).then(|| self.integral_value_micros / self.covered_micros as f64)
    }

    #[must_use]
    pub fn power_to_energy_hours(self) -> f64 {
        self.integral_value_micros / 3_600_000_000.0
    }

    /// Combines exact adjacent fine buckets into one coarser closed state.
    #[must_use]
    pub fn merge_exact(self, next: Self) -> Self {
        assert_eq!(self.end, next.start, "rollup buckets must be adjacent");
        Self {
            start: self.start,
            end: next.end,
            count: self.count + next.count,
            sum: self.sum + next.sum,
            min: option_min(self.min, next.min),
            max: option_max(self.max, next.max),
            first: self.first.or(next.first),
            last: next.last.or(self.last),
            integral_value_micros: self.integral_value_micros + next.integral_value_micros,
            covered_micros: self.covered_micros + next.covered_micros,
        }
    }
}

/// Materialized fixed-duration rollups for one gauge series.
#[derive(Clone, Debug)]
pub struct FixedGaugeRollup {
    resolution_micros: i64,
    buckets: BTreeMap<i64, GaugeBucket>,
}

/// Materialized calendar rollups whose bucket edges are local midnights in an
/// IANA time zone. The stored edges remain UTC microseconds, so 23/25-hour DST
/// days and variable-length months retain their true energy duration.
#[derive(Clone, Debug)]
pub struct CalendarGaugeRollup {
    resolution: RollupResolution,
    buckets: BTreeMap<i64, GaugeBucket>,
}

impl CalendarGaugeRollup {
    pub fn build(
        points: &[Point],
        unit: CalendarUnit,
        iana_timezone: &str,
        max_gap_micros: i64,
    ) -> Result<Self> {
        if max_gap_micros < 0 {
            return Err(Error::InvalidConfig(
                "calendar rollup maximum gap must not be negative",
            ));
        }

        // Resolve the zone even for an empty input. A typo in durable policy
        // must fail maintenance instead of remaining latent until data arrives.
        let _ = Timestamp::UNIX_EPOCH
            .in_tz(iana_timezone)
            .map_err(calendar_error)?;
        let mut ordered = points.to_vec();
        ordered.sort_by_key(|point| point.valid_time);
        let mut buckets = BTreeMap::new();

        for point in &ordered {
            let (start, end) = calendar_bucket_bounds(point.valid_time, unit, iana_timezone)?;
            buckets
                .entry(start)
                .or_insert_with(|| GaugeBucket::empty(start, end))
                .add_sample(Sample::new(point.valid_time, point.value));
        }

        for pair in ordered.windows(2) {
            let previous = pair[0];
            let next = pair[1];
            let gap = next.valid_time - previous.valid_time;
            if gap <= 0 || gap > max_gap_micros {
                continue;
            }
            add_calendar_interval(
                &mut buckets,
                unit,
                iana_timezone,
                previous.valid_time,
                next.valid_time,
                previous.value,
            )?;
        }

        Ok(Self {
            resolution: RollupResolution::Calendar {
                unit,
                iana_timezone: iana_timezone.to_owned(),
            },
            buckets,
        })
    }

    #[must_use]
    pub const fn resolution(&self) -> &RollupResolution {
        &self.resolution
    }

    #[must_use]
    pub fn buckets(&self) -> impl ExactSizeIterator<Item = &GaugeBucket> {
        self.buckets.values()
    }

    #[must_use]
    pub fn range(&self, start: i64, end: i64) -> Vec<GaugeBucket> {
        self.buckets
            .values()
            .filter(|bucket| bucket.end > start && bucket.start < end)
            .copied()
            .collect()
    }
}

impl FixedGaugeRollup {
    /// Builds rollups from latest-revision points sorted internally by valid
    /// time. Interpolation is previous-value hold. A gap larger than
    /// `max_gap_micros` contributes neither coverage nor energy.
    #[must_use]
    pub fn build(points: &[Point], resolution_micros: i64, max_gap_micros: i64) -> Self {
        assert!(resolution_micros > 0, "resolution must be positive");
        assert!(max_gap_micros >= 0, "maximum gap must not be negative");

        let mut ordered = points.to_vec();
        ordered.sort_by_key(|point| point.valid_time);
        let mut buckets = BTreeMap::new();

        for point in &ordered {
            let start = bucket_start(point.valid_time, resolution_micros);
            buckets
                .entry(start)
                .or_insert_with(|| GaugeBucket::empty(start, start + resolution_micros))
                .add_sample(Sample::new(point.valid_time, point.value));
        }

        for pair in ordered.windows(2) {
            let previous = pair[0];
            let next = pair[1];
            let gap = next.valid_time - previous.valid_time;
            if gap <= 0 || gap > max_gap_micros {
                continue;
            }
            add_interval(
                &mut buckets,
                resolution_micros,
                previous.valid_time,
                next.valid_time,
                previous.value,
            );
        }

        Self {
            resolution_micros,
            buckets,
        }
    }

    #[must_use]
    pub const fn resolution_micros(&self) -> i64 {
        self.resolution_micros
    }

    #[must_use]
    pub fn buckets(&self) -> impl ExactSizeIterator<Item = &GaugeBucket> {
        self.buckets.values()
    }

    #[must_use]
    pub fn range(&self, start: i64, end: i64) -> Vec<GaugeBucket> {
        let first = bucket_start(start, self.resolution_micros);
        self.buckets
            .range(first..end)
            .map(|(_, bucket)| *bucket)
            .collect()
    }
}

fn add_interval(
    buckets: &mut BTreeMap<i64, GaugeBucket>,
    resolution: i64,
    mut start: i64,
    end: i64,
    value: f64,
) {
    while start < end {
        let bucket = bucket_start(start, resolution);
        let bucket_end = bucket + resolution;
        let interval_end = end.min(bucket_end);
        buckets
            .entry(bucket)
            .or_insert_with(|| GaugeBucket::empty(bucket, bucket_end))
            .add_covered_interval(value, interval_end - start);
        start = interval_end;
    }
}

fn add_calendar_interval(
    buckets: &mut BTreeMap<i64, GaugeBucket>,
    unit: CalendarUnit,
    timezone: &str,
    mut start: i64,
    end: i64,
    value: f64,
) -> Result<()> {
    while start < end {
        let (bucket_start, bucket_end) = calendar_bucket_bounds(start, unit, timezone)?;
        let interval_end = end.min(bucket_end);
        buckets
            .entry(bucket_start)
            .or_insert_with(|| GaugeBucket::empty(bucket_start, bucket_end))
            .add_covered_interval(value, interval_end - start);
        start = interval_end;
    }
    Ok(())
}

pub(crate) fn calendar_bucket_bounds(
    timestamp_micros: i64,
    unit: CalendarUnit,
    timezone: &str,
) -> Result<(i64, i64)> {
    let zoned = Timestamp::from_microsecond(timestamp_micros)
        .and_then(|timestamp| timestamp.in_tz(timezone))
        .map_err(calendar_error)?;
    let start = match unit {
        CalendarUnit::Day => zoned.start_of_day(),
        CalendarUnit::Month => zoned
            .first_of_month()
            .map_err(calendar_error)?
            .start_of_day(),
        CalendarUnit::Year => zoned
            .first_of_year()
            .map_err(calendar_error)?
            .start_of_day(),
    }
    .map_err(calendar_error)?;
    let end = match unit {
        CalendarUnit::Day => start.checked_add(1.day()),
        CalendarUnit::Month => start.checked_add(1.month()),
        CalendarUnit::Year => start.checked_add(1.year()),
    }
    .map_err(calendar_error)?;
    Ok((
        start.timestamp().as_microsecond(),
        end.timestamp().as_microsecond(),
    ))
}

fn calendar_error(error: jiff::Error) -> Error {
    Error::InvalidModel(format!("invalid calendar rollup: {error}"))
}

fn bucket_start(timestamp: i64, resolution: i64) -> i64 {
    timestamp.div_euclid(resolution) * resolution
}

fn option_min(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    }
}

fn option_max(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    }
}

#[cfg(test)]
mod tests {
    use super::{CalendarGaugeRollup, FixedGaugeRollup, calendar_bucket_bounds};
    use crate::{CalendarUnit, Point};
    use jiff::civil::date;

    const SECOND: i64 = 1_000_000;

    #[test]
    fn clips_integrals_at_bucket_edges() {
        let points = [
            Point::actual(1, 0, 2.0),
            Point::actual(1, 7 * SECOND, 4.0),
            Point::actual(1, 12 * SECOND, 8.0),
        ];
        let rollup = FixedGaugeRollup::build(&points, 5 * SECOND, 60 * SECOND);
        let buckets: Vec<_> = rollup.buckets().copied().collect();

        assert_eq!(buckets.len(), 3);
        assert_eq!(buckets[0].covered_micros, 5 * SECOND);
        assert_eq!(buckets[0].time_weighted_mean(), Some(2.0));
        assert_eq!(buckets[1].covered_micros, 5 * SECOND);
        assert_eq!(buckets[1].time_weighted_mean(), Some(3.2));
        assert_eq!(buckets[2].covered_micros, 2 * SECOND);
        assert_eq!(buckets[2].time_weighted_mean(), Some(4.0));
    }

    #[test]
    fn negative_timestamps_use_euclidean_bucket_alignment() {
        let points = [Point::actual(1, -SECOND, 1.0)];
        let rollup = FixedGaugeRollup::build(&points, 5 * SECOND, 5 * SECOND);
        assert_eq!(rollup.buckets().next().unwrap().start, -5 * SECOND);
    }

    #[test]
    fn exact_merge_preserves_closed_integrals_without_bridging_gaps() {
        let points = [
            Point::actual(1, 0, 2.0),
            Point::actual(1, 5 * SECOND, 4.0),
            Point::actual(1, 10 * SECOND, 8.0),
        ];
        let rollup = FixedGaugeRollup::build(&points, 5 * SECOND, 5 * SECOND);
        let buckets: Vec<_> = rollup.buckets().copied().collect();
        let merged = buckets[0].merge_exact(buckets[1]);
        assert_eq!(merged.start, 0);
        assert_eq!(merged.end, 10 * SECOND);
        assert_eq!(merged.covered_micros, 10 * SECOND);
        assert_eq!(merged.time_weighted_mean(), Some(3.0));
    }

    #[test]
    fn stockholm_calendar_days_follow_dst_not_24_hour_assumptions() {
        let spring_noon = date(2026, 3, 29)
            .at(12, 0, 0, 0)
            .in_tz("Europe/Stockholm")
            .unwrap()
            .timestamp()
            .as_microsecond();
        let fall_noon = date(2026, 10, 25)
            .at(12, 0, 0, 0)
            .in_tz("Europe/Stockholm")
            .unwrap()
            .timestamp()
            .as_microsecond();
        let spring =
            calendar_bucket_bounds(spring_noon, CalendarUnit::Day, "Europe/Stockholm").unwrap();
        let fall =
            calendar_bucket_bounds(fall_noon, CalendarUnit::Day, "Europe/Stockholm").unwrap();

        assert_eq!(spring.1 - spring.0, 23 * 3_600 * SECOND);
        assert_eq!(fall.1 - fall.0, 25 * 3_600 * SECOND);
    }

    #[test]
    fn calendar_energy_uses_real_bucket_duration() {
        let start = date(2026, 3, 29)
            .at(0, 0, 0, 0)
            .in_tz("Europe/Stockholm")
            .unwrap()
            .timestamp()
            .as_microsecond();
        let end = date(2026, 3, 30)
            .at(0, 0, 0, 0)
            .in_tz("Europe/Stockholm")
            .unwrap()
            .timestamp()
            .as_microsecond();
        let points = [
            Point::actual(1, start, 1_000.0),
            Point::actual(1, end, 1_000.0),
        ];
        let rollup = CalendarGaugeRollup::build(
            &points,
            CalendarUnit::Day,
            "Europe/Stockholm",
            26 * 3_600 * SECOND,
        )
        .unwrap();
        let first = *rollup.buckets().next().unwrap();

        assert_eq!(first.covered_micros, 23 * 3_600 * SECOND);
        assert_eq!(first.power_to_energy_hours(), 23_000.0);
    }
}
