//! Mergeable aggregate states used by the rollup pyramid.

/// A scalar sample at a UTC timestamp expressed in microseconds since Unix
/// epoch.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Sample {
    pub timestamp: i64,
    pub value: f64,
}

impl Sample {
    #[must_use]
    pub const fn new(timestamp: i64, value: f64) -> Self {
        Self { timestamp, value }
    }
}

/// Mergeable state for gauge-like values such as power or temperature.
///
/// `sum / count` is the sample mean. `integral_value_micros / covered_micros`
/// is the time-weighted mean under a previous-value-hold interpolation model.
/// Gaps larger than the caller's `max_gap_micros` are not counted as covered.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GaugeAggregate {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub first: Sample,
    pub last: Sample,
    pub integral_value_micros: f64,
    pub covered_micros: i64,
}

impl GaugeAggregate {
    #[must_use]
    pub fn from_sample(sample: Sample) -> Self {
        Self {
            count: 1,
            sum: sample.value,
            min: sample.value,
            max: sample.value,
            first: sample,
            last: sample,
            integral_value_micros: 0.0,
            covered_micros: 0,
        }
    }

    /// Adds a sample in non-decreasing timestamp order.
    pub fn push(&mut self, sample: Sample, max_gap_micros: i64) {
        assert!(
            sample.timestamp >= self.last.timestamp,
            "samples must be time ordered"
        );
        let gap = sample.timestamp - self.last.timestamp;
        if gap <= max_gap_micros {
            self.integral_value_micros += self.last.value * gap as f64;
            self.covered_micros += gap;
        }
        self.count += 1;
        self.sum += sample.value;
        self.min = self.min.min(sample.value);
        self.max = self.max.max(sample.value);
        self.last = sample;
    }

    /// Combines adjacent aggregate states without revisiting their raw points.
    #[must_use]
    pub fn merge(self, next: Self, max_gap_micros: i64) -> Self {
        assert!(
            next.first.timestamp >= self.last.timestamp,
            "aggregate states must be time ordered"
        );
        let gap = next.first.timestamp - self.last.timestamp;
        let (bridge_integral, bridge_coverage) = if gap <= max_gap_micros {
            (self.last.value * gap as f64, gap)
        } else {
            (0.0, 0)
        };

        Self {
            count: self.count + next.count,
            sum: self.sum + next.sum,
            min: self.min.min(next.min),
            max: self.max.max(next.max),
            first: self.first,
            last: next.last,
            integral_value_micros: self.integral_value_micros
                + bridge_integral
                + next.integral_value_micros,
            covered_micros: self.covered_micros + bridge_coverage + next.covered_micros,
        }
    }

    #[must_use]
    pub fn sample_mean(self) -> f64 {
        self.sum / self.count as f64
    }

    #[must_use]
    pub fn time_weighted_mean(self) -> Option<f64> {
        (self.covered_micros > 0).then(|| self.integral_value_micros / self.covered_micros as f64)
    }

    /// Converts a power integral to energy while preserving the caller's unit
    /// prefix: W -> Wh and kW -> kWh.
    #[must_use]
    pub fn power_to_energy_hours(self) -> f64 {
        self.integral_value_micros / 3_600_000_000.0
    }
}

/// Mergeable state for monotonically increasing meter counters.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CounterAggregate {
    pub count: u64,
    pub first: Sample,
    pub last: Sample,
    pub positive_delta: f64,
    pub resets: u64,
}

impl CounterAggregate {
    #[must_use]
    pub fn from_sample(sample: Sample) -> Self {
        Self {
            count: 1,
            first: sample,
            last: sample,
            positive_delta: 0.0,
            resets: 0,
        }
    }

    pub fn push(&mut self, sample: Sample) {
        assert!(
            sample.timestamp >= self.last.timestamp,
            "samples must be time ordered"
        );
        let (delta, reset) = counter_step(self.last.value, sample.value);
        self.positive_delta += delta;
        self.resets += u64::from(reset);
        self.count += 1;
        self.last = sample;
    }

    #[must_use]
    pub fn merge(self, next: Self) -> Self {
        assert!(
            next.first.timestamp >= self.last.timestamp,
            "aggregate states must be time ordered"
        );
        let (bridge_delta, reset) = counter_step(self.last.value, next.first.value);
        Self {
            count: self.count + next.count,
            first: self.first,
            last: next.last,
            positive_delta: self.positive_delta + bridge_delta + next.positive_delta,
            resets: self.resets + u64::from(reset) + next.resets,
        }
    }
}

fn counter_step(previous: f64, next: f64) -> (f64, bool) {
    if next >= previous {
        (next - previous, false)
    } else {
        // Conventional reset-to-zero semantics: the post-reset reading is the
        // accumulated amount since reset.
        (next.max(0.0), true)
    }
}

#[cfg(test)]
mod tests {
    use super::{CounterAggregate, GaugeAggregate, Sample};

    #[test]
    fn merged_gauge_matches_direct_aggregation() {
        let max_gap = 10_000_000;
        let samples = [
            Sample::new(0, 1.0),
            Sample::new(1_000_000, 3.0),
            Sample::new(3_000_000, 5.0),
            Sample::new(6_000_000, 2.0),
        ];

        let mut direct = GaugeAggregate::from_sample(samples[0]);
        for sample in &samples[1..] {
            direct.push(*sample, max_gap);
        }

        let mut left = GaugeAggregate::from_sample(samples[0]);
        left.push(samples[1], max_gap);
        let mut right = GaugeAggregate::from_sample(samples[2]);
        right.push(samples[3], max_gap);

        assert_eq!(direct, left.merge(right, max_gap));
        assert_eq!(direct.time_weighted_mean(), Some(22.0 / 6.0));
    }

    #[test]
    fn long_gaps_are_not_treated_as_observed_energy() {
        let mut aggregate = GaugeAggregate::from_sample(Sample::new(0, 2.0));
        aggregate.push(Sample::new(60_000_000, 2.0), 5_000_000);
        assert_eq!(aggregate.covered_micros, 0);
        assert_eq!(aggregate.time_weighted_mean(), None);
    }

    #[test]
    fn counter_resets_are_counted_and_do_not_go_negative() {
        let mut aggregate = CounterAggregate::from_sample(Sample::new(0, 100.0));
        aggregate.push(Sample::new(1, 105.0));
        aggregate.push(Sample::new(2, 2.0));
        aggregate.push(Sample::new(3, 7.0));
        assert_eq!(aggregate.positive_delta, 12.0);
        assert_eq!(aggregate.resets, 1);
    }
}
