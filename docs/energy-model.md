# Energy model, plans, and rollups

## Connected domain model

WattDB will keep domain state and time-series values in one transactional
database rather than requiring PostgreSQL beside a TSDB.

### Asset and topology catalog

- `Entity`: stable 128-bit ID, type, name, parent, validity interval, typed
  properties.
- `Relation`: stable ID, source, target, type, direction, validity interval,
  properties. Lines, transformers, pipes, meters, and logical membership are
  relations rather than hard-coded tree assumptions.
- `Series`: integer ID, owning entity/relation, physical quantity, canonical
  unit, value semantics, interpolation/gap policy, quality schema, and
  retention/rollup policy.

### Runs, scenarios, and plans

- `Run`: forecast, optimization, import, control, or reconciliation run;
  workflow/model/version, creation and knowledge times, input snapshot ID,
  parent run, status, annotations, and actor.
- `Scenario`: assumptions and probability attached to a run.
- `Plan`: horizon, resolution, objective terms/value, constraints snapshot,
  solver status, superseded plan, and approval/deployment state.
- Planned schedules and setpoints are ordinary interval series linked by
  `run_id`. Actual outcomes use the same physical series identity or an
  explicitly related outcome series, making plan-versus-actual joins cheap.

A transaction must eventually be able to create run metadata and its first
point batches atomically. A run is immutable after completion; corrections
append a new version or superseding run.

## Three-dimensional time

Each value contains:

- `valid_time` / `valid_time_end`: when the value applies;
- `knowledge_time`: forecast issue time or when the fact became available;
- `change_time`: when this stored revision was written;
- `run_id`: provenance for forecast, optimization, import, or control output.

This supports latest state, complete revision history, strict “as known then”
backtests, and comparisons between plan generations and actual outcomes.

## Value semantics

| Semantic | Example | Default aggregate |
|---|---|---|
| Gauge | power, temperature, SoC | time-weighted mean, min/max, integral |
| Interval total | energy for a 15-minute settlement period | sum |
| Counter | cumulative import meter | positive delta with reset count |
| State | operating mode, tariff zone | duration by state, last |
| Event | alarm, dispatch decision | count, first/last |

Sample mean is retained for diagnostics but must not silently replace a
time-weighted mean for irregular energy telemetry. Gaps longer than the series'
configured maximum are excluded from coverage and integrals.

## Rollup pyramid

A policy may materialize fixed and calendar tiers, for example:

```text
raw for 14 days
  -> 5 minutes for 90 days
  -> 30 minutes for 2 years
  -> 1 hour for 5 years
  -> local calendar day forever
  -> local calendar month forever
```

Each gauge bucket stores mergeable state rather than only `avg`:

- count and arithmetic sum;
- min and max;
- first/last timestamp and value;
- value-time integral;
- covered duration and missing duration;
- quality counts and revision watermark.

Counter buckets additionally store positive delta and reset count. This permits
30-minute, daily, and monthly rollups to be built from 5-minute states without
reading raw points.

Fixed buckets are aligned to Unix time. Calendar day/month buckets carry an
IANA timezone and are built from UTC instants; their duration can vary across
DST transitions. A calendar month is never represented as a fixed number of
seconds.

## Late data and corrections

Every rollup records the maximum source commit/revision it includes. A late or
corrected point adds the affected source interval to a durable invalidation
set. Queries fall back to finer data for invalid buckets until they are rebuilt.
Raw retention is blocked while a required bucket is invalid or not durably
published.

