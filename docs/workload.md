# Deterministic energy workload v1

The benchmark generator is part of the library and CLI, not an unpublished
script. A configuration contains seed, site count, day count, cadence, and UTC
start. Its SplitMix64 stream and formulas are host-independent. The canonical
Postcard snapshot gets a CRC32 dataset identity; CSV exports let non-Rust
adapters ingest the exact same records.

## Included domain behavior

For every site the generator creates:

- grid, solar, and battery power gauges;
- state of charge, outdoor temperature, and hourly spot price;
- a cumulative import-energy meter with an explicit reset;
- deterministic missing spans and non-zero quality flags;
- day-ahead forecast runs and a later revision of future values;
- optimization runs linked to their forecast input snapshot;
- deployed plan metadata and interval battery setpoints;
- actual battery outcomes that can be compared with the plan.

The default start is 2026-01-01. A year-long Europe/Stockholm workload crosses
both DST boundaries. Series policies request 5-minute, 30-minute, local-day,
and local-month rollups.

## Portable bundle

`ftwdb generate <directory>` writes:

| File | Contents |
|---|---|
| `entities.csv` | site identity and validity |
| `series.csv` | quantities, units, and semantics |
| `runs.csv` | forecast/optimization provenance |
| `plans.csv` | plan horizon, scenario, and objective |
| `points.csv` | valid/knowledge/change times, run, value, quality, flags |
| `workload.postcard` | canonical lossless benchmark input |
| `summary.txt` | configuration counts and CRC32 identity |

The FTWDB runner emits one JSON result line with dataset CRC, result CRC,
ingest and maintenance duration, cold/warm query duration, stored bytes, and
durability mode. It refuses a non-empty target directory to prevent accidental
append-to-old-data results.

## Correctness before speed

The native energy comparison loads catalog-like records and all points into
both FTWDB and SQLite. Before Criterion measures queries, the 5-minute vectors
must match exactly on bucket time, count, sum, minimum, and maximum. Persistent
FTWDB query results also carry a checksum over their full energy aggregate
state, including integral and covered duration.

Server adapters must report unsupported record classes instead of dropping
them silently. A telemetry-only result is a valid secondary chart but is not a
full-workload result.
