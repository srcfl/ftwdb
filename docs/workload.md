# Deterministic energy workload v1

The benchmark generator is part of the library and CLI, not an unpublished
script. A configuration contains seed, site count, day count, cadence, and UTC
start. Its SplitMix64 stream is fixed, but floating-point formula results may
differ across host math libraries. The canonical Postcard snapshot gets a
CRC32 dataset identity; CSV exports let non-Rust adapters ingest the exact same
records.

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

## Size and memory contract

The generator, writer, and reader use the same v1 checks. A configuration may
have at most 256 sites, 366 days, a cadence from 1 to 86,400 seconds, and four
million points. Their combined point count must also stay below that limit.
Text fields may use 1,024 bytes, property maps 64 entries, rollup policies 16
tiers, and plan objective maps 64 entries. The canonical Postcard file may use
at most 256 MiB.

These limits include the normal one-site annual run: 366 days at a 60-second
cadence has an upper bound of 3,460,902 points. The 256 MiB file limit keeps an
input error from causing an unbounded read on an edge host. The decoded model
still needs memory for its records and points.

The v1 byte layout has not changed. The writer sends Postcard bytes and CRC32
through one bounded stream instead of making a second full copy. The reader
uses an 8 KiB input buffer and 1 KiB field buffer. It checks every declared
sequence and map length before reserving space, then checks the shared model
rules, summary counts, trailing bytes, and CRC32.

## Correctness before speed

The native energy comparison loads catalog-like records and all points into
both FTWDB and SQLite. Before Criterion measures queries, the 5-minute vectors
must match exactly on bucket time, count, sum, minimum, and maximum. Persistent
FTWDB query results also carry a checksum over their full energy aggregate
state, including integral and covered duration.

Server adapters must report unsupported record classes instead of dropping
them silently. A telemetry-only result is a valid secondary chart but is not a
full-workload result.
