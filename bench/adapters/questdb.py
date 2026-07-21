#!/usr/bin/env python3
"""Store every portable point in QuestDB and verify one fixed query subset."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterator


BUCKET_MICROS = 300_000_000
MAX_POINTS = 4_000_000
TABLE = "ftwdb_points"
QUESTDB_IMAGE = (
    "questdb/questdb:9.4.3@"
    "sha256:3fd139f9f16015afc1b064fe4591b271be26cdec10315415e5511e9d80a5919e"
)
SUMMARY_KEYS = (
    "format",
    "seed",
    "entities",
    "series",
    "runs",
    "plans",
    "points",
    "crc32",
)
POINT_HEADER = (
    "series_id",
    "valid_time",
    "valid_time_end",
    "knowledge_time",
    "change_time",
    "run_id",
    "value",
    "quality",
    "flags",
)
UNSIGNED_RE = re.compile(r"0|[1-9][0-9]*")
SIGNED_RE = re.compile(r"-?(?:0|[1-9][0-9]*)")
FLOAT_RE = re.compile(
    r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?"
)
CRC_RE = re.compile(r"[0-9a-f]{8}")
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class Summary:
    seed: int
    entities: int
    series: int
    runs: int
    plans: int
    points: int
    crc32: str


@dataclass(frozen=True)
class PointRow:
    series_id: int
    valid_time: int
    valid_time_end: int
    knowledge_time: int
    event_time: int
    run_id: str
    value: float
    value_text: str
    quality: int
    flags: int


@dataclass(frozen=True)
class Bundle:
    directory: Path
    summary: Summary
    grid_rows: tuple[PointRow, ...]
    summary_file_sha256: str
    points_file_crc32: str
    points_file_sha256: str


@dataclass(frozen=True)
class Bucket:
    start: int
    count: int
    total: float
    minimum: float
    maximum: float


@dataclass(frozen=True)
class QueryResult:
    columns: tuple[str, ...]
    rows: tuple[tuple[object, ...], ...]


def parse_unsigned(value: str, field: str, maximum: int) -> int:
    if not UNSIGNED_RE.fullmatch(value):
        raise RuntimeError(f"{field} is not a canonical unsigned integer: {value!r}")
    parsed = int(value)
    if parsed > maximum:
        raise RuntimeError(f"{field} exceeds {maximum}: {value}")
    return parsed


def parse_signed(value: str, field: str) -> int:
    if not SIGNED_RE.fullmatch(value):
        raise RuntimeError(f"{field} is not a canonical signed integer: {value!r}")
    parsed = int(value)
    if not -(1 << 63) <= parsed < (1 << 63):
        raise RuntimeError(f"{field} is outside the signed 64-bit range: {value}")
    return parsed


def parse_float(value: str, field: str) -> float:
    if not FLOAT_RE.fullmatch(value):
        raise RuntimeError(f"{field} is not a canonical finite number: {value!r}")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise RuntimeError(f"{field} is not finite: {value!r}")
    return parsed


def read_summary(path: Path) -> Summary:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        raise RuntimeError(f"cannot read {path}: {error}") from error
    pairs: list[tuple[str, str]] = []
    for line_number, line in enumerate(lines, 1):
        if line.count("=") != 1:
            raise RuntimeError(f"{path}:{line_number}: expected one key=value pair")
        key, value = line.split("=", 1)
        pairs.append((key, value))
    keys = tuple(key for key, _ in pairs)
    if keys != SUMMARY_KEYS:
        raise RuntimeError(
            f"{path}: expected summary keys {SUMMARY_KEYS}, got {keys}"
        )
    values = dict(pairs)
    if values["format"] != "ftwdb-energy-workload-v1":
        raise RuntimeError(f"{path}: unsupported format {values['format']!r}")
    crc32 = values["crc32"]
    if not CRC_RE.fullmatch(crc32):
        raise RuntimeError(f"{path}: malformed crc32 {crc32!r}")
    summary = Summary(
        seed=parse_unsigned(values["seed"], "summary seed", (1 << 64) - 1),
        entities=parse_unsigned(values["entities"], "summary entities", MAX_POINTS),
        series=parse_unsigned(values["series"], "summary series", MAX_POINTS),
        runs=parse_unsigned(values["runs"], "summary runs", MAX_POINTS),
        plans=parse_unsigned(values["plans"], "summary plans", MAX_POINTS),
        points=parse_unsigned(values["points"], "summary points", MAX_POINTS),
        crc32=crc32,
    )
    if summary.points == 0:
        raise RuntimeError(f"{path}: points must be positive")
    return summary


def parse_point(row: list[str], path: Path, line_number: int) -> PointRow:
    if len(row) != len(POINT_HEADER):
        raise RuntimeError(
            f"{path}:{line_number}: expected {len(POINT_HEADER)} columns, got {len(row)}"
        )
    fields = dict(zip(POINT_HEADER, row))
    series_id = parse_unsigned(fields["series_id"], "series_id", (1 << 63) - 1)
    valid_time = parse_signed(fields["valid_time"], "valid_time")
    valid_time_end = parse_signed(fields["valid_time_end"], "valid_time_end")
    if valid_time_end < valid_time:
        raise RuntimeError(
            f"{path}:{line_number}: valid_time_end precedes valid_time"
        )
    knowledge_time = parse_signed(fields["knowledge_time"], "knowledge_time")
    event_time = parse_signed(fields["change_time"], "change_time")
    run_id_number = parse_unsigned(fields["run_id"], "run_id", (1 << 128) - 1)
    run_id = str(run_id_number)
    quality = parse_unsigned(fields["quality"], "quality", (1 << 63) - 1)
    flags = parse_unsigned(fields["flags"], "flags", (1 << 63) - 1)
    value_text = fields["value"]
    value = parse_float(value_text, "value")
    return PointRow(
        series_id=series_id,
        valid_time=valid_time,
        valid_time_end=valid_time_end,
        knowledge_time=knowledge_time,
        event_time=event_time,
        run_id=run_id,
        value=value,
        value_text=value_text,
        quality=quality,
        flags=flags,
    )


def iter_points(bundle: Path) -> Iterator[PointRow]:
    path = bundle / "points.csv"
    try:
        source = path.open(encoding="utf-8", newline="")
    except OSError as error:
        raise RuntimeError(f"cannot read {path}: {error}") from error
    with source:
        reader = csv.reader(source, strict=True)
        try:
            header = next(reader)
        except StopIteration as error:
            raise RuntimeError(f"{path}: missing header") from error
        except (csv.Error, UnicodeError) as error:
            raise RuntimeError(f"{path}: malformed CSV header: {error}") from error
        if tuple(header) != POINT_HEADER:
            raise RuntimeError(
                f"{path}: expected header {POINT_HEADER}, got {tuple(header)}"
            )
        try:
            for line_number, row in enumerate(reader, 2):
                try:
                    yield parse_point(row, path, line_number)
                except RuntimeError as error:
                    raise RuntimeError(f"{path}:{line_number}: {error}") from error
        except (csv.Error, UnicodeError) as error:
            raise RuntimeError(f"{path}:{reader.line_num}: malformed CSV: {error}") from error


def file_checksums(path: Path) -> tuple[str, str]:
    checksum = 0
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            while True:
                chunk = source.read(1024 * 1024)
                if not chunk:
                    break
                checksum = zlib.crc32(chunk, checksum)
                digest.update(chunk)
    except OSError as error:
        raise RuntimeError(f"cannot checksum {path}: {error}") from error
    return f"{checksum & 0xFFFFFFFF:08x}", digest.hexdigest()


def load_bundle(directory: Path) -> Bundle:
    summary_path = directory / "summary.txt"
    points_path = directory / "points.csv"
    initial_checksums = (file_checksums(summary_path), file_checksums(points_path))
    summary = read_summary(summary_path)
    grid_rows: list[PointRow] = []
    point_count = 0
    for row in iter_points(directory):
        point_count += 1
        if point_count > summary.points:
            raise RuntimeError(
                "points.csv count exceeds summary: "
                f"summary has {summary.points}, CSV has at least {point_count}"
            )
        if row.series_id == 1 and row.run_id == "0":
            grid_rows.append(row)
    if point_count != summary.points:
        raise RuntimeError(
            f"points.csv count mismatch: summary has {summary.points}, CSV has {point_count}"
        )
    if len(grid_rows) < 2:
        raise RuntimeError("bundle has fewer than two site-0 grid actual points")
    final_checksums = (file_checksums(summary_path), file_checksums(points_path))
    if final_checksums != initial_checksums:
        raise RuntimeError("bundle input changed during validation")
    return Bundle(
        directory=directory,
        summary=summary,
        grid_rows=tuple(grid_rows),
        summary_file_sha256=final_checksums[0][1],
        points_file_crc32=final_checksums[1][0],
        points_file_sha256=final_checksums[1][1],
    )


def assert_bundle_unchanged(bundle: Bundle, phase: str) -> None:
    current_sha256 = (
        file_checksums(bundle.directory / "summary.txt")[1],
        file_checksums(bundle.directory / "points.csv")[1],
    )
    expected_sha256 = (bundle.summary_file_sha256, bundle.points_file_sha256)
    if current_sha256 != expected_sha256:
        raise RuntimeError(f"bundle input changed {phase}")


def reject_control(value: str, field: str) -> None:
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise ValueError(f"{field} contains a control character")


def escape_ilp_symbol(value: str) -> str:
    reject_control(value, "ILP symbol")
    return (
        value.replace("\\", "\\\\")
        .replace(" ", "\\ ")
        .replace(",", "\\,")
        .replace("=", "\\=")
    )


def escape_ilp_string(value: str) -> str:
    reject_control(value, "ILP string")
    return value.replace("\\", "\\\\").replace('"', '\\"')


def encode_ilp(row: PointRow, dataset_crc: str) -> str:
    return (
        f"{TABLE},dataset_crc={escape_ilp_symbol(dataset_crc)} "
        f"series_id={row.series_id}i,event_time={row.event_time}t,"
        f"knowledge_time={row.knowledge_time}t,"
        f'run_id="{escape_ilp_string(row.run_id)}",value={row.value_text},'
        f"valid_time_end={row.valid_time_end}t,quality={row.quality}i,"
        f"flags={row.flags}i {row.valid_time}\n"
    )


def request(url: str, data: bytes | None = None, timeout: float = 60.0) -> bytes:
    headers = {"Content-Type": "text/plain; charset=utf-8"} if data is not None else {}
    try:
        with urllib.request.urlopen(
            urllib.request.Request(url, data=data, headers=headers), timeout=timeout
        ) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace").strip()
        detail = f": {body}" if body else ""
        raise RuntimeError(f"QuestDB HTTP {error.code}{detail}") from error
    except (urllib.error.URLError, OSError) as error:
        raise RuntimeError(f"QuestDB request failed for {url}: {error}") from error


def execute_sql(base_url: str, sql: str) -> QueryResult:
    parameters = urllib.parse.urlencode({"query": sql, "fmt": "json"})
    raw = request(f"{base_url.rstrip('/')}/exec?{parameters}")
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise RuntimeError("QuestDB returned malformed JSON") from error
    if not isinstance(payload, dict):
        raise RuntimeError("QuestDB returned a non-object JSON response")
    if "error" in payload:
        raise RuntimeError(f"QuestDB SQL failed: {payload['error']}")
    if "columns" not in payload and "dataset" not in payload:
        return QueryResult((), ())
    columns_payload = payload.get("columns")
    rows_payload = payload.get("dataset")
    if not isinstance(columns_payload, list) or not isinstance(rows_payload, list):
        raise RuntimeError("QuestDB query JSON lacks columns or dataset arrays")
    columns: list[str] = []
    for column in columns_payload:
        if not isinstance(column, dict) or not isinstance(column.get("name"), str):
            raise RuntimeError("QuestDB query JSON has a malformed column")
        columns.append(column["name"])
    rows: list[tuple[object, ...]] = []
    for row in rows_payload:
        if not isinstance(row, list) or len(row) != len(columns):
            raise RuntimeError("QuestDB query JSON has a malformed row")
        rows.append(tuple(row))
    return QueryResult(tuple(columns), tuple(rows))


def wait_ready(base_url: str, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    last_error = "no response"
    while True:
        try:
            execute_sql(base_url, "SELECT 1 AS ready")
            return
        except RuntimeError as error:
            last_error = str(error)
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"QuestDB did not become ready within {timeout:g} seconds: {last_error}"
                ) from error
            time.sleep(0.25)


Executor = Callable[[str, str], QueryResult]


def ensure_fresh(base_url: str, execute: Executor = execute_sql) -> None:
    result = execute(base_url, "SELECT table_name FROM tables()")
    if result.columns != ("table_name",):
        raise RuntimeError(
            f"unexpected tables() columns while checking fresh volume: {result.columns}"
        )
    if result.rows:
        names: list[str] = []
        for row in result.rows:
            if len(row) != 1 or not isinstance(row[0], str):
                raise RuntimeError(f"unexpected tables() row: {row}")
            names.append(row[0])
        raise RuntimeError(
            "QuestDB volume is not fresh; existing tables: " + ", ".join(names)
        )


CREATE_TABLE_SQL = f"""CREATE TABLE {TABLE} (
    dataset_crc SYMBOL,
    series_id LONG,
    event_time TIMESTAMP,
    knowledge_time TIMESTAMP,
    run_id VARCHAR,
    value DOUBLE,
    valid_time_end TIMESTAMP,
    quality LONG,
    flags LONG,
    valid_time TIMESTAMP
) TIMESTAMP(valid_time) PARTITION BY DAY WAL"""


def create_table(base_url: str) -> None:
    execute_sql(base_url, CREATE_TABLE_SQL)


def import_points(base_url: str, bundle: Bundle, batch_rows: int = 10_000) -> int:
    if batch_rows <= 0:
        raise ValueError("batch_rows must be positive")
    assert_bundle_unchanged(bundle, "before import")
    endpoint = f"{base_url.rstrip('/')}/write?precision=u"
    batch: list[str] = []
    count = 0
    for row in iter_points(bundle.directory):
        count += 1
        if count > bundle.summary.points:
            raise RuntimeError(
                "points.csv changed during import: "
                f"expected {bundle.summary.points} rows, found more"
            )
        batch.append(encode_ilp(row, bundle.summary.crc32))
        if len(batch) == batch_rows:
            request(endpoint, "".join(batch).encode("utf-8"))
            batch.clear()
    if batch:
        request(endpoint, "".join(batch).encode("utf-8"))
    if count != bundle.summary.points:
        raise RuntimeError(
            f"points.csv changed during import: expected {bundle.summary.points}, got {count}"
        )
    assert_bundle_unchanged(bundle, "during import")
    return count


def parse_count(result: QueryResult) -> int:
    if result.columns != ("row_count",) or len(result.rows) != 1:
        raise RuntimeError(f"unexpected row-count response: {result}")
    value = result.rows[0][0]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RuntimeError(f"QuestDB returned an invalid row count: {value!r}")
    return value


def visible_count(base_url: str, crc32: str) -> int:
    if not CRC_RE.fullmatch(crc32):
        raise ValueError("dataset CRC32 is not safe for SQL")
    return parse_count(
        execute_sql(
            base_url,
            f"SELECT count() AS row_count FROM {TABLE} "
            f"WHERE dataset_crc = '{crc32}'",
        )
    )


def wait_visible(base_url: str, crc32: str, expected: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        count = visible_count(base_url, crc32)
        if count == expected:
            return
        if count > expected:
            raise RuntimeError(
                f"QuestDB has {count} rows for dataset {crc32}; expected exactly {expected}"
            )
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"QuestDB made {count} of {expected} rows visible within {timeout:g} seconds"
            )
        time.sleep(0.1)


def query_range(rows: tuple[PointRow, ...]) -> tuple[int, int]:
    start = min(row.valid_time for row in rows)
    end = max(row.valid_time for row in rows)
    if end <= start:
        raise RuntimeError("site-0 grid actual range has no positive span")
    return start, end


def expected_buckets(
    rows: list[PointRow] | tuple[PointRow, ...], start: int, end: int
) -> list[Bucket]:
    values_by_bucket: dict[int, list[float]] = {}
    for row in rows:
        if start <= row.valid_time < end:
            bucket_start = (
                start + ((row.valid_time - start) // BUCKET_MICROS) * BUCKET_MICROS
            )
            values_by_bucket.setdefault(bucket_start, []).append(row.value)
    return [
        Bucket(
            start=bucket_start,
            count=len(values),
            total=sum(values),
            minimum=min(values),
            maximum=max(values),
        )
        for bucket_start, values in sorted(values_by_bucket.items())
    ]


def micros_to_iso(value: int) -> str:
    try:
        timestamp = EPOCH + timedelta(microseconds=value)
    except (OverflowError, ValueError) as error:
        raise RuntimeError(f"timestamp is outside the supported ISO range: {value}") from error
    return timestamp.strftime("%Y-%m-%dT%H:%M:%S.") + f"{timestamp.microsecond:06d}Z"


def iso_to_micros(value: object) -> int:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise RuntimeError(f"QuestDB returned an invalid UTC timestamp: {value!r}")
    try:
        timestamp = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise RuntimeError(f"QuestDB returned an invalid UTC timestamp: {value!r}") from error
    delta = timestamp - EPOCH
    return (
        delta.days * 86_400_000_000
        + delta.seconds * 1_000_000
        + delta.microseconds
    )


def build_sample_query(crc32: str, start: int, end: int) -> str:
    if not CRC_RE.fullmatch(crc32):
        raise ValueError("dataset CRC32 is not safe for SQL")
    return f"""SELECT
    valid_time AS bucket_start,
    count() AS bucket_count,
    sum(value) AS bucket_sum,
    min(value) AS bucket_min,
    max(value) AS bucket_max
FROM {TABLE}
WHERE dataset_crc = '{crc32}'
  AND series_id = 1
  AND run_id = '0'
  AND valid_time >= '{micros_to_iso(start)}'
  AND valid_time < '{micros_to_iso(end)}'
SAMPLE BY 5m
ALIGN TO FIRST OBSERVATION"""


def query_number(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuntimeError(f"QuestDB returned a non-number for {field}: {value!r}")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise RuntimeError(f"QuestDB returned a non-finite {field}: {value!r}")
    return parsed


def query_buckets(base_url: str, crc32: str, start: int, end: int) -> list[Bucket]:
    result = execute_sql(base_url, build_sample_query(crc32, start, end))
    expected_columns = (
        "bucket_start",
        "bucket_count",
        "bucket_sum",
        "bucket_min",
        "bucket_max",
    )
    if result.columns != expected_columns:
        raise RuntimeError(
            f"unexpected SAMPLE BY columns: expected {expected_columns}, got {result.columns}"
        )
    buckets: list[Bucket] = []
    for row in result.rows:
        count = row[1]
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            raise RuntimeError(f"QuestDB returned an invalid bucket count: {count!r}")
        buckets.append(
            Bucket(
                start=iso_to_micros(row[0]),
                count=count,
                total=query_number(row[2], "bucket_sum"),
                minimum=query_number(row[3], "bucket_min"),
                maximum=query_number(row[4], "bucket_max"),
            )
        )
    return buckets


def assert_results(expected: list[Bucket], actual: list[Bucket]) -> None:
    if len(actual) != len(expected):
        raise RuntimeError(
            f"result length mismatch: expected {len(expected)}, got {len(actual)}"
        )
    for index, (wanted, got) in enumerate(zip(expected, actual)):
        exact = wanted.start == got.start and wanted.count == got.count
        close = all(
            math.isclose(left, right, rel_tol=1e-11, abs_tol=1e-8)
            for left, right in (
                (wanted.total, got.total),
                (wanted.minimum, got.minimum),
                (wanted.maximum, got.maximum),
            )
        )
        if not exact or not close:
            raise RuntimeError(
                f"bucket {index} mismatch: expected {wanted}, got {got}"
            )


def result_crc32(buckets: list[Bucket]) -> str:
    canonical = "".join(
        f"{bucket.start},{bucket.count},{bucket.total:.9f},"
        f"{bucket.minimum:.9f},{bucket.maximum:.9f}\n"
        for bucket in buckets
    ).encode("ascii")
    return f"{zlib.crc32(canonical) & 0xFFFFFFFF:08x}"


def run(bundle_path: Path, base_url: str, visibility_timeout: float) -> dict[str, object]:
    bundle = load_bundle(bundle_path)
    start, end = query_range(bundle.grid_rows)
    expected = expected_buckets(bundle.grid_rows, start, end)
    if not expected or expected[0].start != start:
        raise RuntimeError("local query does not start at the first grid observation")

    wait_ready(base_url)
    ensure_fresh(base_url)
    assert_bundle_unchanged(bundle, "before table creation")
    create_table(base_url)

    ingest_started = time.perf_counter()
    imported = import_points(base_url, bundle)
    ingest_seconds = time.perf_counter() - ingest_started
    wait_visible(
        base_url,
        bundle.summary.crc32,
        bundle.summary.points,
        visibility_timeout,
    )

    query_started = time.perf_counter()
    actual = query_buckets(base_url, bundle.summary.crc32, start, end)
    query_seconds = time.perf_counter() - query_started
    assert_results(expected, actual)
    queried_points = sum(bucket.count for bucket in expected)

    return {
        "configuration": {
            "ingest": "ilp_http_microseconds",
            "query_alignment": "first_observation",
            "query_range": "[start,end)",
            "table": TABLE,
            "table_mode": "wal_partition_by_day",
        },
        "dataset_crc32": bundle.summary.crc32,
        "durability": "questdb_wal_http_ack_not_ftwdb_equivalent",
        "engine": "questdb",
        "engine_unsupported": [],
        "format": "ftwdb-benchmark-result-v1",
        "image_version": QUESTDB_IMAGE,
        "ingest_seconds": ingest_seconds,
        "points": imported,
        "points_csv_crc32": bundle.points_file_crc32,
        "points_per_second": imported / ingest_seconds,
        "query_buckets": len(expected),
        "query_points": queried_points,
        "query_seconds": query_seconds,
        "result_crc32": result_crc32(expected),
        "scope": "all_points_stored_site0_grid_actual_5m_verified",
        "stored_fields": [
            "series_id",
            "event_time_from_change_time",
            "valid_time_designated",
            "valid_time_end",
            "knowledge_time",
            "run_id",
            "value",
            "quality",
            "flags",
        ],
        "not_implemented": [
            "catalog_import",
            "plan_record_import",
            "dst_calendar_query",
        ],
        "visible_points": visible_count(base_url, bundle.summary.crc32),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--url", default="http://127.0.0.1:9000")
    parser.add_argument("--visibility-timeout", type=float, default=60.0)
    arguments = parser.parse_args()
    if arguments.visibility_timeout <= 0:
        parser.error("--visibility-timeout must be positive")
    try:
        result = run(arguments.bundle, arguments.url, arguments.visibility_timeout)
    except (RuntimeError, ValueError) as error:
        parser.exit(1, f"questdb adapter: {error}\n")
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
