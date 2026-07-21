#!/usr/bin/env python3
"""Result-verified VictoriaMetrics adapter for the telemetry subset.

The engine cannot natively represent FTWDB's catalog, plan records, or three
time dimensions. This adapter therefore publishes a separately labelled
telemetry-only result and must never be presented as a full-workload score.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import time
import urllib.parse
import urllib.request
import zlib
from pathlib import Path


BUCKET_MICROS = 300_000_000


def request(url: str, data: bytes | None = None, timeout: float = 60.0) -> bytes:
    headers = {"Content-Type": "text/plain"} if data is not None else {}
    with urllib.request.urlopen(
        urllib.request.Request(url, data=data, headers=headers), timeout=timeout
    ) as response:
        return response.read()


def wait_ready(base_url: str) -> None:
    deadline = time.monotonic() + 30.0
    while True:
        try:
            request(f"{base_url}/health", timeout=2.0)
            return
        except OSError:
            if time.monotonic() >= deadline:
                raise RuntimeError("VictoriaMetrics did not become ready within 30 seconds")
            time.sleep(0.25)


def dataset_crc(bundle: Path) -> str:
    for line in (bundle / "summary.txt").read_text().splitlines():
        if line.startswith("crc32="):
            crc = line.split("=", 1)[1]
            if not re.fullmatch(r"[0-9a-f]{8}", crc):
                raise RuntimeError(
                    f"bundle summary has a malformed crc32: {crc!r}"
                )
            return crc
    raise RuntimeError("bundle summary has no crc32")


def read_telemetry(bundle: Path) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    all_rows: list[dict[str, str]] = []
    grid_rows: list[dict[str, str]] = []
    with (bundle / "points.csv").open(newline="") as source:
        for row in csv.DictReader(source):
            if row["run_id"] != "0":
                continue
            all_rows.append(row)
            if row["series_id"] == "1":
                grid_rows.append(row)
    if not grid_rows:
        raise RuntimeError("bundle contains no site-0 grid telemetry")
    return all_rows, grid_rows


def expected_buckets(rows: list[dict[str, str]], start: int, end: int) -> list[tuple[int, int, float, float, float]]:
    buckets: dict[int, list[float]] = {}
    for row in rows:
        timestamp = int(row["valid_time"])
        if start <= timestamp < end:
            bucket = start + ((timestamp - start) // BUCKET_MICROS) * BUCKET_MICROS
            buckets.setdefault(bucket, []).append(float(row["value"]))
    return [
        (bucket, len(values), sum(values), min(values), max(values))
        for bucket, values in sorted(buckets.items())
    ]


def import_rows(base_url: str, rows: list[dict[str, str]], crc: str) -> None:
    batch: list[str] = []
    for row in rows:
        timestamp_ms = int(row["valid_time"]) // 1_000
        batch.append(
            f'ftwdb_value{{series_id="{row["series_id"]}",dataset_crc="{crc}"}} '
            f'{row["value"]} {timestamp_ms}\n'
        )
        if len(batch) == 20_000:
            request(f"{base_url}/api/v1/import/prometheus", "".join(batch).encode())
            batch.clear()
    if batch:
        request(f"{base_url}/api/v1/import/prometheus", "".join(batch).encode())


def ensure_absent(base_url: str, crc: str, start_micros: int, end_micros: int) -> None:
    params = urllib.parse.urlencode(
        {
            "match[]": f'ftwdb_value{{dataset_crc="{crc}"}}',
            "start": f"{start_micros / 1_000_000:.3f}",
            "end": f"{end_micros / 1_000_000:.3f}",
        }
    )
    payload = json.loads(request(f"{base_url}/api/v1/series?{params}"))
    if payload.get("data"):
        raise RuntimeError(
            "dataset already exists in VictoriaMetrics; use a fresh benchmark volume"
        )


def metricsql(
    base_url: str,
    function: str,
    crc: str,
    start_micros: int,
    end_micros: int,
) -> list[float]:
    # Evaluate one millisecond before each right edge. The explicit 5m window
    # then represents [bucket_start,bucket_end), matching the portable spec.
    query = (
        f'{function}(ftwdb_value{{series_id="1",dataset_crc="{crc}"}}[5m])'
    )
    params = urllib.parse.urlencode(
        {
            "query": query,
            "start": f"{start_micros / 1_000_000 + 300 - 0.001:.3f}",
            "end": f"{end_micros / 1_000_000 - 0.001:.3f}",
            "step": "300s",
            "nocache": "1",
        }
    )
    payload = json.loads(request(f"{base_url}/api/v1/query_range?{params}"))
    if payload.get("status") != "success":
        raise RuntimeError(f"MetricsQL query failed: {payload}")
    result = payload["data"]["result"]
    if not result:
        return []
    if len(result) != 1:
        raise RuntimeError(f"expected one result series, got {len(result)}")
    return [float(value) for _, value in result[0]["values"]]


def wait_visible(
    base_url: str,
    crc: str,
    start_micros: int,
    end_micros: int,
    expected_buckets: int,
) -> None:
    deadline = time.monotonic() + 30.0
    while True:
        values = metricsql(
            base_url, "count_over_time", crc, start_micros, end_micros
        )
        if len(values) == expected_buckets:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"import did not become query-visible: expected {expected_buckets} "
                f"buckets, got {len(values)}"
            )
        time.sleep(0.1)


def assert_results(
    expected: list[tuple[int, int, float, float, float]],
    counts: list[float],
    sums: list[float],
    minimums: list[float],
    maximums: list[float],
) -> None:
    actual_columns = [counts, sums, minimums, maximums]
    if any(len(column) != len(expected) for column in actual_columns):
        raise RuntimeError(
            f"result length mismatch: expected {len(expected)}, "
            f"actual {[len(column) for column in actual_columns]}"
        )
    for index, (_, count, total, minimum, maximum) in enumerate(expected):
        wanted = [float(count), total, minimum, maximum]
        actual = [column[index] for column in actual_columns]
        if not all(
            math.isclose(left, right, rel_tol=1e-11, abs_tol=1e-8)
            for left, right in zip(wanted, actual, strict=True)
        ):
            raise RuntimeError(
                f"bucket {index} mismatch: expected {wanted}, actual {actual}"
            )


def result_crc(expected: list[tuple[int, int, float, float, float]]) -> str:
    canonical = "".join(
        f"{start},{count},{total:.9f},{minimum:.9f},{maximum:.9f}\n"
        for start, count, total, minimum, maximum in expected
    ).encode()
    return f"{zlib.crc32(canonical) & 0xFFFFFFFF:08x}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--url", default="http://127.0.0.1:8428")
    arguments = parser.parse_args()

    wait_ready(arguments.url)
    crc = dataset_crc(arguments.bundle)
    telemetry, grid = read_telemetry(arguments.bundle)
    start = min(int(row["valid_time"]) for row in grid)
    # The final generator point closes the previous interval and is excluded
    # from the query horizon, like FTWDB's benchmark runner.
    end = max(int(row["valid_time"]) for row in grid)
    expected = expected_buckets(grid, start, end)

    ensure_absent(arguments.url, crc, start, end)
    ingest_started = time.perf_counter()
    import_rows(arguments.url, telemetry, crc)
    ingest_seconds = time.perf_counter() - ingest_started
    wait_visible(arguments.url, crc, start, end, len(expected))

    query_started = time.perf_counter()
    counts = metricsql(arguments.url, "count_over_time", crc, start, end)
    sums = metricsql(arguments.url, "sum_over_time", crc, start, end)
    minimums = metricsql(arguments.url, "min_over_time", crc, start, end)
    maximums = metricsql(arguments.url, "max_over_time", crc, start, end)
    query_seconds = time.perf_counter() - query_started
    assert_results(expected, counts, sums, minimums, maximums)

    print(
        json.dumps(
            {
                "format": "ftwdb-benchmark-result-v1",
                "engine": "victoriametrics",
                "scope": "telemetry_subset",
                "durability": "server_default",
                "dataset_crc32": crc,
                "result_crc32": result_crc(expected),
                "points": len(telemetry),
                "ingest_seconds": ingest_seconds,
                "points_per_second": len(telemetry) / ingest_seconds,
                "query_seconds": query_seconds,
                "query_buckets": len(expected),
                "unsupported": ["revisions_3d", "catalog", "plans", "dst_calendar"],
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
