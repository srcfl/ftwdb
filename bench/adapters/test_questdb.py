from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from bench.adapters import questdb


POINT_HEADER = (
    "series_id,valid_time,valid_time_end,knowledge_time,change_time,"
    "run_id,value,quality,flags"
)


def write_bundle(
    directory: Path,
    rows: list[str],
    *,
    header: str = POINT_HEADER,
    crc32: str = "1234abcd",
    point_count: int | None = None,
    extra_summary: str = "",
) -> None:
    count = len(rows) if point_count is None else point_count
    (directory / "summary.txt").write_text(
        "format=ftwdb-energy-workload-v1\n"
        "seed=42\n"
        "entities=1\n"
        "series=6\n"
        "runs=2\n"
        "plans=1\n"
        f"points={count}\n"
        f"crc32={crc32}\n"
        f"{extra_summary}",
        encoding="utf-8",
    )
    (directory / "points.csv").write_text(
        header + "\n" + "\n".join(rows) + "\n",
        encoding="utf-8",
    )


def point(
    valid_time: int,
    value: str,
    *,
    series_id: str = "1",
    run_id: str = "0",
    change_time: int | None = None,
) -> str:
    event_time = valid_time if change_time is None else change_time
    return (
        f"{series_id},{valid_time},{valid_time},{valid_time},{event_time},"
        f"{run_id},{value},0,0"
    )


def point_row(valid_time: int, value: float) -> questdb.PointRow:
    return questdb.PointRow(
        series_id=1,
        valid_time=valid_time,
        valid_time_end=valid_time,
        knowledge_time=valid_time,
        event_time=valid_time,
        run_id="0",
        value=value,
        value_text=str(value),
        quality=0,
        flags=0,
    )


class BundleParserTests(unittest.TestCase):
    def test_reads_strict_portable_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            bundle = Path(temporary)
            write_bundle(
                bundle,
                [point(1_000_001, "1.25"), point(301_000_001, "2.5")],
            )

            parsed = questdb.load_bundle(bundle)

            self.assertEqual(parsed.summary.points, 2)
            self.assertEqual(parsed.summary.crc32, "1234abcd")
            self.assertEqual(len(parsed.grid_rows), 2)
            self.assertEqual(parsed.grid_rows[0].event_time, 1_000_001)
            self.assertRegex(parsed.summary_file_sha256, r"^[0-9a-f]{64}$")
            self.assertRegex(parsed.points_file_crc32, r"^[0-9a-f]{8}$")
            self.assertRegex(parsed.points_file_sha256, r"^[0-9a-f]{64}$")

    def test_rejects_bad_header_summary_count_and_numeric_input(self) -> None:
        cases = {
            "header": {
                "header": POINT_HEADER.replace("change_time", "event_time"),
                "rows": [point(1, "1.0")],
            },
            "summary key": {
                "rows": [point(1, "1.0")],
                "extra_summary": "unknown=1\n",
            },
            "summary crc": {
                "rows": [point(1, "1.0")],
                "crc32": "1234ABCD",
            },
            "point count": {
                "rows": [point(1, "1.0")],
                "point_count": 2,
            },
            "too many points": {
                "rows": [point(1, "1.0"), point(2, "2.0")],
                "point_count": 1,
            },
            "non-finite value": {"rows": [point(1, "nan")]},
            "numeric injection": {
                "rows": [point(1, "1.0", series_id="1 OR 1=1")],
            },
        }
        for name, arguments in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                bundle = Path(temporary)
                write_bundle(bundle, **arguments)
                with self.assertRaises(RuntimeError):
                    questdb.load_bundle(bundle)

    def test_detects_input_changed_during_validation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            bundle = Path(temporary)
            write_bundle(
                bundle,
                [point(1_000_001, "1.25"), point(301_000_001, "2.5")],
            )
            original = questdb.iter_points

            def mutating_rows(directory: Path):
                yield from original(directory)
                with (directory / "points.csv").open("a", encoding="utf-8") as target:
                    target.write("\n")

            with mock.patch.object(questdb, "iter_points", mutating_rows):
                with self.assertRaisesRegex(RuntimeError, "during validation"):
                    questdb.load_bundle(bundle)

    def test_refuses_changed_input_before_first_write(self) -> None:
        for changed_file in ("summary.txt", "points.csv"):
            with self.subTest(changed_file=changed_file):
                with tempfile.TemporaryDirectory() as temporary:
                    bundle = Path(temporary)
                    write_bundle(
                        bundle,
                        [point(1_000_001, "1.25"), point(301_000_001, "2.5")],
                    )
                    parsed = questdb.load_bundle(bundle)
                    with (bundle / changed_file).open("a", encoding="utf-8") as target:
                        target.write("\n")

                    with mock.patch.object(questdb, "request") as request:
                        with self.assertRaisesRegex(RuntimeError, "before import"):
                            questdb.import_points("http://127.0.0.1:9000", parsed)
                        request.assert_not_called()


class IlpTests(unittest.TestCase):
    def test_escapes_symbols_strings_and_encodes_all_point_fields(self) -> None:
        self.assertEqual(
            questdb.escape_ilp_symbol("a b,c=d\\e"),
            "a\\ b\\,c\\=d\\\\e",
        )
        self.assertEqual(questdb.escape_ilp_string('a"b\\c'), 'a\\"b\\\\c')
        with self.assertRaises(ValueError):
            questdb.escape_ilp_symbol("bad\nline")

        row = questdb.PointRow(
            series_id=1,
            valid_time=2,
            valid_time_end=5,
            knowledge_time=4,
            event_time=3,
            run_id='a"b\\c',
            value=1.25,
            value_text="1.25",
            quality=6,
            flags=7,
        )
        self.assertEqual(
            questdb.encode_ilp(row, "a b,c=d\\e"),
            "ftwdb_points,dataset_crc=a\\ b\\,c\\=d\\\\e "
            'series_id=1i,event_time=3t,knowledge_time=4t,run_id="a\\"b\\\\c",'
            "value=1.25,valid_time_end=5t,quality=6i,flags=7i 2\n",
        )


class BucketTests(unittest.TestCase):
    def test_first_observation_keeps_an_unaligned_microsecond_anchor(self) -> None:
        start = 1_767_225_601_234_567
        end = start + 2 * questdb.BUCKET_MICROS
        rows = [
            point_row(start, 1.0),
            point_row(start + 60_000_000, 2.0),
            point_row(start + questdb.BUCKET_MICROS, 3.0),
            point_row(end, 99.0),
        ]

        buckets = questdb.expected_buckets(rows, start, end)
        query = questdb.build_sample_query("1234abcd", start, end)

        self.assertEqual(
            [bucket.start for bucket in buckets],
            [start, start + questdb.BUCKET_MICROS],
        )
        self.assertEqual([bucket.count for bucket in buckets], [2, 1])
        self.assertEqual([bucket.total for bucket in buckets], [3.0, 3.0])
        self.assertNotIn(99.0, [bucket.maximum for bucket in buckets])
        self.assertIn("ALIGN TO FIRST OBSERVATION", query)
        self.assertNotIn("WITH OFFSET", query)
        self.assertIn(questdb.micros_to_iso(start), query)
        self.assertIn(questdb.micros_to_iso(end), query)
        self.assertEqual(questdb.iso_to_micros(questdb.micros_to_iso(start)), start)
        with self.assertRaises(ValueError):
            questdb.build_sample_query("bad' OR 1=1", start, end)

    def test_detects_tampered_query_result(self) -> None:
        expected = [questdb.Bucket(1, 2, 3.0, 1.0, 2.0)]
        actual = [questdb.Bucket(1, 2, 3.5, 1.0, 2.0)]
        with self.assertRaisesRegex(RuntimeError, "bucket 0 mismatch"):
            questdb.assert_results(expected, actual)


class FreshVolumeTests(unittest.TestCase):
    def test_refuses_any_existing_table(self) -> None:
        def execute(_base_url: str, _sql: str) -> questdb.QueryResult:
            return questdb.QueryResult(("table_name",), (("unrelated",),))

        with self.assertRaisesRegex(RuntimeError, "not fresh.*unrelated"):
            questdb.ensure_fresh("http://127.0.0.1:9000", execute=execute)

    def test_accepts_a_volume_without_tables(self) -> None:
        def execute(_base_url: str, _sql: str) -> questdb.QueryResult:
            return questdb.QueryResult(("table_name",), ())

        questdb.ensure_fresh("http://127.0.0.1:9000", execute=execute)


if __name__ == "__main__":
    unittest.main()
