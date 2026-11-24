import contextlib
import csv
import hashlib
import io
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

from launchpad.generator import DEFAULT_CHANNEL_WEIGHTS, GeneratorInputError, generate, main
from launchpad.pipeline import PipelineInputError, read_orders, report, run


class GeneratorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def paths(self, name):
        return self.root / f"{name}.csv", self.root / f"{name}.manifest.json"

    def generate(self, name, **overrides):
        csv_path, manifest_path = self.paths(name)
        options = {
            "seed": 42,
            "order_count": 12,
            "max_versions": 4,
            "start_date": date(2026, 1, 1),
            "end_date": date(2026, 1, 4),
            "output_path": csv_path,
            "manifest_path": manifest_path,
        }
        options.update(overrides)
        manifest = generate(**options)
        return csv_path, manifest_path, manifest

    def run_cli(self, *args):
        stdout, stderr = io.StringIO(), io.StringIO()
        with (
            mock.patch("sys.argv", ["launchpad.generator", *args]),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            try:
                main()
                code = 0
            except SystemExit as exc:
                code = exc.code
        return code, stdout.getvalue(), stderr.getvalue()

    def test_same_parameters_produce_byte_identical_cross_platform_files(self):
        first_csv, first_manifest, _ = self.generate("first")
        second_csv, second_manifest, _ = self.generate("second")

        self.assertEqual(first_csv.read_bytes(), second_csv.read_bytes())
        self.assertEqual(first_manifest.read_bytes(), second_manifest.read_bytes())
        self.assertNotIn(b"\r\n", first_csv.read_bytes())
        self.assertTrue(first_manifest.read_bytes().endswith(b"\n"))
        manifest = json.loads(first_manifest.read_text())
        csv_digest = hashlib.sha256(first_csv.read_bytes()).hexdigest()
        manifest_digest = hashlib.sha256(first_manifest.read_bytes()).hexdigest()
        self.assertEqual(csv_digest, manifest["csv_sha256"])
        self.assertEqual(csv_digest, "8bcf1e157b6d803a6185fed5355a830b343dea09febef26a6f844abb6660b257")
        self.assertEqual(
            manifest_digest, "2f4c06e3726817a19bc64827a7ee3110161bc7fb07428b74a7f1017f3a272953"
        )

    def test_different_seeds_change_data_without_breaking_invariants(self):
        first_csv, _, first = self.generate("first", seed=1)
        second_csv, _, second = self.generate("second", seed=2)

        self.assertNotEqual(first_csv.read_bytes(), second_csv.read_bytes())
        for path, manifest in ((first_csv, first), (second_csv, second)):
            rows = read_orders(path)
            self.assertEqual(len(rows), manifest["expected"]["bronze_versions"])
            self.assertEqual(len({row[0] for row in rows}), 12)
            by_order = {}
            for row in rows:
                by_order.setdefault(row[0], []).append(row[5])
            self.assertTrue(all(values == sorted(values) for values in by_order.values()))

    def test_generated_data_runs_pipeline_and_matches_manifest(self):
        csv_path, _, manifest = self.generate(
            "valid",
            status_weights={"paid": 2, "pending": 1, "cancelled": 1, "refunded": 1},
        )
        database = self.root / "orders.db"

        counts = run(csv_path, database)

        expected = manifest["expected"]
        self.assertEqual(counts["input_rows"], expected["csv_rows"])
        self.assertEqual(counts["bronze_versions"], expected["bronze_versions"])
        self.assertEqual(counts["current_orders"], expected["distinct_orders"])
        self.assertEqual(sum(expected["version_status_counts"].values()), expected["bronze_versions"])
        self.assertEqual(sum(expected["current_status_counts"].values()), expected["distinct_orders"])
        self.assertEqual(report(database), expected["daily_revenue"])

    def test_custom_distributions_force_refunds_cancellations_and_channel(self):
        refunded_csv, _, refunded = self.generate(
            "refunded",
            order_count=4,
            max_versions=3,
            status_weights={"refunded": 1},
            channel_weights={"mobile": 1},
        )
        cancelled_csv, _, cancelled = self.generate(
            "cancelled",
            order_count=4,
            max_versions=2,
            status_weights={"cancelled": 1},
        )

        self.assertEqual(refunded["expected"]["current_status_counts"]["refunded"], 4)
        self.assertEqual(refunded["expected"]["current_channel_counts"]["mobile"], 4)
        self.assertEqual(cancelled["expected"]["current_status_counts"]["cancelled"], 4)
        for path, final_status in ((refunded_csv, "refunded"), (cancelled_csv, "cancelled")):
            histories = {}
            for row in read_orders(path):
                histories.setdefault(row[0], []).append(row[4])
            for history in histories.values():
                self.assertEqual(history[-1], final_status)
                if final_status == "refunded" and len(history) > 1:
                    self.assertEqual(history[-2], "paid")
                if final_status == "cancelled" and len(history) > 1:
                    self.assertTrue(all(status == "pending" for status in history[:-1]))

    def test_late_arrival_window_changes_order_but_not_rows_or_displacement_bound(self):
        ordered_csv, _, _ = self.generate("ordered", late_arrival_window=0)
        late_csv, _, _ = self.generate("late", late_arrival_window=3)
        with ordered_csv.open(newline="") as handle:
            ordered = list(csv.reader(handle))[1:]
        with late_csv.open(newline="") as handle:
            late = list(csv.reader(handle))[1:]

        self.assertNotEqual(ordered, late)
        self.assertCountEqual(ordered, late)
        original_positions = {tuple(row): index for index, row in enumerate(ordered)}
        self.assertTrue(all(
            abs(index - original_positions[tuple(row)]) <= 3 for index, row in enumerate(late)
        ))

    def test_each_invalid_mode_fails_for_only_its_named_reason(self):
        expected = {
            "duplicate-header": "CSV must contain exactly these columns",
            "conflicting-event": "Conflicting version for order",
            "missing-timezone": "updated_at must include a timezone",
            "oversized-amount": "amount_cents must be between 0 and",
        }
        for mode, message in expected.items():
            with self.subTest(mode=mode):
                csv_path, _, manifest = self.generate(mode, invalid_mode=mode)
                with self.assertRaisesRegex(PipelineInputError, message):
                    run(csv_path, self.root / f"{mode}.db")
                self.assertEqual(manifest["invalid_fixture"]["mode"], mode)
                self.assertIn(message, manifest["invalid_fixture"]["expected_error"])

    def test_cli_writes_default_manifest_and_machine_readable_summary(self):
        output = self.root / "cli.csv"
        code, stdout, stderr = self.run_cli(
            "--seed", "7", "--orders", "3", "--max-versions", "2",
            "--start-date", "2026-01-01", "--end-date", "2026-01-02",
            "--output", str(output),
        )

        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        summary = json.loads(stdout)
        self.assertEqual(summary["output"], str(output))
        self.assertTrue(Path(summary["manifest"]).is_file())
        self.assertEqual(json.loads(Path(summary["manifest"]).read_text())["parameters"]["seed"], 7)

    def test_invalid_parameters_create_no_files(self):
        output, manifest = self.paths("invalid")
        with self.assertRaisesRegex(GeneratorInputError, "start_date"):
            generate(
                seed=1,
                order_count=1,
                max_versions=1,
                start_date=date(2026, 2, 1),
                end_date=date(2026, 1, 1),
                output_path=output,
                manifest_path=manifest,
                channel_weights=DEFAULT_CHANNEL_WEIGHTS,
            )
        self.assertFalse(output.exists())
        self.assertFalse(manifest.exists())

    def test_empty_distribution_is_rejected_instead_of_silently_using_defaults(self):
        output, manifest = self.paths("empty-distribution")
        with self.assertRaisesRegex(GeneratorInputError, "status distribution"):
            generate(
                seed=1,
                order_count=1,
                max_versions=1,
                start_date=date(2026, 1, 1),
                end_date=date(2026, 1, 1),
                output_path=output,
                manifest_path=manifest,
                status_weights={},
            )
        self.assertFalse(output.exists())
        self.assertFalse(manifest.exists())

    def test_potential_output_size_is_bounded_before_generation(self):
        output, manifest = self.paths("too-large")
        with self.assertRaisesRegex(GeneratorInputError, "times max_versions"):
            generate(
                seed=1,
                order_count=20_001,
                max_versions=100,
                start_date=date(2026, 1, 1),
                end_date=date(2026, 1, 1),
                output_path=output,
                manifest_path=manifest,
            )
        self.assertFalse(output.exists())
        self.assertFalse(manifest.exists())


if __name__ == "__main__":
    unittest.main()
