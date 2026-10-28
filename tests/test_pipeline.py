import contextlib
import csv
import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

from launchpad.pipeline import initialize, main, read_orders, report, run

HEADER = ["order_id", "customer_id", "order_date", "amount_cents", "status", "updated_at"]


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.source = self.directory / "orders.csv"
        self.database = self.directory / "orders.db"

    def write(self, rows):
        with self.source.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(HEADER)
            writer.writerows(rows)

    def test_rerun_is_idempotent_and_latest_version_wins(self):
        self.write([
            ["a", "c1", "2026-01-01", "100", "pending", "2026-01-01T10:00:00Z"],
            ["a", "c1", "2026-01-01", "100", "paid", "2026-01-02T10:00:00Z"],
            ["b", "c2", "2026-01-02", "200", "paid", "2026-01-02T11:00:00Z"],
        ])
        first = run(self.source, self.database)
        self.assertEqual(first["bronze_versions"], 3)
        self.assertEqual(first["current_orders"], 2)
        self.assertEqual(run(self.source, self.database), first)
        self.assertEqual(report(self.database), [
            {"order_date": "2026-01-01", "paid_orders": 1, "revenue_cents": 100},
            {"order_date": "2026-01-02", "paid_orders": 1, "revenue_cents": 200},
        ])

    def test_refund_removes_revenue(self):
        self.write([
            ["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z"],
            ["a", "c1", "2026-01-01", "100", "refunded", "2026-01-03T10:00:00Z"],
        ])
        run(self.source, self.database)
        self.assertEqual(report(self.database), [])

    def test_conflicting_version_rolls_back(self):
        self.write([["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z"]])
        run(self.source, self.database)
        self.write([
            ["b", "c2", "2026-01-01", "200", "paid", "2026-01-01T11:00:00Z"],
            ["a", "c1", "2026-01-01", "999", "paid", "2026-01-01T10:00:00Z"],
        ])
        with self.assertRaisesRegex(ValueError, "Conflicting version"):
            run(self.source, self.database)
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM bronze_orders").fetchone()[0], 1)
        self.assertEqual(report(self.database)[0]["revenue_cents"], 100)

    def test_invalid_later_row_never_writes(self):
        self.write([
            ["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z"],
            ["b", "c2", "2026-01-01", "-1", "paid", "2026-01-01T11:00:00Z"],
        ])
        with self.assertRaisesRegex(ValueError, "row 3"):
            run(self.source, self.database)
        self.assertFalse(self.database.exists())

    def test_timezone_normalization(self):
        self.write([["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T15:30:00+05:30"]])
        self.assertEqual(read_orders(self.source)[0][-1], "2026-01-01T10:00:00+00:00")

    def test_cross_midnight_normalization_preserves_order_date_and_rerun(self):
        cases = [
            ("2026-01-01T00:30:00+05:30", "2025-12-31T19:00:00+00:00"),
            ("2026-01-01T23:30:00-02:30", "2026-01-02T02:00:00+00:00"),
        ]
        for index, (timestamp, expected_utc) in enumerate(cases):
            with self.subTest(timestamp=timestamp):
                database = self.directory / f"midnight-{index}.db"
                self.write([["tz-1", "c1", "2026-01-01", "100", "paid", timestamp]])
                expected_row = ("tz-1", "c1", "2026-01-01", 100, "paid", expected_utc)
                self.assertEqual(read_orders(self.source), [expected_row])
                expected_counts = {
                    "input_rows": 1, "bronze_versions": 1,
                    "current_orders": 1, "revenue_days": 1,
                }
                expected_report = [
                    {"order_date": "2026-01-01", "paid_orders": 1, "revenue_cents": 100},
                ]
                for attempt in range(2):
                    with self.subTest(attempt=attempt):
                        self.assertEqual(run(self.source, database), expected_counts)
                        self.assertEqual(report(database), expected_report)
                        with closing(sqlite3.connect(database)) as connection:
                            for table in ("bronze_orders", "silver_orders"):
                                self.assertEqual(
                                    connection.execute(f"SELECT * FROM {table}").fetchall(),
                                    [expected_row],
                                )

    def test_missing_report_database_fails(self):
        with self.assertRaises(FileNotFoundError):
            report(self.database)

    def run_cli(self, *argv):
        stdout, stderr = io.StringIO(), io.StringIO()
        with (
            mock.patch("sys.argv", ["launchpad.pipeline", *argv]),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            try:
                main()
                code = 0
            except SystemExit as exit_:
                code = exit_.code
        return code, stdout.getvalue(), stderr.getvalue()

    def test_cli_missing_input_is_actionable(self):
        missing = self.directory / "missing.csv"
        code, out, err = self.run_cli("run", "--input", str(missing), "--db", str(self.database))
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertIn(str(missing), err)
        self.assertIn("--input", err)
        self.assertIn("No such file", err)  # original error stays visible
        self.assertNotIn("Traceback", err)
        self.assertFalse(self.database.exists())

    def test_cli_missing_report_database_is_actionable(self):
        code, out, err = self.run_cli("report", "--db", str(self.database))
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertIn(str(self.database), err)
        self.assertIn("launchpad.pipeline run", err)
        self.assertNotIn("Traceback", err)

    def test_cli_unreadable_report_database_keeps_original_error(self):
        self.database.write_text("not a database", encoding="utf-8")
        code, out, err = self.run_cli("report", "--db", str(self.database))
        self.assertEqual(code, 2)
        self.assertIn(str(self.database), err)
        self.assertIn("file is not a database", err)
        self.assertNotIn("Traceback", err)

    def test_cli_run_unreadable_database_is_actionable(self):
        self.write([["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z"]])
        self.database.write_text("not a database", encoding="utf-8")
        code, out, err = self.run_cli(
            "run", "--input", str(self.source), "--db", str(self.database)
        )
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertIn(str(self.database), err)
        self.assertIn("SQLite", err)
        self.assertIn("file is not a database", err)
        self.assertNotIn("Traceback", err)

    def test_cli_run_database_directory_is_actionable(self):
        self.write([["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z"]])
        database_directory = self.directory / "database-directory"
        database_directory.mkdir()
        code, out, err = self.run_cli(
            "run", "--input", str(self.source), "--db", str(database_directory)
        )
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertIn(str(database_directory), err)
        self.assertIn("SQLite", err)
        self.assertIn("unable to open database file", err)
        self.assertNotIn("Traceback", err)

    def test_cli_success_output_unchanged(self):
        self.write([["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z"]])
        code, out, _ = self.run_cli("run", "--input", str(self.source), "--db", str(self.database))
        self.assertEqual(code, 0)
        self.assertEqual(out, json.dumps(
            {"input_rows": 1, "bronze_versions": 1, "current_orders": 1, "revenue_days": 1}, indent=2
        ) + "\n")
        code, out, _ = self.run_cli("report", "--db", str(self.database))
        self.assertEqual(code, 0)
        self.assertEqual(out, json.dumps(
            [{"order_date": "2026-01-01", "paid_orders": 1, "revenue_cents": 100}], indent=2
        ) + "\n")


class ReadOrdersValidationTests(unittest.TestCase):
    """Malformed CSV input must fail with a clear error and never touch the database."""

    VALID_ROW = ["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z"]

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.source = self.directory / "orders.csv"
        self.new_database = self.directory / "new.db"
        self.existing_database = self.directory / "existing.db"
        self.seed_existing_database()

    def seed_existing_database(self):
        # Populate the database directly and close it, so the file is not left open.
        with closing(sqlite3.connect(self.existing_database)) as connection:
            initialize(connection)
            with connection:
                connection.execute(
                    "INSERT INTO bronze_orders VALUES (?, ?, ?, ?, ?, ?)",
                    ("seed", "c0", "2025-12-31", 50, "paid", "2025-12-31T09:00:00+00:00"),
                )
                connection.execute(
                    "INSERT INTO silver_orders VALUES (?, ?, ?, ?, ?, ?)",
                    ("seed", "c0", "2025-12-31", 50, "paid", "2025-12-31T09:00:00+00:00"),
                )
                connection.execute("INSERT INTO daily_revenue VALUES ('2025-12-31', 1, 50)")
        self.original_contents = self.database_contents(self.existing_database)

    @staticmethod
    def database_contents(database):
        with closing(sqlite3.connect(database)) as connection:
            return {
                table: connection.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
                for table in ("bronze_orders", "silver_orders", "daily_revenue")
            }

    def write_csv(self, text):
        self.source.write_text(text, encoding="utf-8")

    def write_rows(self, rows, header=HEADER):
        with self.source.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(header)
            writer.writerows(rows)

    def assert_rejected(self, message):
        """Fail for a new database path and for an already populated one."""
        with self.assertRaisesRegex(ValueError, message):
            run(self.source, self.new_database)
        self.assertFalse(self.new_database.exists())

        with self.assertRaisesRegex(ValueError, message):
            run(self.source, self.existing_database)
        self.assertEqual(self.database_contents(self.existing_database), self.original_contents)

    def test_blank_order_id_reports_row_number(self):
        self.write_rows([self.VALID_ROW, ["  ", "c2", "2026-01-01", "200", "paid", "2026-01-01T11:00:00Z"]])
        self.assert_rejected(r"row 3: missing ID")

    def test_blank_customer_id_reports_row_number(self):
        self.write_rows([["a", "", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z"]])
        self.assert_rejected(r"row 2: missing ID")

    def test_unsupported_status_reports_row_number(self):
        self.write_rows([self.VALID_ROW, ["b", "c2", "2026-01-01", "200", "shipped", "2026-01-01T11:00:00Z"]])
        self.assert_rejected(r"row 3: missing ID or unsupported status")

    def test_missing_column_in_header_is_rejected(self):
        self.write_rows([self.VALID_ROW[:5]], header=HEADER[:5])
        self.assert_rejected(r"^CSV must contain exactly these columns")

    def test_extra_column_in_header_is_rejected(self):
        self.write_rows([self.VALID_ROW + ["note"]], header=HEADER + ["note"])
        self.assert_rejected(r"^CSV must contain exactly these columns")

    def test_empty_file_is_rejected(self):
        self.write_csv("")
        self.assert_rejected(r"^CSV must contain exactly these columns")

    def test_header_only_file_is_rejected(self):
        self.write_rows([])
        self.assert_rejected(r"^CSV contains no orders$")

    def test_row_with_too_many_fields_is_rejected(self):
        # csv.DictReader stores surplus values under the None key instead of failing.
        self.write_rows([self.VALID_ROW, ["b", "c2", "2026-01-01", "200", "paid", "2026-01-01T11:00:00Z", "extra"]])
        self.assert_rejected(r"row 3: expected 6 fields, found more")


if __name__ == "__main__":
    unittest.main()
