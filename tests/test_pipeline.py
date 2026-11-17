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

from launchpad.pipeline import PipelineInputError, initialize, main, read_orders, report, run

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

    def test_optional_sales_channel_defaults_and_persists_across_replays(self):
        self.write([["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z"]])
        self.assertEqual(read_orders(self.source)[0][-1], "unknown")
        first = run(self.source, self.database)
        self.assertEqual(run(self.source, self.database), first)
        with closing(sqlite3.connect(self.database)) as connection:
            for table in ("bronze_orders", "silver_orders"):
                self.assertEqual(
                    connection.execute(f"SELECT sales_channel FROM {table}").fetchall(),
                    [("unknown",)],
                )

    def test_sales_channel_is_stored_and_part_of_version_conflict(self):
        with self.source.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow([*HEADER, "sales_channel"])
            writer.writerow(["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z", " web "])
        row = read_orders(self.source)[0]
        self.assertEqual(row[-1], "web")
        run(self.source, self.database)
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(connection.execute("SELECT sales_channel FROM silver_orders").fetchone(), ("web",))
        with self.source.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow([*HEADER, "sales_channel"])
            writer.writerow(["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z", "store"])
        with self.assertRaisesRegex(ValueError, "Conflicting version"):
            run(self.source, self.database)

    def test_existing_database_migrates_sales_channel_to_unknown(self):
        with closing(sqlite3.connect(self.database)) as connection:
            connection.executescript("""
                CREATE TABLE bronze_orders (
                    order_id TEXT NOT NULL, customer_id TEXT NOT NULL, order_date TEXT NOT NULL,
                    amount_cents INTEGER NOT NULL, status TEXT NOT NULL, updated_at TEXT NOT NULL,
                    PRIMARY KEY (order_id, updated_at));
                CREATE TABLE silver_orders (
                    order_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, order_date TEXT NOT NULL,
                    amount_cents INTEGER NOT NULL, status TEXT NOT NULL, updated_at TEXT NOT NULL);
                INSERT INTO bronze_orders VALUES ('a','c1','2026-01-01',100,'paid','2026-01-01T10:00:00+00:00');
                INSERT INTO silver_orders VALUES ('a','c1','2026-01-01',100,'paid','2026-01-01T10:00:00+00:00');
            """)
        self.write([["b", "c2", "2026-01-02", "200", "paid", "2026-01-02T10:00:00Z"]])
        run(self.source, self.database)
        with closing(sqlite3.connect(self.database)) as connection:
            for table in ("bronze_orders", "silver_orders"):
                self.assertEqual(
                    connection.execute(f"SELECT sales_channel FROM {table} ORDER BY order_id").fetchall(),
                    [("unknown",), ("unknown",)],
                )

    def test_unknown_csv_column_is_rejected(self):
        with self.source.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow([*HEADER, "unexpected"])
            writer.writerow(["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z", "x"])
        with self.assertRaisesRegex(ValueError, "sales_channel is optional"):
            read_orders(self.source)
        self.assertFalse(self.database.exists())

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
        self.assertEqual(read_orders(self.source)[0][-2], "2026-01-01T10:00:00+00:00")

    def test_cross_midnight_normalization_preserves_order_date_and_rerun(self):
        cases = [
            ("2026-01-01T00:30:00+05:30", "2025-12-31T19:00:00+00:00"),
            ("2026-01-01T23:30:00-02:30", "2026-01-02T02:00:00+00:00"),
        ]
        for index, (timestamp, expected_utc) in enumerate(cases):
            with self.subTest(timestamp=timestamp):
                database = self.directory / f"midnight-{index}.db"
                self.write([["tz-1", "c1", "2026-01-01", "100", "paid", timestamp]])
                expected_row = ("tz-1", "c1", "2026-01-01", 100, "paid", expected_utc, "unknown")
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

    def test_cli_malformed_header_is_actionable(self):
        self.source.write_text("order_id,customer\na,c1\n", encoding="utf-8")
        code, out, err = self.run_cli("run", "--input", str(self.source), "--db", str(self.database))
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertIn("error: CSV must contain exactly these columns", err)
        self.assertNotIn("Traceback", err)
        self.assertFalse(self.database.exists())

    def test_cli_malformed_row_reports_row_number(self):
        self.write([
            ["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z"],
            ["b", "c2", "2026-01-01", "200", "shipped", "2026-01-01T11:00:00Z"],
        ])
        code, out, err = self.run_cli("run", "--input", str(self.source), "--db", str(self.database))
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertIn("error: Invalid CSV row 3: missing ID or unsupported status", err)
        self.assertNotIn("Traceback", err)
        self.assertFalse(self.database.exists())

    def test_cli_conflicting_version_leaves_database_unchanged(self):
        self.write([["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z"]])
        run(self.source, self.database)
        before = self.database.read_bytes()
        self.write([
            ["b", "c2", "2026-01-01", "200", "paid", "2026-01-01T11:00:00Z"],
            ["a", "c1", "2026-01-01", "999", "paid", "2026-01-01T10:00:00Z"],
        ])
        code, out, err = self.run_cli("run", "--input", str(self.source), "--db", str(self.database))
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertIn("error: Conflicting version for order a at 2026-01-01T10:00:00+00:00", err)
        self.assertNotIn("Traceback", err)
        self.assertEqual(self.database.read_bytes(), before)

    def test_cli_conflicting_versions_in_one_file_create_nothing(self):
        self.write([
            ["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z"],
            ["a", "c1", "2026-01-01", "999", "paid", "2026-01-01T10:00:00Z"],
        ])
        database = self.directory / "new-folder" / "orders.db"
        code, out, err = self.run_cli("run", "--input", str(self.source), "--db", str(database))
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertIn("error: Conflicting version for order a at 2026-01-01T10:00:00+00:00", err)
        self.assertNotIn("Traceback", err)
        self.assertFalse(database.parent.exists())

    def test_identical_duplicate_versions_are_accepted(self):
        row = ["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z"]
        self.write([row, row])
        self.assertEqual(run(self.source, self.database), {
            "input_rows": 2, "bronze_versions": 1, "current_orders": 1, "revenue_days": 1,
        })

    def test_row_error_keeps_original_cause(self):
        self.write([["a", "c1", "2026-01-01", "100", "shipped", "2026-01-01T10:00:00Z"]])
        with self.assertRaisesRegex(ValueError, "Invalid CSV row 2") as caught:
            read_orders(self.source)
        self.assertIsInstance(caught.exception.__cause__, ValueError)
        self.assertIn("unsupported status", str(caught.exception.__cause__))

    def create_legacy_database(self):
        """The schema written by the pipeline before sales_channel existed (six columns)."""
        with closing(sqlite3.connect(self.database)) as connection:
            connection.executescript("""
                CREATE TABLE bronze_orders (
                    order_id TEXT NOT NULL,
                    customer_id TEXT NOT NULL,
                    order_date TEXT NOT NULL,
                    amount_cents INTEGER NOT NULL CHECK (amount_cents >= 0),
                    status TEXT NOT NULL CHECK (status IN ('paid', 'pending', 'cancelled', 'refunded')),
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (order_id, updated_at));
                CREATE TABLE silver_orders (
                    order_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, order_date TEXT NOT NULL,
                    amount_cents INTEGER NOT NULL, status TEXT NOT NULL, updated_at TEXT NOT NULL);
                CREATE TABLE daily_revenue (
                    order_date TEXT PRIMARY KEY, paid_orders INTEGER NOT NULL, revenue_cents INTEGER NOT NULL);
                INSERT INTO bronze_orders VALUES ('a','c1','2026-01-01',100,'paid','2026-01-01T10:00:00+00:00');
                INSERT INTO silver_orders VALUES ('a','c1','2026-01-01',100,'paid','2026-01-01T10:00:00+00:00');
                INSERT INTO daily_revenue VALUES ('2026-01-01', 1, 100);
            """)

    def database_state(self):
        with closing(sqlite3.connect(self.database)) as connection:
            state = {
                "schema": connection.execute(
                    "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
                ).fetchall(),
                "bronze_columns": connection.execute("PRAGMA table_info(bronze_orders)").fetchall(),
                "silver_columns": connection.execute("PRAGMA table_info(silver_orders)").fetchall(),
                "bronze_rows": connection.execute("SELECT * FROM bronze_orders ORDER BY 1, 6").fetchall(),
                "silver_rows": connection.execute("SELECT * FROM silver_orders ORDER BY 1").fetchall(),
            }
        state["gold_report"] = report(self.database)
        return state

    def test_rejected_import_leaves_legacy_database_unchanged(self):
        self.create_legacy_database()
        before = self.database_state()
        self.assertNotIn("sales_channel", [column[1] for column in before["bronze_columns"]])
        self.write([
            ["b", "c2", "2026-01-02", "200", "paid", "2026-01-02T10:00:00Z"],
            ["a", "c1", "2026-01-01", "999", "paid", "2026-01-01T10:00:00Z"],
        ])
        with self.assertRaisesRegex(ValueError, "Conflicting version for order a"):
            run(self.source, self.database)
        after = self.database_state()
        for key in before:
            with self.subTest(key=key):
                self.assertEqual(after[key], before[key])

    def test_successful_import_after_rejection_migrates_legacy_database(self):
        self.create_legacy_database()
        self.write([["a", "c1", "2026-01-01", "999", "paid", "2026-01-01T10:00:00Z"]])
        with self.assertRaisesRegex(ValueError, "Conflicting version"):
            run(self.source, self.database)
        self.write([["b", "c2", "2026-01-02", "200", "paid", "2026-01-02T10:00:00Z"]])
        self.assertEqual(run(self.source, self.database), {
            "input_rows": 1, "bronze_versions": 2, "current_orders": 2, "revenue_days": 2,
        })
        state = self.database_state()
        for key in ("bronze_columns", "silver_columns"):
            self.assertEqual(state[key][-1][1:5], ("sales_channel", "TEXT", 1, "'unknown'"))
        self.assertEqual([row[-1] for row in state["bronze_rows"]], ["unknown", "unknown"])
        self.assertEqual(state["gold_report"], [
            {"order_date": "2026-01-01", "paid_orders": 1, "revenue_cents": 100},
            {"order_date": "2026-01-02", "paid_orders": 1, "revenue_cents": 200},
        ])

    def test_cli_propagates_unexpected_value_error(self):
        self.write([["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z"]])
        bug = ValueError("unexpected internal value bug")
        for command, target, argv in (
            ("run", "launchpad.pipeline.run", ("run", "--input", str(self.source), "--db", str(self.database))),
            ("report", "launchpad.pipeline.report", ("report", "--db", str(self.database))),
        ):
            with self.subTest(command=command), mock.patch(target, side_effect=bug):
                with self.assertRaises(ValueError) as caught:
                    self.run_cli(*argv)
                self.assertIs(caught.exception, bug)

    def test_cli_converts_pipeline_input_error(self):
        self.write([["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z"]])
        error = PipelineInputError("expected input problem")
        with mock.patch("launchpad.pipeline.run", side_effect=error):
            code, out, err = self.run_cli("run", "--input", str(self.source), "--db", str(self.database))
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertIn("error: expected input problem", err)
        self.assertNotIn("Traceback", err)

    def test_cli_non_utf8_csv_is_actionable(self):
        header = ",".join(HEADER).encode()
        for name, content in {
            "row": header + b"\na,caf\xe9,2026-01-01,100,paid,2026-01-01T10:00:00Z\n",
            "header": b"order_id\xe9" + header[len("order_id"):] + b"\n",
        }.items():
            with self.subTest(name=name):
                self.source.write_bytes(content)
                code, out, err = self.run_cli("run", "--input", str(self.source), "--db", str(self.database))
                self.assertEqual(code, 2)
                self.assertEqual(out, "")
                self.assertIn("UTF-8", err)
                self.assertIn(str(self.source), err)
                self.assertNotIn("Traceback", err)
                self.assertFalse(self.database.exists())

    def test_expected_input_failures_raise_pipeline_input_error(self):
        valid = ["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z"]
        self.write([valid])
        run(self.source, self.database)
        changed = valid[:3] + ["999"] + valid[4:]
        cases = {
            "header": ("order_id,customer\na,c1\n", "CSV must contain exactly these columns"),
            "row": ([valid[:4] + ["shipped", valid[5]]], "Invalid CSV row 2"),
            "empty": ([], "CSV contains no orders"),
            "conflict in file": ([valid, changed], "Conflicting version"),
            "conflict with database": ([changed], "Conflicting version"),
        }
        for name, (content, message) in cases.items():
            with self.subTest(name=name):
                if isinstance(content, str):
                    self.source.write_text(content, encoding="utf-8")
                else:
                    self.write(content)
                with self.assertRaisesRegex(PipelineInputError, message) as caught:
                    run(self.source, self.database)
                self.assertIsInstance(caught.exception, ValueError)  # direct callers keep working

    def test_cli_does_not_convert_unexpected_errors(self):
        self.write([["a", "c1", "2026-01-01", "100", "paid", "2026-01-01T10:00:00Z"]])
        with mock.patch("launchpad.pipeline.run", side_effect=RuntimeError("unexpected bug")):
            with self.assertRaisesRegex(RuntimeError, "unexpected bug"):
                self.run_cli("run", "--input", str(self.source), "--db", str(self.database))


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
                    "INSERT INTO bronze_orders "
                    "(order_id, customer_id, order_date, amount_cents, status, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    ("seed", "c0", "2025-12-31", 50, "paid", "2025-12-31T09:00:00+00:00"),
                )
                connection.execute(
                    "INSERT INTO silver_orders "
                    "(order_id, customer_id, order_date, amount_cents, status, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
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

    def test_conflicting_versions_in_one_file_are_rejected(self):
        self.write_rows([self.VALID_ROW, ["a", "c1", "2026-01-01", "999", "paid", "2026-01-01T10:00:00Z"]])
        self.assertEqual(len(read_orders(self.source)), 2)  # parsing alone does not compare rows
        self.assert_rejected(r"^Conflicting version for order a at 2026-01-01T10:00:00\+00:00$")


if __name__ == "__main__":
    unittest.main()
