import contextlib
import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime
from pathlib import Path
from unittest import mock

from launchpad.pipeline import check, main, run

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "labs" / "01-orders" / "orders.csv"
AS_OF = datetime.fromisoformat("2026-01-04T12:00:00+00:00")


class ReconciliationMetricsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "orders.db"
        run(SAMPLE, self.database)

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

    def test_healthy_database_reports_deterministic_counts_and_freshness(self):
        result = check(self.database, AS_OF)

        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["as_of"], "2026-01-04T12:00:00+00:00")
        self.assertEqual(result["counts"], {
            "bronze_versions": 4,
            "current_orders": 3,
            "paid_orders": 3,
            "revenue_days": 2,
        })
        self.assertEqual(result["freshness"], {
            "maximum_source_updated_at": "2026-01-03T12:00:00+00:00",
            "age_seconds": 86400,
        })
        self.assertEqual(result["reconciliation"], {
            "passed": True,
            "missing_dates": [],
            "extra_dates": [],
            "mismatched_dates": [],
        })

    def test_timezone_offset_as_of_is_normalized_before_freshness_calculation(self):
        result = check(self.database, datetime.fromisoformat("2026-01-04T17:30:00+05:30"))

        self.assertEqual(result["as_of"], "2026-01-04T12:00:00+00:00")
        self.assertEqual(result["freshness"]["age_seconds"], 86400)

    def test_missing_gold_date_is_identified_and_check_is_read_only(self):
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("DELETE FROM daily_revenue WHERE order_date = '2026-01-02'")
        before = self.database.read_bytes()

        result = check(self.database, AS_OF)

        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["reconciliation"]["missing_dates"], [{
            "order_date": "2026-01-02",
            "expected": {"paid_orders": 2, "revenue_cents": 3799},
        }])
        self.assertEqual(result["reconciliation"]["extra_dates"], [])
        self.assertEqual(result["reconciliation"]["mismatched_dates"], [])
        self.assertEqual(self.database.read_bytes(), before)

    def test_extra_gold_date_is_identified(self):
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("INSERT INTO daily_revenue VALUES ('2099-01-01', 1, 999)")

        result = check(self.database, AS_OF)

        self.assertEqual(result["reconciliation"]["extra_dates"], [{
            "order_date": "2099-01-01",
            "actual": {"paid_orders": 1, "revenue_cents": 999},
        }])

    def test_mismatched_gold_values_are_identified(self):
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "UPDATE daily_revenue SET paid_orders = 99, revenue_cents = 1 "
                "WHERE order_date = '2026-01-03'"
            )

        result = check(self.database, AS_OF)

        self.assertEqual(result["reconciliation"]["mismatched_dates"], [{
            "order_date": "2026-01-03",
            "expected": {"paid_orders": 1, "revenue_cents": 1999},
            "actual": {"paid_orders": 99, "revenue_cents": 1},
        }])

    def test_failed_cli_check_prints_json_exits_one_and_does_not_modify_database(self):
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("DELETE FROM daily_revenue WHERE order_date = '2026-01-02'")
        before = self.database.read_bytes()

        code, output, error = self.run_cli(
            "check", "--db", str(self.database), "--as-of", AS_OF.isoformat()
        )

        self.assertEqual(code, 1)
        self.assertEqual(error, "")
        self.assertEqual(json.loads(output)["status"], "fail")
        self.assertEqual(self.database.read_bytes(), before)

    def test_cli_requires_timezone_aware_as_of(self):
        code, output, error = self.run_cli(
            "check", "--db", str(self.database), "--as-of", "2026-01-04T12:00:00"
        )

        self.assertEqual(code, 2)
        self.assertEqual(output, "")
        self.assertIn("--as-of must include a timezone", error)


if __name__ == "__main__":
    unittest.main()
