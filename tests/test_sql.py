import sqlite3
import unittest
from pathlib import Path


LAB = Path(__file__).resolve().parents[1] / "labs" / "02-sql"


class UnexpectedGoldTests(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.addCleanup(self.connection.close)
        self.connection.executescript(
            (LAB / "unexpected_gold_fixture.sql").read_text(encoding="utf-8")
        )
        self.query = (LAB / "unexpected_gold.sql").read_text(encoding="utf-8")

    def test_finds_the_extra_gold_date_without_returning_the_matching_date(self):
        self.assertEqual(self.connection.execute(self.query).fetchall(), [
            ("2099-01-01", 1, 999),
        ])

    def test_removing_the_extra_row_leaves_no_unexpected_dates(self):
        self.connection.execute("DELETE FROM daily_revenue WHERE order_date = '2099-01-01'")
        self.assertEqual(self.connection.execute(self.query).fetchall(), [])

    def test_a_date_with_only_unpaid_orders_is_still_unexpected_in_gold(self):
        self.connection.execute("INSERT INTO daily_revenue VALUES ('2026-01-02', 1, 500)")
        self.assertEqual(self.connection.execute(self.query).fetchall(), [
            ("2026-01-02", 1, 500),
            ("2099-01-01", 1, 999),
        ])


if __name__ == "__main__":
    unittest.main()
