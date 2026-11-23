"""A small, deterministic batch pipeline using only the Python standard library."""

import argparse
import csv
import json
import sqlite3
from contextlib import closing
from datetime import date, datetime, timezone
from pathlib import Path

FIELDS = ("order_id", "customer_id", "order_date", "amount_cents", "status", "updated_at")
MAX_AMOUNT_CENTS = 2**63 - 1
OPTIONAL_FIELDS = ("sales_channel",)
STATUSES = {"paid", "pending", "cancelled", "refunded"}


class PipelineInputError(ValueError):
    """Invalid CSV input or a conflicting order version: the user can fix the input.

    It subclasses ValueError so callers that already catch ValueError keep working.
    """


def parse_as_of(value: str) -> datetime:
    """Parse a timezone-aware CLI timestamp and normalize it to UTC."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "--as-of must be an ISO 8601 timestamp with a timezone"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("--as-of must include a timezone")
    return parsed.astimezone(timezone.utc)


def read_orders(path: Path) -> list[tuple[str, str, str, int, str, str, str]]:
    """Parse and validate the entire input before opening a write transaction."""
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if (
            reader.fieldnames is None
            or len(reader.fieldnames) != len(set(reader.fieldnames))
            or not set(FIELDS).issubset(reader.fieldnames)
            or set(reader.fieldnames) - set(FIELDS) - set(OPTIONAL_FIELDS)
        ):
            expected = ", ".join((*FIELDS, "[sales_channel]"))
            raise PipelineInputError(f"CSV must contain exactly these columns (sales_channel is optional): {expected}")
        rows = []
        for number, raw in enumerate(reader, start=2):
            try:
                if None in raw:
                    raise ValueError(f"expected {len(reader.fieldnames)} fields, found more")
                order_id = raw["order_id"].strip()
                customer_id = raw["customer_id"].strip()
                order_date = date.fromisoformat(raw["order_date"].strip()).isoformat()
                amount_text = raw["amount_cents"].strip()
                if not amount_text.isascii() or not amount_text.isdecimal():
                    raise ValueError("amount_cents must be a non-negative integer")
                # Compare digits before int() so arbitrarily long inputs get the
                # same storage-range error, without hitting Python's digit limit.
                amount_digits = amount_text.lstrip("0") or "0"
                maximum = str(MAX_AMOUNT_CENTS)
                if len(amount_digits) > len(maximum) or (
                    len(amount_digits) == len(maximum) and amount_digits > maximum
                ):
                    raise ValueError(f"amount_cents must be between 0 and {MAX_AMOUNT_CENTS}")
                amount_cents = int(amount_digits)
                status = raw["status"].strip().lower()
                sales_channel = (raw.get("sales_channel") or "").strip() or "unknown"
                timestamp = datetime.fromisoformat(raw["updated_at"].strip())
                if timestamp.tzinfo is None:
                    raise ValueError("updated_at must include a timezone")
                updated_at = timestamp.astimezone(timezone.utc).isoformat()
                if not order_id or not customer_id or status not in STATUSES:
                    raise ValueError("missing ID or unsupported status")
                if timestamp.date() < date.fromisoformat(order_date):
                    raise ValueError("updated_at precedes order_date")
                rows.append((order_id, customer_id, order_date, amount_cents, status, updated_at, sales_channel))
            except (KeyError, AttributeError, TypeError, ValueError) as exc:
                raise PipelineInputError(f"Invalid CSV row {number}: {exc}") from exc
    if not rows:
        raise PipelineInputError("CSV contains no orders")
    return rows


def initialize(connection: sqlite3.Connection) -> None:
    """Create or migrate the tables. Does not commit: the caller owns the transaction."""
    # One execute() per statement, not executescript(): executescript() commits any open
    # transaction first, which would make run()'s schema changes permanent before ingestion.
    for statement in (
        """
        CREATE TABLE IF NOT EXISTS bronze_orders (
            order_id TEXT NOT NULL,
            customer_id TEXT NOT NULL,
            order_date TEXT NOT NULL,
            amount_cents INTEGER NOT NULL CHECK (amount_cents >= 0),
            status TEXT NOT NULL CHECK (status IN ('paid', 'pending', 'cancelled', 'refunded')),
            updated_at TEXT NOT NULL,
            sales_channel TEXT NOT NULL DEFAULT 'unknown',
            PRIMARY KEY (order_id, updated_at)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS silver_orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            order_date TEXT NOT NULL,
            amount_cents INTEGER NOT NULL,
            status TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            sales_channel TEXT NOT NULL DEFAULT 'unknown'
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS daily_revenue (
            order_date TEXT PRIMARY KEY,
            paid_orders INTEGER NOT NULL,
            revenue_cents INTEGER NOT NULL
        )
        """,
    ):
        connection.execute(statement)
    # Keep databases created by the original six-column pipeline usable.
    for table in ("bronze_orders", "silver_orders"):
        columns = {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}
        if "sales_channel" not in columns:
            connection.execute(
                f"ALTER TABLE {table} ADD COLUMN sales_channel TEXT NOT NULL DEFAULT 'unknown'"
            )


def run(input_path: Path, database_path: Path) -> dict[str, int]:
    try:
        rows = read_orders(input_path)
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"Input CSV not found: {input_path}. Check the --input path "
            f"(the Lab 01 sample is labs/01-orders/orders.csv). Original error: {exc}"
        ) from exc
    except UnicodeDecodeError as exc:
        raise PipelineInputError(
            f"Input CSV is not valid UTF-8: {input_path}. Save it with UTF-8 encoding "
            f"and rerun. Original error: {exc}"
        ) from exc
    # Reject conflicting versions inside the file before creating the database:
    # same (order_id, updated_at) key, different values. Identical repeats pass.
    versions = {}
    for row in rows:
        if versions.setdefault((row[0], row[5]), row) != row:
            raise PipelineInputError(f"Conflicting version for order {row[0]} at {row[5]}")
    database_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        # The explicit BEGIN covers the schema migration (DDL) as well as the ingestion;
        # isolation_level=None keeps the sqlite3 module from adding its own BEGIN/COMMIT.
        # On any error `with connection` rolls everything back, leaving the file unchanged.
        with closing(sqlite3.connect(database_path, isolation_level=None)) as connection:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                initialize(connection)
                for row in rows:
                    existing = connection.execute(
                        "SELECT order_id, customer_id, order_date, amount_cents, status, updated_at, sales_channel "
                        "FROM bronze_orders WHERE order_id = ? AND updated_at = ?",
                        (row[0], row[5]),
                    ).fetchone()
                    if existing is not None:
                        if existing != row:
                            raise PipelineInputError(f"Conflicting version for order {row[0]} at {row[5]}")
                        continue
                    connection.execute(
                        "INSERT INTO bronze_orders "
                        "(order_id, customer_id, order_date, amount_cents, status, updated_at, sales_channel) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)", row
                    )

                connection.execute("DELETE FROM silver_orders")
                connection.execute(
                    """
                    INSERT INTO silver_orders
                    SELECT order_id, customer_id, order_date, amount_cents, status, updated_at, sales_channel
                    FROM (
                        SELECT *, ROW_NUMBER() OVER (
                            PARTITION BY order_id ORDER BY updated_at DESC
                        ) AS rank FROM bronze_orders
                    ) WHERE rank = 1
                    """
                )
                connection.execute("DELETE FROM daily_revenue")
                connection.execute(
                    """
                    INSERT INTO daily_revenue
                    SELECT order_date, COUNT(*), SUM(amount_cents)
                    FROM silver_orders WHERE status = 'paid'
                    GROUP BY order_date
                    """
                )
                bronze = connection.execute("SELECT COUNT(*) FROM bronze_orders").fetchone()[0]
                silver = connection.execute("SELECT COUNT(*) FROM silver_orders").fetchone()[0]
                gold = connection.execute("SELECT COUNT(*) FROM daily_revenue").fetchone()[0]
                if silver > bronze or gold > silver:
                    raise RuntimeError("Data quality check failed: invalid table counts")
    except sqlite3.DatabaseError as exc:
        raise type(exc)(
            f"Cannot use database {database_path} for run. Use a SQLite file path "
            f"created by the pipeline. Original error: {exc}"
        ) from exc
    return {"input_rows": len(rows), "bronze_versions": bronze, "current_orders": silver, "revenue_days": gold}


def report(database_path: Path) -> list[dict[str, int | str]]:
    if not database_path.is_file():
        raise FileNotFoundError(
            f"Database not found: {database_path}. Check the --db path, or create it first "
            "by running `python3 -m launchpad.pipeline run` with a valid CSV and this "
            "database path."
        )
    try:
        with closing(sqlite3.connect(database_path)) as connection:
            return [
                {"order_date": day, "paid_orders": count, "revenue_cents": cents}
                for day, count, cents in connection.execute(
                    "SELECT order_date, paid_orders, revenue_cents FROM daily_revenue ORDER BY order_date"
                )
            ]
    except sqlite3.DatabaseError as exc:
        raise type(exc)(
            f"Cannot read report database {database_path}. Use a SQLite file created by "
            f"`python3 -m launchpad.pipeline run`. Original error: {exc}"
        ) from exc


def check(database_path: Path, as_of: datetime) -> dict[str, object]:
    """Return deterministic freshness and gold reconciliation metrics without writing."""
    if not database_path.is_file():
        raise FileNotFoundError(
            f"Database not found: {database_path}. Check the --db path, or create it first "
            "by running `python3 -m launchpad.pipeline run` with a valid CSV and this "
            "database path."
        )
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise PipelineInputError("as_of must include a timezone")
    as_of_utc = as_of.astimezone(timezone.utc)

    try:
        uri = f"{database_path.resolve().as_uri()}?mode=ro"
        with closing(sqlite3.connect(uri, uri=True)) as connection:
            connection.execute("BEGIN")
            bronze_versions = connection.execute(
                "SELECT COUNT(*) FROM bronze_orders"
            ).fetchone()[0]
            current_orders = connection.execute(
                "SELECT COUNT(*) FROM silver_orders"
            ).fetchone()[0]
            paid_orders = connection.execute(
                "SELECT COUNT(*) FROM silver_orders WHERE status = 'paid'"
            ).fetchone()[0]
            maximum_updated_at = connection.execute(
                "SELECT MAX(updated_at) FROM bronze_orders"
            ).fetchone()[0]
            expected_rows = connection.execute(
                """
                SELECT order_date, COUNT(*), SUM(amount_cents)
                FROM silver_orders WHERE status = 'paid'
                GROUP BY order_date ORDER BY order_date
                """
            ).fetchall()
            actual_rows = connection.execute(
                """
                SELECT order_date, paid_orders, revenue_cents
                FROM daily_revenue ORDER BY order_date
                """
            ).fetchall()
    except sqlite3.DatabaseError as exc:
        raise type(exc)(
            f"Cannot check database {database_path}. Use a SQLite file created by "
            f"`python3 -m launchpad.pipeline run`. Original error: {exc}"
        ) from exc

    expected = {
        day: {"paid_orders": count, "revenue_cents": cents}
        for day, count, cents in expected_rows
    }
    actual = {
        day: {"paid_orders": count, "revenue_cents": cents}
        for day, count, cents in actual_rows
    }
    missing_dates = [
        {"order_date": day, "expected": expected[day]}
        for day in sorted(expected.keys() - actual.keys())
    ]
    extra_dates = [
        {"order_date": day, "actual": actual[day]}
        for day in sorted(actual.keys() - expected.keys())
    ]
    mismatched_dates = [
        {"order_date": day, "expected": expected[day], "actual": actual[day]}
        for day in sorted(expected.keys() & actual.keys())
        if expected[day] != actual[day]
    ]
    reconciliation_passed = not (missing_dates or extra_dates or mismatched_dates)

    freshness_age_seconds = None
    if maximum_updated_at is not None:
        try:
            maximum_timestamp = datetime.fromisoformat(maximum_updated_at)
        except ValueError as exc:
            raise PipelineInputError(
                f"bronze_orders contains an invalid updated_at value: {maximum_updated_at}"
            ) from exc
        if maximum_timestamp.tzinfo is None or maximum_timestamp.utcoffset() is None:
            raise PipelineInputError(
                f"bronze_orders contains an updated_at without a timezone: {maximum_updated_at}"
            )
        freshness_age_seconds = int(
            (as_of_utc - maximum_timestamp.astimezone(timezone.utc)).total_seconds()
        )

    return {
        "schema_version": 1,
        "status": "pass" if reconciliation_passed else "fail",
        "as_of": as_of_utc.isoformat(),
        "counts": {
            "bronze_versions": bronze_versions,
            "current_orders": current_orders,
            "paid_orders": paid_orders,
            "revenue_days": len(actual_rows),
        },
        "freshness": {
            "maximum_source_updated_at": maximum_updated_at,
            "age_seconds": freshness_age_seconds,
        },
        "reconciliation": {
            "passed": reconciliation_passed,
            "missing_dates": missing_dates,
            "extra_dates": extra_dates,
            "mismatched_dates": mismatched_dates,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_command = subparsers.add_parser("run", help="ingest and transform CSV orders")
    run_command.add_argument("--input", type=Path, required=True)
    run_command.add_argument("--db", type=Path, required=True)
    report_command = subparsers.add_parser("report", help="show daily paid revenue")
    report_command.add_argument("--db", type=Path, required=True)
    check_command = subparsers.add_parser(
        "check", help="report deterministic reconciliation and freshness metrics"
    )
    check_command.add_argument("--db", type=Path, required=True)
    check_command.add_argument("--as-of", type=parse_as_of, required=True)
    args = parser.parse_args()
    try:
        if args.command == "run":
            result = run(args.input, args.db)
        elif args.command == "report":
            result = report(args.db)
        else:
            result = check(args.db, args.as_of)
    except (FileNotFoundError, sqlite3.DatabaseError, PipelineInputError) as exc:
        parser.error(str(exc))
    print(json.dumps(result, indent=2))
    if args.command == "check" and not result["reconciliation"]["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
