"""Run the contributor checks and verify the Lab 01 sample without touching user data."""

import argparse
import json
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COUNTS = {"input_rows": 4, "bronze_versions": 4, "current_orders": 3, "revenue_days": 2}
REVENUE = [
    {"order_date": "2026-01-02", "paid_orders": 2, "revenue_cents": 3799},
    {"order_date": "2026-01-03", "paid_orders": 1, "revenue_cents": 1999},
]


def check_stage(name: str, args: list[str], expected: object = None) -> int:
    command = [sys.executable, "-m", *args]
    print(f"Running {name}...", flush=True)
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)
    if result.returncode:
        print(f"FAIL {name} (exit {result.returncode}): {shlex.join(command)}", file=sys.stderr)
        if result.stdout:
            print(result.stdout, end="", file=sys.stderr)
        if result.stderr:
            print(result.stderr, end="", file=sys.stderr)
        return result.returncode
    if expected is not None:
        try:
            actual = json.loads(result.stdout)
        except json.JSONDecodeError:
            print(f"FAIL {name}: expected JSON from {shlex.join(command)}", file=sys.stderr)
            return 1
        if actual != expected:
            print(
                f"FAIL {name}: expected {expected!r}, got {actual!r}\n{shlex.join(command)}",
                file=sys.stderr,
            )
            return 1
    print(f"PASS {name}")
    return 0


def main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(description=__doc__).parse_args(argv)
    with tempfile.TemporaryDirectory(prefix="launchpad-preflight-") as directory:
        database = str(Path(directory) / "orders.db")
        ingest = [
            "launchpad.pipeline", "run", "--input", "labs/01-orders/orders.csv", "--db", database,
        ]
        stages = [
            ("unit tests", ["unittest", "discover", "-s", "tests", "-v"], None),
            ("Lab 01 ingest", ingest, COUNTS),
            ("Lab 01 replay", ingest, COUNTS),
            ("Lab 01 revenue report", ["launchpad.pipeline", "report", "--db", database], REVENUE),
        ]
        for name, args, expected in stages:
            exit_code = check_stage(name, args, expected)
            if exit_code:
                return exit_code
    print("Preflight passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
