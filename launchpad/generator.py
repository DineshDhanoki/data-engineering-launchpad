"""Generate deterministic synthetic order-version fixtures for Lab 01."""

import argparse
import csv
import hashlib
import io
import json
import os
import tempfile
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from launchpad.pipeline import MAX_AMOUNT_CENTS, STATUSES

CHANNELS = ("web", "mobile", "store")
DEFAULT_STATUS_WEIGHTS = {"paid": 60, "pending": 15, "cancelled": 15, "refunded": 10}
DEFAULT_CHANNEL_WEIGHTS = {"web": 50, "mobile": 35, "store": 15}
INVALID_MODES = ("duplicate-header", "conflicting-event", "missing-timezone", "oversized-amount")
FIELDS = (
    "order_id", "customer_id", "order_date", "amount_cents", "status", "updated_at",
    "sales_channel",
)
MAX_ORDERS = 1_000_000
MAX_VERSIONS = 100
MAX_POTENTIAL_VERSIONS = 2_000_000


class GeneratorInputError(ValueError):
    """Invalid generator parameters that the caller can correct."""


def _number(seed: int, label: str, upper_bound: int) -> int:
    """Return a stable number without depending on random module implementation details."""
    digest = hashlib.sha256(f"launchpad-v1\0{seed}\0{label}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % upper_bound


def _validate_weights(
    weights: dict[str, int], allowed: set[str], description: str
) -> dict[str, int]:
    if not weights or set(weights) - allowed:
        unknown = sorted(set(weights) - allowed)
        detail = f"; unsupported values: {', '.join(unknown)}" if unknown else ""
        raise GeneratorInputError(f"{description} must contain supported values{detail}")
    if any(type(weight) is not int or weight < 0 for weight in weights.values()):
        raise GeneratorInputError(f"{description} weights must be non-negative integers")
    if sum(weights.values()) == 0:
        raise GeneratorInputError(f"{description} must contain at least one positive weight")
    return {name: weights[name] for name in sorted(weights)}


def parse_weights(value: str, allowed: set[str], description: str) -> dict[str, int]:
    """Parse NAME=WEIGHT comma lists used by the CLI."""
    parsed: dict[str, int] = {}
    try:
        for item in value.split(","):
            name, raw_weight = item.split("=", 1)
            name = name.strip()
            if name in parsed:
                raise ValueError(f"duplicate value: {name}")
            if not raw_weight.strip().isascii() or not raw_weight.strip().isdecimal():
                raise ValueError(f"weight for {name or '<empty>'} must be a non-negative integer")
            parsed[name] = int(raw_weight)
        return _validate_weights(parsed, allowed, description)
    except (GeneratorInputError, ValueError) as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("dates must use YYYY-MM-DD") from exc


def _weighted_choice(seed: int, label: str, weights: dict[str, int]) -> str:
    selected = _number(seed, label, sum(weights.values()))
    for name, weight in weights.items():
        if selected < weight:
            return name
        selected -= weight
    raise AssertionError("validated weights always select a value")


def _status_history(final_status: str, versions: int) -> list[str]:
    if versions == 1:
        return [final_status]
    if final_status == "pending":
        return ["pending"] * versions
    if final_status in {"paid", "cancelled"}:
        return ["pending"] * (versions - 1) + [final_status]
    if versions == 2:
        return ["paid", "refunded"]
    return ["pending"] * (versions - 2) + ["paid", "refunded"]


def _late_shuffle(rows: list[list[str]], seed: int, window: int) -> list[list[str]]:
    if window == 0:
        return rows
    shuffled: list[list[str]] = []
    block_size = window + 1
    for start in range(0, len(rows), block_size):
        block = rows[start:start + block_size]
        if len(block) > 1:
            shift = 1 + _number(seed, f"shuffle:{start}", len(block) - 1)
            block = block[shift:] + block[:shift]
        shuffled.extend(block)
    return shuffled


def _csv_bytes(header: list[str], rows: list[list[str]]) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8")


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def generate(
    *,
    seed: int,
    order_count: int,
    max_versions: int,
    start_date: date,
    end_date: date,
    output_path: Path,
    manifest_path: Path,
    status_weights: dict[str, int] | None = None,
    channel_weights: dict[str, int] | None = None,
    late_arrival_window: int = 0,
    invalid_mode: str | None = None,
) -> dict[str, object]:
    """Generate the CSV and manifest, returning the manifest document."""
    if not 1 <= order_count <= MAX_ORDERS:
        raise GeneratorInputError(f"order_count must be between 1 and {MAX_ORDERS}")
    if not 1 <= max_versions <= MAX_VERSIONS:
        raise GeneratorInputError(f"max_versions must be between 1 and {MAX_VERSIONS}")
    if order_count * max_versions > MAX_POTENTIAL_VERSIONS:
        raise GeneratorInputError(
            f"orders times max_versions must not exceed {MAX_POTENTIAL_VERSIONS}"
        )
    if start_date > end_date:
        raise GeneratorInputError("start_date must not be after end_date")
    if not 0 <= late_arrival_window <= MAX_POTENTIAL_VERSIONS:
        raise GeneratorInputError(
            f"late_arrival_window must be between 0 and {MAX_POTENTIAL_VERSIONS}"
        )
    try:
        datetime.combine(end_date, time(12), timezone.utc) + timedelta(hours=max_versions - 1)
    except OverflowError as exc:
        raise GeneratorInputError(
            "end_date is too late for the requested maximum version history"
        ) from exc
    if invalid_mode not in (None, *INVALID_MODES):
        raise GeneratorInputError(f"unsupported invalid_mode: {invalid_mode}")
    if output_path.resolve() == manifest_path.resolve():
        raise GeneratorInputError("output and manifest paths must be different")

    statuses = _validate_weights(
        DEFAULT_STATUS_WEIGHTS if status_weights is None else status_weights,
        set(STATUSES),
        "status distribution",
    )
    channels = _validate_weights(
        DEFAULT_CHANNEL_WEIGHTS if channel_weights is None else channel_weights,
        set(CHANNELS),
        "channel distribution",
    )
    day_count = (end_date - start_date).days + 1
    rows: list[list[str]] = []
    version_status_counts = {status: 0 for status in sorted(STATUSES)}
    final_status_counts = {status: 0 for status in sorted(STATUSES)}
    final_channel_counts = {channel: 0 for channel in sorted(CHANNELS)}
    daily_revenue: dict[str, dict[str, int]] = {}

    for index in range(1, order_count + 1):
        order_id = f"ord-{index:08d}"
        customer_pool = max(1, (order_count + 2) // 3)
        customer_id = f"cust-{1 + _number(seed, f'customer:{index}', customer_pool):08d}"
        order_day = start_date + timedelta(days=_number(seed, f"date:{index}", day_count))
        amount = 100 + _number(seed, f"amount:{index}", 99_901)
        version_count = 1 + _number(seed, f"versions:{index}", max_versions)
        final_status = _weighted_choice(seed, f"status:{index}", statuses)
        channel = _weighted_choice(seed, f"channel:{index}", channels)
        history = _status_history(final_status, version_count)
        for version, status in enumerate(history):
            version_status_counts[status] += 1
            updated_at = datetime.combine(order_day, time(12), timezone.utc) + timedelta(hours=version)
            rows.append([
                order_id,
                customer_id,
                order_day.isoformat(),
                str(amount),
                status,
                updated_at.isoformat(),
                channel,
            ])
        final_status_counts[final_status] += 1
        final_channel_counts[channel] += 1
        if final_status == "paid":
            aggregate = daily_revenue.setdefault(
                order_day.isoformat(), {"paid_orders": 0, "revenue_cents": 0}
            )
            aggregate["paid_orders"] += 1
            aggregate["revenue_cents"] += amount

    logical_version_count = len(rows)
    rows = _late_shuffle(rows, seed, late_arrival_window)
    header = list(FIELDS)
    expected_error = None
    if invalid_mode == "duplicate-header":
        header.insert(5, "status")
        rows = [row[:5] + [row[4]] + row[5:] for row in rows]
        expected_error = "CSV must contain exactly these columns"
    elif invalid_mode == "conflicting-event":
        conflict = rows[0].copy()
        conflict[3] = str(int(conflict[3]) + 1)
        rows.insert(1, conflict)
        expected_error = f"Conflicting version for order {conflict[0]} at {conflict[5]}"
    elif invalid_mode == "missing-timezone":
        rows[0][5] = rows[0][5].removesuffix("+00:00")
        expected_error = "updated_at must include a timezone"
    elif invalid_mode == "oversized-amount":
        rows[0][3] = str(MAX_AMOUNT_CENTS + 1)
        expected_error = f"amount_cents must be between 0 and {MAX_AMOUNT_CENTS}"

    csv_content = _csv_bytes(header, rows)
    manifest: dict[str, object] = {
        "schema_version": 1,
        "generator": "launchpad.generator.v1",
        "parameters": {
            "seed": seed,
            "order_count": order_count,
            "max_versions": max_versions,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "status_weights": statuses,
            "channel_weights": channels,
            "late_arrival_window": late_arrival_window,
            "invalid_mode": invalid_mode,
        },
        "expected": {
            "csv_rows": len(rows),
            "bronze_versions": logical_version_count,
            "distinct_orders": order_count,
            "version_status_counts": version_status_counts,
            "current_status_counts": final_status_counts,
            "current_channel_counts": final_channel_counts,
            "paid_revenue_cents": sum(item["revenue_cents"] for item in daily_revenue.values()),
            "daily_revenue": [
                {"order_date": day, **daily_revenue[day]} for day in sorted(daily_revenue)
            ],
        },
        "invalid_fixture": {
            "mode": invalid_mode,
            "expected_error": expected_error,
        } if invalid_mode else None,
        "csv_sha256": hashlib.sha256(csv_content).hexdigest(),
    }
    manifest_content = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")
    _atomic_write(output_path, csv_content)
    _atomic_write(manifest_path, manifest_content)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--orders", type=int, required=True, help=f"order count (1-{MAX_ORDERS})")
    parser.add_argument("--max-versions", type=int, required=True, help=f"per order (1-{MAX_VERSIONS})")
    parser.add_argument("--start-date", type=parse_date, required=True, help="inclusive YYYY-MM-DD")
    parser.add_argument("--end-date", type=parse_date, required=True, help="inclusive YYYY-MM-DD")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, help="default: <output>.manifest.json")
    parser.add_argument(
        "--status-weights",
        type=lambda value: parse_weights(value, set(STATUSES), "status distribution"),
        default=DEFAULT_STATUS_WEIGHTS,
        metavar="STATUS=WEIGHT,...",
        help="default: paid=60,pending=15,cancelled=15,refunded=10",
    )
    parser.add_argument(
        "--channel-weights",
        type=lambda value: parse_weights(value, set(CHANNELS), "channel distribution"),
        default=DEFAULT_CHANNEL_WEIGHTS,
        metavar="CHANNEL=WEIGHT,...",
        help="default: web=50,mobile=35,store=15",
    )
    parser.add_argument(
        "--late-arrival-window", type=int, default=0, help="maximum row displacement (default: 0)"
    )
    parser.add_argument(
        "--invalid",
        choices=INVALID_MODES,
        metavar="MODE",
        help="one of: " + ", ".join(INVALID_MODES),
    )
    args = parser.parse_args()
    manifest_path = args.manifest or Path(f"{args.output}.manifest.json")
    try:
        manifest = generate(
            seed=args.seed,
            order_count=args.orders,
            max_versions=args.max_versions,
            start_date=args.start_date,
            end_date=args.end_date,
            output_path=args.output,
            manifest_path=manifest_path,
            status_weights=args.status_weights,
            channel_weights=args.channel_weights,
            late_arrival_window=args.late_arrival_window,
            invalid_mode=args.invalid,
        )
    except (GeneratorInputError, OSError) as exc:
        parser.error(str(exc))
    print(json.dumps({
        "output": str(args.output),
        "manifest": str(manifest_path),
        "csv_sha256": manifest["csv_sha256"],
        "csv_rows": manifest["expected"]["csv_rows"],
    }, indent=2))


if __name__ == "__main__":
    main()
