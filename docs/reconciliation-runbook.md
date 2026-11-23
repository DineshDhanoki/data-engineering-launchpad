# Lab 01 reconciliation and freshness runbook

Use the read-only `check` command to compare `daily_revenue` with a fresh
aggregation of current paid orders. Supply an explicit, timezone-aware
`--as-of` value so freshness is reproducible and never depends on the machine
clock.

```bash
python3 -m launchpad.pipeline check \
  --db /tmp/launchpad-orders.db \
  --as-of 2026-01-04T12:00:00+00:00
```

For the committed sample, the exact output is:

```json
{
  "schema_version": 1,
  "status": "pass",
  "as_of": "2026-01-04T12:00:00+00:00",
  "counts": {
    "bronze_versions": 4,
    "current_orders": 3,
    "paid_orders": 3,
    "revenue_days": 2
  },
  "freshness": {
    "maximum_source_updated_at": "2026-01-03T12:00:00+00:00",
    "age_seconds": 86400
  },
  "reconciliation": {
    "passed": true,
    "missing_dates": [],
    "extra_dates": [],
    "mismatched_dates": []
  }
}
```

`maximum_source_updated_at` is the maximum event timestamp in bronze.
`age_seconds` is `as_of - maximum_source_updated_at`; it can be negative when
the database contains an event later than the declared observation time. This
command reports that value but does not impose a toy freshness threshold.
Timestamps are normalized to UTC in the output.

## Failure fields and response

| Failure field | Likely cause | Owner and action | Recovery verification |
| --- | --- | --- | --- |
| `missing_dates` | Gold rebuild stopped early, a row was manually deleted, or publication was incomplete. | Pipeline operator: validate the named silver date, rerun the original synthetic input, then rerun `check` with the same `--as-of`. | The date moves out of `missing_dates`; counts and the revenue report match the expected entry. |
| `extra_dates` | A stale/manual gold row remains, or unpaid silver state was not propagated. | Pipeline operator: inspect the named gold row and current silver statuses; replay from the trusted source rather than editing gold as the normal recovery. | The date moves out of `extra_dates`, and a second replay is unchanged. |
| `mismatched_dates` | Gold count or sum is stale, partially updated, or calculated with different filters. | Pipeline operator: compare the named expected/actual values, verify paid silver rows, then replay the pipeline. | The date moves out of `mismatched_dates` and the report matches a fresh SQL aggregation. |
| Negative `age_seconds` | `--as-of` predates the newest source event, or a source timestamp is unexpectedly in the future. | Data owner: verify the declared observation time and source clock before changing data. | Rerun with a justified observation time; record any source-clock correction separately. |

A reconciliation failure prints the complete JSON report and exits `1`.
Invalid arguments or an unreadable database exit `2`. A passing check exits
`0`. The command opens the database with SQLite `mode=ro`, starts a consistent
read transaction, and never creates, repairs, or updates tables.

## Safe failure practice

Work on a disposable copy, never the database you want to preserve:

```bash
cp /tmp/launchpad-orders.db /tmp/launchpad-orders-tampered.db
python3 -c "import sqlite3; c=sqlite3.connect('/tmp/launchpad-orders-tampered.db'); c.execute(\"DELETE FROM daily_revenue WHERE order_date='2026-01-02'\"); c.commit(); c.close()"
python3 -m launchpad.pipeline check --db /tmp/launchpad-orders-tampered.db --as-of 2026-01-04T12:00:00+00:00
```

The failed report identifies `2026-01-02` under `missing_dates`, expects two
paid orders and 3799 revenue cents, and exits `1`. Repeat with a copied
database and either an extra date or altered count/sum to exercise the other
two failure lists. Delete only the disposable files you created.

## Limits and references

This is a local teaching check, not a monitoring server, scheduler, alert,
production SLO, or substitute for source-to-target reconciliation. It checks
the current SQLite silver-to-gold boundary; it does not prove source
completeness, event-time correctness, or business validity. The sample is
synthetic and contains no personal data.

Primary references, reviewed 2026-11-23 (UTC):

- Python 3.11 [`sqlite3` URI mode](https://docs.python.org/3.11/library/sqlite3.html#how-to-work-with-sqlite-uris)
- Python 3.11 [`datetime.astimezone`](https://docs.python.org/3.11/library/datetime.html#datetime.datetime.astimezone)
