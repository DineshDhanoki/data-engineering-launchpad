# Lab 02: SQL for data engineers

Prerequisite: complete [Lab 01](../01-orders/README.md) and keep its SQLite database. This lab practices grain, aggregation, window functions, and reconciliation with the same synthetic data.

## Exercises

1. Write a query that returns one row per current order. What is its grain? Which column is the key?
2. Calculate total paid revenue by `order_date`, in cents. Compare the result with `daily_revenue`.
3. Find customers with more than one current order. Why should you not join `bronze_orders` directly to customers for this question?
4. Using `ROW_NUMBER`, select the latest version of each order from `bronze_orders`. What happens if two versions share the same `updated_at`?
5. Find dates where the gold table disagrees with a fresh aggregation of `silver_orders`. Your result should be empty for the sample data.

## Reference queries

Try the exercises first. The queries below are examples, not the only valid answers.

```sql
-- 1. One row per order_id.
SELECT order_id, customer_id, status FROM silver_orders ORDER BY order_id;

-- 2. The WHERE clause precedes grouping.
SELECT order_date, SUM(amount_cents) AS revenue_cents
FROM silver_orders WHERE status = 'paid'
GROUP BY order_date ORDER BY order_date;

-- 3. COUNT(*) counts orders only because silver_orders has one row per order.
SELECT customer_id, COUNT(*) AS orders
FROM silver_orders GROUP BY customer_id HAVING COUNT(*) > 1;

-- 4. A source version needs an unambiguous ordering key.
SELECT * FROM (
  SELECT order_id, status, updated_at,
         ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
  FROM bronze_orders
) WHERE rn = 1;

-- 5. Both missing gold rows and mismatched totals should be visible.
WITH expected AS (
  SELECT order_date, COUNT(*) AS paid_orders, SUM(amount_cents) AS revenue_cents
  FROM silver_orders WHERE status = 'paid' GROUP BY order_date
)
SELECT e.order_date, e.paid_orders, e.revenue_cents, g.paid_orders, g.revenue_cents
FROM expected e LEFT JOIN daily_revenue g USING (order_date)
WHERE g.order_date IS NULL OR e.paid_orders <> g.paid_orders
   OR e.revenue_cents <> g.revenue_cents;
```

For a real reconciliation, also check for **extra** rows in `daily_revenue` with a second anti-join or a full outer join where supported.

## Exercise 6: Find an unexpected gold date

**Prerequisites:** Python 3.11+, basic `SELECT` and `LEFT JOIN`, and the grain of the
Lab 01 tables. Run the command below from the repository root. It uses a separate,
in-memory SQLite database; it does not change your Lab 01 database or need extra packages.

**Objectives:** distinguish a gold-only date from a missing or mismatched gold row,
and write an anti-join that starts from the gold table. `silver_orders` has one row
per `order_id`; `daily_revenue` has one row per `order_date`. Dates are non-null.

1. Inspect [the synthetic fixture](unexpected_gold_fixture.sql). It keeps only the
   columns needed here: two paid orders on `2026-01-01`, a pending order on
   `2026-01-02`, and an unexpected gold row on `2099-01-01`.
2. Before opening [the reference query](unexpected_gold.sql), write a query that
   returns gold dates with no matching **paid** silver orders. Include the gold
   counts and cents, ordered by date. Why does exercise 5's join miss these rows?
3. Run the reference query and then remove the extra row to check recovery:

```bash
python3 -c "
import sqlite3
from contextlib import closing
from pathlib import Path

lab = Path('labs/02-sql')
with closing(sqlite3.connect(':memory:')) as connection:
    connection.executescript((lab / 'unexpected_gold_fixture.sql').read_text(encoding='utf-8'))
    query = (lab / 'unexpected_gold.sql').read_text(encoding='utf-8')
    for row in connection.execute(query):
        print(*row, sep=' | ')
    connection.execute('DELETE FROM daily_revenue WHERE order_date = ?', ('2099-01-01',))
    print('After removal:', connection.execute(query).fetchall())
"
```

Expected output:

```text
2099-01-01 | 1 | 999
After removal: []
```

The reference query builds the distinct expected dates from paid silver orders.
Its `LEFT JOIN` preserves every gold row; `e.order_date IS NULL` selects those
without a matching expected date. Starting from `expected` instead would miss
the extra date. Omitting the null filter would also return the valid gold date.
A gold row on the pending-only date would be unexpected too.

**Cleanup:** leaving `closing(...)` closes and discards the in-memory database,
including on an exception. No database file is created. Run
`python3 -m unittest discover -s tests -v` to verify detection, removal and the
pending-only case against the same fixture and reference query.

**Limitations:** this check detects unexpected dates only. Keep exercise 5's
check for missing rows and incorrect totals; this query alone is not a complete
reconciliation. The reduced synthetic fixture does not test production volume,
concurrent writes, nullable join keys or other database dialects.

Primary sources: [SQLite LEFT JOIN and WHERE processing](https://www.sqlite.org/lang_select.html),
[SQLite in-memory databases](https://www.sqlite.org/inmemorydb.html), and
[Python's `contextlib.closing`](https://docs.python.org/3.11/library/contextlib.html#contextlib.closing).
Reviewed: 2026-10-23.
