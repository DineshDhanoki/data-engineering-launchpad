# Interview practice bank

These are representative practice prompts, **not** claims about a company's actual or most-frequently-asked interview questions. For every answer: state assumptions, show a small example, discuss edge cases, and explain how you would verify it.

## Newcomer

1. What is the grain of a table? How can an incorrect grain double-count revenue? *Strong answer:* identify the unique business key and demonstrate a one-to-many join trap. See [worked example](#worked-example-grain-and-revenue-double-counting-newcomer-1).
2. Write SQL to find customers with no orders. *Strong answer:* `LEFT JOIN ... IS NULL` or `NOT EXISTS`, with null behavior explained. See [worked example](#worked-example-customers-with-no-orders-newcomer-2).
3. Explain `WHERE` vs `HAVING`. *Strong answer:* row filtering vs aggregate-group filtering and a concrete query.
4. What is an idempotent pipeline? *Strong answer:* same input and run key produce the same result after retries; demonstrate with Lab 01.
5. Why use integer cents for money in this lab? *Strong answer:* avoid binary floating-point rounding; mention decimals and currency rules in production.
6. When would you choose a batch load instead of a stream? *Strong answer:* latency needs, complexity, cost, and failure recovery.

### Worked examples (newcomer 1 and 2)

Both examples use the tiny synthetic fixture in [`sql/newcomer_joins_fixture.sql`](sql/newcomer_joins_fixture.sql) (customers, orders, order_items; money in integer cents). The SQL is written for SQLite.

```bash
sqlite3 demo.db < docs/sql/newcomer_joins_fixture.sql
sqlite3 demo.db
```

| Table | Grain (one row per) | Key |
|---|---|---|
| `customers` | customer | `customer_id` |
| `orders` | order | `order_id` |
| `order_items` | order line | `(order_id, line_no)` |

#### Worked example: grain and revenue double-counting (newcomer 1)

**Assumptions:** `order_total_cents` is the full value of one order and is stored once per order. `order_items` has one or more lines per order.

**Common wrong query:** join orders to their items, then sum the order-level total.

```sql
SELECT SUM(o.order_total_cents) AS revenue_cents
FROM orders o
JOIN order_items i ON i.order_id = o.order_id;
```

| revenue_cents |
|---|
| 9800 |

This is wrong. The join changes the grain from one row per order to one row per order line, so each order total is repeated once per line: order 101 (2 lines) is counted twice and order 103 (3 lines) three times. The same mistake per customer gives Asha 3800 and Ben 6000 instead of 2300 and 2000.

**Correct query:** sum the measure at its own grain.

```sql
SELECT SUM(order_total_cents) AS revenue_cents
FROM orders;
```

| revenue_cents |
|---|
| 4300 |

If you also need item details, aggregate `order_items` to one row per `order_id` first, then join that result to `orders`, so the join stays one-to-one.

**How to verify:**

- Confirm the key really is unique; this should return no rows:
  ```sql
  SELECT order_id, COUNT(*) FROM orders GROUP BY order_id HAVING COUNT(*) > 1;
  ```
- Compare row counts before and after the join (3 orders vs 6 joined rows). A jump means the join fans out.
- Cross-check with the line-level measure: `SELECT SUM(line_amount_cents) FROM order_items;` also returns 4300.

#### Worked example: customers with no orders (newcomer 2)

**Assumptions:** `customers.customer_id` is unique and every order has a non-null `customer_id`.

**Correct query (anti-join):**

```sql
SELECT c.customer_id, c.name
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.customer_id
WHERE o.order_id IS NULL
ORDER BY c.customer_id;
```

`NOT EXISTS` returns the same rows:

```sql
SELECT c.customer_id, c.name
FROM customers c
WHERE NOT EXISTS (
  SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id
)
ORDER BY c.customer_id;
```

| customer_id | name |
|---|---|
| 3 | Chen |
| 4 | Dara |

**Common wrong query:** using an `INNER JOIN` with the same `WHERE o.order_id IS NULL`. The inner join has already dropped customers without a match, so it returns zero rows.

**Edge cases and NULL behavior:**

- The left join fills the order columns with NULL when there is no match, which is why the check is `o.order_id IS NULL` (a non-null key column), not `= NULL`.
- `NOT IN (SELECT customer_id FROM orders)` returns no rows at all if any `customer_id` in the subquery is NULL. `NOT EXISTS` does not have this problem.
- A filter on `orders` (for example, only orders from one year) belongs in the `ON` clause. Putting it in `WHERE` turns the left join back into an inner join.

**How to verify:** customers with orders plus customers without orders should equal all customers (2 + 2 = 4), and no customer should appear in both lists.

#### Rubric (both examples)

A strong answer:

- names the grain and key of each table before joining
- explains NULL behavior: why `IS NULL` follows a left join and why `NOT IN` is risky with NULLs
- shows the wrong total, the correct total, and why aggregating at the measure's grain fixes it
- proves the result with a check query (key uniqueness, row counts, or totals that reconcile)

References: [SQLite SELECT](https://www.sqlite.org/lang_select.html), [NULL handling in SQLite](https://www.sqlite.org/nulls.html)

## Around 2 years

1. An incremental load missed updates arriving three days late. How would you repair it? *Strong answer:* bounded lookback or CDC, idempotent merge, backfill, reconciliation.
2. A scheduled job reruns after failing halfway through. How do you prevent duplicate data? *Strong answer:* transaction or atomic publish, stable keys, upsert, and retry tests.
3. How would you test a transformation pipeline? *Strong answer:* schema, uniqueness, null, relationships, business invariants, fixtures, CI, and production monitoring.
4. What causes a slowly changing dimension Type 2 table to be wrong? *Strong answer:* overlapping validity intervals, tie-breaking, late events, and current-row uniqueness.
5. Your dashboard differs from a finance report. How do you investigate? *Strong answer:* agree grain and definition, compare filters/time zones, trace lineage, reconcile counts.
6. Which operational metrics would you collect for Lab 01? *Strong answer:* freshness, input/output/reject counts, lag, runtime, and alert thresholds.

## Around 4 years

1. Design a CDC pipeline from an OLTP database to analytics. *Strong answer:* snapshot + log handoff, ordering, deduplication, schema evolution, replay, and recovery.
2. A warehouse query became 10x slower. Diagnose it. *Strong answer:* query plan, scan size, join cardinality, skew, partitions/clustering, and measured changes.
3. Compare a star schema with a wide denormalized table. *Strong answer:* workload, governance, semantic consistency, performance, and maintenance trade-offs.
4. How do you set a data product SLO? *Strong answer:* consumer need, freshness/availability/correctness, error budget, ownership, and alert actionability.
5. A team wants to stream every source. What questions do you ask? *Strong answer:* required latency, correctness semantics, cost, operational capacity, and failure modes.

## Around 6+ years

1. Migrate a critical pipeline without breaking consumers. *Strong answer:* inventory, contracts, dual-run comparison, staged cutover, rollback, ownership.
2. A privacy incident exposed sensitive fields in analytics. Lead the response. *Strong answer:* containment, access audit, deletion/retention, communication, root cause, preventive controls.
3. Choose between managed warehouse, lakehouse, and custom streaming stack. *Strong answer:* workloads, team skills, governance, portability, total cost, and reversible decisions.
4. How do you prioritize reliability work against feature requests? *Strong answer:* quantify incident impact, SLOs, risk, effort, and stakeholder alignment.
5. Review a junior engineer's pipeline that passes tests but fails on replays. *Strong answer:* reproduce, teach deterministic keys/atomicity, add regression tests, and improve review checklist.

Contribute more questions with a level, example data, evaluation rubric, and links for any tool-specific claims. Avoid publishing confidential interview material.