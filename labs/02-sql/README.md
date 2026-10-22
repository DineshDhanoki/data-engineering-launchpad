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
