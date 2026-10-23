WITH expected AS (
    SELECT DISTINCT order_date FROM silver_orders WHERE status = 'paid'
)
SELECT g.order_date, g.paid_orders, g.revenue_cents
FROM daily_revenue g LEFT JOIN expected e USING (order_date)
WHERE e.order_date IS NULL
ORDER BY g.order_date;
