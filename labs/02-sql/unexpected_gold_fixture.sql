-- Synthetic, reduced tables: only columns needed for this exercise.
CREATE TABLE silver_orders (
    order_id TEXT PRIMARY KEY,
    order_date TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    status TEXT NOT NULL
);
CREATE TABLE daily_revenue (
    order_date TEXT PRIMARY KEY,
    paid_orders INTEGER NOT NULL,
    revenue_cents INTEGER NOT NULL
);

INSERT INTO silver_orders VALUES
    ('example-1', '2026-01-01', 100, 'paid'),
    ('example-2', '2026-01-01', 200, 'paid'),
    ('example-3', '2026-01-02', 500, 'pending');
INSERT INTO daily_revenue VALUES
    ('2026-01-01', 2, 300),
    ('2099-01-01', 1, 999);
