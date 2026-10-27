-- Tiny synthetic data for docs/interviews.md newcomer prompts 1 and 2.
-- Money is stored as integer cents.
CREATE TABLE customers (
  customer_id INTEGER PRIMARY KEY,
  name        TEXT NOT NULL
);

CREATE TABLE orders (
  order_id          INTEGER PRIMARY KEY,           -- grain: one row per order
  customer_id       INTEGER NOT NULL REFERENCES customers(customer_id),
  order_total_cents INTEGER NOT NULL
);

CREATE TABLE order_items (
  order_id          INTEGER NOT NULL REFERENCES orders(order_id),
  line_no           INTEGER NOT NULL,
  sku               TEXT NOT NULL,
  line_amount_cents INTEGER NOT NULL,
  PRIMARY KEY (order_id, line_no)                   -- grain: one row per order line
);

INSERT INTO customers VALUES (1, 'Asha'), (2, 'Ben'), (3, 'Chen'), (4, 'Dara');

INSERT INTO orders VALUES (101, 1, 1500), (102, 1, 800), (103, 2, 2000);

INSERT INTO order_items VALUES
  (101, 1, 'pen',   500), (101, 2, 'book', 1000),
  (102, 1, 'mug',   800),
  (103, 1, 'lamp', 1200), (103, 2, 'cable', 500), (103, 3, 'bulb', 300);