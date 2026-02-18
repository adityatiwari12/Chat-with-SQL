-- File: db/create_tables.sql
-- Purpose: Re-runnable DDL for Chat with SQL system (Streamlit App)

BEGIN;

-- 1. Drop tables in reverse dependency order to avoid FK errors
DROP TABLE IF EXISTS payments CASCADE;
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- 2. Create Tables

CREATE TABLE customers (
    customer_id  SERIAL PRIMARY KEY,
    name         VARCHAR(100) NOT NULL,
    email        VARCHAR(150) UNIQUE NOT NULL,
    country      VARCHAR(60) NOT NULL,
    created_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE products (
    product_id      SERIAL PRIMARY KEY,
    product_name    VARCHAR(150) NOT NULL,
    category        VARCHAR(60) NOT NULL,
    price           NUMERIC(10,2) NOT NULL CHECK (price > 0),
    stock_quantity  INTEGER NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0)
);

CREATE TABLE orders (
    order_id      SERIAL PRIMARY KEY,
    customer_id   INTEGER NOT NULL REFERENCES customers(customer_id),
    order_date    DATE NOT NULL,
    status        VARCHAR(20) NOT NULL CHECK (
                    status IN ('pending','processing',
                               'shipped','delivered','cancelled')
                  ),
    total_amount  NUMERIC(12,2) NOT NULL DEFAULT 0.00 CHECK (total_amount >= 0)
);

CREATE TABLE order_items (
    item_id     SERIAL PRIMARY KEY,
    order_id    INTEGER NOT NULL REFERENCES orders(order_id),
    product_id  INTEGER NOT NULL REFERENCES products(product_id),
    quantity    INTEGER NOT NULL CHECK (quantity > 0),
    unit_price  NUMERIC(10,2) NOT NULL CHECK (unit_price > 0)
);

CREATE TABLE payments (
    payment_id    SERIAL PRIMARY KEY,
    order_id      INTEGER NOT NULL REFERENCES orders(order_id),
    payment_date  DATE NOT NULL,
    amount        NUMERIC(12,2) NOT NULL CHECK (amount > 0),
    method        VARCHAR(30) NOT NULL CHECK (
                    method IN ('credit_card','debit_card',
                               'paypal','bank_transfer','cash')
                  )
);

-- 3. Create Indexes for Performance

-- orders
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(status);

-- order_items
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);

-- payments
CREATE INDEX idx_payments_order_id ON payments(order_id);

-- products
CREATE INDEX idx_products_category ON products(category);

COMMIT;
