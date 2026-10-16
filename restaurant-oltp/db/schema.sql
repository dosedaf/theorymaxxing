CREATE TABLE IF NOT EXISTS customers (
    id          BIGSERIAL PRIMARY KEY,
    first_name  TEXT        NOT NULL,
    last_name   TEXT        NOT NULL,
    phone       TEXT        NOT NULL UNIQUE,
    email       TEXT        NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS employees (
    id         BIGSERIAL PRIMARY KEY,
    first_name TEXT        NOT NULL,
    last_name  TEXT        NOT NULL,
    role       TEXT        NOT NULL CHECK (role IN ('waiter', 'chef', 'manager', 'cashier')),
    hire_date  DATE        NOT NULL
);

CREATE TABLE IF NOT EXISTS restaurant_tables (
    id          BIGSERIAL PRIMARY KEY,
    table_number INT NOT NULL UNIQUE,
    capacity    INT NOT NULL CHECK (capacity BETWEEN 1 AND 20)
);

CREATE TABLE IF NOT EXISTS menu_items (
    id       BIGSERIAL PRIMARY KEY,
    name     TEXT           NOT NULL UNIQUE,
    category TEXT           NOT NULL CHECK (category IN ('appetizer', 'main', 'dessert', 'beverage')),
    price    NUMERIC(10, 2) NOT NULL CHECK (price > 0)
);

CREATE TABLE IF NOT EXISTS orders (
    id          BIGSERIAL PRIMARY KEY,
    customer_id BIGINT      NOT NULL REFERENCES customers (id),
    table_id    BIGINT      NOT NULL REFERENCES restaurant_tables (id),
    employee_id BIGINT      NOT NULL REFERENCES employees (id),
    status      TEXT        NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending', 'served', 'paid', 'cancelled')),
    order_time  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_items (
    id           BIGSERIAL PRIMARY KEY,
    order_id     BIGINT         NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    menu_item_id BIGINT         NOT NULL REFERENCES menu_items (id),
    quantity     INT            NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(10, 2) NOT NULL CHECK (unit_price > 0)
);

CREATE TABLE IF NOT EXISTS payments (
    id         BIGSERIAL PRIMARY KEY,
    order_id   BIGINT         NOT NULL UNIQUE REFERENCES orders (id) ON DELETE CASCADE,
    method     TEXT           NOT NULL CHECK (method IN ('cash', 'credit_card', 'debit_card', 'qris', 'e_wallet')),
    amount     NUMERIC(10, 2) NOT NULL CHECK (amount > 0),
    paid_at    TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_table_id    ON orders (table_id);
CREATE INDEX IF NOT EXISTS idx_orders_employee_id ON orders (employee_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_time  ON orders (order_time);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id     ON order_items (order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_menu_item_id ON order_items (menu_item_id);
CREATE INDEX IF NOT EXISTS idx_payments_paid_at ON payments (paid_at);
