# restaurant-oltp

Dummy OLTP data generator for a restaurant, backed by Postgres in Docker.

## Schema

`customers`, `employees`, `restaurant_tables`, `menu_items`, `orders`, `order_items`, `payments` — a small relational OLTP model with foreign keys and indexes.

## Usage

```bash
# 1. Start Postgres
docker compose up -d

# 2. One-time init: create schema, seed reference data, backfill 5,000 historical orders
go run . -init -orders 5000

# 3. Stream live orders like a real POS feed (~5 orders/sec), Ctrl+C to stop
go run .
```

Flags:

| Flag      | Default                  | Description                                        |
|-----------|--------------------------|----------------------------------------------------|
| `-init`   | off                      | Seed + backfill historical orders, then exit       |
| `-orders` | `5000`                   | Historical orders to backfill in `-init` mode      |
| `-rate`   | `5`                      | Orders per second in stream mode                   |
| `-dsn`    | `$DATABASE_URL` or local | Postgres connection URL                            |

Stream mode refuses to run against an empty database — run `-init` first.

## Behavior

- **Init**: 500 customers, 40 employees, 30 tables, ~46 menu items, then backfills
  orders spread over the last 90 days with lunch/dinner rush-hour weighting
  (90% paid with payments, some cancelled, some left pending/served).
- **Stream**: each order is inserted as `pending` with line items, then
  transitions to `served` and finally `paid` with a payment row — all in real
  time, like a live restaurant. Shutdown is graceful (in-flight orders finish).
- Idempotent: safe to run `-init` again; existing data is kept.
