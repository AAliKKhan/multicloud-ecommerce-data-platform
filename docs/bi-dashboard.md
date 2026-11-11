# Business dashboard (Metabase)

A Metabase dashboard over the curated marts, answering the business questions in
[docs/07 section 7.3](07-PORTFOLIO-AND-DEMO.md): revenue, orders and average order value, payment outcomes, top
products and categories, low stock, and data freshness. Design decisions and limits:
[ADR 0014](decisions/0014-bi-metabase.md).

Everything on it is **synthetic demo data**, and the dashboard says so at the top. Metric definitions are not
re-derived in Metabase: every figure comes from a dbt mart ([data dictionary](data-dictionary.md)).

## Start it

```bash
uv run ecomm-demo                                   # a warehouse to show (skip if you already have data/demo)
python docker/bi/generate_env.py --print-login      # once: random secrets, git-ignored; prints the Metabase login
docker compose --profile bi up -d                   # Postgres + Metabase (about 1.2 GB; first start takes a few minutes)
uv run ecomm-bi all                                 # publish the marts, then create the dashboard
```

Open `http://127.0.0.1:3001` and sign in as `admin@ecomm.local` with the generated password
(`python docker/bi/generate_env.py --print-login` prints it again). Re-running `ecomm-bi all` after new data has
been loaded refreshes the dashboard; it never creates duplicates.

| Command | What it does |
|---|---|
| `ecomm-bi publish` | Copies the four marts and a one-row freshness table into Postgres, in one transaction. |
| `ecomm-bi provision` | Creates (or updates) the admin user, the database connection, 13 questions and the dashboard. |
| `ecomm-bi all` | Both, in that order. |

On an 8 GB machine stop Kafka UI and Spark first. Metabase is capped at 1.1 GB and the whole `bi` profile uses
about 1.2 GB.

## What the dashboard shows

| Row | Content | Source |
|---|---|---|
| Notice | Synthetic-data statement and the metric definitions | fixed text |
| KPIs | Net sales, orders, average order value, payment success rate (all-time totals) | `mart_daily_sales`, `mart_payment_health` |
| Trends | Net sales per day, orders per day (with cancelled), AOV per day | `mart_daily_sales` |
| Payments | Outcomes per day (stacked), success rate per day, data freshness and last successful load | `mart_payment_health`, `data_freshness` |
| Products | Top 10 products by net sales, net sales and units by category | `mart_product_performance` |
| Inventory | Products low or out of stock (below 20 units) | `mart_inventory_risk` |

The questions are plain SQL in [`dashboard_spec.py`](../src/ecomm/bi/dashboard_spec.py), so they are reviewed and
versioned like code. A test checks that every question only reads published marts, that the layout does not
overlap, and that every item of the suggested dashboard is covered.

## How it is verified

With the stack running, `tests/integration/test_bi.py` checks through Metabase's own API and a direct database
login that:

- every one of the 13 questions runs without error and returns rows;
- the **numbers equal the DuckDB marts** (net sales, orders, AOV, payment success rate, the per-day series, the
  top-10 products, the low-stock count, the number of loaded batches);
- provisioning twice leaves one collection, one connection, 13 cards and one dashboard;
- the database role Metabase uses can read the marts but cannot insert, delete, drop or create anything;
- the API refuses anonymous callers, and first-run setup is closed once an admin exists.

CI repeats this on every push (job `bi`). I also opened the dashboard in a browser and confirmed it renders; that
visual check is manual and is not part of CI.

## Design points worth knowing

- **Metabase reads a copy, not the warehouse.** The local warehouse is DuckDB, which allows one writer or many
  read-only openers, never both; a BI tool holding the file would block the pipeline's loads. So the pipeline
  publishes finished marts to Postgres. In the cloud Metabase would connect to BigQuery directly and this step
  would not exist.
- **Atomic publish.** The tables are replaced in one transaction and row counts are verified before the commit, so
  the dashboard shows the old marts or the new ones, never a mix or an empty table.
- **Only curated marts are published**, never raw events, staging or intermediate models.
- **Least privilege.** Three Postgres roles: `metabase` (its own application database), `bi_publisher` (owns the
  `bi` database, writes the marts) and `bi_reader` (`SELECT` on the `analytics` schema only; this is what Metabase
  queries with). The Postgres superuser password lives in its own file and is given to the Postgres container only.
- **Secrets are generated, random and git-ignored** (`docker/bi/.env`, `docker/bi/.env.postgres`). Metabase and
  Postgres refuse to start without them and say how to create them. The Metabase admin password is generated to
  satisfy Metabase's complexity policy (a plain random token was rejected as "too common").
- **Ports are loopback only**: Metabase on 3001, Postgres on 5433 (the host-side publisher needs it).

## Not covered

- No row-level security, SSO, alerting subscriptions or scheduled email reports; one admin account.
- The dashboard has no date filter and shows the three demo days. Charts are only as rich as the demo data.
- Metabase against BigQuery (the production shape) is untested: no cloud account exists.
- Dashboard appearance is checked by hand, not by an automated visual test.
