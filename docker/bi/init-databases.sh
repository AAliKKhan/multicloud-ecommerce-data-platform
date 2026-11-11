#!/bin/sh
# First start of the Postgres volume only (the image runs /docker-entrypoint-initdb.d once).
# Creates three least-privilege roles and two databases:
#   metabase      owns the "metabase" database (Metabase's own application state)
#   bi_publisher  owns the "bi" database and writes the published marts
#   bi_reader     SELECT only on the "bi" analytics schema (what Metabase queries)
# Passwords are generated URL-safe values (letters, digits, "-" and "_"), so plain quoting is safe.
set -eu

for name in METABASE_DB_PASSWORD BI_PUBLISHER_PASSWORD BI_READER_PASSWORD; do
  eval "value=\${$name:-}"
  if [ -z "$value" ]; then
    echo "ERROR: $name is not set. Run: python docker/bi/generate_env.py" >&2
    exit 1
  fi
done

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<SQL
CREATE ROLE metabase LOGIN PASSWORD '${METABASE_DB_PASSWORD}';
CREATE DATABASE metabase OWNER metabase;
CREATE ROLE bi_publisher LOGIN PASSWORD '${BI_PUBLISHER_PASSWORD}';
CREATE ROLE bi_reader LOGIN PASSWORD '${BI_READER_PASSWORD}';
CREATE DATABASE bi OWNER bi_publisher;
REVOKE ALL ON DATABASE metabase FROM PUBLIC;
REVOKE ALL ON DATABASE bi FROM PUBLIC;
GRANT CONNECT ON DATABASE bi TO bi_reader;
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname bi <<SQL
REVOKE ALL ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA analytics AUTHORIZATION bi_publisher;
GRANT USAGE ON SCHEMA analytics TO bi_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE bi_publisher IN SCHEMA analytics GRANT SELECT ON TABLES TO bi_reader;
SQL
