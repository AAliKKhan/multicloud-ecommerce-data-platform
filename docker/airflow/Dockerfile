# Airflow for the e-commerce pipeline (local development image).
#
# Airflow's own dependency set is pinned by the official constraints file, and dbt's would
# conflict with it. So the project's code and dbt live in a SEPARATE virtualenv
# (/home/airflow/ecomm-venv); the DAGs reach them through shell commands. Versions below are the
# ones locked in uv.lock, so the DAGs run exactly what the tests ran.
#
# The source tree and the dbt project are bind-mounted by docker-compose (no rebuild per edit).
FROM apache/airflow:2.10.5-python3.11

ARG DUCKDB_VERSION=1.5.6
ARG DBT_CORE_VERSION=1.12.6
ARG DBT_DUCKDB_VERSION=1.11.1
ARG PROMETHEUS_CLIENT_VERSION=0.26.0
ARG PYDANTIC_VERSION=2.14.3
ARG PYDANTIC_SETTINGS_VERSION=2.16.2

USER airflow

# Long timeouts and retries: big wheels over a slow connection otherwise abort the build.
RUN python -m venv /home/airflow/ecomm-venv \
    && /home/airflow/ecomm-venv/bin/pip install --no-cache-dir --default-timeout=180 --retries 10 \
        "duckdb==${DUCKDB_VERSION}" \
        "dbt-core==${DBT_CORE_VERSION}" \
        "dbt-duckdb==${DBT_DUCKDB_VERSION}" \
        "prometheus-client==${PROMETHEUS_CLIENT_VERSION}" \
        "pydantic==${PYDANTIC_VERSION}" \
        "pydantic-settings==${PYDANTIC_SETTINGS_VERSION}" \
    && /home/airflow/ecomm-venv/bin/python -c "import duckdb, dbt.version, pydantic; print('venv ok', duckdb.__version__)"

# World-readable so the container also works under an arbitrary UID (AIRFLOW_UID, as on CI).
RUN chmod -R a+rX /home/airflow/ecomm-venv

COPY --chown=airflow:0 docker/airflow/init.sh /opt/airflow/init.sh
COPY --chown=airflow:0 docker/airflow/require-secrets.sh /opt/airflow/require-secrets.sh
RUN chmod 0755 /opt/airflow/init.sh /opt/airflow/require-secrets.sh

ENV ECOMM_PYTHON=/home/airflow/ecomm-venv/bin/python \
    ECOMM_SRC=/opt/ecomm/src \
    ECOMM_DBT_PROJECT=/opt/ecomm/dbt \
    ECOMM_DATA_DIR=/data/pipeline
