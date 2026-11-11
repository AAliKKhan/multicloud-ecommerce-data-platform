# Pipeline metrics exporter (ecomm-metrics --serve) as a small, non-root container.
#
# It only needs the standard library plus DuckDB (read-only warehouse access), the Prometheus
# client and Pydantic. Versions are the ones locked in uv.lock (a static test compares them), so
# the exporter reads the warehouse with the same DuckDB that wrote it. The source tree is
# bind-mounted read-only by docker-compose, so code edits need no rebuild.
FROM python:3.11-slim-bookworm

ARG DUCKDB_VERSION=1.5.6
ARG PROMETHEUS_CLIENT_VERSION=0.26.0
ARG PYDANTIC_VERSION=2.14.3
ARG PYDANTIC_SETTINGS_VERSION=2.16.2

# Long timeouts and retries: wheels over a slow connection otherwise abort the build.
RUN pip install --no-cache-dir --default-timeout=180 --retries 10 \
        "duckdb==${DUCKDB_VERSION}" \
        "prometheus-client==${PROMETHEUS_CLIENT_VERSION}" \
        "pydantic==${PYDANTIC_VERSION}" \
        "pydantic-settings==${PYDANTIC_SETTINGS_VERSION}" \
    && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin exporter

ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER 10001
WORKDIR /app
EXPOSE 9108

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:9108/metrics', timeout=4)" || exit 1

ENTRYPOINT ["python", "-m", "ecomm.observability.cli"]
CMD ["--demo-dir", "/data/pipeline", "--serve", "9108", "--addr", "0.0.0.0"]
