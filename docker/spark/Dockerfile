# Spark Structured Streaming runtime for the pipeline (local development image).
#
# Python 3.11 is required by the project (it uses StrEnum and datetime.UTC), which the
# stock Spark images do not ship, so the image is built from python:3.11 + Java 17 +
# the pip-installed PySpark. The Kafka connector jars are baked in: nothing is
# downloaded when a job starts.
#
# Runs as root for simplicity of bind-mounted data in local development; this image is
# not meant to be deployed as-is.
FROM python:3.11-slim-bookworm

ARG SPARK_VERSION=3.5.6
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL_VERSION=2.11.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps tini \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# Optional local cache of the (317 MB) PySpark sdist: on a slow connection, download it
# once with a resumable `curl -C -` into docker/spark/dist/ and the build installs it
# from there. When the folder only holds .gitkeep (CI), pip downloads it as usual.
COPY docker/spark/dist/ /tmp/dist/

# numpy<2 and pinned pyarrow/pandas: the versions PySpark 3.5 is tested with.
# Long timeouts and retries because a slow connection otherwise aborts big downloads.
RUN set -eux; \
    if ls /tmp/dist/pyspark-*.tar.gz >/dev/null 2>&1; then \
        PYSPARK="$(ls /tmp/dist/pyspark-*.tar.gz | head -n 1)"; \
    else \
        PYSPARK="pyspark==${SPARK_VERSION}"; \
    fi; \
    pip install --default-timeout=180 --retries 10 \
        "${PYSPARK}" \
        "numpy==1.26.4" \
        "pandas==2.2.3" \
        "pyarrow==16.1.0" \
        "pydantic>=2.7,<3" \
        "pydantic-settings>=2.3,<3"; \
    rm -rf /tmp/dist

# Kafka source for Structured Streaming (versions match Spark ${SPARK_VERSION}).
RUN set -eux; \
    JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"; \
    MAVEN=https://repo1.maven.org/maven2; \
    cd "${JARS}"; \
    curl -fsSLO "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL_VERSION}/commons-pool2-${COMMONS_POOL_VERSION}.jar"; \
    ls -1 "${JARS}" | grep -E 'kafka|commons-pool2'

# S3 client for the S3-compatible raw-zone destination (MinIO, see docs/minio.md). It is the last
# layer on purpose, so the large PySpark and connector layers above stay cached. The versions
# equal uv.lock (a unit test checks), so the container runs the adapter the tests ran against.
ARG BOTO3_VERSION=1.43.127
ARG BOTOCORE_VERSION=1.43.127
RUN pip install --default-timeout=180 --retries 10 \
        "boto3==${BOTO3_VERSION}" "botocore==${BOTOCORE_VERSION}"

# The project source is bind-mounted at /app/src by docker-compose (no rebuild per edit).
ENV PYTHONPATH=/app/src
WORKDIR /app

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "ecomm.spark.job", "--help"]
