import pytest

from ecomm.config import AppEnv, ConfigError, Settings, load_settings

CLOUD_VARS = (
    "AWS_REGION",
    "AWS_S3_BUCKET",
    "GCP_PROJECT_ID",
    "GCP_REGION",
    "GCS_BUCKET",
    "BIGQUERY_DATASET",
)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "APP_ENV",
        "LOG_LEVEL",
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_TOPIC_PREFIX",
        "LATE_EVENT_THRESHOLD_MINUTES",
        *CLOUD_VARS,
    ):
        monkeypatch.delenv(name, raising=False)


def test_local_defaults_need_no_cloud_credentials() -> None:
    settings = load_settings(_env_file=None)
    assert settings.app_env is AppEnv.LOCAL
    assert settings.kafka_bootstrap_servers == "localhost:9092"
    assert settings.kafka_topic_prefix == "commerce"
    assert settings.log_level == "INFO"
    assert settings.aws_s3_bucket is None
    assert settings.is_cloud is False


def test_topic_names_use_prefix() -> None:
    settings = Settings(_env_file=None, kafka_topic_prefix="shop")
    assert settings.topic("orders") == "shop.orders"
    assert settings.topic("dlq") == "shop.dlq"


def test_env_vars_override_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    monkeypatch.setenv("LOG_LEVEL", "debug")
    settings = load_settings(_env_file=None)
    assert settings.kafka_bootstrap_servers == "broker:29092"
    assert settings.log_level == "DEBUG"


def test_cloud_env_reports_every_missing_variable() -> None:
    with pytest.raises(ConfigError) as exc:
        load_settings(_env_file=None, app_env="dev-cloud")
    message = str(exc.value)
    for name in CLOUD_VARS:
        assert name in message


def test_cloud_env_valid_when_all_set() -> None:
    settings = load_settings(
        _env_file=None,
        app_env="dev-cloud",
        aws_region="us-east-1",
        aws_s3_bucket="ecomm-dev-raw",
        gcp_project_id="my-project",
        gcp_region="us-central1",
        gcs_bucket="ecomm-dev-raw-gcs",
        bigquery_dataset="ecomm_dev",
    )
    assert settings.is_cloud is True


def test_invalid_app_env_is_rejected() -> None:
    with pytest.raises(ConfigError, match="APP_ENV"):
        load_settings(_env_file=None, app_env="staging")


def test_invalid_log_level_is_rejected() -> None:
    with pytest.raises(ConfigError, match="LOG_LEVEL"):
        load_settings(_env_file=None, log_level="LOUD")


def test_bucket_names_must_be_valid() -> None:
    with pytest.raises(ConfigError, match="AWS_S3_BUCKET"):
        load_settings(
            _env_file=None,
            app_env="dev-cloud",
            aws_region="us-east-1",
            aws_s3_bucket="Bad_Bucket!",
            gcp_project_id="p",
            gcp_region="r",
            gcs_bucket="ok-bucket",
            bigquery_dataset="ds",
        )


def test_late_event_threshold_must_be_positive() -> None:
    with pytest.raises(ConfigError, match="LATE_EVENT_THRESHOLD_MINUTES"):
        load_settings(_env_file=None, late_event_threshold_minutes=0)
