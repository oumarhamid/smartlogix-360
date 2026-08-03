from pathlib import Path

from smartlogix.config import Settings


def test_default_settings() -> None:
    settings = Settings(_env_file=None)

    assert settings.smartlogix_env == "development"
    assert settings.smartlogix_log_level == "INFO"
    assert settings.postgres_port == 5432
    assert settings.default_chunk_size == 100_000
    assert settings.kafka_delivery_events_topic == "delivery-events"


def test_resolve_relative_path() -> None:
    settings = Settings(_env_file=None)

    resolved_path = settings.resolve_path(Path("data/raw"))

    assert resolved_path.is_absolute()
    assert resolved_path == settings.project_root / "data/raw"


def test_create_local_directories(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        project_root=tmp_path,
        data_root=Path("data"),
        raw_data_dir=Path("data/raw"),
        generated_data_dir=Path("data/generated"),
        processed_data_dir=Path("data/processed"),
        quarantine_data_dir=Path("data/quarantine"),
        lade_data_dir=Path("data/raw/lade"),
        weather_data_dir=Path("data/raw/weather"),
    )

    settings.create_local_directories()

    assert (tmp_path / "data/raw/lade").exists()
    assert (tmp_path / "data/raw/weather").exists()
    assert (tmp_path / "data/generated").exists()
    assert (tmp_path / "data/processed").exists()
    assert (tmp_path / "data/quarantine").exists()


def test_postgres_url() -> None:
    settings = Settings(
        _env_file=None,
        postgres_host="postgres",
        postgres_port=5432,
        postgres_database="smartlogix",
        postgres_user="smartlogix",
        postgres_password="secret",
    )

    assert settings.postgres_url == (
        "postgresql+psycopg://smartlogix:"
        "secret@postgres:5432/smartlogix"
    )