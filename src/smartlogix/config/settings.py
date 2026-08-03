from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration centralisée de SmartLogix 360."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    smartlogix_env: Literal[
        "development",
        "test",
        "integration",
        "production",
    ] = "development"

    smartlogix_log_level: Literal[
        "DEBUG",
        "INFO",
        "WARNING",
        "ERROR",
        "CRITICAL",
    ] = "INFO"

    # Répertoires
    project_root: Path = Field(
        default_factory=lambda: Path(__file__).resolve().parents[3]
    )

    data_root: Path = Path("data")
    raw_data_dir: Path = Path("data/raw")
    generated_data_dir: Path = Path("data/generated")
    processed_data_dir: Path = Path("data/processed")
    quarantine_data_dir: Path = Path("data/quarantine")

    lade_data_dir: Path = Path("data/raw/lade")
    weather_data_dir: Path = Path("data/raw/weather")

    # Dataset LaDe
    lade_dataset_repository: str = "Cainiao-AI/LaDe"

    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_database: str = "smartlogix"
    postgres_user: str = "smartlogix"
    postgres_password: str = "change_me"

    # MinIO
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "change_me"
    minio_secret_key: str = "change_me"
    minio_secure: bool = False

    minio_raw_bucket: str = "smartlogix-raw"
    minio_bronze_bucket: str = "smartlogix-bronze"
    minio_silver_bucket: str = "smartlogix-silver"
    minio_gold_bucket: str = "smartlogix-gold"
    minio_quarantine_bucket: str = "smartlogix-quarantine"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_delivery_events_topic: str = "delivery-events"
    kafka_delivery_alerts_topic: str = "delivery-alerts"
    kafka_dead_letter_topic: str = "delivery-dead-letter"

    # Génération et ingestion
    default_chunk_size: int = 100_000
    default_random_seed: int = 42

    def resolve_path(self, path: Path) -> Path:
        """Transforme un chemin relatif en chemin absolu du projet."""
        if path.is_absolute():
            return path

        return self.project_root / path

    @property
    def resolved_data_root(self) -> Path:
        return self.resolve_path(self.data_root)

    @property
    def resolved_raw_data_dir(self) -> Path:
        return self.resolve_path(self.raw_data_dir)

    @property
    def resolved_generated_data_dir(self) -> Path:
        return self.resolve_path(self.generated_data_dir)

    @property
    def resolved_processed_data_dir(self) -> Path:
        return self.resolve_path(self.processed_data_dir)

    @property
    def resolved_quarantine_data_dir(self) -> Path:
        return self.resolve_path(self.quarantine_data_dir)

    @property
    def resolved_lade_data_dir(self) -> Path:
        return self.resolve_path(self.lade_data_dir)

    @property
    def resolved_weather_data_dir(self) -> Path:
        return self.resolve_path(self.weather_data_dir)

    @property
    def postgres_url(self) -> str:
        """Construit l'URL SQLAlchemy de PostgreSQL."""
        return (
            f"postgresql+psycopg://{self.postgres_user}:"
            f"{self.postgres_password}@{self.postgres_host}:"
            f"{self.postgres_port}/{self.postgres_database}"
        )

    def create_local_directories(self) -> None:
        """Crée les répertoires locaux nécessaires s'ils n'existent pas."""
        directories = [
            self.resolved_data_root,
            self.resolved_raw_data_dir,
            self.resolved_generated_data_dir,
            self.resolved_processed_data_dir,
            self.resolved_quarantine_data_dir,
            self.resolved_lade_data_dir,
            self.resolved_weather_data_dir,
        ]

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)


@lru_cache
def get_settings() -> Settings:
    """Retourne une instance unique de la configuration."""
    return Settings()