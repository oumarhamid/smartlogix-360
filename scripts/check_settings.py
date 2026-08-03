from smartlogix.common import configure_logging, get_logger
from smartlogix.config import get_settings


def main() -> None:
    configure_logging()

    logger = get_logger(__name__)
    settings = get_settings()

    settings.create_local_directories()

    logger.info(
        "smartlogix_configuration_loaded",
        environment=settings.smartlogix_env,
        project_root=str(settings.project_root),
        lade_directory=str(settings.resolved_lade_data_dir),
        postgres_host=settings.postgres_host,
        kafka_servers=settings.kafka_bootstrap_servers,
    )

    print("Configuration SmartLogix 360 chargée avec succès.")
    print(f"Environnement : {settings.smartlogix_env}")
    print(f"Racine du projet : {settings.project_root}")
    print(f"Répertoire LaDe : {settings.resolved_lade_data_dir}")


if __name__ == "__main__":
    main()