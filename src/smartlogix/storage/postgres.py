from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from io import StringIO
from typing import Any

import pandas as pd
from psycopg import Error as PsycopgError
from sqlalchemy import Engine, create_engine, inspect, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

from smartlogix.common import get_logger

logger = get_logger(__name__)

POSTGRES_IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")

COPY_NULL_SENTINEL = "__SMARTLOGIX_NULL_7F9E2C5A__"


GOLD_POSTGRES_TABLES = {
    "delivery_fact": {
        "required_columns": (
            "order_id",
            "delivery_date",
            "region_id",
            "city",
            "courier_id",
            "delivery_duration_minutes",
            "is_valid_duration",
            "is_within_sla",
            "is_late_delivery",
            "has_complete_gps",
            "is_quality_warning",
            "delivery_count",
        ),
        "unique_columns": (
            "order_id",
        ),
        "indexes": (
            (
                "ix_delivery_fact_date",
                ("delivery_date",),
                False,
            ),
            (
                "ix_delivery_fact_courier_date",
                (
                    "courier_id",
                    "delivery_date",
                ),
                False,
            ),
            (
                "ix_delivery_fact_city_date",
                (
                    "city",
                    "delivery_date",
                ),
                False,
            ),
            (
                "ix_delivery_fact_late_date",
                (
                    "is_late_delivery",
                    "delivery_date",
                ),
                False,
            ),
        ),
    },
    "courier_daily_performance": {
        "required_columns": (
            "delivery_date",
            "region_id",
            "city",
            "courier_id",
            "orders_total",
            "orders_valid_duration",
            "orders_within_sla",
            "orders_late",
            "avg_duration_minutes",
            "sla_compliance_rate",
            "gps_completeness_rate",
            "quality_warning_rate",
        ),
        "unique_columns": (
            "delivery_date",
            "region_id",
            "city",
            "courier_id",
        ),
        "indexes": (
            (
                "ix_courier_daily_city_date",
                (
                    "city",
                    "delivery_date",
                ),
                False,
            ),
            (
                "ix_courier_daily_sla",
                ("sla_compliance_rate",),
                False,
            ),
        ),
    },
    "city_daily_performance": {
        "required_columns": (
            "delivery_date",
            "region_id",
            "city",
            "orders_total",
            "orders_valid_duration",
            "orders_within_sla",
            "orders_late",
            "unique_couriers",
            "avg_duration_minutes",
            "sla_compliance_rate",
            "gps_completeness_rate",
            "quality_warning_rate",
        ),
        "unique_columns": (
            "delivery_date",
            "region_id",
            "city",
        ),
        "indexes": (
            (
                "ix_city_daily_date",
                ("delivery_date",),
                False,
            ),
            (
                "ix_city_daily_sla",
                ("sla_compliance_rate",),
                False,
            ),
        ),
    },
}


class PostgresGoldLoadError(RuntimeError):
    """Erreur rencontrée pendant le chargement PostgreSQL."""


@dataclass(frozen=True, slots=True)
class PostgresTableLoadResult:
    """Résumé du chargement d'une table PostgreSQL."""

    schema_name: str
    table_name: str
    row_count: int
    column_count: int
    replaced_existing_data: bool
    loaded_at: datetime

    def to_dict(self) -> dict[str, Any]:
        """Convertit le résultat en dictionnaire."""

        data = asdict(self)
        data["loaded_at"] = self.loaded_at.isoformat()

        return data


@dataclass(frozen=True, slots=True)
class PostgresGoldLoadResult:
    """Résumé du chargement des trois tables Gold."""

    database_name: str
    database_user: str
    schema_name: str
    loaded_at: datetime
    delivery_fact: PostgresTableLoadResult
    courier_daily_performance: PostgresTableLoadResult
    city_daily_performance: PostgresTableLoadResult

    def to_dict(self) -> dict[str, Any]:
        """Convertit le résultat global en dictionnaire."""

        data = asdict(self)
        data["loaded_at"] = self.loaded_at.isoformat()

        for table_name in (
            "delivery_fact",
            "courier_daily_performance",
            "city_daily_performance",
        ):
            table_data = data[table_name]
            table_data["loaded_at"] = (
                table_data["loaded_at"].isoformat()
            )

        return data


class PostgresGoldLoader:
    """Charge les tables Gold dans PostgreSQL avec COPY."""

    def __init__(
        self,
        database_url: str,
        schema_name: str = "analytics",
        chunksize: int = 2000,
        engine: Engine | None = None,
    ) -> None:
        self._validate_identifier(schema_name)

        if chunksize <= 0:
            raise ValueError(
                "chunksize doit être strictement positif."
            )

        if not database_url.strip():
            raise ValueError(
                "L'URL PostgreSQL est obligatoire."
            )

        self.database_url = database_url
        self.schema_name = schema_name
        self.chunksize = chunksize

        self.engine = engine or create_engine(
            database_url,
            pool_pre_ping=True,
            future=True,
        )

        self._owns_engine = engine is None

    @staticmethod
    def _validate_identifier(
        identifier: str,
    ) -> None:
        """Vérifie qu'un identifiant SQL est sûr."""

        if not POSTGRES_IDENTIFIER_PATTERN.fullmatch(
            identifier
        ):
            raise ValueError(
                "Identifiant PostgreSQL invalide : "
                f"{identifier!r}"
            )

    @staticmethod
    def _quote_identifier(
        identifier: str,
    ) -> str:
        """Protège un identifiant PostgreSQL validé."""

        PostgresGoldLoader._validate_identifier(
            identifier
        )

        return f'"{identifier}"'

    def _qualified_table_name(
        self,
        table_name: str,
    ) -> str:
        """Retourne le nom SQL pleinement qualifié."""

        self._validate_identifier(table_name)

        return (
            f"{self._quote_identifier(self.schema_name)}."
            f"{self._quote_identifier(table_name)}"
        )

    def _qualified_index_name(
        self,
        index_name: str,
    ) -> str:
        """Retourne le nom SQL pleinement qualifié d'un index."""

        self._validate_identifier(index_name)

        return (
            f"{self._quote_identifier(self.schema_name)}."
            f"{self._quote_identifier(index_name)}"
        )

    def _validate_dataframe(
        self,
        dataframe: pd.DataFrame,
        table_name: str,
    ) -> None:
        """Valide le DataFrame avant son chargement."""

        if table_name not in GOLD_POSTGRES_TABLES:
            raise PostgresGoldLoadError(
                "Table Gold PostgreSQL non prise en charge : "
                f"{table_name}"
            )

        if dataframe.empty:
            raise PostgresGoldLoadError(
                f"Le DataFrame {table_name} est vide."
            )

        table_configuration = (
            GOLD_POSTGRES_TABLES[table_name]
        )

        required_columns = set(
            table_configuration["required_columns"]
        )

        missing_columns = required_columns.difference(
            dataframe.columns
        )

        if missing_columns:
            missing_text = ", ".join(
                sorted(missing_columns)
            )

            raise PostgresGoldLoadError(
                f"Colonnes obligatoires absentes de "
                f"{table_name} : {missing_text}"
            )

        unique_columns = list(
            table_configuration["unique_columns"]
        )

        if dataframe[
            unique_columns
        ].isna().any().any():
            raise PostgresGoldLoadError(
                f"La clé de {table_name} contient "
                "des valeurs nulles."
            )

        if dataframe.duplicated(
            subset=unique_columns
        ).any():
            raise PostgresGoldLoadError(
                f"La clé de {table_name} contient "
                "des doublons."
            )

        for column in dataframe.columns:
            self._validate_identifier(
                str(column)
            )

    def test_connection(
        self,
    ) -> tuple[str, str]:
        """Teste la connexion PostgreSQL."""

        try:
            with self.engine.connect() as connection:
                (
                    database_name,
                    database_user,
                ) = connection.execute(
                    text(
                        "SELECT "
                        "current_database(), "
                        "current_user"
                    )
                ).one()

        except SQLAlchemyError as error:
            raise PostgresGoldLoadError(
                "Impossible de se connecter à PostgreSQL."
            ) from error

        logger.info(
            "postgres_connection_validated",
            database_name=database_name,
            database_user=database_user,
            schema_name=self.schema_name,
        )

        return (
            str(database_name),
            str(database_user),
        )

    def _ensure_schema(
        self,
        connection: Connection,
    ) -> None:
        """Crée le schéma analytique si nécessaire."""

        schema_identifier = (
            self._quote_identifier(
                self.schema_name
            )
        )

        connection.execute(
            text(
                f"CREATE SCHEMA IF NOT EXISTS "
                f"{schema_identifier}"
            )
        )

    def _table_exists(
        self,
        connection: Connection,
        table_name: str,
    ) -> bool:
        """Vérifie si une table existe déjà."""

        return inspect(
            connection
        ).has_table(
            table_name,
            schema=self.schema_name,
        )

    def _truncate_table(
        self,
        connection: Connection,
        table_name: str,
    ) -> None:
        """Vide une table existante."""

        qualified_table = (
            self._qualified_table_name(
                table_name
            )
        )

        connection.execute(
            text(
                f"TRUNCATE TABLE "
                f"{qualified_table}"
            )
        )

    def _create_empty_table(
        self,
        connection: Connection,
        dataframe: pd.DataFrame,
        table_name: str,
    ) -> None:
        """Crée uniquement la structure d'une nouvelle table."""

        dataframe.head(0).to_sql(
            name=table_name,
            con=connection,
            schema=self.schema_name,
            if_exists="fail",
            index=False,
        )

    def _index_names(
        self,
        table_name: str,
    ) -> tuple[str, ...]:
        """Retourne les index gérés par SmartLogix."""

        configuration = (
            GOLD_POSTGRES_TABLES[table_name]
        )

        secondary_indexes = tuple(
            index_name
            for (
                index_name,
                _,
                _,
            ) in configuration["indexes"]
        )

        return (
            f"uq_{table_name}_key",
            *secondary_indexes,
        )

    def _drop_indexes(
        self,
        connection: Connection,
        table_name: str,
    ) -> None:
        """Supprime les index avant le chargement massif."""

        for index_name in self._index_names(
            table_name
        ):
            qualified_index = (
                self._qualified_index_name(
                    index_name
                )
            )

            connection.execute(
                text(
                    f"DROP INDEX IF EXISTS "
                    f"{qualified_index}"
                )
            )

    def _copy_dataframe(
        self,
        connection: Connection,
        dataframe: pd.DataFrame,
        table_name: str,
    ) -> None:
        """Charge un DataFrame via COPY FROM STDIN."""

        qualified_table = (
            self._qualified_table_name(
                table_name
            )
        )

        quoted_columns = ", ".join(
            self._quote_identifier(
                str(column)
            )
            for column in dataframe.columns
        )

        copy_sql = (
            f"COPY {qualified_table} "
            f"({quoted_columns}) "
            "FROM STDIN WITH ("
            "FORMAT CSV, "
            f"NULL '{COPY_NULL_SENTINEL}'"
            ")"
        )

        dbapi_connection = (
            connection.connection
        )

        try:
            with dbapi_connection.cursor() as cursor, cursor.copy(
                copy_sql
            ) as copy:
                for start in range(
                    0,
                    len(dataframe),
                    self.chunksize,
                ):
                    stop = min(
                        start + self.chunksize,
                        len(dataframe),
                    )

                    chunk = dataframe.iloc[
                        start:stop
                    ]

                    buffer = StringIO()

                    chunk.to_csv(
                        buffer,
                        index=False,
                        header=False,
                        na_rep=COPY_NULL_SENTINEL,
                        lineterminator="\n",
                    )

                    copy.write(
                        buffer.getvalue().encode(
                            "utf-8"
                        )
                    )

        except (
            PsycopgError,
            AttributeError,
            TypeError,
            ValueError,
        ) as error:
            raise PostgresGoldLoadError(
                "COPY PostgreSQL impossible pour "
                f"{self.schema_name}.{table_name}."
            ) from error

    def _write_dataframe(
        self,
        connection: Connection,
        dataframe: pd.DataFrame,
        table_name: str,
        table_exists: bool,
    ) -> None:
        """Prépare la table puis effectue le COPY."""

        if table_exists:
            self._drop_indexes(
                connection=connection,
                table_name=table_name,
            )

            self._truncate_table(
                connection=connection,
                table_name=table_name,
            )

        else:
            self._create_empty_table(
                connection=connection,
                dataframe=dataframe,
                table_name=table_name,
            )

        self._copy_dataframe(
            connection=connection,
            dataframe=dataframe,
            table_name=table_name,
        )

    def _count_rows(
        self,
        connection: Connection,
        table_name: str,
    ) -> int:
        """Compte les lignes chargées."""

        qualified_table = (
            self._qualified_table_name(
                table_name
            )
        )

        return int(
            connection.execute(
                text(
                    f"SELECT COUNT(*) "
                    f"FROM {qualified_table}"
                )
            ).scalar_one()
        )

    def _create_unique_index(
        self,
        connection: Connection,
        table_name: str,
        columns: tuple[str, ...],
    ) -> None:
        """Crée l'index unique métier."""

        index_name = (
            f"uq_{table_name}_key"
        )

        self._validate_identifier(
            index_name
        )

        quoted_columns = ", ".join(
            self._quote_identifier(column)
            for column in columns
        )

        qualified_table = (
            self._qualified_table_name(
                table_name
            )
        )

        connection.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS "
                f"{self._quote_identifier(index_name)} "
                f"ON {qualified_table} "
                f"({quoted_columns})"
            )
        )

    def _create_secondary_indexes(
        self,
        connection: Connection,
        table_name: str,
    ) -> None:
        """Crée les index analytiques secondaires."""

        table_configuration = (
            GOLD_POSTGRES_TABLES[table_name]
        )

        for (
            index_name,
            columns,
            unique,
        ) in table_configuration["indexes"]:
            self._validate_identifier(
                index_name
            )

            quoted_columns = ", ".join(
                self._quote_identifier(column)
                for column in columns
            )

            unique_keyword = (
                "UNIQUE "
                if unique
                else ""
            )

            qualified_table = (
                self._qualified_table_name(
                    table_name
                )
            )

            connection.execute(
                text(
                    f"CREATE {unique_keyword}"
                    "INDEX IF NOT EXISTS "
                    f"{self._quote_identifier(index_name)} "
                    f"ON {qualified_table} "
                    f"({quoted_columns})"
                )
            )

    def _create_indexes(
        self,
        connection: Connection,
        table_name: str,
    ) -> None:
        """Crée les index nécessaires."""

        table_configuration = (
            GOLD_POSTGRES_TABLES[table_name]
        )

        unique_columns = tuple(
            table_configuration[
                "unique_columns"
            ]
        )

        self._create_unique_index(
            connection=connection,
            table_name=table_name,
            columns=unique_columns,
        )

        self._create_secondary_indexes(
            connection=connection,
            table_name=table_name,
        )

    def _analyze_table(
        self,
        connection: Connection,
        table_name: str,
    ) -> None:
        """Met à jour les statistiques PostgreSQL."""

        qualified_table = (
            self._qualified_table_name(
                table_name
            )
        )

        connection.execute(
            text(
                f"ANALYZE {qualified_table}"
            )
        )

    def load_dataframe(
        self,
        dataframe: pd.DataFrame,
        table_name: str,
        loaded_at: datetime | None = None,
    ) -> PostgresTableLoadResult:
        """Charge atomiquement une table Gold."""

        self._validate_identifier(
            table_name
        )

        self._validate_dataframe(
            dataframe=dataframe,
            table_name=table_name,
        )

        load_timestamp = (
            loaded_at
            or datetime.now(UTC)
        )

        logger.info(
            "postgres_gold_table_load_started",
            schema_name=self.schema_name,
            table_name=table_name,
            row_count=int(
                len(dataframe)
            ),
            column_count=int(
                len(dataframe.columns)
            ),
            copy_chunk_rows=self.chunksize,
        )

        try:
            with self.engine.begin() as connection:
                self._ensure_schema(
                    connection
                )

                table_exists = (
                    self._table_exists(
                        connection=connection,
                        table_name=table_name,
                    )
                )

                self._write_dataframe(
                    connection=connection,
                    dataframe=dataframe,
                    table_name=table_name,
                    table_exists=table_exists,
                )

                loaded_row_count = (
                    self._count_rows(
                        connection=connection,
                        table_name=table_name,
                    )
                )

                if loaded_row_count != len(
                    dataframe
                ):
                    raise PostgresGoldLoadError(
                        "Le nombre de lignes chargé dans "
                        f"{table_name} est incorrect : "
                        f"{loaded_row_count} au lieu de "
                        f"{len(dataframe)}."
                    )

                self._create_indexes(
                    connection=connection,
                    table_name=table_name,
                )

                self._analyze_table(
                    connection=connection,
                    table_name=table_name,
                )

        except PostgresGoldLoadError:
            raise

        except (
            SQLAlchemyError,
            PsycopgError,
            ValueError,
            TypeError,
        ) as error:
            logger.exception(
                "postgres_gold_table_load_failed",
                schema_name=self.schema_name,
                table_name=table_name,
                error_type=type(error).__name__,
            )

            raise PostgresGoldLoadError(
                "Impossible de charger "
                f"{self.schema_name}.{table_name}."
            ) from error

        logger.info(
            "postgres_gold_table_load_completed",
            schema_name=self.schema_name,
            table_name=table_name,
            row_count=loaded_row_count,
            replaced_existing_data=table_exists,
        )

        return PostgresTableLoadResult(
            schema_name=self.schema_name,
            table_name=table_name,
            row_count=loaded_row_count,
            column_count=int(
                len(dataframe.columns)
            ),
            replaced_existing_data=table_exists,
            loaded_at=load_timestamp,
        )

    def load_gold_tables(
        self,
        delivery_fact: pd.DataFrame,
        courier_daily_performance: pd.DataFrame,
        city_daily_performance: pd.DataFrame,
        loaded_at: datetime | None = None,
    ) -> PostgresGoldLoadResult:
        """Charge les trois tables analytiques Gold."""

        load_timestamp = (
            loaded_at
            or datetime.now(UTC)
        )

        (
            database_name,
            database_user,
        ) = self.test_connection()

        delivery_result = self.load_dataframe(
            dataframe=delivery_fact,
            table_name="delivery_fact",
            loaded_at=load_timestamp,
        )

        courier_result = self.load_dataframe(
            dataframe=courier_daily_performance,
            table_name="courier_daily_performance",
            loaded_at=load_timestamp,
        )

        city_result = self.load_dataframe(
            dataframe=city_daily_performance,
            table_name="city_daily_performance",
            loaded_at=load_timestamp,
        )

        return PostgresGoldLoadResult(
            database_name=database_name,
            database_user=database_user,
            schema_name=self.schema_name,
            loaded_at=load_timestamp,
            delivery_fact=delivery_result,
            courier_daily_performance=(
                courier_result
            ),
            city_daily_performance=(
                city_result
            ),
        )

    def dispose(self) -> None:
        """Libère les connexions créées par le chargeur."""

        if self._owns_engine:
            self.engine.dispose()

    def __enter__(
        self,
    ) -> PostgresGoldLoader:
        """Entre dans le contexte du chargeur."""

        return self

    def __exit__(
        self,
        exception_type: object,
        exception_value: object,
        traceback: object,
    ) -> None:
        """Ferme le moteur en sortie de contexte."""

        self.dispose()