from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TaskGroup

PROJECT_ROOT = "/opt/smartlogix"

CITIES = {
    "jl": "Jilin",
    "yt": "Yantai",
    "cq": "Chongqing",
    "sh": "Shanghai",
    "hz": "Hangzhou",
}


with DAG(
    dag_id="smartlogix_lade_batch_pipeline",
    description=(
        "Pipeline batch multi-villes LaDe-D de SmartLogix 360 : "
        "Bronze, Silver, Gold, PostgreSQL et dbt."
    ),
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        8,
        14,
        tz="UTC",
    ),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=[
        "smartlogix",
        "lade",
        "batch",
        "multi-city",
    ],
) as dag:

    preflight = BashOperator(
        task_id="preflight",
        cwd=PROJECT_ROOT,
        bash_command="""
        set -euo pipefail

        echo "=== SmartLogix 360 - PrÃ©flight ==="

        for city in jl yt cq sh hz
        do
            raw="data/raw/lade/delivery/delivery_${city}.csv"
            manifest="data/raw/lade/_metadata/downloads/delivery__delivery_${city}.csv.download.json"

            test -f "$raw" || {
                echo "CSV absent : $raw"
                exit 1
            }

            test -f "$manifest" || {
                echo "Manifeste absent : $manifest"
                exit 1
            }

            echo "${city}: RAW + manifeste OK"
        done

        test -f scripts/build_lade_bronze.py
        test -f scripts/build_lade_silver.py
        test -f scripts/build_lade_gold.py
        test -f scripts/consolidate_lade_gold.py
        test -f scripts/load_lade_gold_postgres.py

        echo "PrÃ©flight terminÃ© avec succÃ¨s."
        """,
    )

    city_gold_tasks = []

    for city_code, city_name in CITIES.items():
        with TaskGroup(
            group_id=f"city_{city_code}",
            tooltip=f"Pipeline LaDe-D pour {city_name}",
        ) as city_group:

            bronze = BashOperator(
                task_id="quality_and_bronze",
                cwd=PROJECT_ROOT,
                bash_command=(
                    "python scripts/build_lade_bronze.py "
                    f"--input data/raw/lade/delivery/delivery_{city_code}.csv "
                    "--download-manifest "
                    "data/raw/lade/_metadata/downloads/"
                    f"delivery__delivery_{city_code}.csv.download.json "
                    f"--output data/bronze/lade/delivery/delivery_{city_code}.parquet "
                    "--compression zstd"
                ),
            )

            silver = BashOperator(
                task_id="build_silver",
                cwd=PROJECT_ROOT,
                bash_command=(
                    "python scripts/build_lade_silver.py "
                    f"--input data/bronze/lade/delivery/delivery_{city_code}.parquet "
                    f"--output data/silver/lade/delivery/delivery_{city_code}.parquet "
                    "--compression zstd"
                ),
            )

            gold = BashOperator(
                task_id="build_gold",
                cwd=PROJECT_ROOT,
                bash_command=(
                    "python scripts/build_lade_gold.py "
                    f"--input data/silver/lade/delivery/delivery_{city_code}.parquet "
                    "--output-directory "
                    f"data/gold/lade/delivery/by_city/{city_code} "
                    "--compression zstd"
                ),
            )

            bronze >> silver >> gold

        preflight >> city_group
        city_gold_tasks.append(gold)

    consolidate_gold = BashOperator(
        task_id="consolidate_gold",
        cwd=PROJECT_ROOT,
        bash_command=(
            "python scripts/consolidate_lade_gold.py"
        ),
    )

    load_postgres = BashOperator(
        task_id="load_gold_to_postgres",
        cwd=PROJECT_ROOT,
        bash_command=(
            "python scripts/load_lade_gold_postgres.py "
            "--gold-directory data/gold/lade/delivery "
            "--schema analytics "
            "--chunksize 50000"
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        cwd=PROJECT_ROOT,
        bash_command=(
            "dbt build "
            "--project-dir dbt/smartlogix "
            "--profiles-dir dbt/smartlogix"
        ),
    )

    for gold_task in city_gold_tasks:
        gold_task >> consolidate_gold

    consolidate_gold >> load_postgres >> dbt_build
