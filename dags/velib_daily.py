"""DAG quotidien : archive la veille (événements Kafka + météo) en Parquet.

Déclenché chaque nuit à 00h15 (Paris). `{{ ds }}` — la logical date, début de
l'intervalle couvert par le run — vaut la veille et est passé aux CLIs : rejouer
un ancien run archive la bonne date, sans calcul de « hier » dans le code.
Les deux tâches sont indépendantes (l'échec de l'une ne bloque pas l'autre) et
retentées 3 fois. Airflow n'orchestre que ; le travail vit dans les modules velib.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="velib_daily",
    description="Archive la veille : événements Kafka → Parquet, météo → Parquet",
    schedule="15 0 * * *",
    start_date=pendulum.datetime(2026, 7, 18, tz="Europe/Paris"),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["velib", "batch"],
) as dag:
    # cwd=/opt/velib : les CLIs écrivent dans data/ en relatif (repo monté par
    # docker-compose) ; PYTHONPATH et l'adresse Kafka viennent de l'environnement.
    archive_events = BashOperator(
        task_id="archive_events",
        bash_command="python -m velib.archiver {{ ds }}",
        cwd="/opt/velib",
    )

    ingest_weather = BashOperator(
        task_id="ingest_weather",
        bash_command="python -m velib.weather {{ ds }}",
        cwd="/opt/velib",
    )
