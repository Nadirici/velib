"""DAG quotidien : archive la veille (événements Kafka + météo) en Parquet.

Les concepts Airflow à connaître, incarnés ici :

- **DAG** (directed acyclic graph) : un ensemble de tâches et leurs
  dépendances. Airflow ne fait qu'orchestrer — planifier, lancer, réessayer,
  historiser — le travail réel reste dans nos modules CLI.

- **schedule + logical date** : le cron "15 0 * * *" déclenche chaque nuit à
  00h15 (Paris, via la start_date tz-aware). Subtilité fondatrice d'Airflow :
  un run « couvre » un intervalle de données, et `{{ ds }}` (template Jinja)
  vaut le DÉBUT de cet intervalle — donc LA VEILLE au moment du déclenchement.
  C'est exactement l'argument qu'attendent nos CLIs : le run du 20 à 00h15
  archive le 19. On ne calcule jamais « hier » nous-mêmes : si on relance un
  vieux run, {{ ds }} vaut la bonne date historique (idempotence + rejouabilité).

- **catchup=False** : au premier démarrage, ne pas générer tous les runs
  manqués depuis start_date (la rétention Kafka de 7 jours les bornerait de
  toute façon). Un rattrapage ciblé reste possible depuis l'UI (▶ Trigger avec
  une logical date) ou `airflow dags backfill`.

- **retries** : une nuit où l'API météo tousse ne doit pas coûter la journée —
  3 tentatives espacées de 10 min, puis l'échec devient visible (case rouge).

Les deux tâches sont indépendantes (pas de >>) : l'échec de la météo ne bloque
pas l'archivage des événements, et inversement. Le futur chargement PostgreSQL,
lui, dépendra des deux : [archive_events, ingest_weather] >> load_postgres.
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
    # cwd=/opt/velib : les CLIs écrivent dans data/ en relatif, et le repo est
    # monté là par docker-compose. PYTHONPATH et l'adresse Kafka interne
    # (VELIB_BOOTSTRAP_SERVERS=kafka:19092) viennent de l'environnement du
    # conteneur — le code est identique dedans et dehors.
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
