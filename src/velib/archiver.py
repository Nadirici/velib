"""Couche batch de la BI : archive les événements d'une journée en Parquet.

Architecture lambda, versant « batch layer » : chaque soir (ou à la demande),
ce job relit dans le journal Kafka la totalité des événements d'une journée
et les fige en un fichier Parquet partitionné par date :

    data/events/date=2026-07-19/events.parquet

Pourquoi Parquet : format colonne compressé, LE standard analytique — DuckDB
requête ensuite `data/events/*/*.parquet` en SQL directement (le répertoire
`date=...` est reconnu comme partition Hive, la colonne `date` apparaît toute
seule). Pourquoi depuis Kafka : le journal est la source de vérité ; ce job
est idempotent (le relancer réécrit le même fichier) et indépendant de Redis.

Mécanique Kafka à connaître : `offsets_for_times` — le broker indexe les
messages par horodatage et sait répondre « le premier offset ≥ minuit ». On
borne ainsi la journée [minuit, minuit+24h) partition par partition, sans
group.id ni commit : lecture par `assign()` (position explicite), pas
d'abonnement dynamique.

Usage :
    uv run python -m velib.archiver               → archive HIER
    uv run python -m velib.archiver 2026-07-19    → archive une date précise
    (à planifier chaque soir : Planificateur de tâches Windows / cron)
"""

from __future__ import annotations

import json
import sys
import uuid
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path

import duckdb
from confluent_kafka import Consumer, TopicPartition

from .kafka_producer import BOOTSTRAP_SERVERS, TOPIC

DATA_DIR = Path("data") / "events"

COLUMNS = [
    "ts", "hour", "station_id", "station_code", "name", "lat", "lon",
    "capacity", "last_reported", "mechanical", "ebike", "bikes_available",
    "docks_available", "is_installed", "is_renting", "is_returning",
    "bikes_delta",
]


def _epoch_ms(d: date) -> int:
    """Minuit local du jour `d`, en millisecondes epoch (l'unité de Kafka)."""
    return int(datetime.combine(d, dtime.min).astimezone().timestamp() * 1000)


def fetch_day_events(day: date) -> list[dict]:
    """Relit tous les événements de la journée depuis le journal Kafka."""
    start_ms, end_ms = _epoch_ms(day), _epoch_ms(day + timedelta(days=1))

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        # group.id obligatoire mais sans rôle ici : assign() explicite,
        # aucun commit — le job ne laisse aucune trace côté Kafka.
        "group.id": f"velib-archiver-{uuid.uuid4().hex[:8]}",
        "enable.auto.commit": False,
    })
    try:
        meta = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC]
        partitions = list(meta.partitions)

        # Borne de départ : premier offset ≥ minuit, par partition.
        starts = consumer.offsets_for_times(
            [TopicPartition(TOPIC, p, start_ms) for p in partitions], timeout=10
        )
        # Borne de fin : premier offset ≥ minuit+24h ; -1 (aucun message
        # après la borne) → fin actuelle du journal (high watermark).
        ends = {
            tp.partition: tp.offset
            for tp in consumer.offsets_for_times(
                [TopicPartition(TOPIC, p, end_ms) for p in partitions], timeout=10
            )
        }
        todo: dict[int, int] = {}  # partition -> offset de fin (exclu)
        assigned = []
        for tp in starts:
            _, high = consumer.get_watermark_offsets(tp, timeout=10)
            end = ends[tp.partition] if ends[tp.partition] >= 0 else high
            if tp.offset < 0 or tp.offset >= end:
                continue  # rien pour ce jour dans cette partition
            todo[tp.partition] = end
            assigned.append(tp)
        if not assigned:
            return []
        consumer.assign(assigned)

        events: list[dict] = []
        while todo:
            msg = consumer.poll(5.0)
            if msg is None or msg.error():
                continue
            p = msg.partition()
            if p not in todo:
                continue
            if msg.offset() < todo[p]:
                events.append(json.loads(msg.value()))
            if msg.offset() + 1 >= todo[p]:
                del todo[p]
        return events
    finally:
        consumer.close()


def write_parquet(day: date, events: list[dict]) -> Path:
    """Fige les événements en Parquet via DuckDB (COPY ... TO)."""
    out_dir = DATA_DIR / f"date={day.isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "events.parquet"

    # L'heure LOCALE est calculée ici, à l'archivage : la couche requête
    # (profil horaire) n'aura pas à jongler avec les fuseaux.
    rows = [
        tuple(
            e[c] if c != "hour"
            else datetime.fromtimestamp(e["ts"]).astimezone().hour
            for c in COLUMNS
        )
        for e in sorted(events, key=lambda e: e["ts"])
    ]

    con = duckdb.connect()
    con.execute("""
        CREATE TABLE ev (
            ts BIGINT, hour TINYINT, station_id BIGINT, station_code VARCHAR,
            name VARCHAR, lat DOUBLE, lon DOUBLE, capacity SMALLINT,
            last_reported BIGINT, mechanical SMALLINT, ebike SMALLINT,
            bikes_available SMALLINT, docks_available SMALLINT,
            is_installed BOOLEAN, is_renting BOOLEAN, is_returning BOOLEAN,
            bikes_delta SMALLINT
        )
    """)
    con.executemany(
        f"INSERT INTO ev VALUES ({', '.join('?' * len(COLUMNS))})", rows
    )
    con.execute(f"COPY ev TO '{out.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    return out


def main() -> None:
    day = (
        date.fromisoformat(sys.argv[1])
        if len(sys.argv) > 1
        else date.today() - timedelta(days=1)
    )
    print(f"Archivage du {day.isoformat()}…")
    events = fetch_day_events(day)
    if not events:
        print("Aucun événement dans le journal pour cette date (rétention Kafka ?).")
        return
    out = write_parquet(day, events)
    taken = sum(-e["bikes_delta"] for e in events if e["bikes_delta"] < 0)
    print(f"{len(events)} événements → {out}")
    print(f"  vélos pris : {taken} · vélos rendus : "
          f"{sum(e['bikes_delta'] for e in events if e['bikes_delta'] > 0)}")


if __name__ == "__main__":
    main()
