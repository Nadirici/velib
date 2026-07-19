"""Producer temps réel : API Vélib' → détection de changements → Kafka.

La logique de diff pure est isolée dans `producer.py` (testable sans infra) ;
ce module gère le réseau (API, broker) et la boucle de collecte.

    python -m velib.kafka_producer
"""

from __future__ import annotations

import os
import time

import httpx
from confluent_kafka import Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .models import StationChangeEvent
from .producer import detect_changes
from .state_store import InMemoryStateStore
from .velib_api import VelibClient

# Surchargeable par variable d'env : kafka:19092 (listener interne) en
# conteneur, localhost:9092 en local.
BOOTSTRAP_SERVERS = os.getenv("VELIB_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "velib.station.changes"

# L'open data Vélib' se rafraîchit ~toutes les 60 s : sonder plus vite relirait
# les mêmes données.
POLL_INTERVAL_S = 60


def ensure_topic(admin: AdminClient | None = None) -> None:
    """Crée le topic s'il n'existe pas (idempotent).

    3 partitions (parallélisme et ordre par clé = station_id), réplication 1
    (broker unique).
    """
    if admin is None:
        admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    if TOPIC in admin.list_topics(timeout=5).topics:
        return
    future = admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)])
    future[TOPIC].result()  # attend la création et remonte l'erreur éventuelle


def create_producer() -> Producer:
    """Producer idempotent, acks=all (ni perte ni doublon sur retry), lots lz4."""
    return Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
        "compression.type": "lz4",
        "client.id": "velib-producer",
    })


def delivery_report(err, msg: Message) -> None:
    """Callback de livraison — ne trace que les échecs définitifs (perte)."""
    if err is not None:
        print(f"Échec de livraison (clé {msg.key()}) : {err}")


def publish_events(producer: Producer, events: list[StationChangeEvent]) -> None:
    """Publie un lot (clé = station_id, valeur = JSON), puis flush."""
    for event in events:
        producer.produce(
            topic=TOPIC,
            key=event.key(),
            value=event.to_json(),
            on_delivery=delivery_report,
        )
        producer.poll(0)  # traite les callbacks en attente, évite le BufferError
    producer.flush()


def run() -> None:
    """Boucle de collecte, toutes les POLL_INTERVAL_S. Une erreur API n'interrompt
    pas la boucle ; Ctrl+C flush les messages en attente avant de quitter."""
    client = VelibClient()
    store = InMemoryStateStore()
    producer = create_producer()
    ensure_topic()
    try:
        while True:
            try:
                info = client.fetch_information()
                states = client.fetch_states(info)
                events = detect_changes(states, store)
                publish_events(producer, events)
                print(f"{len(states)} stations relevées, {len(events)} événements publiés.")
            except httpx.HTTPError as e:
                print(f"Erreur API Vélib' : {e}")
            time.sleep(POLL_INTERVAL_S)
    except KeyboardInterrupt:
        print("Arrêt en cours…")
        producer.flush()


if __name__ == "__main__":
    run()
