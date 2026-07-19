"""Consumer générique : lit `velib.station.changes` et délègue à un handler.

La boucle poll/commit est indépendante du traitement (groupe et handler
injectables), et sert de socle au moniteur console comme au consumer Redis.

    python -m velib.kafka_consumer
"""

from __future__ import annotations

import json

from confluent_kafka import Consumer, Message

from .kafka_producer import BOOTSTRAP_SERVERS, TOPIC
from .models import StationChangeEvent

# Le group.id porte la position de lecture : deux groupes lisent le même topic
# indépendamment, chacun à son rythme.
GROUP_ID = "velib-monitor"


def create_consumer(group_id: str = GROUP_ID) -> Consumer:
    """Consumer commençant au début du journal pour un groupe neuf, commit
    manuel après traitement (sémantique « au moins une fois »)."""
    return Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })


def parse_event(msg: Message) -> StationChangeEvent:
    """Désérialise la valeur JSON en StationChangeEvent (miroir de to_json)."""
    return StationChangeEvent(**json.loads(msg.value()))


def handle_event(event: StationChangeEvent) -> None:
    """Handler par défaut du moniteur console : affiche l'événement."""
    print(f"{event.name[:35]:35} {event.bikes_available:3d} vélos ({event.bikes_delta:+d})")


def run(group_id: str = GROUP_ID, handler=handle_event) -> None:
    """Boucle poll → parse → handle → commit.

    Le commit APRÈS traitement garantit l'« au moins une fois ». Un message
    inparsable est écarté (et committé) pour ne pas bloquer la partition en
    boucle. `close()` quitte le groupe proprement (rebalance immédiat).
    """
    consumer = create_consumer(group_id)
    consumer.subscribe([TOPIC])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Erreur Kafka : {msg.error()}")
                continue
            try:
                handler(parse_event(msg))
            except Exception as e:
                # Message empoisonné (malformé) : écarté au lieu de bloquer la
                # partition indéfiniment. En production : dead-letter queue.
                print(f"Message écarté (poison) partition={msg.partition()} "
                      f"offset={msg.offset()} : {e!r}")
            consumer.commit(msg)
    except KeyboardInterrupt:
        print("Arrêt du consumer.")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
