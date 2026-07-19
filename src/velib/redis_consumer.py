"""Consumer Redis : matérialise l'état courant de chaque station (vue matérialisée).

Kafka tient l'historique des changements ; Redis tient le dernier état connu,
lu en < 1 ms par le dashboard. Chaque événement étant auto-suffisant, l'écriture
est un « remplace tout » idempotent : la vue est reconstructible en rejouant le
topic.

Modèle de données :
- velib:station:{id} → HASH de l'état complet de la station ;
- velib:stations     → SET des station_id connus (pour énumérer) ;
- velib:last_ts      → ts du dernier événement traité (fraîcheur de la vue).

    python -m velib.redis_consumer
"""

from __future__ import annotations

import os
from dataclasses import asdict
from typing import Callable

import redis

from . import kafka_consumer
from .models import StationChangeEvent

REDIS_HOST = os.getenv("VELIB_REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("VELIB_REDIS_PORT", "6379"))

# Groupe distinct du moniteur : position de lecture indépendante.
GROUP_ID = "velib-redis"


def create_redis() -> redis.Redis:
    """Connexion Redis (réponses en str), vérifiée immédiatement (fail fast)."""
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    return r


def station_key(station_id: int) -> str:
    return f"velib:station:{station_id}"


def make_handler(r: redis.Redis) -> Callable[[StationChangeEvent], None]:
    """Handler branché sur `r` (injection de dépendance par closure)."""

    def handle(event: StationChangeEvent) -> None:
        # Redis ne stocke pas les bool : convertis en 0/1 (bool est sous-classe
        # d'int en Python, d'où le test isinstance strict).
        fields = {
            k: int(v) if isinstance(v, bool) else v
            for k, v in asdict(event).items()
        }
        r.hset(station_key(event.station_id), mapping=fields)
        r.sadd("velib:stations", event.station_id)
        r.set("velib:last_ts", event.ts)

    return handle


def run() -> None:
    """Branche l'écriture Redis sur la boucle générique de kafka_consumer."""
    r = create_redis()
    kafka_consumer.run(group_id=GROUP_ID, handler=make_handler(r))


if __name__ == "__main__":
    run()
