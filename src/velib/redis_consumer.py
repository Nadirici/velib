"""Consumer Redis : matérialise l'état courant des stations dans Redis.

L'idée d'architecture (à retenir, elle a un nom : **vue matérialisée**) :
- Kafka tient le FILM : l'historique ordonné de tous les changements ;
- Redis tient la PHOTO : le dernier état connu de chaque station, lisible
  en < 1 ms par le dashboard, qui ne parlera donc JAMAIS à Kafka directement.

Comme chaque événement est auto-suffisant (il embarque tout l'état de la
station), l'écriture Redis est un simple « remplace tout » — donc IDEMPOTENTE :
rejouer deux fois le même événement donne exactement le même état. C'est ce
qui rend le « au moins une fois » de Kafka indolore ici : un doublon ne
corrompt rien. Corollaire puissant : la vue Redis est JETABLE — on peut la
détruire (FLUSHDB) et la reconstruire intégralement en rejouant le topic.

Modèle de données Redis (conventions : préfixes séparés par `:`) :
- velib:station:{station_id} → HASH {champ: valeur} = dernier état complet.
  Un hash plutôt qu'une chaîne JSON : lisible dans redis-cli (HGETALL), et le
  dashboard pourra lire UN champ (HGET ... bikes_available) sans tout parser.
- velib:stations             → SET des station_id connus, pour pouvoir
  énumérer les stations (les clés Redis ne se « listent » pas efficacement).
- velib:last_ts              → STRING, ts du dernier événement traité
  (fraîcheur de la vue, à afficher sur le dashboard).

Prérequis :
    docker compose up -d      (le service redis a été ajouté)
    uv add redis              (client Python officiel, redis-py)

Lancer :
    uv run python -m velib.redis_consumer
"""

from __future__ import annotations

import os
from dataclasses import asdict
from typing import Callable

import redis
from .models import StationChangeEvent

REDIS_HOST = os.getenv("VELIB_REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("VELIB_REDIS_PORT", "6379"))

# Groupe DIFFÉRENT du moniteur : ce consumer a sa propre position de lecture,
# indépendante — les deux peuvent tourner en même temps sur le même topic.
GROUP_ID = "velib-redis"


def create_redis() -> redis.Redis:
    """Connexion Redis, avec vérification immédiate.

    - decode_responses=True : redis-py renvoie des str au lieu de bytes —
      indispensable pour manipuler les réponses confortablement.
    - Appeler r.ping() avant de retourner : la connexion redis-py est
      paresseuse (rien ne part sur le réseau avant la première commande) ;
      sans ping, un Redis éteint ne se détecterait qu'au premier événement.
      On préfère échouer franchement AU DÉMARRAGE (fail fast).
    """
    # TODO: redis.Redis(host=..., port=..., decode_responses=True) + ping()
    decode_responses = True
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=decode_responses)
    r.ping()
    return r
   


def station_key(station_id: int) -> str:
    """Clé du hash d'une station : velib:station:{id}."""
    # TODO (une ligne)
    return f"velib:station:{station_id}"


def make_handler(r: redis.Redis) -> Callable[[StationChangeEvent], None]:
    """Fabrique le handler branché sur CE client Redis (une closure).

    Pourquoi une fabrique : la boucle de consommation appelle handler(event)
    sans rien savoir de Redis. La closure capture `r` — c'est l'équivalent
    fonctionnel de l'injection de dépendance qu'on a faite avec
    PreviousStateStore côté producer.

    Le handler doit :
    1. écrire tout l'état : r.hset(station_key(...), mapping=...) —
       asdict(event) donne le dict des champs. PIÈGE à découvrir : Redis ne
       stocke que chaînes/nombres ; regarde bien les TYPES des champs de
       l'événement et demande-toi lequel va poser problème…
    2. r.sadd("velib:stations", event.station_id)
    3. r.set("velib:last_ts", event.ts)
    """
  
    def handle(event: StationChangeEvent) -> None:
        # TODO: les 3 écritures ci-dessus
        # Redis n'accepte pas les bool : on les stocke en 0/1 (bool est une
        # sous-classe d'int en Python, d'où le test isinstance strict).
        fields = {
            k: int(v) if isinstance(v, bool) else v
            for k, v in asdict(event).items()
        }
        r.hset(station_key(event.station_id), mapping=fields)
        r.sadd("velib:stations", event.station_id)
        r.set("velib:last_ts", event.ts)
       

    return handle


def run() -> None:
    """Boucle : Kafka → Redis. Réutilise la mécanique de kafka_consumer.

    Prérequis (petite refactorisation à faire dans kafka_consumer.py) :
    généraliser sa fonction run() en run(group_id=GROUP_ID, handler=handle_event)
    — deux paramètres avec ces valeurs par défaut, pour que le moniteur
    continue de marcher à l'identique. La boucle poll/commit est générique ;
    seul ce qu'on FAIT d'un événement change. Ici, il ne reste alors qu'à :

    1. r = create_redis()
    2. déléguer : kafka_consumer.run(group_id=GROUP_ID, handler=make_handler(r))
    """
    r = create_redis()
    from . import kafka_consumer
    kafka_consumer.run(group_id=GROUP_ID, handler=make_handler(r))

    

if __name__ == "__main__":
    run()
