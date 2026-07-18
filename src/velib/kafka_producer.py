"""Publication Kafka : la boucle temps réel du producer.

Ce module est volontairement séparé de `producer.py` (qui reste de la logique
pure, testable sans infra) : ici on touche au réseau — API Vélib d'un côté,
broker Kafka de l'autre. La chaîne complète :

    VelibClient.fetch_states() ──► detect_changes() ──► publish_events() ──► Kafka
                                        │
                                 PreviousStateStore

Prérequis :
    docker compose up -d          (démarre le broker, voir docker-compose.yml)
    pip install confluent-kafka   (client Kafka officiel de Confluent, wrapper
                                   de librdkafka — le standard de facto en Python)

Lancer :
    python -m velib.kafka_producer
"""

from __future__ import annotations

import time

from confluent_kafka import Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .models import StationChangeEvent
from .producer import detect_changes
from .state_store import InMemoryStateStore
from .velib_api import VelibClient
import httpx

# Adresse de bootstrap : le point d'entrée du cluster. Le client s'y connecte,
# découvre la topologie (brokers, partitions), puis parle aux bons brokers.
BOOTSTRAP_SERVERS = "localhost:9092"

TOPIC = "velib.station.changes"

# L'open data Vélib est rafraîchi environ toutes les 60 s côté serveur :
# interroger plus souvent ne ferait que relire les mêmes données.
POLL_INTERVAL_S = 60


def ensure_topic(admin: AdminClient | None = None) -> None:
    """Crée le topic s'il n'existe pas (idempotent : ne fait rien sinon).

    Décisions à prendre (et à assumer dans le rapport de projet) :
    - `num_partitions` : la partition est l'unité de parallélisme ET d'ordre.
      Notre clé étant station_id, toutes les stations dont le hash tombe dans
      la même partition sont ordonnées entre elles. Avec ~1500 stations et un
      seul broker local, quelques partitions (ex. 3) suffisent — assez pour
      démontrer le partitionnement, pas assez pour compliquer le debug.
    - `replication_factor` : nombre de copies de chaque partition. Forcément 1
      ici (un seul broker) ; 3 en prod pour survivre à la perte d'un broker.
    - `config` : penser à `retention.ms` — combien de temps Kafka garde les
      messages avant de les purger (7 jours par défaut).
    """

    if admin is None: 
        admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    if TOPIC in admin.list_topics(timeout=5).topics:
        return

    else:
        future = admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)])
        future[TOPIC].result() 
 



def create_producer() -> Producer:
    """Construit le Producer Kafka avec sa configuration.

    Le Producer de confluent-kafka est ASYNCHRONE : `produce()` ne fait que
    déposer le message dans une file en mémoire ; un thread interne (librdkafka)
    regroupe les messages en lots (batches) et les envoie au broker. C'est ce
    qui le rend rapide — mais ça veut dire qu'un `produce()` qui « réussit »
    n'a encore rien envoyé sur le réseau.

    Réglages à connaître (dict de config, clés en chaînes) :
    - "bootstrap.servers" : obligatoire.
    - "acks" : combien de brokers doivent confirmer l'écriture avant de dire
      « OK ». "0" = on n'attend rien (rapide, on peut perdre des messages),
      "1" = le leader de la partition, "all" = tous les réplicas synchronisés
      (aucune perte tant qu'un réplica survit). Avec 1 broker, "1" et "all"
      sont équivalents, mais autant prendre l'habitude de "all".
    - "enable.idempotence" : True → le broker déduplique les retries internes
      du producer (un timeout réseau ne crée pas de doublon). Implique acks=all.
    - "linger.ms" : combien de temps attendre pour remplir un batch avant de
      l'envoyer. Quelques ms suffisent pour grouper une vague d'événements.
    - "compression.type" : ex. "gzip" ou "lz4" — nos events JSON se
      compressent très bien.
    """
    
    config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
        "compression.type": "lz4",
        "client.id": "velib-producer",
      
    }
    return Producer(config)



def delivery_report(err, msg: Message) -> None:
    """Callback appelé pour CHAQUE message, une fois son sort connu.

    C'est le pendant du produce() asynchrone : c'est ICI qu'on apprend si le
    message a vraiment été écrit dans Kafka ou non. Le callback est invoqué
    pendant les appels à producer.poll() / producer.flush().

    - err is not None → échec définitif (après les retries internes) :
      à logger avec le contexte (topic, clé) — c'est notre trace de perte.
    - err is None → succès : msg.topic(), msg.partition(), msg.offset()
      disent où le message a atterri. Log utile en debug pour VOIR le
      partitionnement par clé à l'œuvre.
    """
    # TODO: traiter les deux cas (un simple print suffit pour l'instant)
    if err is not None:
        print(f"Message delivery failed for key {msg.key()}: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def publish_events(producer: Producer, events: list[StationChangeEvent]) -> None:
    """Publie un lot d'événements dans le topic.

    Les briques existent déjà côté modèle :
    - event.key()     → la clé du message (station_id encodé). Kafka hashe la
      clé pour choisir la partition → même station = même partition = ordre
      garanti par station. C'est LE point qui justifie notre choix de clé.
    - event.to_json() → la valeur (payload JSON).

    Points d'attention :
    - produce() peut lever BufferError si la file interne est pleine ; la
      parade : appeler producer.poll(0) régulièrement (ce qui déclenche aussi
      les delivery_report en attente), puis retenter.
    - En fin de lot, producer.flush(timeout) force l'envoi de tout ce qui
      reste en file et attend les confirmations. Pour notre cadence (1 lot
      par minute), flush après chaque lot est simple et sûr.
    """
    # TODO: boucler sur events → producer.produce(topic=..., key=..., value=...,
    #       on_delivery=delivery_report)
    for event in events:
        producer.produce(
            topic=TOPIC,
            key=event.key(),
            value=event.to_json(),
            on_delivery=delivery_report
        )
        producer.poll(0) 
    producer.flush()
    
    


def run() -> None:
    """Boucle principale : API → diff → Kafka, toutes les POLL_INTERVAL_S.

    Squelette attendu :
    1. ensure_topic()
    2. créer le producer, le VelibClient, le store (InMemoryStateStore pour
       l'instant — Redis prendra sa place sans changer cette boucle, grâce au
       Protocol PreviousStateStore)
    3. charger le statique UNE fois (fetch_information)
    4. boucle infinie :
       a. fetch_states → detect_changes → publish_events
       b. log court (n stations relevées, n événements publiés)
       c. time.sleep(POLL_INTERVAL_S)

    Robustesse minimale :
    - une erreur HTTP de l'API Vélib (timeout, 5xx) ne doit PAS tuer la
      boucle : logger et attendre le prochain tour ;
    - sur Ctrl+C (KeyboardInterrupt), sortir proprement : flush() du producer
      pour ne pas perdre les messages encore en file.
    """
    # TODO
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
                print(f"Fetched {len(states)} stations, published {len(events)} events.")
            except httpx.HTTPError as e:
                print(f"Error during processing: {e}")
            time.sleep(POLL_INTERVAL_S)
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
        producer.flush()
    


if __name__ == "__main__":
    run()
