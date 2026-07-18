"""Consumer : lit le topic `velib.station.changes` et traite les événements.

Premier consumer du pipeline : un « moniteur » console qui affiche les
changements en direct. Simple exprès — le but est d'apprendre la mécanique
consumer (groupes, offsets, commits). Les consumers « utiles » (Redis,
dashboard) reprendront exactement la même structure.

Le modèle mental, différent du producer :
- Le producer POUSSE vers Kafka ; le consumer TIRE (poll) depuis Kafka.
  Le broker ne pousse jamais rien : c'est toujours le client qui demande.
- Un consumer appartient à un **groupe** (group.id). Kafka répartit les
  partitions du topic entre les membres du groupe : chaque partition est lue
  par EXACTEMENT UN membre. 1 membre → il lit tout ; 3 membres → ~1 partition
  chacun ; ça se réorganise tout seul quand un membre arrive/part (rebalance).
- La position de lecture (offset) est mémorisée PAR GROUPE, côté Kafka, dans
  un topic interne (__consumer_offsets). Deux groupes différents lisent donc
  le même topic indépendamment, chacun à son rythme — c'est ce qui permettra
  d'avoir Redis ET le dashboard ET Spark branchés sur le même flux.

Lancer :
    uv run python -m velib.kafka_consumer
"""

from __future__ import annotations

import json

from confluent_kafka import Consumer, KafkaError, Message

from .kafka_producer import BOOTSTRAP_SERVERS, TOPIC
from .models import StationChangeEvent

# Le nom du groupe EST l'identité de lecture : changer de group.id = repartir
# avec une position vierge (et donc relire selon auto.offset.reset).
GROUP_ID = "velib-monitor"


def create_consumer(group_id: str = GROUP_ID) -> Consumer:
    """Construit le Consumer avec sa configuration.

    Les trois clés qui comptent :
    - "group.id" : obligatoire dès qu'on utilise subscribe(). Voir GROUP_ID.
    - "auto.offset.reset" : que faire quand le groupe n'a AUCUN offset commité
      (première fois qu'il lit ce topic, ou offsets expirés). "earliest" = tout
      relire depuis le début du journal ; "latest" = ne prendre que ce qui
      arrive à partir de maintenant. ATTENTION au contresens classique : ce
      réglage ne sert QUE dans ce cas-là — un groupe qui a déjà commité
      reprend toujours à son dernier offset, quel que soit ce réglage.
      Pour nous : "earliest", on veut rejouer l'historique accumulé.
    - "enable.auto.commit" : par défaut (True), la lib commite les offsets en
      arrière-plan toutes les 5 s — simple, mais on peut committer un message
      pas encore traité (perte silencieuse si crash entre les deux). On le met
      à False pour committer NOUS-MÊMES après traitement → sémantique
      « au moins une fois » : au pire on retraite, jamais on ne perd.
    """
    # TODO: dict de config avec les 3 clés ci-dessus + bootstrap.servers,
    #       puis return Consumer(config)
    config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    return Consumer(config)
    


def parse_event(msg: Message) -> StationChangeEvent:
    """Désérialise un message Kafka en StationChangeEvent.

    Miroir de `StationChangeEvent.to_json()` côté producer : msg.value() est
    un `bytes` JSON dont les clés correspondent exactement aux champs de la
    dataclass — l'unpacking `StationChangeEvent(**d)` fait donc le travail.

    (Ce couple to_json/parse_event EST notre « contrat de schéma ». Version
    industrielle : Avro/Protobuf + Schema Registry — bon sujet de rapport.)
    """
    # TODO: json.loads + unpacking
    data = json.loads(msg.value())
    return StationChangeEvent(**data)
    


def handle_event(event: StationChangeEvent) -> None:
    """Traite un événement — ici : l'afficher lisiblement.

    C'est LE point de branchement du pipeline : la version Redis remplacera ce
    print par une écriture Redis, sans toucher au reste du fichier.
    """
    # TODO: un print d'une ligne, par ex. nom de station (tronqué/aligné),
    #       bikes_delta signé (format {:+d}), vélos dispo / capacité.
    print(f"Event received: {event}")
    


def run(group_id=GROUP_ID,handler=handle_event) -> None:
    """Boucle de consommation : poll → parse → handle → commit.

    Squelette attendu :
    1. consumer = create_consumer()
    2. consumer.subscribe([TOPIC])  ← déclenche l'entrée dans le groupe ;
       l'assignation des partitions arrive quelques secondes après, au premier poll
    3. boucle infinie :
       a. msg = consumer.poll(1.0) — attend jusqu'à 1 s, renvoie UN message
          ou None (silence : personne n'a publié, c'est normal, on re-poll)
       b. si msg.error() n'est pas None → erreur à afficher, puis continue
       c. event = parse_event(msg) ; handle_event(event)
       d. consumer.commit(msg) — enregistre « ce message est traité » ;
          l'ORDRE traiter-PUIS-committer est ce qui donne le « au moins une fois »
    4. sortie sur Ctrl+C, et dans un finally : consumer.close() — quitte le
       groupe proprement (rebalance immédiat au lieu d'un timeout de session)

    """
    # TODO
    consumer = create_consumer(group_id)
    consumer.subscribe([TOPIC])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            event = parse_event(msg)
            handler(event)
            consumer.commit(msg)
    except KeyboardInterrupt:
        print("Consumer interrupted by user.")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
