"""Cœur du producer : détection des changements d'état des stations.

Logique pure, sans I/O, testable sans infrastructure. La publication Kafka et
la boucle temps réel vivent dans `kafka_producer.py`.
"""

from __future__ import annotations

from .models import StationChangeEvent, StationState
from .state_store import PreviousStateStore


def detect_changes(
    current: list[StationState],
    store: PreviousStateStore,
) -> list[StationChangeEvent]:
    """Compare le relevé courant à l'état précédent et renvoie les événements.

    Première observation d'une station : événement « initial » (bikes_delta=0),
    pour peupler les consumers dès le premier tour. Ensuite, un événement n'est
    émis que si la signature `tracked` a changé (delta = vélos courant − précédent).
    Le store est mis à jour à la fin : le relevé courant devient le « précédent »
    du prochain tour.
    """
    events: list[StationChangeEvent] = []
    for state in current:
        prev = store.get(state.station_id)
        if prev is None:
            events.append(StationChangeEvent.from_state(state, bikes_delta=0))
        elif prev.tracked != state.tracked:
            delta = state.bikes_available - prev.bikes_available
            events.append(StationChangeEvent.from_state(state, bikes_delta=delta))
    store.set_many(current)
    return events
