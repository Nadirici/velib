"""Producer : lit l'état des stations, détecte les changements, les publie.

Pour l'instant seul le **cœur** est implémenté : `detect_changes`, la logique
pure qui compare le relevé courant à l'état précédent et produit les événements.
La boucle temps réel et la publication Kafka seront ajoutées quand l'infra
(Docker : Kafka + Redis) sera en place — `detect_changes` ne changera pas.
"""

from __future__ import annotations

from dataclasses import replace

from .models import StationChangeEvent, StationState
from .state_store import InMemoryStateStore, PreviousStateStore
from .velib_api import VelibClient


def detect_changes(
    current: list[StationState],
    store: PreviousStateStore,
) -> list[StationChangeEvent]:
    """Compare le relevé courant à l'état précédent et renvoie les événements.

    - Première observation d'une station (aucun état précédent) : on émet un
      événement « initial » avec bikes_delta=0, pour que les consumers en aval
      (Redis, dashboard) soient peuplés dès le premier tour.
    - Ensuite : un événement n'est émis que si la signature `tracked` a changé
      (un compteur ou un flag de statut). Le delta = vélos courant - précédent.

    Après comparaison, le store est mis à jour : le relevé courant devient le
    « précédent » du prochain tour.
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


def _demo() -> None:  # pragma: no cover
    """Test manuel de la logique de diff, sans attendre de vrai changement.

    1. Premier relevé réel → tout est « initial ».
    2. On rejoue le MÊME relevé → zéro changement attendu.
    3. On mute artificiellement 3 stations → on doit voir exactement 3 événements
       avec le bon bikes_delta.
    """
    with VelibClient() as client:
        info = client.fetch_information()
        states = client.fetch_states(info)

    store = InMemoryStateStore()

    initial = detect_changes(states, store)
    print(f"1) Premier relevé  : {len(initial)} événements initiaux "
          f"(attendu = {len(states)} stations)")

    none = detect_changes(states, store)
    print(f"2) Relevé identique: {len(none)} événements (attendu = 0)")

    muted = []
    for state in states[:3]:
        # -2 vélos mécaniques, total ajusté en conséquence
        muted.append(
            replace(
                state,
                mechanical=max(0, state.mechanical - 2),
                bikes_available=max(0, state.bikes_available - 2),
            )
        )
    muted.extend(states[3:])

    changed = detect_changes(muted, store)
    print(f"3) 3 stations mutées: {len(changed)} événements (attendu = 3)")
    for event in changed:
        print(f"   - {event.name[:35]:35} bikes_delta={event.bikes_delta:+d} "
              f"-> {event.bikes_available} vélos")


if __name__ == "__main__":
    _demo()
