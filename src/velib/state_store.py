"""Store de l'« état précédent » que le producer compare d'un relevé à l'autre.

On définit une interface (`PreviousStateStore`) et une implémentation en mémoire.
La logique de diff du producer ne dépend que de l'interface : le jour où l'on
branche Redis (une fois Docker en place), on ajoute une classe `RedisStateStore`
qui respecte le même contrat, sans toucher au producer.
"""

from __future__ import annotations

from typing import Iterable, Protocol

from .models import StationState


class PreviousStateStore(Protocol):
    """Contrat minimal dont le producer a besoin pour détecter les changements."""

    def get(self, station_id: int) -> StationState | None:
        """État précédent d'une station, ou None si jamais observée."""
        ...

    def set_many(self, states: Iterable[StationState]) -> None:
        """Enregistre les états courants comme « précédents » du prochain relevé."""
        ...


class InMemoryStateStore:
    """Store en mémoire — pour développer et tester sans infra.

    Se comporte exactement comme la future version Redis du point de vue du
    producer, mais tout vit dans un dict et disparaît à l'arrêt du process.
    """

    def __init__(self) -> None:
        self._data: dict[int, StationState] = {}

    def get(self, station_id: int) -> StationState | None:
        return self._data.get(station_id)

    def set_many(self, states: Iterable[StationState]) -> None:
        for state in states:
            self._data[state.station_id] = state

    def __len__(self) -> int:
        return len(self._data)
