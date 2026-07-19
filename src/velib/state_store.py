"""État « précédent » comparé d'un relevé à l'autre par le producer.

Le producer ne dépend que du Protocol `PreviousStateStore` : une implémentation
Redis pourrait remplacer la version en mémoire sans le modifier.
"""

from __future__ import annotations

from typing import Iterable, Protocol

from .models import StationState


class PreviousStateStore(Protocol):
    """Contrat minimal attendu par le producer."""

    def get(self, station_id: int) -> StationState | None:
        """État précédent d'une station, ou None si jamais observée."""
        ...

    def set_many(self, states: Iterable[StationState]) -> None:
        """Enregistre les états courants comme « précédents » du prochain relevé."""
        ...


class InMemoryStateStore:
    """Implémentation en mémoire (dict volatile, disparaît à l'arrêt du process)."""

    def __init__(self) -> None:
        self._data: dict[int, StationState] = {}

    def get(self, station_id: int) -> StationState | None:
        return self._data.get(station_id)

    def set_many(self, states: Iterable[StationState]) -> None:
        for state in states:
            self._data[state.station_id] = state

    def __len__(self) -> int:
        return len(self._data)
