"""Modèles de données du pipeline.

On distingue :
- `StationInfo` : le contexte fixe d'une station (position, nom, capacité),
  lu une seule fois depuis station_information.json ;
- `StationState` : l'état complet d'une station à un instant donné, obtenu en
  joignant le statique et le dynamique. C'est ce que le producer compare d'un
  relevé à l'autre pour détecter les changements.

Tous les horodatages sont des epoch UTC (secondes), jamais convertis en heure
locale — ce qui permet de croiser les données par temps d'événement.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class StationInfo:
    """Métadonnées fixes d'une station (station_information.json)."""

    station_id: int
    station_code: str
    name: str
    lat: float
    lon: float
    capacity: int


@dataclass(frozen=True, slots=True)
class StationState:
    """État complet d'une station à un instant (statique + dynamique joints)."""

    station_id: int
    station_code: str
    name: str
    lat: float
    lon: float
    capacity: int
    # Horodatages (epoch UTC, secondes)
    ts: int  # instant du relevé (lastUpdatedOther du flux status)
    last_reported: int  # dernière remontée de la station elle-même
    # Compteurs dynamiques
    mechanical: int
    ebike: int
    bikes_available: int
    docks_available: int
    # Flags de statut
    is_installed: bool
    is_renting: bool
    is_returning: bool

    @property
    def tracked(self) -> tuple:
        """Signature comparée d'un relevé au suivant : un événement est émis dès
        qu'elle change. Inclut compteurs et flags de statut (pour capter les
        mises hors-service), exclut les horodatages (qui bougent à chaque relevé).
        """
        return (
            self.mechanical,
            self.ebike,
            self.bikes_available,
            self.docks_available,
            self.is_installed,
            self.is_renting,
            self.is_returning,
        )


@dataclass(frozen=True, slots=True)
class StationChangeEvent:
    """Message publié dans le topic `velib.station.changes`.

    `StationState` auto-suffisant (il embarque le contexte fixe, les consumers
    n'ont rien à rejoindre) augmenté de `bikes_delta` : la variation de vélos
    depuis le relevé précédent.
    """

    station_id: int
    station_code: str
    name: str
    lat: float
    lon: float
    capacity: int
    ts: int
    last_reported: int
    mechanical: int
    ebike: int
    bikes_available: int
    docks_available: int
    is_installed: bool
    is_renting: bool
    is_returning: bool
    bikes_delta: int

    @classmethod
    def from_state(cls, state: StationState, bikes_delta: int) -> StationChangeEvent:
        return cls(
            station_id=state.station_id,
            station_code=state.station_code,
            name=state.name,
            lat=state.lat,
            lon=state.lon,
            capacity=state.capacity,
            ts=state.ts,
            last_reported=state.last_reported,
            mechanical=state.mechanical,
            ebike=state.ebike,
            bikes_available=state.bikes_available,
            docks_available=state.docks_available,
            is_installed=state.is_installed,
            is_renting=state.is_renting,
            is_returning=state.is_returning,
            bikes_delta=bikes_delta,
        )

    def key(self) -> bytes:
        """Clé de partition Kafka : l'id station, pour garantir l'ordre par station."""
        return str(self.station_id).encode()

    def to_json(self) -> bytes:
        """Sérialisation pour la valeur du message Kafka."""
        return json.dumps(asdict(self)).encode()


def parse_bike_types(raw: list[dict] | None) -> tuple[int, int]:
    """Aplatit `num_bikes_available_types` — renvoyé par l'API sous forme de
    liste d'objets à une clé, [{"mechanical": 0}, {"ebike": 2}] — en un couple
    (mechanical, ebike).
    """
    counts: dict[str, int] = {}
    for item in raw or []:
        counts.update(item)
    return int(counts.get("mechanical", 0)), int(counts.get("ebike", 0))
