"""Client de l'API Vélib' Métropole (open data).

Deux endpoints :
- station_information.json : le contexte fixe (nom, position, capacité) ;
- station_status.json      : le dynamique (vélos, bornettes, statut).

Le client charge le statique une fois, puis peut relire le dynamique à volonté
et renvoyer des `StationState` déjà joints et prêts à consommer.
"""

from __future__ import annotations

import ssl

import httpx
import truststore

from .models import StationInfo, StationState, parse_bike_types

# Utilise le magasin de certificats de l'OS (Windows) plutôt que le bundle
# certifi de httpx. Indispensable derrière un proxy SSL d'entreprise, qui
# ré-signe les certificats avec une CA racine présente dans le magasin système.
_SSL_CONTEXT = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

BASE_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole"
INFORMATION_URL = f"{BASE_URL}/station_information.json"
STATUS_URL = f"{BASE_URL}/station_status.json"

# Certains serveurs open data filtrent le User-Agent par défaut des libs HTTP.
_HEADERS = {"User-Agent": "velib-dashboard/0.1 (portfolio project)"}


class VelibClient:
    """Accès aux deux endpoints Vélib, avec jointure statique/dynamique."""

    def __init__(self, client: httpx.Client | None = None, timeout: float = 10.0):
        self._client = client or httpx.Client(
            timeout=timeout, headers=_HEADERS, verify=_SSL_CONTEXT
        )
        self._owns_client = client is None

    def fetch_information(self) -> dict[int, StationInfo]:
        """Charge le contexte fixe, indexé par station_id."""
        resp = self._client.get(INFORMATION_URL)
        resp.raise_for_status()
        stations = resp.json()["data"]["stations"]
        info: dict[int, StationInfo] = {}
        for s in stations:
            sid = s["station_id"]
            # `or` couvre à la fois la clé absente et la valeur null (que le
            # défaut de dict.get, lui, ne remplacerait pas).
            info[sid] = StationInfo(
                station_id=sid,
                station_code=s.get("stationCode") or "",
                name=s.get("name") or "",
                lat=s["lat"],
                lon=s["lon"],
                capacity=s.get("capacity") or 0,
            )
        return info

    def _fetch_status(self) -> tuple[int, list[dict]]:
        """Renvoie (lastUpdatedOther, liste brute des stations)."""
        resp = self._client.get(STATUS_URL)
        resp.raise_for_status()
        payload = resp.json()
        return payload["lastUpdatedOther"], payload["data"]["stations"]

    def fetch_states(self, info: dict[int, StationInfo]) -> list[StationState]:
        """Lit le dynamique et le joint au contexte fixe fourni.

        Les stations présentes dans le status mais absentes du fichier
        d'information (cas rare, station en cours de déploiement) sont ignorées :
        sans coordonnées, on ne peut ni les afficher ni les exploiter.
        """
        ts, stations = self._fetch_status()
        states: list[StationState] = []
        for s in stations:
            sid = s["station_id"]
            meta = info.get(sid)
            if meta is None:
                continue
            mechanical, ebike = parse_bike_types(s.get("num_bikes_available_types"))
            # Test explicite sur None (pas `or`) : 0 est une valeur légitime,
            # distincte du fallback mechanical + ebike.
            bikes = s.get("num_bikes_available")
            if bikes is None:
                bikes = mechanical + ebike
            states.append(
                StationState(
                    station_id=sid,
                    station_code=meta.station_code,
                    name=meta.name,
                    lat=meta.lat,
                    lon=meta.lon,
                    capacity=meta.capacity,
                    ts=ts,
                    last_reported=s.get("last_reported") or ts,
                    mechanical=mechanical,
                    ebike=ebike,
                    bikes_available=bikes,
                    docks_available=s.get("num_docks_available") or 0,
                    is_installed=bool(s.get("is_installed", 0)),
                    is_renting=bool(s.get("is_renting", 0)),
                    is_returning=bool(s.get("is_returning", 0)),
                )
            )
        return states

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> VelibClient:
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def main() -> None:  # pragma: no cover
    """Test manuel : lit l'API et affiche un résumé + une station exemple."""
    with VelibClient() as client:
        info = client.fetch_information()
        states = client.fetch_states(info)

    total_bikes = sum(s.bikes_available for s in states)
    empty = sum(1 for s in states if s.bikes_available == 0 and s.is_renting)
    print(f"Stations (info)        : {len(info)}")
    print(f"Stations (états joints): {len(states)}")
    print(f"Vélos disponibles      : {total_bikes}")
    print(f"Stations vides (ouvertes): {empty}")
    print("\nExemple de station :")
    sample = states[0]
    for field in sample.__slots__:
        print(f"  {field:16} = {getattr(sample, field)}")


if __name__ == "__main__":
    main()
