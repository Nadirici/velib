"""Tests du client API, HTTP mocké (aucun accès réseau)."""

from __future__ import annotations

import json

import httpx
import pytest

from velib.velib_api import VelibClient

# Deux stations dans l'info ; le status en renvoie une TROISIEME (id 3) absente
# de l'info -> elle doit être ignorée faute de coordonnées.
_INFORMATION = {
    "data": {
        "stations": [
            {
                "station_id": 1,
                "stationCode": "00001",
                "name": "Alpha",
                "lat": 48.86,
                "lon": 2.35,
                "capacity": 30,
            },
            {
                "station_id": 2,
                "stationCode": "00002",
                "name": "Beta",
                "lat": 48.87,
                "lon": 2.36,
                "capacity": 20,
            },
        ]
    }
}

_STATUS = {
    "lastUpdatedOther": 1_784_296_000,
    "ttl": 3600,
    "data": {
        "stations": [
            {
                "station_id": 1,
                "num_bikes_available": 5,
                "num_bikes_available_types": [{"mechanical": 3}, {"ebike": 2}],
                "num_docks_available": 25,
                "is_installed": 1,
                "is_renting": 1,
                "is_returning": 1,
                "last_reported": 1_784_295_000,
            },
            {
                "station_id": 3,  # absente de l'info -> ignorée
                "num_bikes_available": 9,
                "num_bikes_available_types": [{"mechanical": 9}, {"ebike": 0}],
                "num_docks_available": 1,
                "is_installed": 1,
                "is_renting": 1,
                "is_returning": 1,
                "last_reported": 1_784_295_000,
            },
        ]
    },
}


def _handler(request: httpx.Request) -> httpx.Response:
    if request.url.path.endswith("station_information.json"):
        return httpx.Response(200, json=_INFORMATION)
    if request.url.path.endswith("station_status.json"):
        return httpx.Response(200, json=_STATUS)
    return httpx.Response(404)


@pytest.fixture
def client() -> VelibClient:
    mock = httpx.Client(transport=httpx.MockTransport(_handler))
    return VelibClient(client=mock)


class TestCycleDeVie:
    def test_context_manager_ferme_le_client_possede(self):
        """Construction sans réseau, puis fermeture via le context-manager."""
        with VelibClient() as vc:
            assert vc._client is not None
        # sortie du `with` -> close() appelé sur le client possédé, sans erreur
        assert vc._client.is_closed

    def test_ne_ferme_pas_un_client_injecte(self):
        """Si le client est fourni de l'extérieur, on ne le ferme pas."""
        injected = httpx.Client(transport=httpx.MockTransport(_handler))
        vc = VelibClient(client=injected)
        vc.close()
        assert injected.is_closed is False
        injected.close()


class TestFetchInformation:
    def test_indexe_par_station_id(self, client):
        info = client.fetch_information()
        assert set(info) == {1, 2}
        assert info[1].name == "Alpha"
        assert info[2].capacity == 20


class TestFetchStates:
    def test_joint_info_et_status(self, client):
        info = client.fetch_information()
        states = {s.station_id: s for s in client.fetch_states(info)}
        assert states[1].name == "Alpha"  # vient de l'info
        assert states[1].mechanical == 3  # vient du status, parsé
        assert states[1].ebike == 2
        assert states[1].bikes_available == 5
        assert states[1].ts == 1_784_296_000  # lastUpdatedOther du flux

    def test_station_absente_de_l_info_est_ignoree(self, client):
        info = client.fetch_information()
        states = client.fetch_states(info)
        # la station 3 (dans le status mais pas l'info) ne doit pas apparaître
        assert {s.station_id for s in states} == {1}

    def test_flags_convertis_en_bool(self, client):
        info = client.fetch_information()
        state = client.fetch_states(info)[0]
        assert state.is_renting is True
        assert isinstance(state.is_renting, bool)
