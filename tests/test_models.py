"""Tests des modèles : parse_bike_types, StationState.tracked, StationChangeEvent."""

from __future__ import annotations

import json

from velib.models import StationChangeEvent, parse_bike_types


class TestParseBikeTypes:
    def test_format_nominal(self):
        assert parse_bike_types([{"mechanical": 3}, {"ebike": 2}]) == (3, 2)

    def test_ordre_indifferent(self):
        assert parse_bike_types([{"ebike": 2}, {"mechanical": 3}]) == (3, 2)

    def test_type_manquant_vaut_zero(self):
        assert parse_bike_types([{"mechanical": 4}]) == (4, 0)
        assert parse_bike_types([{"ebike": 1}]) == (0, 1)

    def test_liste_vide(self):
        assert parse_bike_types([]) == (0, 0)

    def test_none(self):
        assert parse_bike_types(None) == (0, 0)


class TestTracked:
    def test_ignore_les_horodatages(self, make_state):
        """Un changement de ts/last_reported ne doit PAS être vu comme un changement."""
        a = make_state(ts=1000, last_reported=999)
        b = make_state(ts=2000, last_reported=1999)
        assert a.tracked == b.tracked

    def test_detecte_changement_compteur(self, make_state):
        a = make_state(mechanical=3)
        b = make_state(mechanical=1)
        assert a.tracked != b.tracked

    def test_detecte_changement_statut(self, make_state):
        a = make_state(is_renting=True)
        b = make_state(is_renting=False)
        assert a.tracked != b.tracked


class TestStationChangeEvent:
    def test_from_state_copie_les_champs(self, make_state):
        state = make_state(station_id=42, mechanical=7)
        event = StationChangeEvent.from_state(state, bikes_delta=-3)
        assert event.station_id == 42
        assert event.mechanical == 7
        assert event.bikes_delta == -3

    def test_key_est_id_station_en_bytes(self, make_state):
        event = StationChangeEvent.from_state(make_state(station_id=213688169), 0)
        assert event.key() == b"213688169"

    def test_to_json_serialise_tout(self, make_state):
        event = StationChangeEvent.from_state(make_state(station_id=1), bikes_delta=-2)
        payload = json.loads(event.to_json())
        assert payload["station_id"] == 1
        assert payload["bikes_delta"] == -2
        assert payload["name"] == "Station Test"
