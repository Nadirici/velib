"""Tests du cœur du producer : detect_changes."""

from __future__ import annotations

from velib.producer import detect_changes
from velib.state_store import InMemoryStateStore


class TestDetectChanges:
    def test_premier_releve_emet_tout_en_initial(self, make_state):
        store = InMemoryStateStore()
        states = [make_state(station_id=1), make_state(station_id=2)]
        events = detect_changes(states, store)
        assert len(events) == 2
        assert all(e.bikes_delta == 0 for e in events)
        assert len(store) == 2  # le store est peuplé après coup

    def test_releve_identique_n_emet_rien(self, make_state):
        store = InMemoryStateStore()
        states = [make_state(station_id=1)]
        detect_changes(states, store)  # premier tour
        events = detect_changes(states, store)  # même relevé
        assert events == []

    def test_changement_de_compteur_emet_avec_delta(self, make_state):
        store = InMemoryStateStore()
        detect_changes([make_state(station_id=1, bikes_available=5)], store)
        events = detect_changes([make_state(station_id=1, bikes_available=2)], store)
        assert len(events) == 1
        assert events[0].bikes_delta == -3
        assert events[0].bikes_available == 2

    def test_changement_de_statut_seul_emet_un_evenement(self, make_state):
        """Décision de conception : un flag de statut qui change déclenche un événement."""
        store = InMemoryStateStore()
        detect_changes([make_state(station_id=1, is_renting=True)], store)
        events = detect_changes([make_state(station_id=1, is_renting=False)], store)
        assert len(events) == 1
        assert events[0].bikes_delta == 0  # les compteurs n'ont pas bougé

    def test_changement_d_horodatage_seul_n_emet_rien(self, make_state):
        """ts/last_reported changent à chaque relevé : ne doivent rien déclencher."""
        store = InMemoryStateStore()
        detect_changes([make_state(station_id=1, ts=1000, last_reported=999)], store)
        events = detect_changes(
            [make_state(station_id=1, ts=2000, last_reported=1999)], store
        )
        assert events == []

    def test_seules_les_stations_modifiees_sont_emises(self, make_state):
        store = InMemoryStateStore()
        base = [make_state(station_id=i, bikes_available=5) for i in range(1, 4)]
        detect_changes(base, store)
        # seule la station 2 change
        modified = [
            make_state(station_id=1, bikes_available=5),
            make_state(station_id=2, bikes_available=1),
            make_state(station_id=3, bikes_available=5),
        ]
        events = detect_changes(modified, store)
        assert len(events) == 1
        assert events[0].station_id == 2
