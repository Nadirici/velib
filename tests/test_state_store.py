"""Tests du store d'état précédent (version en mémoire)."""

from __future__ import annotations

from velib.state_store import InMemoryStateStore


class TestInMemoryStateStore:
    def test_get_inconnu_renvoie_none(self):
        store = InMemoryStateStore()
        assert store.get(999) is None

    def test_set_many_puis_get(self, make_state):
        store = InMemoryStateStore()
        s1 = make_state(station_id=1)
        s2 = make_state(station_id=2)
        store.set_many([s1, s2])
        assert store.get(1) is s1
        assert store.get(2) is s2
        assert len(store) == 2

    def test_set_many_ecrase_l_etat_precedent(self, make_state):
        store = InMemoryStateStore()
        store.set_many([make_state(station_id=1, mechanical=5)])
        store.set_many([make_state(station_id=1, mechanical=2)])
        assert store.get(1).mechanical == 2
        assert len(store) == 1
