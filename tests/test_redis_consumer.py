"""Tests du consumer Redis (fakeredis : vrai protocole, zéro serveur)."""

from __future__ import annotations

import fakeredis
import pytest

import velib.kafka_consumer as kc
import velib.redis_consumer as rc
from velib.models import StationChangeEvent


@pytest.fixture
def r():
    return fakeredis.FakeRedis(decode_responses=True)


def test_station_key():
    assert rc.station_key(42) == "velib:station:42"


class TestHandler:
    def test_ecrit_hash_set_et_last_ts(self, r, make_state):
        event = StationChangeEvent.from_state(make_state(station_id=7), -2)
        rc.make_handler(r)(event)

        stored = r.hgetall(rc.station_key(7))
        assert stored["name"] == "Station Test"
        assert stored["bikes_available"] == "5"
        assert r.smembers("velib:stations") == {"7"}
        assert r.get("velib:last_ts") == str(event.ts)

    def test_bools_stockes_en_0_1(self, r, make_state):
        """Non-régression du DataError : Redis ne connaît pas les bool."""
        event = StationChangeEvent.from_state(
            make_state(is_renting=False, is_installed=True), 0
        )
        rc.make_handler(r)(event)
        stored = r.hgetall(rc.station_key(event.station_id))
        assert stored["is_renting"] == "0"
        assert stored["is_installed"] == "1"

    def test_idempotent_rejouer_ne_change_rien(self, r, make_state):
        event = StationChangeEvent.from_state(make_state(), -1)
        handler = rc.make_handler(r)
        handler(event)
        before = r.hgetall(rc.station_key(event.station_id))
        handler(event)  # doublon « au moins une fois »
        assert r.hgetall(rc.station_key(event.station_id)) == before


def test_create_redis_ping_immediat(monkeypatch):
    # FakeRedis accepte host/port/decode_responses comme le vrai client.
    monkeypatch.setattr(rc.redis, "Redis", fakeredis.FakeRedis)
    client = rc.create_redis()
    assert client.ping() is True


def test_run_delegue_a_la_boucle_generique(monkeypatch, r):
    monkeypatch.setattr(rc, "create_redis", lambda: r)
    called = {}

    def fake_run(group_id=None, handler=None):
        called["group_id"] = group_id
        called["handler"] = handler

    monkeypatch.setattr(kc, "run", fake_run)
    rc.run()

    assert called["group_id"] == rc.GROUP_ID
    assert callable(called["handler"])
