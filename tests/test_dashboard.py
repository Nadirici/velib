"""Tests du serveur dashboard : endpoints (fakeredis), flux SSE, relais Kafka."""

from __future__ import annotations

import queue
import sys
from datetime import date
from types import SimpleNamespace

import fakeredis
import pytest
from confluent_kafka import TopicPartition
from fastapi.testclient import TestClient

import velib.redis_consumer as rc
from velib.models import StationChangeEvent


@pytest.fixture
def dash(monkeypatch):
    """Importe un module dashboard NEUF, branché sur un Redis simulé.

    L'import a un effet de bord (connexion Redis au chargement) : on patche
    redis.Redis avant, et on force une réimportation pour repartir d'un état
    vierge (_activity, _clients) à chaque test.
    """
    # Un FakeServer par test : sans lui, fakeredis partage un état global
    # entre instances et les tests se contamineraient entre eux.
    server = fakeredis.FakeServer()
    monkeypatch.setattr(
        "redis.Redis",
        lambda *a, **kw: fakeredis.FakeRedis(server=server, **kw),
    )
    sys.modules.pop("velib.dashboard", None)
    import velib.dashboard as module
    yield module
    sys.modules.pop("velib.dashboard", None)


def seed_station(dash, make_state, delta=0, **overrides):
    """Peuple la photo Redis via le VRAI handler du consumer (intégration)."""
    event = StationChangeEvent.from_state(make_state(**overrides), delta)
    rc.make_handler(dash._r)(event)
    return event


def act(dash, station_id=1, delta=-1, offset_s=60):
    dash._activity.add({
        "ts": dash._activity.day_start + offset_s,
        "station_id": station_id,
        "bikes_delta": delta,
    })


class TestStations:
    def test_snapshot_retype(self, dash, make_state):
        seed_station(dash, make_state, station_id=1, is_renting=False)
        payload = TestClient(dash.app).get("/api/stations").json()

        [station] = payload["stations"]
        assert station["is_renting"] is False       # bool reconstruit
        assert station["bikes_available"] == 5      # int reconstruit
        assert station["lat"] == pytest.approx(48.86)
        assert isinstance(payload["last_ts"], int)

    def test_snapshot_vide(self, dash):
        payload = TestClient(dash.app).get("/api/stations").json()
        assert payload["stations"] == []
        assert payload["last_ts"] is None


class TestActivity:
    def test_serie_et_top(self, dash, make_state):
        seed_station(dash, make_state, station_id=1)
        act(dash, station_id=1, delta=-2)
        payload = TestClient(dash.app).get("/api/activity?step=300").json()

        assert payload["step"] == 300
        assert sum(p["taken"] for p in payload["points"]) == 2
        [top] = payload["top"]
        assert top["name"] == "Station Test"        # nom résolu via Redis
        assert top["moves"] == 2

    def test_granularite_invalide_retombe_sur_300(self, dash):
        assert TestClient(dash.app).get("/api/activity?step=999").json()["step"] == 300


class TestBusiness:
    def test_vue_exploitant(self, dash, make_state):
        # A : ouverte et VIDE, très demandée → priorité de rééquilibrage n°1
        seed_station(dash, make_state, station_id=1, name="A vide",
                     bikes_available=0, mechanical=0, ebike=0)
        act(dash, station_id=1, delta=-6)
        # B : ouverte et PLEINE
        seed_station(dash, make_state, station_id=2, name="B pleine",
                     docks_available=0)
        act(dash, station_id=2, delta=2)
        # C : saine, puits net (+4)
        seed_station(dash, make_state, station_id=3, name="C puits")
        act(dash, station_id=3, delta=4)
        # D : hors service, sa demande est « à risque »
        seed_station(dash, make_state, station_id=4, name="D HS",
                     is_installed=False)
        act(dash, station_id=4, delta=-1)

        b = TestClient(dash.app).get("/api/business").json()

        assert b["today_taken"] == 7                # 6 + 1
        assert b["today_returned"] == 6             # 2 + 4
        assert [s["name"] for s in b["rebalance"]] == ["A vide", "B pleine"]
        assert b["rebalance"][0]["state"] == "vide"
        assert b["rebalance"][1]["state"] == "pleine"
        # Demande à risque : A (6) + B (2) + D (1) = 9 sur 13 mouvements
        assert b["at_risk_moves"] == 9
        assert b["total_moves"] == 13
        assert [s["name"] for s in b["sinks"]] == ["C puits", "B pleine"]
        assert [s["name"] for s in b["sources"]] == ["A vide", "D HS"]
        # Σ|net| = 6+2+4+1 = 13 → 13 // 2
        assert b["to_move"] == 6


class TestHistory:
    def test_sans_archives(self, dash, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        client = TestClient(dash.app)
        assert client.get("/api/history/daily").json() == {"days": []}
        assert client.get("/api/history/profile").json() == {"hours": []}

    def test_avec_archives(self, dash, tmp_path, monkeypatch, make_state):
        monkeypatch.chdir(tmp_path)
        import velib.archiver as ar
        day = date(2026, 7, 18)
        # Deux événements sérialisés comme dans le topic, replacés dans le jour.
        ts = ar._epoch_ms(day) // 1000 + 8 * 3600
        import json as _json
        e1 = _json.loads(StationChangeEvent.from_state(make_state(), -2).to_json())
        e2 = _json.loads(StationChangeEvent.from_state(make_state(), 3).to_json())
        e1["ts"], e2["ts"] = ts, ts + 60
        ar.write_parquet(day, [e1, e2])

        client = TestClient(dash.app)
        [d] = client.get("/api/history/daily").json()["days"]
        assert d["date"] == "2026-07-18"
        assert d["events"] == 2 and d["taken"] == 2 and d["returned"] == 3

        hours = client.get("/api/history/profile").json()["hours"]
        assert sum(h["taken"] for h in hours) == 2


class TestStream:
    def test_push_et_desinscription(self, dash):
        import asyncio

        # Starlette enrobe le générateur sync en itérateur async (threadpool) :
        # on le consomme donc dans une boucle asyncio.
        response = dash.stream()
        gen = response.body_iterator
        [q] = list(dash._clients)
        q.put("{'x': 1}")

        async def scenario():
            line = await anext(gen)
            await gen.aclose()            # le navigateur ferme la connexion
            return line

        assert asyncio.run(scenario()) == "data: {'x': 1}\n\n"
        assert dash._clients == set()     # le finally a désinscrit la file


class Boom(Exception):
    pass


class FakeMsg:
    def __init__(self, offset, payload: bytes, partition=0):
        self._o, self._v, self._p = offset, payload, partition

    def error(self): return None
    def value(self): return self._v
    def partition(self): return self._p
    def offset(self): return self._o


class TestRelayLoop:
    def test_replay_puis_direct(self, dash, monkeypatch, capsys):
        import json as _json
        mk = lambda sid, d: _json.dumps(
            {"ts": dash._activity.day_start + 60,
             "station_id": sid, "bikes_delta": d}).encode()
        replay = [FakeMsg(0, mk(1, -1)), FakeMsg(1, mk(2, -1))]
        live = FakeMsg(2, mk(3, -1))

        class FakeConsumer:
            def __init__(self, script):
                self._script = list(script)

            def list_topics(self, topic, timeout=None):
                return SimpleNamespace(
                    topics={topic: SimpleNamespace(partitions={0: None})})

            def offsets_for_times(self, tps, timeout=None):
                return [TopicPartition(tp.topic, tp.partition, 0) for tp in tps]

            def get_watermark_offsets(self, tp, timeout=None):
                return (0, 2)             # le replay couvre les offsets 0 et 1

            def assign(self, tps):
                self.assigned = tps

            def poll(self, timeout):
                if not self._script:
                    raise Boom            # fin du scénario : sortir du direct
                item = self._script.pop(0)
                if item is None:
                    return None
                return item

        consumer = FakeConsumer([*replay, None, live])
        monkeypatch.setattr(dash, "Consumer", lambda cfg: consumer)

        q: queue.Queue = queue.Queue(maxsize=10)
        dash._clients.add(q)

        with pytest.raises(Boom):
            dash._relay_loop()

        # Les 3 événements ont nourri l'agrégation (2 replay + 1 direct)…
        points = dash._activity.series(
            60, dash._activity.day_start, dash._activity.day_start + 120)
        assert sum(p["taken"] for p in points) == 3
        # …mais SEUL l'événement du direct a été diffusé aux navigateurs.
        assert q.get_nowait() == live.value().decode()
        assert q.empty()
        assert "2 événements rejoués" in capsys.readouterr().out


class TestRelayDegrade:
    def test_kafka_injoignable_ne_tue_pas_le_serveur(self, dash, monkeypatch, capsys):
        """Cloud Run sans accès au broker : le relais abandonne proprement,
        le dashboard sert la photo et l'historique sans temps réel."""
        class DeadConsumer:
            def list_topics(self, topic, timeout=None):
                raise RuntimeError("broker unreachable")

        monkeypatch.setattr(dash, "Consumer", lambda cfg: DeadConsumer())
        dash._relay_loop()    # ne doit PAS lever
        assert "temps réel désactivé" in capsys.readouterr().out


def test_index_sert_la_page(dash):
    response = TestClient(dash.app).get("/")
    assert response.status_code == 200
    assert "Vélib" in response.text
