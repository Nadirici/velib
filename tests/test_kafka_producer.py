"""Tests du module de publication Kafka (sans broker : doubles de test)."""

from __future__ import annotations

import httpx
import pytest
from confluent_kafka import Producer

import velib.kafka_producer as kp
from velib.models import StationChangeEvent


class FakeFuture:
    def __init__(self):
        self.waited = False

    def result(self):
        self.waited = True


class FakeAdmin:
    def __init__(self, existing: set[str]):
        self._existing = existing
        self.created: list = []
        self.future = FakeFuture()

    def list_topics(self, timeout=None):
        from types import SimpleNamespace
        return SimpleNamespace(topics={name: None for name in self._existing})

    def create_topics(self, new_topics):
        self.created.extend(new_topics)
        return {t.topic: self.future for t in new_topics}


class FakeProducer:
    def __init__(self):
        self.produced: list[dict] = []
        self.polls = 0
        self.flushes = 0

    def produce(self, topic, key, value, on_delivery=None):
        self.produced.append({"topic": topic, "key": key, "value": value,
                              "on_delivery": on_delivery})

    def poll(self, timeout):
        self.polls += 1

    def flush(self, timeout=None):
        self.flushes += 1


class TestEnsureTopic:
    def test_ne_recree_pas_un_topic_existant(self):
        admin = FakeAdmin(existing={kp.TOPIC})
        kp.ensure_topic(admin)
        assert admin.created == []

    def test_cree_le_topic_et_attend_la_future(self):
        admin = FakeAdmin(existing=set())
        kp.ensure_topic(admin)
        [topic] = admin.created
        assert topic.topic == kp.TOPIC
        assert admin.future.waited  # .result() appelé : les erreurs remonteraient


def test_create_producer_config_valide():
    # Producer(config) valide la config localement, sans broker : une clé
    # inconnue lèverait KafkaException dès la construction.
    assert isinstance(kp.create_producer(), Producer)


class TestDeliveryReport:
    def _msg(self):
        class Msg:
            def key(self): return b"1"
            def topic(self): return kp.TOPIC
            def partition(self): return 2
            def offset(self): return 42
        return Msg()

    def test_echec_logge_la_cle(self, capsys):
        kp.delivery_report("boom", self._msg())
        assert "Échec" in capsys.readouterr().out

    def test_succes_silencieux(self, capsys):
        kp.delivery_report(None, self._msg())
        assert capsys.readouterr().out == ""


class TestPublishEvents:
    def test_publie_cle_valeur_et_flush(self, make_state):
        producer = FakeProducer()
        events = [StationChangeEvent.from_state(make_state(station_id=i), -1)
                  for i in (1, 2)]
        kp.publish_events(producer, events)

        assert [p["key"] for p in producer.produced] == [b"1", b"2"]
        assert all(p["topic"] == kp.TOPIC for p in producer.produced)
        assert all(p["on_delivery"] is kp.delivery_report for p in producer.produced)
        assert producer.polls == 2      # poll(0) dans la boucle
        assert producer.flushes == 1    # flush final

    def test_lot_vide_flush_quand_meme(self):
        producer = FakeProducer()
        kp.publish_events(producer, [])
        assert producer.produced == []
        assert producer.flushes == 1


class FakeClient:
    def __init__(self, states, fail_first=False):
        self._states = states
        self._fail = fail_first

    def fetch_information(self):
        if self._fail:
            self._fail = False
            raise httpx.ConnectError("API down")
        return {}

    def fetch_states(self, info):
        return self._states


class TestRun:
    @pytest.fixture
    def wired(self, monkeypatch, make_state):
        """Câble run() sur des doubles ; sleep lève KeyboardInterrupt pour
        sortir de la boucle infinie après le premier tour."""
        producer = FakeProducer()
        calls = {"ensure": 0, "sleeps": 0}
        monkeypatch.setattr(kp, "create_producer", lambda: producer)
        monkeypatch.setattr(kp, "ensure_topic",
                            lambda *a: calls.__setitem__("ensure", calls["ensure"] + 1))

        def fake_sleep(seconds):
            calls["sleeps"] += 1
            raise KeyboardInterrupt

        monkeypatch.setattr(kp.time, "sleep", fake_sleep)
        return producer, calls

    def test_tour_normal_publie_puis_flush_sur_ctrl_c(self, wired, monkeypatch,
                                                      make_state, capsys):
        producer, calls = wired
        monkeypatch.setattr(kp, "VelibClient",
                            lambda: FakeClient([make_state(station_id=1)]))
        kp.run()
        assert calls["ensure"] == 1
        assert len(producer.produced) == 1      # 1 état initial publié
        assert producer.flushes >= 1            # flush de sortie propre
        assert "1 événements publiés" in capsys.readouterr().out

    def test_erreur_api_ne_tue_pas_la_boucle(self, wired, monkeypatch,
                                             make_state, capsys):
        producer, calls = wired
        monkeypatch.setattr(kp, "VelibClient",
                            lambda: FakeClient([], fail_first=True))
        kp.run()
        # L'erreur HTTP a été absorbée : on a quand même atteint le sleep.
        assert calls["sleeps"] == 1
        assert producer.produced == []
        assert "Erreur API" in capsys.readouterr().out
