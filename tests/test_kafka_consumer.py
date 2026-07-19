"""Tests du consumer générique : parse, boucle poll/commit, poison pill."""

from __future__ import annotations

from confluent_kafka import Consumer

import velib.kafka_consumer as kc
from velib.models import StationChangeEvent


class FakeMessage:
    def __init__(self, value=b"", error=None, partition=0, offset=0):
        self._value, self._error = value, error
        self._partition, self._offset = partition, offset

    def value(self): return self._value
    def error(self): return self._error
    def partition(self): return self._partition
    def offset(self): return self._offset


class FakeConsumer:
    """Rejoue un scénario de poll ; KeyboardInterrupt simule le Ctrl+C."""

    def __init__(self, script):
        self._script = list(script)
        self.subscribed = None
        self.commits: list = []
        self.closed = False

    def subscribe(self, topics): self.subscribed = topics

    def poll(self, timeout):
        if not self._script:
            raise KeyboardInterrupt
        item = self._script.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    def commit(self, msg): self.commits.append(msg)
    def close(self): self.closed = True


def test_create_consumer_config_valide():
    consumer = kc.create_consumer("groupe-test")
    assert isinstance(consumer, Consumer)
    consumer.close()


def test_parse_event_est_le_miroir_de_to_json(make_state):
    original = StationChangeEvent.from_state(make_state(station_id=5), -2)
    msg = FakeMessage(value=original.to_json())
    assert kc.parse_event(msg) == original


def test_handle_event_affiche(make_state, capsys):
    kc.handle_event(StationChangeEvent.from_state(make_state(), 0))
    assert "Station Test" in capsys.readouterr().out


class TestRun:
    def test_poll_parse_handle_commit(self, monkeypatch, make_state):
        good = FakeMessage(value=StationChangeEvent.from_state(make_state(), 1).to_json())
        consumer = FakeConsumer([None, good])
        monkeypatch.setattr(kc, "create_consumer", lambda gid: consumer)
        received = []

        kc.run(group_id="g", handler=received.append)

        assert consumer.subscribed == [kc.TOPIC]
        assert len(received) == 1
        assert received[0].bikes_delta == 1
        assert consumer.commits == [good]     # commit APRÈS traitement
        assert consumer.closed                # close() dans le finally

    def test_erreur_kafka_loggee_sans_commit(self, monkeypatch, capsys):
        err = FakeMessage(error="broker down")
        consumer = FakeConsumer([err])
        monkeypatch.setattr(kc, "create_consumer", lambda gid: consumer)

        kc.run(handler=lambda e: None)

        assert consumer.commits == []
        assert "Erreur Kafka" in capsys.readouterr().out

    def test_poison_ecarte_et_committe(self, monkeypatch, make_state, capsys):
        """LE test de non-régression de l'incident : un message inparsable ne
        doit pas bloquer la partition — il est écarté ET committé."""
        poison = FakeMessage(value=b'{"pas": "un event"}', partition=0, offset=3475)
        good = FakeMessage(value=StationChangeEvent.from_state(make_state(), 0).to_json())
        consumer = FakeConsumer([poison, good])
        monkeypatch.setattr(kc, "create_consumer", lambda gid: consumer)
        received = []

        kc.run(handler=received.append)

        assert consumer.commits == [poison, good]   # le poison est passé
        assert len(received) == 1                    # mais pas traité
        out = capsys.readouterr().out
        assert "poison" in out and "3475" in out

    def test_crash_du_handler_est_aussi_un_poison(self, monkeypatch, make_state):
        good = FakeMessage(value=StationChangeEvent.from_state(make_state(), 0).to_json())
        consumer = FakeConsumer([good])
        monkeypatch.setattr(kc, "create_consumer", lambda gid: consumer)

        def bad_handler(event):
            raise ValueError("boom")

        kc.run(handler=bad_handler)     # ne doit pas lever
        assert consumer.commits == [good]
