"""Tests de l'archiveur Parquet : bornes Kafka simulées, écriture réelle."""

from __future__ import annotations

import json
from datetime import date

import duckdb
import pytest
from confluent_kafka import TopicPartition

import velib.archiver as ar


def make_event(ts: int, station_id: int = 1, delta: int = -1) -> dict:
    return {
        "ts": ts, "station_id": station_id, "station_code": "00001",
        "name": "Station Test", "lat": 48.86, "lon": 2.35, "capacity": 30,
        "last_reported": ts, "mechanical": 3, "ebike": 2,
        "bikes_available": 5, "docks_available": 25,
        "is_installed": True, "is_renting": True, "is_returning": True,
        "bikes_delta": delta,
    }


def test_epoch_ms_borne_de_journee():
    d = date(2026, 7, 18)
    assert ar._epoch_ms(date(2026, 7, 19)) - ar._epoch_ms(d) == 86_400_000


class TestWriteParquet:
    def test_ecrit_et_relit_via_duckdb(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        day = date(2026, 7, 18)
        ts = ar._epoch_ms(day) // 1000 + 3600
        out = ar.write_parquet(day, [make_event(ts + 60), make_event(ts, delta=2)])

        assert out.exists()
        rows = duckdb.connect().execute(
            "SELECT ts, hour, bikes_delta, date FROM read_parquet("
            "'data/events/*/*.parquet', hive_partitioning=true) ORDER BY ts"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][0] < rows[1][0]              # trié par ts
        assert str(rows[0][3]) == "2026-07-18"      # partition Hive lue en colonne
        assert 0 <= rows[0][1] <= 23                # heure locale calculée


class FakeMsg:
    def __init__(self, partition, offset, payload):
        self._p, self._o, self._v = partition, offset, payload

    def error(self): return None
    def partition(self): return self._p
    def offset(self): return self._o
    def value(self): return json.dumps(self._v).encode()


class FakeConsumer:
    """Broker simulé : 1 partition, offsets [start_offset, end_offset) pour le
    jour demandé, high watermark au-delà."""

    def __init__(self, cfg, day, start_offset, end_offset, high, events):
        self._start_ms = ar._epoch_ms(day)
        self._end_ms = ar._epoch_ms(day) + 86_400_000
        self._start, self._end, self._high = start_offset, end_offset, high
        self._msgs = [FakeMsg(0, start_offset + i, e) for i, e in enumerate(events)]
        self.assigned = None
        self.closed = False

    def list_topics(self, topic, timeout=None):
        from types import SimpleNamespace
        return SimpleNamespace(
            topics={topic: SimpleNamespace(partitions={0: None})})

    def offsets_for_times(self, tps, timeout=None):
        # En entrée, tp.offset porte le timestamp interrogé (l'API Kafka).
        out = []
        for tp in tps:
            offset = self._start if tp.offset == self._start_ms else self._end
            out.append(TopicPartition(tp.topic, tp.partition, offset))
        return out

    def get_watermark_offsets(self, tp, timeout=None):
        return (0, self._high)

    def assign(self, tps): self.assigned = tps

    def poll(self, timeout):
        return self._msgs.pop(0) if self._msgs else None

    def close(self): self.closed = True


class TestFetchDayEvents:
    day = date(2026, 7, 18)

    def _wire(self, monkeypatch, **kwargs):
        consumer = FakeConsumer(None, self.day, **kwargs)
        monkeypatch.setattr(ar, "Consumer", lambda cfg: consumer)
        return consumer

    def test_lit_exactement_la_journee(self, monkeypatch):
        ts = ar._epoch_ms(self.day) // 1000
        consumer = self._wire(
            monkeypatch, start_offset=5, end_offset=8, high=20,
            events=[make_event(ts + i) for i in range(3)],
        )
        events = ar.fetch_day_events(self.day)
        assert len(events) == 3                      # offsets 5,6,7 — pas le 8
        assert consumer.closed

    def test_journee_vide(self, monkeypatch):
        self._wire(monkeypatch, start_offset=-1, end_offset=-1, high=20, events=[])
        assert ar.fetch_day_events(self.day) == []

    def test_fin_absente_va_jusqu_au_watermark(self, monkeypatch):
        """end = -1 (aucun message après minuit+24h) → borne = high watermark."""
        ts = ar._epoch_ms(self.day) // 1000
        self._wire(monkeypatch, start_offset=0, end_offset=-1, high=2,
                   events=[make_event(ts), make_event(ts + 1)])
        assert len(ar.fetch_day_events(self.day)) == 2


class TestMain:
    def test_journee_vide_message(self, monkeypatch, capsys):
        monkeypatch.setattr(ar, "fetch_day_events", lambda d: [])
        monkeypatch.setattr(ar.sys, "argv", ["archiver", "2026-07-18"])
        ar.main()
        assert "Aucun événement" in capsys.readouterr().out

    def test_ecrit_et_resume(self, monkeypatch, tmp_path, capsys):
        monkeypatch.chdir(tmp_path)
        ts = ar._epoch_ms(date(2026, 7, 18)) // 1000
        monkeypatch.setattr(ar, "fetch_day_events",
                            lambda d: [make_event(ts, delta=-2), make_event(ts, delta=1)])
        monkeypatch.setattr(ar.sys, "argv", ["archiver", "2026-07-18"])
        ar.main()
        out = capsys.readouterr().out
        assert "2 événements" in out
        assert "vélos pris : 2" in out

    def test_envoi_gcs_si_configure(self, monkeypatch, tmp_path, capsys):
        monkeypatch.chdir(tmp_path)
        ts = ar._epoch_ms(date(2026, 7, 18)) // 1000
        monkeypatch.setattr(ar, "fetch_day_events", lambda d: [make_event(ts)])
        monkeypatch.setattr(ar, "upload_if_configured",
                            lambda path: f"gs://bucket/{path.name}")
        monkeypatch.setattr(ar.sys, "argv", ["archiver", "2026-07-18"])
        ar.main()
        assert "envoyé → gs://bucket/events.parquet" in capsys.readouterr().out
