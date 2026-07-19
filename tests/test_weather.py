"""Tests de l'ingestion météo (transport HTTP simulé, écriture réelle)."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import duckdb
import httpx
import pytest

import velib.weather as wx


def mock_client(payload: dict) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["timezone"] == "UTC"
        return httpx.Response(200, json=payload)

    return httpx.Client(transport=httpx.MockTransport(handler))


PAYLOAD = {
    "hourly": {
        "time": ["2026-07-18T00:00", "2026-07-18T01:00"],
        "precipitation": [0.0, 1.2],
        "temperature_2m": [18.5, 17.9],
        "wind_speed_10m": [10.0, 12.3],
        "relative_humidity_2m": [70, 82],
        "weather_code": [0, 61],
    }
}


class TestFetchDay:
    def test_conversion_en_lignes(self):
        rows = wx.fetch_day(date(2026, 7, 18), client=mock_client(PAYLOAD))
        assert len(rows) == 2
        ts, hour, precip, temp, wind, hum, code = rows[1]
        expected_ts = int(datetime(2026, 7, 18, 1, tzinfo=timezone.utc).timestamp())
        assert ts == expected_ts
        assert precip == 1.2 and temp == 17.9 and code == 61
        assert 0 <= hour <= 23  # heure locale

    def test_erreur_http_remonte(self):
        def handler(request):
            return httpx.Response(500)
        client = httpx.Client(transport=httpx.MockTransport(handler))
        with pytest.raises(httpx.HTTPStatusError):
            wx.fetch_day(date(2026, 7, 18), client=client)


def test_write_parquet_relu_par_duckdb(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    rows = wx.fetch_day(date(2026, 7, 18), client=mock_client(PAYLOAD))
    out = wx.write_parquet(date(2026, 7, 18), rows)

    assert out.exists()
    result = duckdb.connect().execute(
        "SELECT date, sum(precipitation), max(temperature) FROM read_parquet("
        "'data/weather/*/*.parquet', hive_partitioning=true) GROUP BY date"
    ).fetchone()
    assert str(result[0]) == "2026-07-18"
    assert result[1] == pytest.approx(1.2)
    assert result[2] == pytest.approx(18.5)


class TestMain:
    def _wire(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        fetched = []
        original = wx.fetch_day   # capturé AVANT le patch (sinon récursion)

        def fake_fetch(day, client=None):
            fetched.append(day)
            return original(day, client=mock_client(PAYLOAD))

        monkeypatch.setattr(wx, "fetch_day", fake_fetch)
        return fetched

    def test_plage_de_dates(self, monkeypatch, tmp_path, capsys):
        fetched = self._wire(monkeypatch, tmp_path)
        monkeypatch.setattr(wx.sys, "argv", ["weather", "2026-07-18", "2026-07-20"])
        wx.main()
        assert fetched == [date(2026, 7, 18), date(2026, 7, 19), date(2026, 7, 20)]
        assert capsys.readouterr().out.count("pluie") == 3

    def test_defaut_hier(self, monkeypatch, tmp_path):
        from datetime import timedelta
        fetched = self._wire(monkeypatch, tmp_path)
        monkeypatch.setattr(wx.sys, "argv", ["weather"])
        wx.main()
        assert fetched == [date.today() - timedelta(days=1)]

    def test_envoi_gcs_si_configure(self, monkeypatch, tmp_path, capsys):
        self._wire(monkeypatch, tmp_path)
        monkeypatch.setattr(wx, "upload_if_configured",
                            lambda path: f"gs://bucket/{path.name}")
        monkeypatch.setattr(wx.sys, "argv", ["weather", "2026-07-18"])
        wx.main()
        assert "envoyé → gs://bucket/weather.parquet" in capsys.readouterr().out
