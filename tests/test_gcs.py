"""Tests de l'envoi GCS (client simulé, piloté par l'environnement)."""

from __future__ import annotations

from pathlib import Path

import velib.gcs as gcs


class FakeBlob:
    def __init__(self, name):
        self.name = name
        self.uploaded_from = None

    def upload_from_filename(self, filename):
        self.uploaded_from = filename


class FakeClient:
    last = None

    def __init__(self):
        FakeClient.last = self
        self.blobs: list[FakeBlob] = []
        self.bucket_name = None

    def bucket(self, name):
        self.bucket_name = name
        return self

    def blob(self, name):
        b = FakeBlob(name)
        self.blobs.append(b)
        return b


def test_sans_variable_env_ne_fait_rien(monkeypatch):
    monkeypatch.delenv("VELIB_GCS_BUCKET", raising=False)
    assert gcs.upload_if_configured(Path("data/events/date=2026-07-18/events.parquet")) is None


def test_envoi_miroir_de_data(monkeypatch):
    monkeypatch.setenv("VELIB_GCS_BUCKET", "mon-bucket")
    import google.cloud.storage as storage
    monkeypatch.setattr(storage, "Client", FakeClient)

    uri = gcs.upload_if_configured(Path("data/weather/date=2026-07-18/weather.parquet"))

    # Le préfixe data/ disparaît : le bucket est le miroir du répertoire local.
    assert uri == "gs://mon-bucket/weather/date=2026-07-18/weather.parquet"
    [blob] = FakeClient.last.blobs
    assert blob.uploaded_from.endswith("weather.parquet")
    assert FakeClient.last.bucket_name == "mon-bucket"
