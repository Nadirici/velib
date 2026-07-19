"""Envoi des Parquet vers un bucket GCS — optionnel, piloté par l'environnement.

En local, rien ne change : sans VELIB_GCS_BUCKET, les archives restent dans
data/. Sur la VM GCE, la variable est posée et chaque Parquet écrit est aussi
poussé dans le bucket, en conservant l'arborescence (events/date=…/,
weather/date=…/). Côté Cloud Run, le même bucket est monté en volume sur
/app/data (Cloud Storage FUSE) : le dashboard lit l'historique comme des
fichiers locaux, sans une ligne de code GCS.

Authentification : aucune clé dans le code — sur GCE/Cloud Run, le client
utilise l'identité de la machine (Application Default Credentials).
"""

from __future__ import annotations

import os
from pathlib import Path


def upload_if_configured(path: Path) -> str | None:
    """Pousse `path` vers gs://$VELIB_GCS_BUCKET/… ; None si non configuré.

    Le nom d'objet reprend le chemin relatif SANS le préfixe data/ : le bucket
    est le miroir exact du répertoire data/ local.
    """
    bucket_name = os.getenv("VELIB_GCS_BUCKET")
    if not bucket_name:
        return None

    # Import paresseux : le module reste importable même là où la dépendance
    # GCP n'est pas utile (tests, machines de dev).
    from google.cloud import storage

    blob_name = path.as_posix().removeprefix("data/")
    blob = storage.Client().bucket(bucket_name).blob(blob_name)
    blob.upload_from_filename(str(path))
    return f"gs://{bucket_name}/{blob_name}"
