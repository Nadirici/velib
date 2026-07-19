"""Ingestion météo : archive la météo horaire de Paris en Parquet.

Pourquoi c'est là dès maintenant : la pluie est le prédicteur n°1 de l'usage
du vélo partagé. Le futur modèle de prédiction de disponibilité (voir
docs/ROADMAP-IA.md) se joindra aux événements par l'heure — d'où le même
partitionnement que l'archiveur d'événements :

    data/weather/date=YYYY-MM-DD/weather.parquet

Source : Open-Meteo (https://open-meteo.com) — gratuit, sans clé d'API.
L'endpoint forecast accepte start_date/end_date jusqu'à ~3 mois en arrière,
ce qui couvre l'ingestion quotidienne ET le rattrapage de jours manqués.
On demande les horaires en UTC : la jointure avec les événements se fait sur
l'epoch, la colonne `hour` (heure LOCALE, comme dans events.parquet) sert aux
profils lisibles par un humain.

Usage :
    uv run python -m velib.weather                          → hier
    uv run python -m velib.weather 2026-07-18               → un jour précis
    uv run python -m velib.weather 2026-07-18 2026-07-25    → une plage (backfill)
    (à planifier chaque soir, à côté de l'archiveur d'événements)
"""

from __future__ import annotations

import ssl
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import httpx
import truststore

# Même raison que velib_api : magasin de certificats de l'OS (proxy SSL).
_SSL_CONTEXT = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

API_URL = "https://api.open-meteo.com/v1/forecast"

# Paris centre — le réseau couvre la métropole mais une seule maille météo
# suffit largement pour capter pluie/température/vent à l'échelle du réseau.
LAT, LON = 48.86, 2.35

# Les variables qui influencent l'usage du vélo, dans l'ordre d'importance.
HOURLY_VARS = [
    "precipitation",
    "temperature_2m",
    "wind_speed_10m",
    "relative_humidity_2m",
    "weather_code",
]

DATA_DIR = Path("data") / "weather"


def fetch_day(day: date, client: httpx.Client | None = None) -> list[tuple]:
    """Les 24 relevés horaires du jour, prêts à insérer.

    `client` injectable (même motif que VelibClient) : les tests passent un
    client monté sur un transport simulé, sans toucher au réseau.
    """
    owns_client = client is None
    if client is None:
        client = httpx.Client(timeout=15.0, verify=_SSL_CONTEXT)
    try:
        resp = client.get(API_URL, params={
            "latitude": LAT,
            "longitude": LON,
            "hourly": ",".join(HOURLY_VARS),
            "timezone": "UTC",
            "start_date": day.isoformat(),
            "end_date": day.isoformat(),
        })
        resp.raise_for_status()
        hourly = resp.json()["hourly"]
    finally:
        if owns_client:
            client.close()

    rows = []
    for i, iso in enumerate(hourly["time"]):
        ts = int(datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp())
        local_hour = datetime.fromtimestamp(ts).astimezone().hour
        rows.append((
            ts,
            local_hour,
            hourly["precipitation"][i],
            hourly["temperature_2m"][i],
            hourly["wind_speed_10m"][i],
            hourly["relative_humidity_2m"][i],
            hourly["weather_code"][i],
        ))
    return rows


def write_parquet(day: date, rows: list[tuple]) -> Path:
    out_dir = DATA_DIR / f"date={day.isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "weather.parquet"

    con = duckdb.connect()
    con.execute("""
        CREATE TABLE wx (
            ts BIGINT, hour TINYINT,
            precipitation DOUBLE, temperature DOUBLE,
            wind_speed DOUBLE, humidity DOUBLE, weather_code SMALLINT
        )
    """)
    con.executemany("INSERT INTO wx VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    con.execute(f"COPY wx TO '{out.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    return out


def main() -> None:
    if len(sys.argv) >= 3:
        start, end = date.fromisoformat(sys.argv[1]), date.fromisoformat(sys.argv[2])
    elif len(sys.argv) == 2:
        start = end = date.fromisoformat(sys.argv[1])
    else:
        start = end = date.today() - timedelta(days=1)

    day = start
    while day <= end:
        rows = fetch_day(day)
        out = write_parquet(day, rows)
        rain = sum(r[2] for r in rows)
        temps = [r[3] for r in rows]
        print(f"{day.isoformat()} → {out}  "
              f"(pluie {rain:.1f} mm · T {min(temps):.0f}–{max(temps):.0f} °C)")
        day += timedelta(days=1)


if __name__ == "__main__":
    main()
