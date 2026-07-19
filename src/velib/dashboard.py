"""Dashboard : carte temps réel + BI sur le flux (architecture lambda).

Trois sources, trois rôles :
- Redis  → la PHOTO : état courant des stations (snapshot au chargement) ;
- Kafka  → le FILM  : chaque événement est poussé aux navigateurs en SSE, et
  agrégé en RAM pour la BI du jour (couche vitesse). Au démarrage, le relais
  REJOUE le journal depuis minuit pour reconstruire l'activité du jour —
  l'état en mémoire est jetable, Kafka reste la seule source de vérité ;
- Parquet (data/events/date=*/events.parquet, produits par archiver.py) → le
  PASSÉ : requêté en SQL par DuckDB pour l'historique (couche batch).

Endpoints :
- GET /                    → la page carte (Leaflet)
- GET /api/stations        → snapshot complet (photo Redis) + fraîcheur
- GET /api/stream          → flux SSE des événements (push temps réel)
- GET /api/activity?step=  → activité du jour agrégée (60/300/900/3600 s)
- GET /api/history/daily   → totaux par jour archivé (Parquet)
- GET /api/history/profile → profil horaire moyen des jours archivés
- GET /docs                → doc interactive auto-générée par FastAPI

Lancer :
    uv run python -m velib.dashboard    → http://localhost:8000
"""

from __future__ import annotations

import json
import os
import queue
import threading
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import duckdb
import uvicorn
from confluent_kafka import Consumer, TopicPartition
from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse

from .activity import GRANULARITIES, ActivityAggregator
from .kafka_producer import BOOTSTRAP_SERVERS, TOPIC
from .redis_consumer import create_redis, station_key

_STATIC = Path(__file__).parent / "static"
_PARQUET_GLOB = "data/events/*/*.parquet"

# ---------------------------------------------------------------------------
# Relais Kafka → navigateurs (fan-out SSE) + agrégation d'activité
# ---------------------------------------------------------------------------
_clients: set[queue.Queue[str]] = set()
_clients_lock = threading.Lock()
_activity = ActivityAggregator()


def _relay_loop() -> None:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        # Groupe éphémère unique : un relais d'affichage veut TOUT le flux
        # (pas de partage de partitions) et n'a aucune position à mémoriser.
        "group.id": f"velib-dashboard-{uuid.uuid4().hex[:8]}",
        "enable.auto.commit": False,
    })

    # --- Phase 1 : reconstruction de l'activité du jour depuis le journal ---
    # offsets_for_times borne la journée [minuit, fin du journal] ; on rejoue
    # sans diffuser (rattrapage). Si Kafka est injoignable (ex. Cloud Run sans
    # accès au broker), le dashboard démarre en mode dégradé : photo Redis +
    # historique Parquet, sans temps réel.
    try:
        meta = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC]
        day_ms = _activity.day_start * 1000
        starts = consumer.offsets_for_times(
            [TopicPartition(TOPIC, p, day_ms) for p in meta.partitions], timeout=10
        )
        todo: dict[int, int] = {}
        for tp in starts:
            _, high = consumer.get_watermark_offsets(tp, timeout=10)
            if tp.offset < 0:          # aucun message depuis minuit :
                tp.offset = high       # se placer à la fin, rien à rejouer
            elif tp.offset < high:
                todo[tp.partition] = high
        consumer.assign(starts)
    except Exception as e:
        print(f"[relay] Kafka injoignable, temps réel désactivé : {e!r}")
        return

    replayed = 0
    while todo:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        _activity.add(json.loads(msg.value()))
        replayed += 1
        p = msg.partition()
        if p in todo and msg.offset() + 1 >= todo[p]:
            del todo[p]
    print(f"[relay] activité du jour reconstruite : {replayed} événements rejoués")

    # --- Phase 2 : le direct ------------------------------------------------
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        payload = msg.value().decode()
        _activity.add(json.loads(payload))
        with _clients_lock:
            for q in _clients:
                try:
                    q.put_nowait(payload)
                except queue.Full:
                    pass  # client trop lent : perdre un event vaut mieux que bloquer


@asynccontextmanager
async def _lifespan(app: FastAPI):
    threading.Thread(target=_relay_loop, daemon=True, name="kafka-sse-relay").start()
    yield


app = FastAPI(
    title="Vélib dashboard",
    description="État temps réel + BI des stations Vélib'.",
    lifespan=_lifespan,
)

# Connexion unique, créée à l'import : fail fast si Redis est éteint, et
# redis-py gère un pool interne — pas besoin d'une connexion par requête.
_r = create_redis()

# Miroir inverse de la sérialisation du consumer : Redis rend tout en str,
# on retype champ par champ (dont les bool stockés en "0"/"1").
_INT_FIELDS = {
    "station_id", "capacity", "ts", "last_reported",
    "mechanical", "ebike", "bikes_available", "docks_available", "bikes_delta",
}
_FLOAT_FIELDS = {"lat", "lon"}
_BOOL_FIELDS = {"is_installed", "is_renting", "is_returning"}


def _parse_station(raw: dict[str, str]) -> dict:
    parsed: dict = {}
    for k, v in raw.items():
        if k in _BOOL_FIELDS:
            parsed[k] = bool(int(v))
        elif k in _INT_FIELDS:
            parsed[k] = int(v)
        elif k in _FLOAT_FIELDS:
            parsed[k] = float(v)
        else:
            parsed[k] = v
    return parsed


def _snapshot_stations() -> list[dict]:
    """La photo Redis complète, retypée (un seul aller-retour via pipeline)."""
    ids = _r.smembers("velib:stations")
    pipe = _r.pipeline()
    for sid in ids:
        pipe.hgetall(station_key(int(sid)))
    return [_parse_station(raw) for raw in pipe.execute() if raw]


@app.get("/api/stations")
def stations() -> dict:
    """Snapshot complet : la photo Redis + horodatages pour la fraîcheur."""
    last_ts = _r.get("velib:last_ts")
    return {
        "now": int(time.time()),
        "last_ts": int(last_ts) if last_ts else None,
        "stations": _snapshot_stations(),
    }


@app.get("/api/business")
def business() -> dict:
    """Vue « exploitant » : jointure de la photo (état courant) et des flux du
    jour par station (couche vitesse). Tout est pensé actionnable :
    - `rebalance` : stations en défaut (vides/pleines) triées par la demande
      qu'elles portent — la liste de tournée des camions de rééquilibrage ;
    - `sinks` / `sources` : où les vélos s'accumulent / d'où ils partent
      (flux net du jour) — le plan de reposition du soir ;
    - `at_risk_moves` : la part de la demande du jour portée par des stations
      actuellement en défaut — un proxy de la demande non servie.
    """
    flows = _activity.station_flows()
    snapshot = _snapshot_stations()

    today_taken = sum(t for t, _ in flows.values())
    today_returned = sum(r for _, r in flows.values())
    total_moves = today_taken + today_returned

    rebalance: list[dict] = []
    at_risk = 0
    nets: list[dict] = []
    for s in snapshot:
        sid = s["station_id"]
        taken, returned = flows.get(sid, (0, 0))
        moves = taken + returned
        net = returned - taken
        if net:
            nets.append({"station_id": sid, "name": s["name"], "net": net,
                         "taken": taken, "returned": returned,
                         "lat": s["lat"], "lon": s["lon"]})

        out_of_order = not s["is_installed"] or not (s["is_renting"] or s["is_returning"])
        empty = s["is_installed"] and s["is_renting"] and s["bikes_available"] == 0
        full = s["is_installed"] and s["is_returning"] and s["docks_available"] == 0
        if out_of_order or empty or full:
            at_risk += moves
        if empty or full:
            rebalance.append({
                "station_id": sid, "name": s["name"],
                "lat": s["lat"], "lon": s["lon"],
                "state": "vide" if empty else "pleine",
                "moves": moves, "capacity": s["capacity"],
                "bikes": s["bikes_available"], "docks": s["docks_available"],
            })

    rebalance.sort(key=lambda x: x["moves"], reverse=True)
    nets.sort(key=lambda x: x["net"], reverse=True)
    sinks = [n for n in nets if n["net"] > 0][:8]
    sources = sorted((n for n in nets if n["net"] < 0), key=lambda x: x["net"])[:8]

    return {
        "now": int(time.time()),
        "today_taken": today_taken,
        "today_returned": today_returned,
        "total_moves": total_moves,
        "at_risk_moves": at_risk,
        # Σ|flux net| / 2 : le volume minimal de vélos à déplacer pour
        # ramener chaque station à son niveau du matin.
        "to_move": sum(abs(n["net"]) for n in nets) // 2,
        "rebalance": rebalance[:12],
        "sinks": sinks,
        "sources": sources,
    }


@app.get("/api/stream")
def stream() -> StreamingResponse:
    """Flux SSE : chaque événement Kafka, poussé dès son arrivée."""
    q: queue.Queue[str] = queue.Queue(maxsize=1000)
    with _clients_lock:
        _clients.add(q)

    def gen():
        try:
            while True:
                try:
                    yield f"data: {q.get(timeout=15)}\n\n"
                except queue.Empty:
                    yield ": keepalive\n\n"
        finally:
            with _clients_lock:
                _clients.discard(q)

    return StreamingResponse(gen(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# BI — couche vitesse (le jour, depuis la RAM)
# ---------------------------------------------------------------------------
@app.get("/api/activity")
def activity(step: int = 300) -> dict:
    """Activité du jour par tranches de `step` secondes (60/300/900/3600)."""
    if step not in GRANULARITIES:
        step = 300
    now = int(time.time())
    top = []
    if ranked := _activity.top_stations(8):
        pipe = _r.pipeline()
        for sid, _ in ranked:
            pipe.hget(station_key(sid), "name")
        names = pipe.execute()
        top = [
            {"station_id": sid, "name": name or str(sid), "moves": moves}
            for (sid, moves), name in zip(ranked, names)
        ]
    return {
        "now": now,
        "step": step,
        "day_start": _activity.day_start,
        "points": _activity.series(step, _activity.day_start, now),
        "top": top,
    }


# ---------------------------------------------------------------------------
# BI — couche batch (l'historique, depuis les Parquet via DuckDB)
# ---------------------------------------------------------------------------
def _has_archives() -> bool:
    return any(Path("data/events").glob("date=*/events.parquet"))


@app.get("/api/history/daily")
def history_daily() -> dict:
    """Totaux par journée archivée (la colonne `date` sort du partitionnement
    Hive : DuckDB la déduit du nom de dossier date=YYYY-MM-DD)."""
    if not _has_archives():
        return {"days": []}
    rows = duckdb.connect().execute(f"""
        SELECT date::VARCHAR,
               count(*)::INT,
               sum(CASE WHEN bikes_delta < 0 THEN -bikes_delta ELSE 0 END)::INT,
               sum(CASE WHEN bikes_delta > 0 THEN  bikes_delta ELSE 0 END)::INT
        FROM read_parquet('{_PARQUET_GLOB}', hive_partitioning = true)
        GROUP BY date ORDER BY date
    """).fetchall()
    return {"days": [
        {"date": d, "events": e, "taken": t, "returned": r}
        for d, e, t, r in rows
    ]}


@app.get("/api/history/profile")
def history_profile() -> dict:
    """Profil horaire moyen (vélos pris/rendus par heure locale, moyenne des
    jours archivés) — la signature jour/nuit du réseau."""
    if not _has_archives():
        return {"hours": []}
    rows = duckdb.connect().execute(f"""
        SELECT hour,
               avg(taken)::INT,
               avg(returned)::INT
        FROM (
            SELECT date, hour,
                   sum(CASE WHEN bikes_delta < 0 THEN -bikes_delta ELSE 0 END) AS taken,
                   sum(CASE WHEN bikes_delta > 0 THEN  bikes_delta ELSE 0 END) AS returned
            FROM read_parquet('{_PARQUET_GLOB}', hive_partitioning = true)
            GROUP BY date, hour
        )
        GROUP BY hour ORDER BY hour
    """).fetchall()
    return {"hours": [{"hour": h, "taken": t, "returned": r} for h, t, r in rows]}


@app.get("/")
def index() -> FileResponse:
    return FileResponse(_STATIC / "dashboard.html")


def main() -> None:  # pragma: no cover
    # Cloud Run impose son port via $PORT et exige d'écouter sur 0.0.0.0
    # (HOST posé par le Dockerfile) ; en local, rien ne change.
    uvicorn.run(
        app,
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "8000")),
    )


if __name__ == "__main__":
    main()
