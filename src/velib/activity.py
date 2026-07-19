"""Couche vitesse de la BI : agrégation en mémoire du flux d'événements.

Architecture lambda, versant « speed layer » : l'activité du jour est agrégée
au fil de l'eau, en RAM, par tranches d'une minute (la granularité de base —
l'API sait ensuite les fusionner en 5/15/60 min à la demande). Rien n'est
persisté : au démarrage, le dashboard REJOUE les événements du jour depuis le
journal Kafka (seek par timestamp) pour reconstruire cet état — Kafka est la
seule source de vérité du direct. L'historique au-delà du jour est le rôle de
la couche batch (archiver.py → Parquet → DuckDB).

Métriques par tranche :
- events   : nombre d'événements (attention : inclut les snapshots « initiaux »
  émis à chaque redémarrage du producer — pic artificiel de ~1500) ;
- taken    : vélos pris   (somme des bikes_delta négatifs, en valeur absolue) ;
- returned : vélos rendus (somme des bikes_delta positifs).

`taken`/`returned` sont les métriques honnêtes de l'activité réelle : un
snapshot initial a bikes_delta=0 et ne les pollue pas.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from datetime import datetime, time as dtime

BASE_BUCKET_S = 60
GRANULARITIES = (60, 300, 900, 3600)


def local_midnight_epoch() -> int:
    """Minuit local (fuseau de la machine) en epoch UTC — la borne du « jour »."""
    midnight = datetime.combine(datetime.now().date(), dtime.min).astimezone()
    return int(midnight.timestamp())


class ActivityAggregator:
    """Compteurs du jour, thread-safe (alimentés par le thread relais Kafka,
    lus par les requêtes HTTP)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # bucket epoch (minute) -> [events, taken, returned]
        self._buckets: dict[int, list[int]] = defaultdict(lambda: [0, 0, 0])
        # station_id -> [taken, returned] du jour : sert au top stations ET aux
        # flux nets par station (puits/sources) de la vue métier.
        self._stations: dict[int, list[int]] = defaultdict(lambda: [0, 0])
        self._day_start = local_midnight_epoch()

    def add(self, event: dict) -> None:
        ts = event["ts"]
        with self._lock:
            # Passage de minuit : l'activité « du jour » repart de zéro.
            if ts >= self._day_start + 86400:
                self._buckets.clear()
                self._stations.clear()
                self._day_start = local_midnight_epoch()
            b = self._buckets[ts // BASE_BUCKET_S * BASE_BUCKET_S]
            b[0] += 1
            delta = event.get("bikes_delta", 0)
            if delta < 0:
                b[1] += -delta
                self._stations[event["station_id"]][0] += -delta
            elif delta > 0:
                b[2] += delta
                self._stations[event["station_id"]][1] += delta

    def series(self, step: int, since: int, until: int) -> list[dict]:
        """Les tranches [since, until] fusionnées à la granularité `step`.

        Renvoie une série CONTINUE (les tranches sans activité valent 0) :
        indispensable pour que les courbes montrent les creux de la nuit au
        lieu de les sauter.
        """
        since = since // step * step
        merged: dict[int, list[int]] = {}
        with self._lock:
            for bucket, (ev, taken, returned) in self._buckets.items():
                if bucket < since or bucket > until:
                    continue
                key = bucket // step * step
                m = merged.setdefault(key, [0, 0, 0])
                m[0] += ev
                m[1] += taken
                m[2] += returned
        return [
            {"t": t, "events": m[0], "taken": m[1], "returned": m[2]}
            for t in range(since, until + 1, step)
            for m in [merged.get(t, [0, 0, 0])]
        ]

    def top_stations(self, n: int = 8) -> list[tuple[int, int]]:
        with self._lock:
            ranked = sorted(
                ((sid, t + r) for sid, (t, r) in self._stations.items()),
                key=lambda kv: kv[1],
                reverse=True,
            )
        return ranked[:n]

    def station_flows(self) -> dict[int, tuple[int, int]]:
        """Flux du jour par station : {station_id: (pris, rendus)}."""
        with self._lock:
            return {sid: (t, r) for sid, (t, r) in self._stations.items()}

    @property
    def day_start(self) -> int:
        return self._day_start
