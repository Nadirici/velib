"""Tests de la couche vitesse : ActivityAggregator."""

from __future__ import annotations

from velib.activity import BASE_BUCKET_S, ActivityAggregator, local_midnight_epoch


def event(ts: int, station_id: int = 1, delta: int = 0) -> dict:
    return {"ts": ts, "station_id": station_id, "bikes_delta": delta}


class TestAdd:
    def test_compteurs_par_tranche(self):
        agg = ActivityAggregator()
        t0 = agg.day_start + 600
        agg.add(event(t0, delta=-2))       # 2 vélos pris
        agg.add(event(t0 + 10, delta=3))   # 3 rendus, même minute
        agg.add(event(t0 + 5, delta=0))    # événement sans mouvement

        [point] = [p for p in agg.series(BASE_BUCKET_S, agg.day_start, t0 + 59)
                   if p["events"]]
        assert point["events"] == 3
        assert point["taken"] == 2
        assert point["returned"] == 3

    def test_rollover_de_minuit_reinitialise(self):
        agg = ActivityAggregator()
        agg.add(event(agg.day_start + 100, delta=-1))
        assert agg.top_stations()

        agg.add(event(agg.day_start + 86400 + 100, delta=-1, station_id=9))
        # L'ancien jour a été purgé : seule la station du nouveau jour reste.
        assert [sid for sid, _ in agg.top_stations()] == [9]


class TestSeries:
    def test_serie_continue_avec_zeros(self):
        agg = ActivityAggregator()
        start = agg.day_start
        agg.add(event(start, delta=-1))
        agg.add(event(start + 600, delta=2))

        points = agg.series(300, start, start + 600)
        assert [p["t"] for p in points] == [start, start + 300, start + 600]
        assert [p["taken"] for p in points] == [1, 0, 0]
        assert [p["returned"] for p in points] == [0, 0, 2]

    def test_fusion_a_la_granularite_demandee(self):
        agg = ActivityAggregator()
        start = agg.day_start
        for i in range(5):  # 5 minutes consécutives, 1 vélo pris chacune
            agg.add(event(start + i * 60, delta=-1))
        [merged] = [p for p in agg.series(300, start, start + 299) if p["taken"]]
        assert merged["taken"] == 5

    def test_hors_bornes_ignore(self):
        agg = ActivityAggregator()
        agg.add(event(agg.day_start + 3600, delta=-1))
        points = agg.series(60, agg.day_start, agg.day_start + 60)
        assert all(p["taken"] == 0 for p in points)


class TestTopEtFlux:
    def test_top_stations_trie_par_activite(self):
        agg = ActivityAggregator()
        t = agg.day_start
        agg.add(event(t, station_id=1, delta=-1))
        agg.add(event(t, station_id=2, delta=-3))
        agg.add(event(t, station_id=2, delta=2))
        assert agg.top_stations(2) == [(2, 5), (1, 1)]

    def test_station_flows_separe_pris_et_rendus(self):
        agg = ActivityAggregator()
        t = agg.day_start
        agg.add(event(t, station_id=7, delta=-4))
        agg.add(event(t, station_id=7, delta=1))
        assert agg.station_flows() == {7: (4, 1)}


def test_local_midnight_epoch_est_un_debut_de_jour():
    from datetime import datetime

    midnight = datetime.fromtimestamp(local_midnight_epoch())
    assert (midnight.hour, midnight.minute, midnight.second) == (0, 0, 0)
