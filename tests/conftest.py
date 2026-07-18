"""Outils partagés par les tests."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from velib.models import StationState


@pytest.fixture
def make_state() -> Callable[..., StationState]:
    """Fabrique de StationState avec des valeurs par défaut sensées.

    Chaque test surcharge uniquement les champs qui l'intéressent :
        make_state(station_id=1, mechanical=5)
    """

    def _make(**overrides) -> StationState:
        defaults = dict(
            station_id=1,
            station_code="00001",
            name="Station Test",
            lat=48.86,
            lon=2.35,
            capacity=30,
            ts=1_784_296_000,
            last_reported=1_784_295_000,
            mechanical=3,
            ebike=2,
            bikes_available=5,
            docks_available=25,
            is_installed=True,
            is_renting=True,
            is_returning=True,
        )
        defaults.update(overrides)
        return StationState(**defaults)

    return _make
