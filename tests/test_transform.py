from datetime import date, timedelta

import polars as pl
import pytest
from conftest import make_payload

from bog_fx_rates_pipeline.transform import build_features
from bog_fx_rates_pipeline.validate import validate_rates

FIRST_DAY = date(2026, 1, 5)


def make_silver(starts: dict[str, float], days: int = 8) -> pl.DataFrame:
    """Build a small Silver table: for each pair, the mid rate rises by 1 each day."""
    rows = []
    for pair, start in starts.items():
        for day in range(days):
            mid = float(start + day)
            rows.append(
                {
                    "rate_date": FIRST_DAY + timedelta(days=day),
                    "pair": pair,
                    "buying": mid - 0.1,
                    "selling": mid + 0.1,
                    "mid": mid,
                }
            )
    return pl.DataFrame(rows)


def test_a_direct_pair_keeps_its_rates():
    gold = build_features(make_silver({"USDGHS": 10.0}))

    first = gold.row(0, named=True)
    assert first["currency"] == "USD"
    assert first["mid_ghs"] == pytest.approx(10.0)
    assert first["spread_ghs"] == pytest.approx(0.2)


def test_a_reversed_pair_is_flipped_to_cedis_per_unit():
    silver = pl.DataFrame(
        {
            "rate_date": [FIRST_DAY],
            "pair": ["GHSNGN"],
            "buying": [4.0],
            "selling": [5.0],
            "mid": [4.5],
        }
    )

    row = build_features(silver).row(0, named=True)

    assert row["currency"] == "NGN"
    assert row["mid_ghs"] == pytest.approx(1 / 4.5, abs=1e-6)
    assert row["spread_ghs"] == pytest.approx(0.05)  # 1/4 - 1/5: buying and selling swap


def test_the_7_day_average_waits_for_7_days_and_keeps_pairs_apart():
    gold = build_features(make_silver({"USDGHS": 1.0, "EURGHS": 100.0}))

    usd = gold.filter(pl.col("pair") == "USDGHS")["mid_7d_avg"].to_list()
    eur = gold.filter(pl.col("pair") == "EURGHS")["mid_7d_avg"].to_list()

    assert usd == [None] * 6 + [4.0, 5.0]  # mean of 1..7, then of 2..8
    assert eur == [None] * 6 + [103.0, 104.0]  # mean of 100..106, then of 101..107


def test_the_first_row_of_each_pair_has_no_change():
    gold = build_features(make_silver({"USDGHS": 1.0, "EURGHS": 100.0}))

    first_day = gold.filter(pl.col("rate_date") == FIRST_DAY)
    assert first_day["mid_change_1d"].to_list() == [None, None]

    usd = gold.filter(pl.col("pair") == "USDGHS")
    assert usd["mid_change_1d"].to_list()[1:3] == [1.0, 1.0]
    assert usd["mid_change_1d_pct"].to_list()[1:3] == [100.0, 50.0]  # 1/1, then 1/2


def test_only_the_low_precision_pair_is_flagged():
    gold = build_features(make_silver({"WAUGHS": 1.0, "USDGHS": 1.0}))

    flagged = gold.filter(pl.col("low_precision"))["pair"].unique().to_list()

    assert flagged == ["WAUGHS"]


def test_gold_has_one_row_per_silver_row():
    silver, _ = validate_rates(make_payload())

    gold = build_features(silver)

    assert gold.height == silver.height == 190
    assert gold.columns == [
        "rate_date",
        "pair",
        "currency",
        "mid_ghs",
        "spread_ghs",
        "mid_change_1d",
        "mid_change_1d_pct",
        "mid_7d_avg",
        "low_precision",
    ]