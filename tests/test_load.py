import sqlite3
from contextlib import closing
from datetime import date

import polars as pl
import pytest
from conftest import make_payload

from bog_fx_rates_pipeline import load
from bog_fx_rates_pipeline.validate import validate_rates

EMPTY = {"fx_rates_daily": 0, "fx_rates_features": 0}
FULL = {"fx_rates_daily": 190, "fx_rates_features": 190}


def test_loading_the_same_batch_twice_changes_nothing(tmp_path):
    database = str(tmp_path / "test.sqlite3")
    silver, _ = validate_rates(make_payload())

    load.save(database, silver)
    load.save(database, silver)

    assert load.count_rows(database) == FULL
    assert load.latest_date(database) == date(2026, 1, 14)


def test_a_corrected_value_updates_the_row_instead_of_adding_one(tmp_path):
    database = str(tmp_path / "test.sqlite3")
    first, _ = validate_rates(make_payload())
    corrected, _ = validate_rates(make_payload(selling=10.4, mid=10.2))

    load.save(database, first)
    load.save(database, corrected)

    assert load.count_rows(database) == FULL
    with closing(sqlite3.connect(database)) as connection:
        (selling,) = connection.execute(
            "SELECT selling FROM fx_rates_daily "
            "WHERE rate_date = '2026-01-05' AND pair = 'AUDGHS'"
        ).fetchone()
    assert selling == 10.4


def test_gold_is_rebuilt_from_all_stored_silver(tmp_path):
    database = str(tmp_path / "test.sqlite3")
    silver, _ = validate_rates(make_payload())
    last_day = date(2026, 1, 14)

    load.save(database, silver.filter(pl.col("rate_date") < last_day))
    load.save(database, silver.filter(pl.col("rate_date") == last_day))

    with closing(sqlite3.connect(database)) as connection:
        (average,) = connection.execute(
            "SELECT mid_7d_avg FROM fx_rates_features "
            "WHERE rate_date = '2026-01-14' AND pair = 'USDGHS'"
        ).fetchone()
    assert average is not None  # the last batch had one day, but the history had nine
    assert load.count_rows(database) == FULL


def test_a_failure_rolls_back_both_tables(tmp_path, monkeypatch):
    database = str(tmp_path / "test.sqlite3")
    silver, _ = validate_rates(make_payload())

    def explode(_):
        raise RuntimeError("Gold failed")

    monkeypatch.setattr(load, "build_features", explode)
    with pytest.raises(RuntimeError, match="Gold failed"):
        load.save(database, silver)

    assert load.count_rows(database) == EMPTY


def test_an_empty_database_has_no_latest_date(tmp_path):
    database = str(tmp_path / "test.sqlite3")

    assert load.latest_date(database) is None
    assert load.count_rows(database) == EMPTY