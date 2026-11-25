"""Load: save Silver and Gold so that loading the same batch twice changes nothing."""

import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import date
from pathlib import Path

import polars as pl

from bog_fx_rates_pipeline.transform import build_features

sqlite3.register_adapter(date, date.isoformat)  # Store dates as YYYY-MM-DD text in SQLite.

TABLES = """
CREATE TABLE IF NOT EXISTS fx_rates_daily (
    rate_date DATE NOT NULL,
    pair TEXT NOT NULL,
    buying DOUBLE PRECISION NOT NULL,
    selling DOUBLE PRECISION NOT NULL,
    mid DOUBLE PRECISION NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (rate_date, pair)
);
CREATE TABLE IF NOT EXISTS fx_rates_features (
    rate_date DATE NOT NULL,
    pair TEXT NOT NULL,
    currency TEXT NOT NULL,
    mid_ghs DOUBLE PRECISION NOT NULL,
    spread_ghs DOUBLE PRECISION NOT NULL,
    mid_change_1d DOUBLE PRECISION,
    mid_change_1d_pct DOUBLE PRECISION,
    mid_7d_avg DOUBLE PRECISION,
    low_precision BOOLEAN NOT NULL,
    PRIMARY KEY (rate_date, pair)
);
"""

UPSERT_SILVER = """
INSERT INTO fx_rates_daily (rate_date, pair, buying, selling, mid)
VALUES ({p}, {p}, {p}, {p}, {p})
ON CONFLICT (rate_date, pair) DO UPDATE SET
    buying = excluded.buying,
    selling = excluded.selling,
    mid = excluded.mid,
    loaded_at = CURRENT_TIMESTAMP
"""
UPSERT_GOLD = """
INSERT INTO fx_rates_features
    (rate_date, pair, currency, mid_ghs, spread_ghs, mid_change_1d, mid_change_1d_pct,
     mid_7d_avg, low_precision)
VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
ON CONFLICT (rate_date, pair) DO UPDATE SET
    currency = excluded.currency,
    mid_ghs = excluded.mid_ghs,
    spread_ghs = excluded.spread_ghs,
    mid_change_1d = excluded.mid_change_1d,
    mid_change_1d_pct = excluded.mid_change_1d_pct,
    mid_7d_avg = excluded.mid_7d_avg,
    low_precision = excluded.low_precision
"""
SILVER_COLUMNS = ["rate_date", "pair", "buying", "selling", "mid"]


@contextmanager
def open_database(database: str) -> Iterator[tuple[sqlite3.Cursor, str]]:
    """Open the database, create the tables, and commit only if everything succeeds."""
    Path(database).parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(database)) as connection:
        connection.executescript(TABLES)
        with connection:  # Commits on success, rolls back on any error.
            yield connection.cursor(), "?"


def save(database: str, silver: pl.DataFrame) -> int:
    """Upsert Silver, rebuild Gold from all stored Silver, and commit both together.

    Returns:
        The number of Gold rows written.
    """
    with open_database(database) as (cursor, p):
        cursor.executemany(UPSERT_SILVER.format(p=p), silver.select(SILVER_COLUMNS).rows())
        cursor.execute(f"SELECT {', '.join(SILVER_COLUMNS)} FROM fx_rates_daily")
        stored = pl.DataFrame(
            cursor.fetchall(), schema=SILVER_COLUMNS, orient="row"
        ).with_columns(pl.col("rate_date").cast(pl.String).str.to_date())
        gold = build_features(stored)
        cursor.executemany(UPSERT_GOLD.format(p=p), gold.rows())
    return gold.height


def latest_date(database: str) -> date | None:
    """Return the newest stored date: a way to see how fresh the data is."""
    with open_database(database) as (cursor, _):
        cursor.execute("SELECT MAX(rate_date) FROM fx_rates_daily")
        (latest,) = cursor.fetchone()
    return date.fromisoformat(str(latest)) if latest else None


def count_rows(database: str) -> dict[str, int]:
    """Count the rows in each table: a quick check that reruns add nothing."""
    with open_database(database) as (cursor, _):
        counts = {}
        for table in ("fx_rates_daily", "fx_rates_features"):
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cursor.fetchone()[0]
    return counts