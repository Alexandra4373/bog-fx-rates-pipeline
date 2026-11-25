"""Transform: compute Gold features from the full Silver history."""

import polars as pl

LOW_PRECISION_PAIRS = ["WAUGHS"]  # Only 4 decimals and buying = selling, so changes are noise.


def build_features(silver: pl.DataFrame) -> pl.DataFrame:
    """Put every pair on one basis and add the values an analyst or a model will use.

    Pass the whole Silver history, not one batch, so that a 7-day average at the start
    of a new batch can still see the days before it.
    """
    is_reversed = pl.col("pair").str.starts_with("GHS")

    on_one_basis = silver.sort("pair", "rate_date").with_columns(
        pl.when(is_reversed)
        .then(pl.col("pair").str.slice(3))
        .otherwise(pl.col("pair").str.slice(0, 3))
        .alias("currency"),
        pl.when(is_reversed)
        .then(1 / pl.col("selling"))
        .otherwise(pl.col("buying"))
        .alias("buying_ghs"),
        pl.when(is_reversed)
        .then(1 / pl.col("buying"))
        .otherwise(pl.col("selling"))
        .alias("selling_ghs"),
        pl.when(is_reversed).then(1 / pl.col("mid")).otherwise(pl.col("mid")).alias("mid_ghs"),
    )

    return on_one_basis.with_columns(
        (pl.col("selling_ghs") - pl.col("buying_ghs")).round(6).alias("spread_ghs"),
        pl.col("mid_ghs").diff().over("pair").round(6).alias("mid_change_1d"),
        (pl.col("mid_ghs").pct_change().over("pair") * 100).round(4).alias("mid_change_1d_pct"),
        pl.col("mid_ghs").rolling_mean(7).over("pair").round(6).alias("mid_7d_avg"),
        pl.col("pair").is_in(LOW_PRECISION_PAIRS).alias("low_precision"),
        pl.col("mid_ghs").round(6),
    ).select(
        "rate_date",
        "pair",
        "currency",
        "mid_ghs",
        "spread_ghs",
        "mid_change_1d",
        "mid_change_1d_pct",
        "mid_7d_avg",
        "low_precision",
    )