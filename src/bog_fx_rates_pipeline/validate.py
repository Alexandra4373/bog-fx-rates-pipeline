"""Validate: turn the raw response into a typed Silver table. Bad rows are set aside."""

from datetime import UTC, datetime

import polars as pl

from bog_fx_rates_pipeline.config import EXPECTED_PAIRS_PER_DAY

FIELDS = {
    "rate_date": "rate_date",
    "pair": "pair",
    "buying": "buying",
    "selling": "selling",
    "mid": "mid",
}
KEY = ["rate_date", "pair"]
MID_TOLERANCE = 0.0001
MAX_BAD_SHARE = 0.01  # If more than 1% of rows are bad, reject the whole batch.


def validate_rates(payload: dict) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Check the data contract and return (silver, quarantined).

    Problems with the delivery itself reject the whole batch. Problems with the values in a
    few rows set those rows aside, with the reason, and keep the rest.

    Raises:
        ValueError: The response is malformed, a day is incomplete, a key repeats, or too
            many rows are bad.
    """
    rows = payload.get("rows")
    if not isinstance(rows, list) or not rows:
        raise ValueError("bog-fx-rates: the response has no rows.")
    if not all(isinstance(row, dict) for row in rows):
        raise ValueError("bog-fx-rates: every row must be an object with named fields.")
    missing = sorted({name for name in FIELDS for row in rows if name not in row})
    if missing:
        raise ValueError(f"bog-fx-rates: missing fields: {', '.join(missing)}.")

    raw = pl.DataFrame(
        {target: [row[source] for row in rows] for source, target in FIELDS.items()},
        strict=False,
    )
    typed = raw.select(
        pl.col("rate_date").cast(pl.String).str.to_date("%Y-%m-%d", strict=False),
        pl.col("pair").cast(pl.String),
        pl.col("buying", "selling", "mid").cast(pl.Float64, strict=False),
    )

    # Rules about the whole delivery: if one breaks, reject the batch.
    keyed = typed.filter(pl.col("rate_date").is_not_null(), pl.col("pair").is_not_null())
    batch_rules = {
        "the same date and pair appear twice": pl.struct(KEY).is_duplicated(),
        f"a day does not have exactly {EXPECTED_PAIRS_PER_DAY} pairs": (
            pl.col("pair").count().over("rate_date") != EXPECTED_PAIRS_PER_DAY
        ),
    }
    for problem, broken in batch_rules.items():
        bad_rows = keyed.filter(broken)
        if bad_rows.height:
            examples = bad_rows.select(KEY).head(3).rows()
            raise ValueError(
                f"bog-fx-rates: {problem} in {bad_rows.height} row(s), e.g. {examples}."
            )

    # Rules about one row's values: a bad row is set aside, not the whole batch.
    rates = pl.col("buying", "selling", "mid")
    average = (pl.col("buying") + pl.col("selling")) / 2
    pair_is_valid = pl.col("pair").str.contains(r"^(GHS[A-Z]{3}|[A-Z]{3}GHS)$") & (
        pl.col("pair") != "GHSGHS"
    )
    row_rules = {
        "a value is missing or has the wrong type": pl.any_horizontal(pl.all().is_null()),
        "a rate is zero or negative": pl.any_horizontal(rates <= 0),
        "the selling rate is below the buying rate": pl.col("selling") < pl.col("buying"),
        "mid is not the average of buying and selling": (pl.col("mid") - average).abs()
        > MID_TOLERANCE,
        "the pair is not six letters with exactly one side GHS": ~pair_is_valid,
        "the date is in the future": pl.col("rate_date") > datetime.now(UTC).date(),
    }
    reasons = pl.concat_str(
        [pl.when(broken).then(pl.lit(problem)) for problem, broken in row_rules.items()],
        separator="; ",
        ignore_nulls=True,
    )
    flagged = typed.with_columns(reasons.alias("problem"))
    quarantined = flagged.filter(pl.col("problem") != "")
    silver = flagged.filter(pl.col("problem") == "").drop("problem")

    if quarantined.height / typed.height > MAX_BAD_SHARE:
        examples = quarantined.select([*KEY, "problem"]).head(3).rows()
        raise ValueError(
            f"bog-fx-rates: {quarantined.height} of {typed.height} rows are bad, "
            f"more than {MAX_BAD_SHARE:.0%}. e.g. {examples}."
        )
    return silver.sort(KEY), quarantined.sort(KEY)