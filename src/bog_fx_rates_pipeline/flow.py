"""Orchestrate: wire the steps into a Prefect flow with retries and a daily schedule."""

import argparse

import polars as pl
from prefect import flow, get_run_logger, task

from bog_fx_rates_pipeline import config, extract, load
from bog_fx_rates_pipeline.validate import validate_rates


def is_temporary(_task, _task_run, state) -> bool:
    """Tell Prefect to retry only errors that might go away on their own."""
    return isinstance(state.result(raise_on_failure=False), extract.TemporarySourceError)


@task(retries=3, retry_delay_seconds=[5, 30, 60], retry_condition_fn=is_temporary)
def fetch() -> dict:
    return extract.fetch_rates()


@task
def keep_bronze(payload: dict) -> None:
    path = extract.save_bronze(payload, config.BRONZE_DIR)
    get_run_logger().info("Bronze copy saved to %s", path)


@task
def check(payload: dict) -> tuple[pl.DataFrame, pl.DataFrame]:
    return validate_rates(payload)


@task
def store(database: str, silver: pl.DataFrame) -> int:
    return load.save(database, silver)


@flow(name="bog-fx-rates-pipeline", log_prints=True)
def fx_pipeline(write_bronze: bool = True) -> None:
    """Load the Bank of Ghana exchange rates: fetch, keep Bronze, validate, save Silver and Gold.

    The source returns its whole history on every call, so each run re-reads everything.
    Upserts make that safe, and a missed run is repaired by the next one.
    """
    database = config.DATABASE
    payload = fetch()
    if write_bronze:
        keep_bronze(payload)
    silver, quarantined = check(payload)
    if quarantined.height:
        rows = quarantined.select("rate_date", "pair", "problem").head(5).rows()
        get_run_logger().warning("%s row(s) set aside: %s", quarantined.height, rows)
    gold_rows = store(database, silver)
    print(f"Saved {silver.height} Silver rows and refreshed {gold_rows} Gold rows.")
    print(f"Newest stored date: {load.latest_date(database)}")


def main() -> None:
    """Command line: run once, or serve on a daily schedule."""
    parser = argparse.ArgumentParser(description="Load Bank of Ghana exchange rates.")
    parser.add_argument("--serve", action="store_true", help="Run every day at 08:00 UTC")
    args = parser.parse_args()
    if args.serve:
        fx_pipeline.serve(name="daily", cron="0 8 * * *")
    else:
        fx_pipeline()
    print(load.count_rows(config.DATABASE))