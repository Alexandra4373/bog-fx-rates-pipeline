"""A sketch of the real pipeline from the Lab 1 plan: each task only says what it will do."""

from prefect import flow, task


@task(retries=2, retry_delay_seconds=3)
def fetch_rates() -> str:
    """Will download the full JSON from the StatsGH bog-fx-rates API."""
    print("Fetch: download the full JSON from the StatsGH bog-fx-rates API")
    return "raw response"


@task
def save_bronze(raw: str) -> str:
    """Will save the raw JSON exactly as received, named by fetch date."""
    print("Bronze: save the raw JSON exactly as received, named by fetch date")
    return "bronze file"


@task
def check_rates(bronze: str) -> str:
    """Will apply the data contract from the plan."""
    print("Check: 19 pairs per day, no duplicates, mid between buying and selling")
    return "checked rows"


@task
def build_silver(checked: str) -> str:
    """Will turn the checked rows into one typed row per pair and day."""
    print("Silver: one typed row per pair and day, with base and quote columns")
    return "silver table"


@task
def build_gold(silver: str) -> str:
    """Will calculate the useful numbers."""
    print("Gold: invert the GHSxxx pairs, then daily change, 7-day average, spread")
    return "gold table"


@flow(log_prints=True)
def plan_sketch() -> str:
    """Run the planned steps in order."""
    print("Starting the sketch of the BoG exchange rate pipeline.")
    raw = fetch_rates()
    bronze = save_bronze(raw)
    checked = check_rates(bronze)
    silver = build_silver(checked)
    gold = build_gold(silver)
    return gold


if __name__ == "__main__":
    plan_sketch()
