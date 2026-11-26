"""Shared test setup."""

from datetime import date, timedelta
import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(autouse=True, scope="session")
def prefect_for_tests():
    """Run every flow in the tests against a throwaway Prefect database."""
    with prefect_test_harness():
        yield

DIRECT = "AUD CAD CHF CNY DKK EUR GBP JPY NOK NZD SEK USD WAU ZAR".split()
REVERSED = "GMD MRO NGN SLL XOF".split()
PAIRS = [f"{code}GHS" for code in DIRECT] + [f"GHS{code}" for code in REVERSED]


def make_payload(days: int = 10, **changes) -> dict:
    """Build a response shaped like StatsGH's, with optional changes to the first row."""
    rows = []
    for day in range(days):
        rate_date = (date(2026, 1, 5) + timedelta(days=day)).isoformat()
        for pair in PAIRS:
            rows.append(
                {
                    "rate_date": rate_date,
                    "pair": pair,
                    "buying": 10.0,
                    "selling": 10.2,
                    "mid": 10.1,
                    "source_url": "https://example.com",
                }
            )
    rows[0].update(changes)
    return {"rows": rows}


@pytest.fixture(autouse=True, scope="session")
def prefect_test_mode():
    """Run all flows in tests against a temporary Prefect server, never a real one."""
    with prefect_test_harness():
        yield