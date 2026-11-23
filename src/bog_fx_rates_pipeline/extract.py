"""Extract: ask the source for data and keep an untouched Bronze copy."""

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx

from bog_fx_rates_pipeline.config import SOURCE_URL


class TemporarySourceError(RuntimeError):
    """The source failed in a way that might work if we try again."""


def fetch_rates() -> dict:
    """Download the full Bank of Ghana exchange rate dataset from StatsGH.

    The API returns every row it holds (about a month of history) in one response.

    Raises:
        TemporarySourceError: Network trouble, rate limiting or a server error.
        httpx.HTTPStatusError: Any other bad answer, such as a 404 for a wrong URL.
    """
    try:
        response = httpx.get(SOURCE_URL, timeout=30)
    except httpx.TransportError as error:
        raise TemporarySourceError(f"Could not reach StatsGH: {error}") from error
    if response.status_code == 429 or response.status_code >= 500:
        raise TemporarySourceError(f"StatsGH answered {response.status_code}. Try later.")
    response.raise_for_status()
    return response.json()


def save_bronze(payload: dict, bronze_dir: str) -> Path:
    """Save the response as received, plus metadata about where and when it came from."""
    now = datetime.now(UTC)
    folder = Path(bronze_dir) / f"ingest_date={now.date()}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{uuid4().hex}.json"
    record = {
        "_source": SOURCE_URL,
        "_ingested_at": now.isoformat(),
        "payload": payload,
    }
    path.write_text(json.dumps(record, indent=2), encoding="utf-8")
    return path
