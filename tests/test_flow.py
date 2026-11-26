import pytest
from conftest import make_payload

from bog_fx_rates_pipeline import config, extract, flow, load

EMPTY = {"fx_rates_daily": 0, "fx_rates_features": 0}
FULL = {"fx_rates_daily": 190, "fx_rates_features": 190}


def use_temporary_files(monkeypatch, tmp_path) -> str:
    """Point the database and Bronze folder at temporary places."""
    database = str(tmp_path / "test.sqlite3")
    monkeypatch.setattr(config, "DATABASE", database)
    monkeypatch.setattr(config, "BRONZE_DIR", str(tmp_path / "bronze"))
    return database


def retry_without_waiting(monkeypatch) -> None:
    """Don't wait between retries in a test."""
    quick = flow.fetch.with_options(retry_delay_seconds=0)
    monkeypatch.setattr(flow, "fetch", quick)


def test_the_flow_runs_end_to_end_without_the_internet(tmp_path, monkeypatch):
    database = use_temporary_files(monkeypatch, tmp_path)
    monkeypatch.setattr(extract, "fetch_rates", lambda: make_payload())

    flow.fx_pipeline()

    assert load.count_rows(database) == FULL
    assert len(list((tmp_path / "bronze").rglob("*.json"))) == 1


def test_running_the_flow_twice_changes_nothing(tmp_path, monkeypatch):
    database = use_temporary_files(monkeypatch, tmp_path)
    monkeypatch.setattr(extract, "fetch_rates", lambda: make_payload())

    flow.fx_pipeline()
    flow.fx_pipeline()

    assert load.count_rows(database) == FULL


def test_a_bad_row_is_set_aside_and_the_flow_still_succeeds(tmp_path, monkeypatch):
    database = use_temporary_files(monkeypatch, tmp_path)
    monkeypatch.setattr(extract, "fetch_rates", lambda: make_payload(selling=9.0))

    flow.fx_pipeline()

    assert load.count_rows(database) == {"fx_rates_daily": 189, "fx_rates_features": 189}


def test_a_rejected_batch_stores_nothing_but_keeps_the_raw_copy(tmp_path, monkeypatch):
    database = use_temporary_files(monkeypatch, tmp_path)
    payload = make_payload()
    payload["rows"].pop()  # the last day now has 18 pairs
    monkeypatch.setattr(extract, "fetch_rates", lambda: payload)

    with pytest.raises(ValueError, match="does not have exactly 19 pairs"):
        flow.fx_pipeline()

    assert load.count_rows(database) == EMPTY
    assert len(list((tmp_path / "bronze").rglob("*.json"))) == 1


def test_a_temporary_failure_is_retried(tmp_path, monkeypatch):
    use_temporary_files(monkeypatch, tmp_path)
    retry_without_waiting(monkeypatch)
    calls = []

    def flaky_fetch():
        calls.append(1)
        if len(calls) == 1:
            raise extract.TemporarySourceError("Simulated timeout")
        return make_payload()

    monkeypatch.setattr(extract, "fetch_rates", flaky_fetch)

    flow.fx_pipeline(write_bronze=False)

    assert len(calls) == 2


def test_a_permanent_failure_is_not_retried(tmp_path, monkeypatch):
    use_temporary_files(monkeypatch, tmp_path)
    retry_without_waiting(monkeypatch)
    calls = []

    def broken_fetch():
        calls.append(1)
        raise ValueError("Permanent problem")

    monkeypatch.setattr(extract, "fetch_rates", broken_fetch)

    with pytest.raises(ValueError, match="Permanent problem"):
        flow.fx_pipeline(write_bronze=False)

    assert len(calls) == 1