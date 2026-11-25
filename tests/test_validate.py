import pytest
from conftest import PAIRS, make_payload

from bog_fx_rates_pipeline.validate import validate_rates


def test_a_good_batch_becomes_typed_silver_rows():
    silver, quarantined = validate_rates(make_payload())

    assert silver.height == 190
    assert quarantined.height == 0
    assert silver.columns == ["rate_date", "pair", "buying", "selling", "mid"]
    assert str(silver["rate_date"].dtype) == "Date"
    assert str(silver["buying"].dtype) == "Float64"


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"selling": 9.0}, "selling rate is below the buying rate"),
        ({"buying": -1.0}, "a rate is zero or negative"),
        ({"buying": None}, "missing or has the wrong type"),
        ({"buying": "abc"}, "missing or has the wrong type"),
        ({"mid": 99.0}, "mid is not the average"),
        ({"pair": "USDEUR"}, "exactly one side GHS"),
    ],
)
def test_one_bad_value_is_set_aside_and_the_rest_kept(changes, message):
    silver, quarantined = validate_rates(make_payload(**changes))

    assert silver.height == 189
    assert quarantined.height == 1
    assert message in quarantined["problem"][0]


def test_too_many_bad_rows_reject_the_whole_batch():
    payload = make_payload()
    for row in payload["rows"][:5]:  # 5 of 190 is more than 1%
        row["buying"] = -1.0

    with pytest.raises(ValueError, match="more than 1%"):
        validate_rates(payload)


def test_a_missing_field_is_named():
    payload = make_payload()
    for row in payload["rows"]:
        del row["mid"]

    with pytest.raises(ValueError, match="missing fields: mid"):
        validate_rates(payload)


def test_a_duplicate_key_rejects_the_batch():
    payload = make_payload()
    payload["rows"].append(dict(payload["rows"][0]))

    with pytest.raises(ValueError, match="appear twice"):
        validate_rates(payload)


def test_an_incomplete_day_rejects_the_batch():
    payload = make_payload()
    payload["rows"].pop()  # the last day now has 18 pairs

    with pytest.raises(ValueError, match="does not have exactly 19 pairs"):
        validate_rates(payload)


def test_an_empty_response_is_rejected():
    with pytest.raises(ValueError, match="no rows"):
        validate_rates({"rows": []})


def test_the_fake_data_has_the_19_pairs_from_the_plan():
    assert len(PAIRS) == 19