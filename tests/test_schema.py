import json

from stream_features import schema

VALID = {
    "event_id": "e1",
    "user_id": "u1",
    "session_id": "s1",
    "event_type": "purchase",
    "ts": "2026-10-24T10:00:00Z",
    "price": 120.5,
    "duration_ms": 900,
}


def parse(spark, *lines):
    raw = spark.createDataFrame([(line,) for line in lines], ["value"])
    return schema.parse_lines(raw)


def reasons_for(spark, *lines) -> list[str | None]:
    _, invalid = schema.validate(parse(spark, *lines))
    return [row["failure_reason"] for row in invalid.collect()]


def test_a_healthy_event_passes(spark):
    valid, invalid = schema.validate(parse(spark, json.dumps(VALID)))

    assert valid.count() == 1
    assert invalid.count() == 0
    row = valid.first()
    assert row["event_time"].year == 2026
    assert row["event_type"] == "purchase"


def test_malformed_json_is_quarantined_by_name(spark):
    reasons = reasons_for(spark, '{"event_id": "broken", "user_id": ', "not json at all")

    assert reasons == ["unparseable_json", "unparseable_json"]


def test_each_broken_rule_gets_its_own_reason(spark):
    cases = {
        "missing_user_id": {**VALID, "user_id": None},
        "invalid_timestamp": {**VALID, "ts": "yesterday"},
        "invalid_event_type": {**VALID, "event_type": "teleport"},
        "negative_price": {**VALID, "price": -1.0},
        "rating_out_of_range": {**VALID, "event_type": "review", "rating": 9},
        "text_too_long": {**VALID, "event_type": "review", "review_text": "x" * 2_001},
        "negative_duration": {**VALID, "duration_ms": -5},
    }

    reasons = reasons_for(spark, *[json.dumps(payload) for payload in cases.values()])

    assert reasons == list(cases)


def test_a_quarantined_row_keeps_its_raw_payload(spark):
    broken = json.dumps({**VALID, "price": -3.0})
    _, invalid = schema.validate(parse(spark, broken))

    row = invalid.first()

    assert row["failure_reason"] == "negative_price"
    assert json.loads(row["raw"])["price"] == -3.0


def test_reasons_are_single_valued_when_several_rules_break(spark):
    payload = json.dumps({**VALID, "user_id": None, "price": -5.0, "event_type": "teleport"})

    reasons = reasons_for(spark, payload)

    assert reasons == ["missing_user_id"]  # the first check in priority order wins


def test_valid_rows_do_not_carry_the_quarantine_columns(spark):
    valid, _ = schema.validate(parse(spark, json.dumps(VALID)))

    assert "failure_reason" not in valid.columns
    assert "raw" not in valid.columns
