from datetime import datetime

from pyspark.sql import functions as F

from stream_features import features

EVENTS = [
    ("s1", "u1", "search", "trail running shoes", None, None, None, 500),
    ("s1", "u1", "view", "trail running shoes", None, None, None, 900),
    ("s1", "u1", "purchase", "trail running shoes", 210.0, None, None, 400),
    ("s2", "u2", "review", None, None, 5, "electrolyte tablets taste fine", 1_200),
    ("s2", "u2", "view", "electrolyte tablets", None, None, None, 300),
]

COLUMNS = [
    "session_id",
    "user_id",
    "event_type",
    "query",
    "price",
    "rating",
    "review_text",
    "duration_ms",
]


def events(spark):
    return spark.createDataFrame(EVENTS, COLUMNS).withColumn(
        "event_time", F.lit(datetime(2026, 10, 24, 10, 0))
    )


def test_session_features_aggregate_the_stream(spark):
    result = {
        row["session_id"]: row
        for row in features.session_features(events(spark)).collect()
    }

    assert result["s1"]["events"] == 3
    assert result["s1"]["searches"] == 1
    assert result["s1"]["purchases"] == 1
    assert result["s1"]["total_spend"] == 210.0
    assert result["s2"]["reviews"] == 1
    assert result["s2"]["avg_rating"] == 5.0


def test_embeddings_are_deterministic_and_fixed_length(spark):
    frame = features.session_features(events(spark))
    rows = {row["session_id"]: row for row in frame.collect()}
    again = {
        row["session_id"]: row
        for row in features.session_features(events(spark)).collect()
    }

    assert len(rows["s1"]["embedding"]) == features.EMBEDDING_DIMENSIONS
    assert rows["s1"]["embedding"] == again["s1"]["embedding"]
    assert rows["s1"]["embedding"] != rows["s2"]["embedding"]


def test_embedding_of_empty_text_is_all_zero(spark):
    frame = spark.createDataFrame([("",)], ["text"])

    vector = frame.select(features.hashing_embedding("text").alias("v")).first()["v"]

    assert set(vector) == {0.0}


def test_scaled_features_land_between_zero_and_one(spark):
    frame = features.session_features(events(spark))

    scaled = features.scaled_features(frame)
    row = scaled.orderBy("session_id").first()

    for column in features.FEATURE_COLUMNS:
        value = row[f"{column}_scaled"]
        assert value is None or 0.0 <= value <= 1.0
    # the largest session must scale to the top of the range
    assert scaled.orderBy(scaled["events_scaled"].desc()).first()["session_id"] == "s1"


def test_review_text_contributes_to_the_embedding(spark):
    """Text, not just counts: a review about electrolytes should find session two."""
    frame = features.session_features(events(spark))

    hits = features.nearest(spark, frame, "electrolyte tablets", limit=1)

    assert hits[0].session_id == "s2"


def test_retrieval_ranks_the_matching_session_first(spark):
    frame = features.session_features(events(spark))

    hits = features.nearest(spark, frame, "trail running shoes", limit=2)

    assert hits[0].session_id == "s1"
    assert hits[0].score > hits[1].score
    assert hits[0].user_id == "u1"


def test_retrieval_uses_the_same_embedder_for_the_query(spark):
    frame = features.session_features(events(spark))

    hits = features.nearest(spark, frame, "electrolyte tablets")

    assert hits[0].session_id == "s2"
