"""Event schema, parsing, and the validation that decides quarantine vs flow.

Two rules shape this module:

* malformed JSON must never kill the stream, so raw lines are parsed explicitly
  and failures become rows with a reason rather than exceptions;
* validation reasons are machine-readable, because a dead-letter queue nobody
  can query is just a slower way to lose data.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

EVENT_TYPES = ("view", "search", "click", "add_to_cart", "purchase", "review")
MAX_TEXT_LENGTH = 2_000

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("ts", StringType(), True),
    StructField("query", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("rating", LongType(), True),
    StructField("review_text", StringType(), True),
    StructField("duration_ms", LongType(), True),
])

REQUIRED = ("event_id", "user_id", "session_id", "event_type", "ts")


def _blank(column: Column) -> Column:
    return column.isNull() | (F.trim(column) == "")


def parse_lines(raw: DataFrame) -> DataFrame:
    """Turn raw text lines into a struct plus the original line.

    The struct is kept intact rather than expanded immediately: a line that is
    not valid JSON parses to a null struct, which the validator can then label
    ``unparseable_json`` instead of guessing from a wall of null fields.
    """
    return raw.select(
        F.col("value").alias("raw"),
        F.from_json(F.col("value"), EVENT_SCHEMA).alias("event"),
    )


def reasons(frame: DataFrame) -> DataFrame:
    """Attach a single machine-readable failure reason per row (null when valid)."""
    checks: list[tuple[Column, str]] = [
        # `from_json` is permissive: an unparseable line yields a struct of nulls,
        # not a null struct, so validity is checked against the raw text instead.
        # This is pure SQL — a Python UDF here would tax every row in the stream.
        (F.get_json_object(F.col("raw"), "$").isNull(), "unparseable_json"),
        (_blank(F.col("user_id")), "missing_user_id"),
        (_blank(F.col("session_id")), "missing_session_id"),
        (_blank(F.col("event_id")), "missing_event_id"),
        (_blank(F.col("ts")), "missing_timestamp"),
        (F.col("event_time").isNull(), "invalid_timestamp"),
        (~F.col("event_type").isin(*EVENT_TYPES), "invalid_event_type"),
        (F.col("price").isNotNull() & (F.col("price") < 0), "negative_price"),
        (F.col("rating").isNotNull() & ((F.col("rating") < 1) | (F.col("rating") > 5)), "rating_out_of_range"),
        (
            F.col("review_text").isNotNull() & (F.length(F.col("review_text")) > MAX_TEXT_LENGTH),
            "text_too_long",
        ),
        (F.col("duration_ms").isNotNull() & (F.col("duration_ms") < 0), "negative_duration"),
    ]

    # first matching check wins, so reasons are stable and single-valued
    reason = F.lit(None).cast(StringType())
    for condition, label in reversed(checks):
        reason = F.when(condition, F.lit(label)).otherwise(reason)

    return frame.withColumn("failure_reason", reason)


def validate(parsed: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split into (valid, quarantine). Both sides carry the event time."""
    expanded = parsed.select("raw", "event", F.col("event.*"))
    with_time = expanded.withColumn("event_time", F.to_timestamp("ts"))
    checked = reasons(with_time)

    valid = checked.filter(F.col("failure_reason").isNull()).drop("failure_reason", "raw", "event")
    invalid = (
        checked.filter(F.col("failure_reason").isNotNull())
        .select("raw", "failure_reason", "event_time")
    )
    return valid, invalid
