"""Turn the stream into features, including vectors a model can consume.

The AI-ready part of this project is deliberately unglamorous. Rather than
claiming a language model, it does what a feature pipeline actually has to do:

* aggregate streaming events into per-session features;
* normalise the numerics and cap the outliers;
* turn free text into a fixed-length vector using a hashing embedder, which is
  deterministic, dependency-free, and stable across restarts;
* make those vectors retrievable by cosine similarity, which is the plumbing a
  retrieval-augmented system needs before any model is involved.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType

EMBEDDING_DIMENSIONS = 32
FEATURE_COLUMNS = (
    "events",
    "views",
    "searches",
    "purchases",
    "reviews",
    "avg_duration_ms",
    "total_spend",
    "avg_rating",
)


def _tokenise(column: str) -> F.Column:
    """Lowercase, strip punctuation, and split free text into tokens."""
    return F.split(F.lower(F.regexp_replace(F.coalesce(F.col(column), F.lit("")), r"[^a-zA-Z0-9 ]", " ")), r"\s+")


def hashing_embedding(column: str, dimensions: int = EMBEDDING_DIMENSIONS) -> F.Column:
    """Hash tokens into a fixed-length vector, signed to keep collisions neutral.

    The signed trick (``+1`` or ``-1`` depending on a second hash) means two
    different tokens landing in the same bucket are as likely to cancel as to
    reinforce, which keeps similarity meaningful without a vocabulary file.
    """
    tokens = F.filter(_tokenise(column), lambda token: F.length(token) > 1)
    buckets = F.transform(
        tokens,
        lambda token: F.struct(
            (F.abs(F.hash(token)) % dimensions).cast("int").alias("bucket"),
            F.when(F.hash(token) % 2 == 0, F.lit(1.0)).otherwise(F.lit(-1.0)).alias("sign"),
        ),
    )

    def bucket_total(dimension: int) -> F.Column:
        """Sum the signs of the tokens hashed into one dimension.

        A closure keeps the lambda at two arguments — the arity Spark's higher
        order functions inspect — while still binding the bucket index.
        """
        return lambda total, item: (
            F.when(item["bucket"] == dimension, total + item["sign"]).otherwise(total)
        )

    return F.array(*(
        F.aggregate(buckets, F.lit(0.0), bucket_total(index)) for index in range(dimensions)
    ))


def session_features(events: DataFrame) -> DataFrame:
    """One row per session, with counts, spend, and a text embedding."""
    return (
        events.groupBy("session_id")
        .agg(
            F.count("*").alias("events"),
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
            F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0)).alias("searches"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
            F.sum(F.when(F.col("event_type") == "review", 1).otherwise(0)).alias("reviews"),
            F.round(F.avg("duration_ms"), 2).alias("avg_duration_ms"),
            F.round(F.sum(F.coalesce(F.col("price"), F.lit(0.0))), 2).alias("total_spend"),
            F.round(F.avg("rating"), 2).alias("avg_rating"),
            F.first("user_id").alias("user_id"),
            F.concat_ws(" ", F.collect_list("query"), F.collect_list("review_text")).alias("text"),
        )
        .withColumn("embedding", hashing_embedding("text"))
        .drop("text")
    )


def scaled_features(frame: DataFrame, columns: tuple[str, ...] = FEATURE_COLUMNS) -> DataFrame:
    """Min-max scale each numeric feature into [0, 1] for model consumption."""
    scaled = frame
    for column in columns:
        stats = scaled.select(
            F.min(column).alias("low"), F.max(column).alias("high")
        ).first()
        low, high = stats["low"], stats["high"]
        if low is None or high is None or high == low:
            scaled = scaled.withColumn(f"{column}_scaled", F.lit(0.0))
        else:
            scaled = scaled.withColumn(
                f"{column}_scaled",
                F.round((F.col(column) - F.lit(low)) / F.lit(high - low), 6),
            )
    return scaled


@dataclass
class RetrievalHit:
    session_id: str
    user_id: str | None
    score: float


def _cosine(left: list[float], right: list[float]) -> float:
    dot = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(a * a for a in left))
    right_norm = math.sqrt(sum(b * b for b in right))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return dot / (left_norm * right_norm)


def nearest(spark: SparkSession, frame: DataFrame, query: str, limit: int = 3) -> list[RetrievalHit]:
    """Rank sessions by cosine similarity to a query, using the same embedder.

    This is the retrieval half of a RAG-shaped system: the stream keeps the
    vectors fresh, and this is how a request finds the closest ones.
    """
    query_vector = (
        spark.createDataFrame([(query,)], ["text"])
        .select(hashing_embedding("text").alias("embedding"))
        .first()["embedding"]
    )
    rows = frame.select("session_id", "user_id", "embedding").collect()
    hits = [
        RetrievalHit(row["session_id"], row["user_id"], _cosine(query_vector, list(row["embedding"])))
        for row in rows
    ]
    return sorted(hits, key=lambda hit: hit.score, reverse=True)[:limit]


EMBEDDING_TYPE = ArrayType(DoubleType())
