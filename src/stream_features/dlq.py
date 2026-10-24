"""Dead-letter handling.

A quarantine folder is only useful if someone can answer "why did these rows
fail, and how many, this hour?" — so failure reasons are single-valued, stable
strings, and the counts are queryable like any other table.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def start_dlq_stream(invalid: DataFrame, path: str, checkpoint: str) -> object:
    """Append rejected rows, with their raw payload, for later inspection."""
    return (
        invalid.writeStream.format("parquet")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .start()
    )


def summary(spark: SparkSession, path: str) -> list[dict]:
    """Failure counts by reason, ordered so the alarming ones come first."""
    if not Path(path).exists():
        return []
    rows = (
        spark.read.parquet(path)
        .groupBy("failure_reason")
        .agg(F.count("*").alias("rows"))
        .orderBy(F.desc("rows"))
        .collect()
    )
    return [{"reason": row["failure_reason"], "rows": row["rows"]} for row in rows]
