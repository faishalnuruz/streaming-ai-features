"""The streaming pipeline: ingest, validate, window, feature, quarantine.

Three sinks hang off one validated source:

* **windows** — tumbling counts per event type, emitted only once the watermark
  passes the window, so a dashboard never shows a half-finished number;
* **features** — per-session vectors for model consumption, using the same
  hashing embedder the retrieval helper uses;
* **dead letters** — every rejected row with a machine-readable reason.

Structured Streaming gives watermarks and state, which is why a batch file
source can stand in for Kafka here: the semantics under test are the streaming
ones. The README is explicit that swapping in a real broker changes one line.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from stream_features import dlq, features, schema


@dataclass
class PipelineConfig:
    landing: Path
    root: Path
    watermark: str = "2 minutes"
    window: str = "5 minutes"
    feature_window: str = "15 minutes"

    def sink(self, name: str) -> str:
        return str(self.root / "sinks" / name)

    def checkpoint(self, name: str) -> str:
        return str(self.root / "checkpoints" / name)


@dataclass
class RunReport:
    input_lines: int = 0
    valid_events: int = 0
    windows: int = 0
    windowed_points: int = 0
    pending_window_events: int = 0
    late_events_dropped: int = 0
    watermark: str | None = None
    feature_rows: int = 0
    dead_letters: int = 0
    dead_letter_reasons: list[dict] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "input_lines": self.input_lines,
            "valid_events": self.valid_events,
            "windows": self.windows,
            "published_events": self.windowed_points,
            "pending_window_events": self.pending_window_events,
            "late_events_dropped": self.late_events_dropped,
            "watermark": self.watermark,
            "feature_rows": self.feature_rows,
            "dead_letters": self.dead_letters,
            "dead_letter_reasons": self.dead_letter_reasons,
        }


def read_events(spark: SparkSession, landing: Path) -> DataFrame:
    """Read the landing directory as a stream of raw text lines."""
    return spark.readStream.text(str(landing))


def windowed_counts(valid: DataFrame, config: PipelineConfig) -> DataFrame:
    return (
        valid.withWatermark("event_time", config.watermark)
        .groupBy(F.window("event_time", config.window).alias("window"), "event_type")
        .agg(
            F.count("*").alias("events"),
            F.round(F.sum(F.coalesce(F.col("price"), F.lit(0.0))), 2).alias("revenue"),
            F.approx_count_distinct("session_id").alias("sessions"),
        )
    )


def session_feature_stream(valid: DataFrame, config: PipelineConfig) -> DataFrame:
    return (
        valid.withWatermark("event_time", config.watermark)
        .groupBy(F.window("event_time", config.feature_window).alias("window"), "session_id")
        .agg(
            F.count("*").alias("events"),
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
            F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0)).alias("searches"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
            F.round(F.avg("duration_ms"), 2).alias("avg_duration_ms"),
            F.round(F.sum(F.coalesce(F.col("price"), F.lit(0.0))), 2).alias("total_spend"),
            F.last("user_id").alias("user_id"),
            F.concat_ws(" ", F.collect_list("query"), F.collect_list("review_text")).alias("text"),
        )
        .withColumn("embedding", features.hashing_embedding("text"))
        .drop("text")
    )


def build_queries(
    spark: SparkSession, config: PipelineConfig, available_now: bool = True
) -> dict[str, StreamingQuery]:
    """Start the three sinks, each with its own checkpoint.

    ``availableNow`` drains what is in the landing directory and terminates,
    which is what makes a streaming job testable and what a scheduled micro-batch
    run uses in production. With it off, the same topology runs continuously.
    """
    raw = read_events(spark, config.landing)
    valid, invalid = schema.validate(schema.parse_lines(raw))

    def sink(frame: DataFrame, name: str) -> StreamingQuery:
        writer = (
            frame.writeStream.format("parquet")
            .option("path", config.sink(name))
            .option("checkpointLocation", config.checkpoint(name))
            .outputMode("append")
        )
        if available_now:
            writer = writer.trigger(availableNow=True)
        return writer.start()

    return {
        "windows": sink(windowed_counts(valid, config), "windows"),
        "features": sink(session_feature_stream(valid, config), "features"),
        "dead_letters": sink(invalid, "dead_letters"),
    }


def _count(spark: SparkSession, path: Path) -> int:
    return spark.read.parquet(str(path)).count() if path.exists() else 0


def window_split(spark: SparkSession, config: PipelineConfig) -> dict:
    """Split the valid events three ways: published, still open, or dropped late.

    The split matters because the three outcomes mean different things:

    * **published** — the window closed and its result is in the sink;
    * **pending** — the watermark has not passed the window yet, so it is
      deliberately withheld. This is the behaviour that stops a dashboard from
      showing a half-finished number;
    * **dropped** — the window closed before the event arrived, so the event is
      genuinely too late to include. Counting it is the only way to notice a
      producer whose clock is behind.
    """
    landing = spark.read.text(str(config.landing))
    valid, _ = schema.validate(schema.parse_lines(landing))

    latest = valid.select(F.max("event_time").alias("latest")).first()["latest"]
    if latest is None:
        return {"published": 0, "pending": 0, "dropped": 0, "watermark": None}

    watermark = spark.sql(
        f"SELECT TIMESTAMP'{latest.isoformat()}' - INTERVAL {config.watermark} AS wm"
    ).first()["wm"]

    per_window = valid.groupBy(
        F.window("event_time", config.window).alias("window")
    ).agg(F.count("*").alias("events"))

    pending = per_window.filter(F.col("window.end") > F.lit(watermark)).select(
        F.coalesce(F.sum("events"), F.lit(0))
    ).first()[0]

    windows_path = Path(config.sink("windows"))
    published = 0
    if windows_path.exists():
        published = (
            spark.read.parquet(str(windows_path)).select(F.coalesce(F.sum("events"), F.lit(0))).first()[0]
        )

    return {
        "published": int(published),
        "pending": int(pending),
        "dropped": int(valid.count() - published - pending),
        "watermark": watermark.isoformat() if watermark else None,
    }


def report(spark: SparkSession, config: PipelineConfig) -> RunReport:
    """Summarise the run, including what the watermark held back or discarded."""
    landing = spark.read.text(str(config.landing))
    valid, invalid = schema.validate(schema.parse_lines(landing))
    windows_path = Path(config.sink("windows"))
    window_rows = spark.read.parquet(str(windows_path)).count() if windows_path.exists() else 0
    split = window_split(spark, config)

    return RunReport(
        input_lines=landing.count(),
        valid_events=valid.count(),
        windows=window_rows,
        windowed_points=split["published"],
        pending_window_events=split["pending"],
        late_events_dropped=split["dropped"],
        watermark=split["watermark"],
        feature_rows=_count(spark, Path(config.sink("features"))),
        dead_letters=invalid.count(),
        dead_letter_reasons=dlq.summary(spark, config.sink("dead_letters")),
    )


def run_bounded(spark: SparkSession, config: PipelineConfig, timeout_seconds: int = 180) -> RunReport:
    """Process everything currently in the landing directory, then stop.

    ``availableNow`` makes a streaming job testable: it drains what exists and
    terminates, which is how the correctness tests get deterministic answers.
    """
    _drain(spark, config, timeout_seconds)
    return report(spark, config)


def _drain(spark: SparkSession, config: PipelineConfig, timeout_seconds: int) -> None:
    """Run one availableNow pass and wait for all three sinks to finish."""
    for name, query in build_queries(spark, config, available_now=True).items():
        if not query.awaitTermination(timeout=timeout_seconds):
            raise TimeoutError(f"sink {name!r} did not finish within {timeout_seconds}s")
        query.stop()


def run_staged(
    spark: SparkSession,
    config: PipelineConfig,
    staging: Path,
    timeout_seconds: int = 180,
) -> RunReport:
    """Feed the landing directory one batch file at a time.

    This is what makes late data real: with every file present at startup, the
    first micro-batch sees the newest timestamp and nothing is late. Moving files
    in one at a time advances the watermark between passes, so an event whose
    window has already closed is genuinely dropped — and counted.
    """
    staged = []
    for batch_file in sorted(staging.glob("*.json")):
        config.landing.mkdir(parents=True, exist_ok=True)
        target = config.landing / batch_file.name
        shutil.move(str(batch_file), str(target))
        staged.append(target)
        _drain(spark, config, timeout_seconds)
    return report(spark, config)
