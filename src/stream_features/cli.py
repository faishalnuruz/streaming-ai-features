"""Command line entry point.

    python -m stream_features.cli generate --landing data/landing
    python -m stream_features.cli run --landing data/landing --root data/run
    python -m stream_features.cli retrieve --features data/run/sinks/features --query "trail shoes"
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession

from stream_features import features, generate, job


def build_spark(app_name: str = "stream-features", cores: str = "local[2]") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master(cores)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="write event batches into the landing directory")
    gen.add_argument("--landing", type=Path, default=Path("data/landing"))
    gen.add_argument("--batches", type=int, default=4)
    gen.add_argument("--events-per-batch", type=int, default=250)
    gen.add_argument("--seed", type=int, default=13)

    run = sub.add_parser("run", help="process the landing directory and summarise")
    run.add_argument("--landing", type=Path, default=Path("data/landing"))
    run.add_argument("--root", type=Path, default=Path("data/run"))
    run.add_argument("--watermark", default="2 minutes")
    run.add_argument("--report", type=Path, default=Path("docs/generated/run-report.json"))
    run.add_argument(
        "--staged-from",
        type=Path,
        default=None,
        help="move batch files from this directory in one at a time, so late events are real",
    )

    retrieve = sub.add_parser("retrieve", help="cosine-search the emitted feature vectors")
    retrieve.add_argument("--features", type=Path, default=Path("data/run/sinks/features"))
    retrieve.add_argument("--query", required=True)
    retrieve.add_argument("--limit", type=int, default=3)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "generate":
        stats = generate.generate(
            args.landing, batches=args.batches, events_per_batch=args.events_per_batch, seed=args.seed
        )
        print(json.dumps(stats.__dict__, indent=2))
        return 0

    spark = build_spark()
    try:
        if args.command == "run":
            config = job.PipelineConfig(
                landing=args.landing, root=args.root, watermark=args.watermark
            )
            if args.staged_from:
                result = job.run_staged(spark, config, args.staged_from)
            else:
                result = job.run_bounded(spark, config)
            payload = result.as_dict()
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(payload, indent=2) + "\n")

            print(f"input lines      {payload['input_lines']:,}")
            print(f"valid events     {payload['valid_events']:,}")
            print(f"windows emitted  {payload['windows']:,}")
            print(f"  published      {payload['published_events']:,} events in closed windows")
            print(f"  still open     {payload['pending_window_events']:,} events awaiting the watermark")
            print(f"  dropped late   {payload['late_events_dropped']:,} events after their window closed")
            print(f"feature rows     {payload['feature_rows']:,}")
            print(f"dead letters     {payload['dead_letters']:,}")
            for reason in payload["dead_letter_reasons"]:
                print(f"  {reason['reason']:24} {reason['rows']:,}")
            print(f"\nReport: {args.report}")
            return 0

        frame = spark.read.parquet(str(args.features))
        print(f"{frame.count():,} feature rows available for retrieval")
        for hit in features.nearest(spark, frame, args.query, limit=args.limit):
            print(f"  {hit.score:6.3f}  {hit.session_id}  ({hit.user_id})")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
