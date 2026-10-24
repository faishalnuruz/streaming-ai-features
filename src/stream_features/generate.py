"""Emit event batches that behave like a real tracker.

The awkward cases are the point, so each batch deliberately contains:

* malformed lines that are not JSON at all;
* parseable events that break a rule (negative price, rating 9, an unknown event
  type, a missing user, an over-long review);
* events whose timestamp is far older than the batch that carries them, which is
  how late data actually arrives.
"""

from __future__ import annotations

import argparse
import json
import random
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

QUERIES = (
    "trail running shoes",
    "ultra marathon hydration vest",
    "carbon plate shoes",
    "trail poles for 100k",
    "night running headlamp",
    "electrolyte tablets",
)
REVIEWS = (
    "grip on wet rock is excellent",
    "too narrow for wide feet",
    "battery lasted the whole night stage",
    "chafed after forty kilometres",
)
MALFORMED = ('{"event_id": "broken", "user_id": ', "{not json at all}", "", '{"event_id": "late"')

BAD_RATINGS = (0, 9, 6)
BAD_EVENT_TYPES = ("teleport", "purchase_attempt", "")


@dataclass
class GenerationStats:
    batches: int
    lines: int
    malformed: int
    off_rule: int
    late: int
    healthy: int

    @property
    def seconds_between_batches(self) -> int:
        return 120


def _healthy_event(rng: random.Random, index: int, when: datetime) -> dict:
    event_type = rng.choices(
        ["view", "search", "click", "add_to_cart", "purchase", "review"],
        weights=[0.42, 0.2, 0.16, 0.1, 0.07, 0.05],
    )[0]
    user = f"user-{rng.randint(1, 40):03d}"
    session = f"{user}-s{rng.randint(1, 3)}"
    event = {
        "event_id": f"evt-{index:06d}",
        "user_id": user,
        "session_id": session,
        "event_type": event_type,
        "ts": when.isoformat().replace("+00:00", "Z"),
        "query": rng.choice(QUERIES) if event_type in {"search", "view"} else None,
        "product_id": f"prd-{rng.randint(1, 60):03d}" if event_type in {"click", "add_to_cart", "purchase"} else None,
        "price": round(rng.uniform(15, 320), 2) if event_type in {"add_to_cart", "purchase"} else None,
        "rating": rng.randint(1, 5) if event_type == "review" else None,
        "review_text": rng.choice(REVIEWS) if event_type == "review" else None,
        "duration_ms": rng.randint(120, 9_000),
    }
    return event


def _off_rule_event(rng: random.Random, index: int, when: datetime) -> dict:
    """Valid JSON, invalid event: this is what fills a dead-letter queue in production."""
    event = _healthy_event(rng, index, when)
    flavour = index % 4
    if flavour == 0:
        event["price"] = -12.5
    elif flavour == 1:
        event["rating"] = rng.choice(BAD_RATINGS)
        event["event_type"] = "review"
    elif flavour == 2:
        event["event_type"] = rng.choice(BAD_EVENT_TYPES)
    else:
        event["review_text"] = "x" * 3_000
    return event


def generate(
    landing_dir: Path,
    batches: int = 4,
    events_per_batch: int = 250,
    seed: int = 13,
    start: datetime | None = None,
) -> GenerationStats:
    rng = random.Random(seed)
    start = start or datetime(2026, 10, 24, 10, 0, tzinfo=UTC)
    landing_dir.mkdir(parents=True, exist_ok=True)

    lines = malformed = off_rule = late = healthy = 0
    counter = 0

    for batch in range(batches):
        arrival = start + timedelta(minutes=2 * batch)
        is_last_batch = batch == batches - 1
        payloads: list[str] = []

        for index in range(events_per_batch):
            counter += 1
            when = arrival - timedelta(seconds=rng.randint(0, 110))

            # A late event is one whose timestamp the watermark has already passed.
            if is_last_batch and rng.random() < 0.06:
                when = arrival - timedelta(minutes=30)
                late += 1

            if rng.random() < 0.05:
                payloads.append(rng.choice(MALFORMED))
                malformed += 1
                continue

            if rng.random() < 0.08:
                payloads.append(json.dumps(_off_rule_event(rng, counter, when)))
                off_rule += 1
                continue

            payloads.append(json.dumps(_healthy_event(rng, counter, when)))
            healthy += 1

        lines += len(payloads)
        (landing_dir / f"batch-{batch:02d}.json").write_text("\n".join(payloads) + "\n")

    return GenerationStats(
        batches=batches, lines=lines, malformed=malformed, off_rule=off_rule, late=late, healthy=healthy
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--landing", type=Path, default=Path("data/landing"))
    parser.add_argument("--batches", type=int, default=4)
    parser.add_argument("--events-per-batch", type=int, default=250)
    parser.add_argument("--seed", type=int, default=13)
    args = parser.parse_args()

    stats = generate(args.landing, args.batches, args.events_per_batch, args.seed)
    print(json.dumps(asdict(stats), indent=2))


if __name__ == "__main__":
    main()
