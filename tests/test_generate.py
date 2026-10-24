import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stream_features.generate import generate


def test_generation_produces_every_awkward_case(tmp_path):
    stats = generate(tmp_path / "landing", batches=3, events_per_batch=50, seed=7)

    assert stats.malformed > 0
    assert stats.off_rule > 0
    assert stats.late > 0
    assert stats.healthy > stats.malformed + stats.off_rule
    assert stats.lines == stats.malformed + stats.off_rule + stats.healthy


def test_one_file_per_batch_and_lines_match_the_count(tmp_path):
    landing = tmp_path / "landing"
    stats = generate(landing, batches=4, events_per_batch=30, seed=7)

    files = sorted(landing.glob("batch-*.json"))
    total_lines = sum(len(path.read_text().strip().splitlines()) for path in files)

    assert len(files) == 4
    assert total_lines == stats.lines


def test_late_events_are_far_older_than_their_batch(tmp_path):
    landing = tmp_path / "landing"
    generate(landing, batches=2, events_per_batch=120, seed=9)

    stamps = []
    for path in sorted(landing.glob("batch-*.json")):
        for line in path.read_text().splitlines():
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            stamps.append((path.name, payload.get("ts")))

    newest = max(stamp for _, stamp in stamps)
    oldest = min(stamp for _, stamp in stamps)

    assert oldest < newest  # there is real spread, produced by the late events


def test_generation_is_reproducible(tmp_path):
    first = generate(tmp_path / "a", batches=2, events_per_batch=25, seed=11)
    second = generate(tmp_path / "b", batches=2, events_per_batch=25, seed=11)

    assert first == second
    assert (tmp_path / "a" / "batch-00.json").read_text() == (tmp_path / "b" / "batch-00.json").read_text()
