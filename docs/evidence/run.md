# Evidence: staged streaming run

Captured on 2026-10-25 on a laptop (`local[2]`, one JVM). Reproduce with:

```bash
python -m stream_features.cli generate --landing data/staging --batches 4 --events-per-batch 250
python -m stream_features.cli run --landing data/landing --root data/run --staged-from data/staging
```

`--staged-from` moves one batch file into the landing directory per pass, so the watermark advances
between passes the way it does on a live stream. Without it, every file is visible at startup and
nothing can be late.

## Generation

```text
{ "batches": 4, "lines": 1000, "malformed": 43, "off_rule": 76, "late": 12, "healthy": 881}
```

## Run report

```text
valid events     881
windows emitted  6
  published      220 events in closed windows
  still open     649 events awaiting the watermark
  dropped late   12 events after their window closed
feature rows     103
dead letters     119
  unparseable_json         43
  invalid_event_type       24
  negative_price           23
  text_too_long            15
  rating_out_of_range      14
```

## What to observe

- **The reconciliation holds**: 220 published + 649 still open + 12 dropped late = 881 valid events.
  The test suite asserts this identity, because leaked events are the failure mode that streaming
  systems hide best.
- **Late events are real here.** The generator marks 6% of the final batch with timestamps 30
  minutes behind, and 12 events were dropped because their window had already closed. Run the same
  command without `--staged-from` and that number is 0 — a test asserts both, so the single-pass
  case cannot quietly mask a broken watermark.
- **Two thirds of the events are legitimately withheld.** 649 events sit in windows the watermark
  has not passed. Publishing them would put a half-finished number on a dashboard.
- **Every rejection has one reason.** 119 dead letters split across five reasons, and a row breaking
  several rules receives the first one in priority order, so counts stay comparable between runs.
- **Nothing was lost twice.** Malformed lines (43) are among the dead letters, not silently dropped
  by the parser.

## Retrieval over the emitted vectors

```text
$ python -m stream_features.cli retrieve --features data/run/sinks/features --query "hydration vest for night running"
103 feature rows available for retrieval
   0.802  user-011-s2  (user-011)
   0.802  user-032-s1  (user-032)
   0.761  user-029-s3  (user-029)
```

An exact-text query scores 1.000, which is what a hash-based embedder should do; the gradation above
comes from partial token overlap. Both behaviours are covered by tests.

## Window contents, verified against a batch recomputation

Streaming results are checked against the same windows computed in batch over the landing directory.
On the single-pass run the sink held two windows and the batch aggregation held four — the same
counts for the windows that had closed:

```text
sink total: 232 | batch total: 881 | valid: 881
event time range: 2026-10-24 16:36 → 17:06
```

That difference is the watermark doing its job, and it is why the report separates "published" from
"still open" rather than reporting one ambiguous number.

## Tests

```text
$ .venv/bin/python -m pytest -q
24 passed in 294.02s
$ .venv/bin/ruff check .
All checks passed!
```

The suite includes two tests that exist specifically to keep this evidence honest: one asserts that
a single pass reports zero dropped events and a non-zero number of open windows, and one asserts
that staged arrival drops events and still satisfies the reconciliation identity.
