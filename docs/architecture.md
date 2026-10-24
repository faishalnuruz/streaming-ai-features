# Architecture and decisions

## Flow

```text
tracker events (NDJSON files now; Kafka by changing one read)
        |
        v
  raw text stream
        |
        +--> parse (from_json, permissive)
        |
        +--> validate  --(failure_reason)--> dead letters (raw payload + reason)
        |                                          |
        |                                          +--> counts by reason
        |
        v
  valid events (event_time)
        |
        +--> watermark 2 min --> tumbling 5 min windows --> windows sink
        |        |                        (published only once the window closes)
        |        +--> tumbling 15 min windows per session --> features sink
        |                                 (counts, spend, hashed text embedding)
        |
        v
  run report: published / still open / dropped late / dead letters by reason
        |
        v
  retrieval: cosine similarity over the emitted vectors
```

## Decisions worth defending

**Every valid event is accounted for.** `published + still open + dropped late == valid events` is
asserted in the tests. Streaming systems leak data quietly; a report that reconciles is the cheapest
possible guard.

**"Still open" is a feature, not a bug.** Two thirds of the events in the demo sit in open windows at
shutdown. That is exactly the behaviour that stops a dashboard from publishing a half-finished
number, and naming it separately keeps it from being confused with data loss.

**Late is measured, not assumed.** With every file present at startup, nothing is late — the
watermark sees the newest timestamp immediately. `run_staged` moves files in one at a time so late
arrival is real, and the report counts what was dropped. A test asserts both behaviours, because the
single-pass case is the one that would otherwise hide a bug.

**Validation is SQL, not a Python UDF.** `from_json` is permissive: a malformed line becomes a struct
of nulls, not a null struct, so "is it JSON at all?" is answered from the raw text with
`get_json_object(raw, "$")`. A UDF would have been shorter to write and would tax every row in the
stream.

**One stable reason per row.** A row failing several rules gets the first reason in priority order,
so counts by reason stay comparable across deploys. The alternative — a set of reasons — makes
"unparseable JSON went up 30%" impossible to say.

**The embedder is deterministic and dependency-free.** A signed hash into 32 buckets: no vocabulary
file, no model download, identical output across restarts. It is not semantic, and the README says
so; the value here is the pipeline from stream to retrievable vector, not the embedder's quality.

**`availableNow` is the production trigger and the test trigger.** It drains what exists and
terminates, which makes correctness tests fast and deterministic without a broker, while remaining
the right operator for a scheduled micro-batch job.

## Failure modes

| Situation | What happens |
| --- | --- |
| A line is not JSON | Quarantined as `unparseable_json`, stream continues |
| A line is valid JSON with bad values | Quarantined with the first matching reason |
| Event arrives after its window closed | Dropped by the watermark and counted in `late_events_dropped` |
| Event arrives out of order but inside the watermark | Included in its window normally |
| New file appears between runs | Picked up on the next pass; checkpoints prevent reprocessing old files |
| Run repeated with no new data | Sinks unchanged — verified by a test |
| Feature window ends with no events | No row emitted for that session/window |
| Empty text in a session | A zero vector, not null — keeps downstream array handling simple |
| Checkpoint directory deleted | The sink is rebuilt from the landing directory on the next run |
