from pathlib import Path

from stream_features import dlq, job


def config_for(landing):
    return job.PipelineConfig(landing=landing["path"], root=landing["root"])


def test_bounded_run_routes_every_row_somewhere(spark, landing):
    config = config_for(landing)

    report = job.run_bounded(spark, config)

    assert report.input_lines == landing["stats"].lines
    assert report.dead_letters == landing["stats"].malformed + landing["stats"].off_rule
    assert report.valid_events == landing["stats"].healthy
    assert report.windows > 0
    assert report.feature_rows > 0
    # every valid event is published, still inside an open window, or dropped late
    assert (
        report.windowed_points + report.pending_window_events + report.late_events_dropped
        == report.valid_events
    )


def test_a_single_pass_never_publishes_a_half_finished_window(spark, landing):
    """Everything at once means every file is seen before the watermark moves."""
    report = job.run_bounded(spark, config_for(landing))

    assert report.late_events_dropped == 0
    assert report.pending_window_events > 0
    assert report.watermark is not None


def test_staged_arrival_makes_late_events_real(spark, landing):
    """Feeding files one at a time advances the watermark and drops the stragglers."""
    staging = landing["path"].parent / "staging"
    from stream_features.generate import generate

    generate(staging, batches=5, events_per_batch=40, seed=17)
    config = job.PipelineConfig(landing=landing["root"] / "landing", root=landing["root"])

    report = job.run_staged(spark, config, staging)

    assert report.late_events_dropped > 0
    assert (
        report.windowed_points + report.pending_window_events + report.late_events_dropped
        == report.valid_events
    )


def test_dead_letters_record_why_each_row_failed(spark, landing):
    job.run_bounded(spark, config_for(landing))

    reasons = {row["reason"]: row["rows"] for row in dlq.summary(spark, config_for(landing).sink("dead_letters"))}

    assert "unparseable_json" in reasons
    assert sum(reasons.values()) == landing["stats"].malformed + landing["stats"].off_rule


def test_a_second_run_does_not_reprocess_the_same_files(spark, landing):
    """Checkpoints make a scheduled micro-batch run safe to repeat."""
    config = config_for(landing)
    first = job.run_bounded(spark, config)
    second = job.run_bounded(spark, config)

    assert first.windows == second.windows
    assert first.dead_letters == second.dead_letters
    assert first.feature_rows == second.feature_rows


def test_a_new_batch_is_picked_up_on_the_next_run(spark, landing):
    config = config_for(landing)
    first = job.run_bounded(spark, config)

    extra = landing["path"] / "batch-99.json"
    extra.write_text(
        '{"event_id": "late-arrival", "user_id": "user-999", "session_id": "user-999-s1",'
        ' "event_type": "view", "ts": "2026-10-24T10:05:30Z", "duration_ms": 500}\n'
    )
    second = job.run_bounded(spark, config)

    assert second.input_lines > first.input_lines
    assert second.valid_events > first.valid_events
    assert Path(config.sink("windows")).exists()


def test_report_is_serialisable_and_complete(spark, landing):
    payload = job.run_bounded(spark, config_for(landing)).as_dict()

    assert set(payload) == {
        "input_lines",
        "valid_events",
        "windows",
        "published_events",
        "pending_window_events",
        "late_events_dropped",
        "watermark",
        "feature_rows",
        "dead_letters",
        "dead_letter_reasons",
    }
    assert isinstance(payload["dead_letter_reasons"], list)
    assert payload["dead_letter_reasons"][0]["rows"] >= payload["dead_letter_reasons"][-1]["rows"]
