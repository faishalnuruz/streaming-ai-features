import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from stream_features.generate import generate


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("stream-features-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def landing(tmp_path):
    """A small landing directory with the same awkward cases as the full one."""
    path = tmp_path / "landing"
    stats = generate(path, batches=3, events_per_batch=40, seed=5)
    return {"path": path, "stats": stats, "root": tmp_path / "run"}
