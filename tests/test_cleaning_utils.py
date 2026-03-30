import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
from cleaning_utils import apply_iqr_filters, pick_best_multiplier


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test_cleaning_utils")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_pick_best_multiplier_prefers_stricter_when_valid():
    results = [
        {"multiplier": 1.5, "retain_ratio": 0.92},
        {"multiplier": 2.0, "retain_ratio": 0.95},
        {"multiplier": 3.0, "retain_ratio": 0.98},
    ]
    chosen = pick_best_multiplier(results, min_retain_ratio=0.9)
    assert chosen["multiplier"] == 1.5


def test_pick_best_multiplier_fallback_to_highest_retain():
    results = [
        {"multiplier": 1.5, "retain_ratio": 0.70},
        {"multiplier": 2.0, "retain_ratio": 0.81},
        {"multiplier": 3.0, "retain_ratio": 0.79},
    ]
    chosen = pick_best_multiplier(results, min_retain_ratio=0.9)
    assert chosen["multiplier"] == 2.0


def test_apply_iqr_filters_removes_extreme_outlier(spark):
    rows = [(1.0,), (1.2,), (1.1,), (1.3,), (100.0,)]
    df = spark.createDataFrame(rows, ["trip_distance"])
    filtered = apply_iqr_filters(df, ["trip_distance"], multiplier=1.5)
    vals = [r["trip_distance"] for r in filtered.collect()]
    assert 100.0 not in vals
    assert len(vals) == 4
