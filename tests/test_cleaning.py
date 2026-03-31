import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
from cleaning_utils import apply_iqr_filters


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test_cleaning")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_cleaning_removes_invalid_rules(spark):
    rows = [
        (1, 1.2, 10.0, 15.0),   # valid
        (2, -1.0, 11.0, 16.0),  # invalid distance
        (3, 2.0, -3.0, 5.0),    # invalid fare
        (4, 0.5, 8.0, -1.0),    # invalid total
    ]
    df = spark.createDataFrame(rows, ["id", "trip_distance", "fare_amount", "total_amount"])
    cleaned = df.filter(
        (df.trip_distance > 0)
        & (df.fare_amount >= 0)
        & (df.total_amount > 0)
    )
    ids = [r["id"] for r in cleaned.collect()]
    assert ids == [1]


def test_cleaning_iqr_drops_extreme_values(spark):
    rows = [
        (1.0, 10.0, 11.0),
        (1.1, 10.5, 12.0),
        (1.2, 11.0, 13.0),
        (40.0, 300.0, 350.0),  # extreme outlier
    ]
    df = spark.createDataFrame(rows, ["trip_distance", "fare_amount", "total_amount"])
    filtered = apply_iqr_filters(df, ["trip_distance", "fare_amount", "total_amount"], multiplier=1.5)
    assert filtered.count() == 3
