import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test_aggregation")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_hourly_aggregation_shape_and_values(spark):
    rows = [
        (8, 1, 10.0),
        (8, 2, 20.0),
        (9, 1, 15.0),
    ]
    df = spark.createDataFrame(rows, ["pickup_hour", "passenger_count", "total_amount"])
    hourly = df.groupBy("pickup_hour").agg(
        F.count("*").alias("total_trips"),
        F.sum("passenger_count").alias("total_passengers"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    )
    result = {r["pickup_hour"]: (r["total_trips"], r["total_passengers"], float(r["total_revenue"])) for r in hourly.collect()}
    assert result[8] == (2, 3, 30.0)
    assert result[9] == (1, 1, 15.0)


def test_vendor_aggregation_basic(spark):
    rows = [
        (1, 15.0),
        (1, 20.0),
        (2, 10.0),
    ]
    df = spark.createDataFrame(rows, ["VendorID", "total_amount"])
    vendor = df.groupBy("VendorID").agg(
        F.count("*").alias("total_trips"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    )
    result = {r["VendorID"]: (r["total_trips"], float(r["total_revenue"])) for r in vendor.collect()}
    assert result[1] == (2, 35.0)
    assert result[2] == (1, 10.0)
