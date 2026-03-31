"""
Group 4 - Week 1 EDA
FILE: src/01_eda.py
Goal: Explore Bronze data and list data quality issues.
Run: python src/01_eda.py
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(__file__))
from config import BRONZE


BRONZE_PATH = BRONZE

spark = (
    SparkSession.builder.appName("NYC_Taxi_EDA")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 60)
print("EDA - NYC Yellow Taxi 01/2026 (Bronze)")
print("=" * 60)

print("\n[1] Read Bronze parquet...")
df = spark.read.parquet(BRONZE_PATH)
total_rows = df.count()
print(f"  Rows    : {total_rows:,}")
print(f"  Columns : {len(df.columns)}")

print("\n[2] Schema")
df.printSchema()

print("\n[3] Sample rows")
df.show(5, truncate=False)

print("\n[4] Descriptive stats")
df.describe("passenger_count", "trip_distance", "fare_amount", "tip_amount", "total_amount").show()

print("\n[5] Null counts")
null_df = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
null_df.show(truncate=False)

print("\n[6] Duplicate rows")
distinct_rows = df.distinct().count()
duplicates = total_rows - distinct_rows
print(f"  Total      : {total_rows:,}")
print(f"  Distinct   : {distinct_rows:,}")
print(f"  Duplicates : {duplicates:,} ({(duplicates / total_rows * 100) if total_rows else 0:.2f}%)")

print("\n[7] Trips by pickup date")
df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime")).groupBy("pickup_date").count().orderBy("pickup_date").show(31)

print("\n[8] Trips by pickup hour")
df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime")).groupBy("pickup_hour").count().orderBy("pickup_hour").show(24)

print("\n[9] Vendor distribution")
df.groupBy("VendorID").count().withColumn("pct", F.round(F.col("count") / total_rows * 100, 2)).orderBy("VendorID").show()

print("\n[10] Value ranges")
df.select(
    F.min("trip_distance").alias("dist_min"),
    F.max("trip_distance").alias("dist_max"),
    F.avg("trip_distance").alias("dist_avg"),
    F.min("fare_amount").alias("fare_min"),
    F.max("fare_amount").alias("fare_max"),
    F.avg("fare_amount").alias("fare_avg"),
    F.min("total_amount").alias("total_min"),
    F.max("total_amount").alias("total_max"),
    F.avg("total_amount").alias("total_avg"),
).show()

print("\n[11] Distribution analysis for trip_distance and fare_amount")
trip_q = df.approxQuantile("trip_distance", [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99], 0.01)
fare_q = df.approxQuantile("fare_amount", [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99], 0.01)
print("  trip_distance quantiles (p10,p25,p50,p75,p90,p95,p99):")
print(f"  {trip_q}")
print("  fare_amount quantiles (p10,p25,p50,p75,p90,p95,p99):")
print(f"  {fare_q}")

print("\n[12] Payment type distribution")
df.groupBy("payment_type").count().withColumn("pct", F.round(F.col("count") / total_rows * 100, 2)).orderBy("payment_type").show()

print("\n[13] Out-of-month rows")
out_of_range = df.filter((F.year("tpep_pickup_datetime") != 2026) | (F.month("tpep_pickup_datetime") != 1)).count()
print(f"  Out-of-month rows: {out_of_range:,}")

print("\n[14] Data issues summary")
issues = []
null_counts = {
    c: df.filter(F.col(c).isNull()).count()
    for c in [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "passenger_count",
        "VendorID",
    ]
}
for col, cnt in null_counts.items():
    if cnt > 0:
        issues.append(f"NULL in '{col}': {cnt:,} rows")

neg_dist = df.filter(F.col("trip_distance") <= 0).count()
neg_fare = df.filter(F.col("fare_amount") < 0).count()
zero_pax = df.filter(F.col("passenger_count") == 0).count()
bad_time = df.filter(F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime")).count()

if neg_dist > 0:
    issues.append(f"trip_distance <= 0: {neg_dist:,} rows")
if neg_fare > 0:
    issues.append(f"fare_amount < 0: {neg_fare:,} rows")
if zero_pax > 0:
    issues.append(f"passenger_count = 0: {zero_pax:,} rows")
if bad_time > 0:
    issues.append(f"dropoff <= pickup: {bad_time:,} rows")
if duplicates > 0:
    issues.append(f"duplicate rows: {duplicates:,}")
if out_of_range > 0:
    issues.append(f"outside 01/2026: {out_of_range:,} rows")

if issues:
    for issue in issues:
        print(f"  - {issue}")
else:
    print("  No critical issues found")
print("\nEDA completed.")

spark.stop()
