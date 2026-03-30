"""
Group 4 - Week 2 Cleaning
FILE: src/02_data_cleaning.py
Goal: Bronze -> Silver (cleaning + feature engineering)
Run: python src/02_data_cleaning.py
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(__file__))
from config import BRONZE, SILVER
from cleaning_utils import apply_iqr_filters, evaluate_iqr_candidates, pick_best_multiplier


BRONZE_PATH = BRONZE
SILVER_PATH = SILVER


spark = (
    SparkSession.builder.appName("NYC_Taxi_Cleaning")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


print("\n[1/7] Read data from Bronze layer...")
df = spark.read.parquet(BRONZE_PATH)
original_count = df.count()
print(f"  Original rows: {original_count:,}")


print("\n[2/7] Drop duplicate rows...")
df = df.dropDuplicates()
print(f"  After dedup: {df.count():,}")


print("\n[3/7] Drop NULLs in critical columns...")
critical_cols = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "PULocationID",
    "DOLocationID",
    "VendorID",
    "passenger_count",
]
df = df.dropna(subset=critical_cols)
print(f"  After null handling: {df.count():,}")


print("\n[4/7] Apply business rules...")
df = df.filter(
    (F.col("trip_distance") > 0)
    & (F.col("trip_distance") <= 200)
    & (F.col("fare_amount") >= 2.5)
    & (F.col("fare_amount") <= 500)
    & (F.col("total_amount") > 0)
    & (F.col("total_amount") <= 1000)
    & (F.col("passenger_count") >= 1)
    & (F.col("passenger_count") <= 6)
    & (F.col("tip_amount") >= 0)
    & (F.col("VendorID").isin([1, 2]))
)
print(f"  After business rules: {df.count():,}")


print("\n[5/7] Filter valid time windows and target month...")
df = df.filter(
    (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    & ((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) >= 60)
    & ((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) <= 21600)
    & (F.year("tpep_pickup_datetime") == 2026)
    & (F.month("tpep_pickup_datetime") == 1)
)
print(f"  After time filtering: {df.count():,}")


print("\n[6/7] Outlier detection tuning (IQR with multiple multipliers)...")
iqr_cols = ["trip_distance", "fare_amount", "total_amount"]
iqr_multipliers = [1.5, 2.0, 2.5, 3.0]
min_retain_ratio = float(os.environ.get("IQR_MIN_RETAIN_RATIO", "0.90"))

candidate_results = evaluate_iqr_candidates(df, iqr_cols, iqr_multipliers)
selected = pick_best_multiplier(candidate_results, min_retain_ratio=min_retain_ratio)
best_multiplier = selected["multiplier"]

for row in candidate_results:
    print(
        f"  m={row['multiplier']:.1f} | rows_after={int(row['rows_after']):,} | "
        f"retain={row['retain_ratio'] * 100:.2f}% | removed={row['remove_ratio'] * 100:.2f}%"
    )

print(f"  Selected multiplier: {best_multiplier}")
df = apply_iqr_filters(df, iqr_cols, multiplier=best_multiplier)
print(f"  After outlier removal: {df.count():,}")


print("\n[7/7] Feature engineering...")
df = (
    df.withColumn(
        "trip_duration_min",
        F.round(
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60,
            2,
        ),
    )
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    .withColumn("pickup_dayofweek", F.dayofweek("tpep_pickup_datetime"))
    .withColumn("pickup_week", F.weekofyear("tpep_pickup_datetime"))
    .withColumn("tip_percentage", F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2))
    .withColumn("speed_mph", F.round(F.col("trip_distance") / (F.col("trip_duration_min") / 60), 2))
)

print(f"\nWrite Silver layer -> {SILVER_PATH}")
df.write.mode("overwrite").parquet(SILVER_PATH)


cleaned_count = df.count()
removed = original_count - cleaned_count

print("\n" + "=" * 60)
print("CLEANING REPORT - Bronze -> Silver")
print("=" * 60)
print(f"Original rows : {original_count:,}")
print(f"Cleaned rows  : {cleaned_count:,}")
print(f"Rows removed  : {removed:,} ({removed / original_count * 100:.1f}%)")
print(f"Silver path   : {SILVER_PATH}")
print("=" * 60)
print("\nCleaning completed.")

spark.stop()
