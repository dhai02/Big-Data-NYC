"""
============================================================
NHÓM 4 – TV2 TUẦN 3
FILE: src/04_aggregation_vendor.py
MỤC ĐÍCH: Silver → Gold  (tổng hợp theo vendor, khu vực, payment)
CHẠY:     python src/04_aggregation_vendor.py
============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("NYC_Taxi_Aggregation_Vendor")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from config import SILVER, GOLD_VENDOR as GOLD_VENDOR_PATH

SILVER_PATH = SILVER
GOLD_VENDOR = GOLD_VENDOR_PATH

df = spark.read.parquet(SILVER_PATH)

# ── Theo Vendor ───────────────────────────────────────────────
print("\nTổng hợp theo Vendor...")
vendor_agg = df.groupBy("VendorID").agg(
    F.count("*").alias("total_trips"),
    F.sum("passenger_count").alias("total_passengers"),
    F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    F.round(F.avg("total_amount"), 2).alias("avg_revenue_per_trip"),
    F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    F.round(F.sum("tip_amount"), 2).alias("total_tips"),
    F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
    F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
    F.round(F.avg("speed_mph"), 2).alias("avg_speed_mph"),
).withColumn("vendor_name",
    F.when(F.col("VendorID") == 1, "Creative Mobile")
     .when(F.col("VendorID") == 2, "VeriFone")
     .otherwise("Unknown")
).orderBy("VendorID")
vendor_agg.write.mode("overwrite").parquet(f"{GOLD_VENDOR}/by_vendor")
vendor_agg.show(truncate=False)

# ── Theo Payment Type ─────────────────────────────────────────
print("\nTổng hợp theo Payment Type...")
payment_agg = df.groupBy("payment_type").agg(
    F.count("*").alias("total_trips"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
    F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
).withColumn("payment_name",
    F.when(F.col("payment_type") == 1, "Credit Card")
     .when(F.col("payment_type") == 2, "Cash")
     .when(F.col("payment_type") == 3, "No Charge")
     .when(F.col("payment_type") == 4, "Dispute")
     .otherwise("Other")
).orderBy("total_trips", ascending=False)
payment_agg.write.mode("overwrite").parquet(f"{GOLD_VENDOR}/by_payment")
payment_agg.show()

# ── Theo Pickup Location Top 20 ───────────────────────────────
print("\nTổng hợp theo Pickup Location (Top 20)...")
pickup_loc = df.groupBy("PULocationID").agg(
    F.count("*").alias("total_trips"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
    F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
).orderBy("total_trips", ascending=False).limit(20)
pickup_loc.write.mode("overwrite").parquet(f"{GOLD_VENDOR}/by_pickup_location_top20")
pickup_loc.show(20)

# ── Theo cặp Route Top 50 ─────────────────────────────────────
print("\nTổng hợp theo cặp Pickup-Dropoff (Top 50)...")
route_agg = df.groupBy("PULocationID", "DOLocationID").agg(
    F.count("*").alias("total_trips"),
    F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
    F.round(F.avg("total_amount"), 2).alias("avg_revenue"),
    F.round(F.avg("trip_duration_min"), 2).alias("avg_duration"),
).orderBy("total_trips", ascending=False).limit(50)
route_agg.write.mode("overwrite").parquet(f"{GOLD_VENDOR}/by_route_top50")

print("\n✅ Aggregation by vendor hoàn tất!")
spark.stop()
