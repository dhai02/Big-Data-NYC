"""
============================================================
NHÓM 4 – TV1 TUẦN 3
FILE: src/03_aggregation_time.py
MỤC ĐÍCH: Silver → Gold  (tổng hợp theo thời gian)
CHẠY:     python src/03_aggregation_time.py
============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("NYC_Taxi_Aggregation_Time")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from config import SILVER, GOLD_TIME as GOLD_TIME_PATH

SILVER_PATH = SILVER
GOLD_TIME   = GOLD_TIME_PATH

df = spark.read.parquet(SILVER_PATH)
print(f"\nĐọc Silver: {df.count():,} dòng")

# ── Theo giờ ─────────────────────────────────────────────────
print("\nTổng hợp theo giờ...")
hourly = df.groupBy("pickup_hour").agg(
    F.count("*").alias("total_trips"),
    F.sum("passenger_count").alias("total_passengers"),
    F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
    F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
    F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
    F.round(F.avg("speed_mph"), 2).alias("avg_speed_mph"),
).orderBy("pickup_hour")
hourly.write.mode("overwrite").parquet(f"{GOLD_TIME}/by_hour")
hourly.show(24)

# ── Theo ngày trong tuần ─────────────────────────────────────
print("\nTổng hợp theo ngày trong tuần...")
daily_dow = df.groupBy("pickup_dayofweek").agg(
    F.count("*").alias("total_trips"),
    F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
).withColumn("day_name",
    F.when(F.col("pickup_dayofweek") == 1, "Sunday")
     .when(F.col("pickup_dayofweek") == 2, "Monday")
     .when(F.col("pickup_dayofweek") == 3, "Tuesday")
     .when(F.col("pickup_dayofweek") == 4, "Wednesday")
     .when(F.col("pickup_dayofweek") == 5, "Thursday")
     .when(F.col("pickup_dayofweek") == 6, "Friday")
     .otherwise("Saturday")
).orderBy("pickup_dayofweek")
daily_dow.write.mode("overwrite").parquet(f"{GOLD_TIME}/by_dayofweek")
daily_dow.show()

# ── Theo ngày trong tháng ────────────────────────────────────
print("\nTổng hợp theo ngày trong tháng...")
daily = df.groupBy("pickup_date").agg(
    F.count("*").alias("total_trips"),
    F.sum("passenger_count").alias("total_passengers"),
    F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    F.round(F.sum("fare_amount"), 2).alias("total_fare"),
    F.round(F.sum("tip_amount"), 2).alias("total_tips"),
    F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
    F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
).orderBy("pickup_date")
daily.write.mode("overwrite").parquet(f"{GOLD_TIME}/by_date")
daily.show(31)

# ── Heatmap giờ × ngày ───────────────────────────────────────
print("\nTổng hợp heatmap giờ × ngày trong tuần...")
heatmap = df.groupBy("pickup_dayofweek", "pickup_hour").agg(
    F.count("*").alias("total_trips"),
    F.round(F.avg("total_amount"), 2).alias("avg_revenue"),
).orderBy("pickup_dayofweek", "pickup_hour")
heatmap.write.mode("overwrite").parquet(f"{GOLD_TIME}/by_dow_hour")

print("\n✅ Aggregation by time hoàn tất!")
spark.stop()
