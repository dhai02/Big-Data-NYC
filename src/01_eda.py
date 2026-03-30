"""
============================================================
NHÓM 4 – TV1/TV3/TV4 TUẦN 1
FILE: src/01_eda.py
MỤC ĐÍCH: Exploratory Data Analysis từ Bronze layer
CHẠY:     python src/01_eda.py
============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── SparkSession ──────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("NYC_Taxi_EDA")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from config import BRONZE, SILVER, GOLD_TIME, GOLD_VENDOR

BRONZE_PATH = BRONZE
SILVER_PATH = SILVER

print("\n" + "=" * 60)
print("  EDA – NYC Yellow Taxi 01/2026  (Bronze Layer)")
print("=" * 60)

# ── 1. Đọc dữ liệu ───────────────────────────────────────────
print("\n[1] Đọc dữ liệu từ HDFS Bronze...")
df = spark.read.parquet(BRONZE_PATH)
total_rows = df.count()
print(f"  Tổng số dòng : {total_rows:,}")
print(f"  Số cột       : {len(df.columns)}")

# ── 2. Schema ────────────────────────────────────────────────
print("\n[2] Schema:")
df.printSchema()

# ── 3. Mẫu dữ liệu ───────────────────────────────────────────
print("\n[3] 5 dòng đầu:")
df.show(5, truncate=False)

# ── 4. Thống kê mô tả ────────────────────────────────────────
print("\n[4] Thống kê mô tả (các cột số):")
df.describe(
    "passenger_count", "trip_distance",
    "fare_amount", "tip_amount", "total_amount"
).show()

# ── 5. Kiểm tra NULL ─────────────────────────────────────────
print("\n[5] Số giá trị NULL theo cột:")
null_df = df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df.columns
])
null_df.show(truncate=False)

# ── 6. Kiểm tra trùng lặp ────────────────────────────────────
print("\n[6] Kiểm tra dòng trùng lặp:")
distinct_rows = df.distinct().count()
duplicates    = total_rows - distinct_rows
print(f"  Tổng dòng    : {total_rows:,}")
print(f"  Dòng duy nhất: {distinct_rows:,}")
print(f"  Trùng lặp    : {duplicates:,}  ({duplicates/total_rows*100:.2f}%)")

# ── 7. Phân phối theo ngày ───────────────────────────────────
print("\n[7] Số chuyến theo ngày (tháng 01/2026):")
df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime")) \
  .groupBy("pickup_date") \
  .count() \
  .orderBy("pickup_date") \
  .show(31)

# ── 8. Phân phối theo giờ ────────────────────────────────────
print("\n[8] Số chuyến theo giờ trong ngày:")
df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime")) \
  .groupBy("pickup_hour") \
  .count() \
  .orderBy("pickup_hour") \
  .show(24)

# ── 9. Phân phối theo Vendor ─────────────────────────────────
print("\n[9] Phân phối theo VendorID:")
df.groupBy("VendorID").count() \
  .withColumn("pct", F.round(F.col("count") / total_rows * 100, 2)) \
  .orderBy("VendorID") \
  .show()

# ── 10. Khoảng giá trị cột số quan trọng ─────────────────────
print("\n[10] Khoảng giá trị các cột số:")
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
    F.min("passenger_count").alias("pax_min"),
    F.max("passenger_count").alias("pax_max"),
).show()

# ── 11. Phân phối payment_type ───────────────────────────────
print("\n[11] Phân phối phương thức thanh toán:")
df.groupBy("payment_type").count() \
  .withColumn("pct", F.round(F.col("count") / total_rows * 100, 2)) \
  .orderBy("payment_type") \
  .show()

# ── 12. Phát hiện dữ liệu ngoài tháng 01/2026 ────────────────
print("\n[12] Dữ liệu ngoài tháng 01/2026:")
out_of_range = df.filter(
    (F.year("tpep_pickup_datetime") != 2026) |
    (F.month("tpep_pickup_datetime") != 1)
).count()
print(f"  Dòng ngoài tháng 01/2026: {out_of_range:,}")

# ── 13. Tóm tắt vấn đề phát hiện ────────────────────────────
print("\n" + "=" * 60)
print("  TÓM TẮT VẤN ĐỀ PHÁT HIỆN (cần xử lý ở Tuần 2)")
print("=" * 60)

issues = []

# Null check
null_counts = {c: df.filter(F.col(c).isNull()).count() for c in [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "trip_distance", "fare_amount", "passenger_count", "VendorID"
]}
for col, cnt in null_counts.items():
    if cnt > 0:
        issues.append(f"  NULL trong '{col}': {cnt:,} dòng")

# Business rule violations
neg_dist  = df.filter(F.col("trip_distance") <= 0).count()
neg_fare  = df.filter(F.col("fare_amount") < 0).count()
zero_pax  = df.filter(F.col("passenger_count") == 0).count()
bad_time  = df.filter(
    F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime")
).count()

if neg_dist  > 0: issues.append(f"  trip_distance <= 0   : {neg_dist:,} dòng")
if neg_fare  > 0: issues.append(f"  fare_amount < 0      : {neg_fare:,} dòng")
if zero_pax  > 0: issues.append(f"  passenger_count = 0  : {zero_pax:,} dòng")
if bad_time  > 0: issues.append(f"  dropoff <= pickup    : {bad_time:,} dòng")
if duplicates > 0: issues.append(f"  Dòng trùng lặp       : {duplicates:,} dòng")
if out_of_range > 0: issues.append(f"  Ngoài tháng 01/2026  : {out_of_range:,} dòng")

if issues:
    for issue in issues:
        print(issue)
else:
    print("  Không phát hiện vấn đề nghiêm trọng")

print("=" * 60)
print("\n✅ EDA hoàn tất! Xem kết quả chi tiết ở trên.")
print("   Bước tiếp theo: python src/02_data_cleaning.py\n")

spark.stop()
