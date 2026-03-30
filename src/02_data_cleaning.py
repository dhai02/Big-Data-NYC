"""
============================================================
NHÓM 4 – TV1/TV2/TV3 TUẦN 2
FILE: src/02_data_cleaning.py
MỤC ĐÍCH: Bronze → Silver  (làm sạch + enrich cột)
CHẠY:     python src/02_data_cleaning.py
============================================================
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# ── SparkSession ──────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("NYC_Taxi_Cleaning")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from config import BRONZE, SILVER

# ── Đường dẫn ────────────────────────────────────────────────
BRONZE_PATH = BRONZE
SILVER_PATH = SILVER

# ── Đọc Bronze ───────────────────────────────────────────────
print("\n[1/7] Đọc dữ liệu từ Bronze layer...")
df = spark.read.parquet(BRONZE_PATH)
original_count = df.count()
print(f"  Dòng gốc: {original_count:,}")

# ── Bước 2: Dedup ────────────────────────────────────────────
print("\n[2/7] Loại bỏ dòng trùng lặp...")
df = df.dropDuplicates()
print(f"  Sau dedup: {df.count():,}")

# ── Bước 3: Xử lý NULL ───────────────────────────────────────
print("\n[3/7] Xử lý NULL ở cột quan trọng...")
critical_cols = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "trip_distance", "fare_amount", "total_amount",
    "PULocationID", "DOLocationID", "VendorID", "passenger_count",
]
df = df.dropna(subset=critical_cols)
print(f"  Sau xử lý NULL: {df.count():,}")

# ── Bước 4: Business rules ───────────────────────────────────
print("\n[4/7] Lọc theo business rules...")
df = df.filter(
    (F.col("trip_distance") > 0)    & (F.col("trip_distance") <= 200) &
    (F.col("fare_amount") >= 2.5)   & (F.col("fare_amount") <= 500)   &
    (F.col("total_amount") > 0)     & (F.col("total_amount") <= 1000) &
    (F.col("passenger_count") >= 1) & (F.col("passenger_count") <= 6) &
    (F.col("tip_amount") >= 0)      &
    (F.col("VendorID").isin([1, 2]))
)
print(f"  Sau business rules: {df.count():,}")

# ── Bước 5: Lọc thời gian ────────────────────────────────────
print("\n[5/7] Lọc thời gian hợp lệ (tháng 01/2026)...")
df = df.filter(
    (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")) &
    ((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) >= 60) &
    ((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) <= 21600) &
    (F.year("tpep_pickup_datetime") == 2026) &
    (F.month("tpep_pickup_datetime") == 1)
)
print(f"  Sau lọc thời gian: {df.count():,}")

# ── Bước 6: Outliers IQR ─────────────────────────────────────
print("\n[6/7] Loại bỏ outliers (IQR × 3.0)...")

def remove_outliers_iqr(df: DataFrame, col_name: str, multiplier: float = 3.0) -> DataFrame:
    q1, q3 = df.approxQuantile(col_name, [0.25, 0.75], 0.01)
    iqr = q3 - q1
    return df.filter(
        (F.col(col_name) >= q1 - multiplier * iqr) &
        (F.col(col_name) <= q3 + multiplier * iqr)
    )

for col in ["trip_distance", "fare_amount", "total_amount"]:
    df = remove_outliers_iqr(df, col)

print(f"  Sau outlier removal: {df.count():,}")

# ── Bước 7: Enrich – thêm cột tính toán ─────────────────────
print("\n[7/7] Thêm cột tính toán (enrich)...")
df = (
    df
    .withColumn("trip_duration_min",
        F.round(
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60,
            2
        )
    )
    .withColumn("pickup_hour",      F.hour("tpep_pickup_datetime"))
    .withColumn("pickup_date",      F.to_date("tpep_pickup_datetime"))
    .withColumn("pickup_dayofweek", F.dayofweek("tpep_pickup_datetime"))  # 1=Sun
    .withColumn("pickup_week",      F.weekofyear("tpep_pickup_datetime"))
    .withColumn("tip_percentage",
        F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2)
    )
    .withColumn("speed_mph",
        F.round(F.col("trip_distance") / (F.col("trip_duration_min") / 60), 2)
    )
)

# ── Lưu Silver ───────────────────────────────────────────────
print(f"\n  Lưu Silver layer → {SILVER_PATH}")
df.write.mode("overwrite").parquet(SILVER_PATH)

# ── Báo cáo ──────────────────────────────────────────────────
cleaned_count = df.count()
removed       = original_count - cleaned_count

print("\n" + "=" * 60)
print("  BÁO CÁO LÀM SẠCH – Bronze → Silver")
print("=" * 60)
print(f"  Dòng gốc (Bronze) : {original_count:,}")
print(f"  Dòng còn lại      : {cleaned_count:,}")
print(f"  Đã xóa            : {removed:,}  ({removed/original_count*100:.1f}%)")
print(f"  Silver path       : {SILVER_PATH}")
print("=" * 60)
print("\n✅ Cleaning hoàn tất!")
print("   Bước tiếp theo: python src/03_aggregation_time.py\n")

spark.stop()
