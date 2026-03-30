"""
============================================================
NHÓM 4 – TV3 TUẦN 3
FILE: src/05_verify_results.py
MỤC ĐÍCH: Kiểm tra kết quả toàn bộ pipeline (Bronze/Silver/Gold)
CHẠY:     python src/05_verify_results.py
============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("NYC_Verify")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from config import HDFS_BASE as BASE

print("\n" + "=" * 60)
print("  KẾT QUẢ PIPELINE – NYC TAXI 01/2026")
print("  Bronze → Silver → Gold")
print("=" * 60)

# Bronze
df_bronze = spark.read.parquet(f"{BASE}/bronze/raw/yellow_tripdata_2026-01.parquet")
bronze_cnt = df_bronze.count()
print(f"\n🥉 Bronze  : {bronze_cnt:,} dòng  (dữ liệu gốc)")

# Silver
df_silver = spark.read.parquet(f"{BASE}/silver/cleaned/yellow_taxi_cleaned_2026_01")
silver_cnt = df_silver.count()
removed    = bronze_cnt - silver_cnt
print(f"🥈 Silver  : {silver_cnt:,} dòng  (đã làm sạch, -{removed/bronze_cnt*100:.1f}%)")

# Gold – by_hour
df_hour = spark.read.parquet(f"{BASE}/gold/aggregated_by_time/by_hour")
print(f"🥇 Gold    : {df_hour.count()} dòng  (by_hour)")

print("\n📈 Top 5 giờ bận nhất:")
df_hour.orderBy("total_trips", ascending=False).show(5)

# Gold – by_date
df_date = spark.read.parquet(f"{BASE}/gold/aggregated_by_time/by_date")
print("📅 Doanh thu 5 ngày đầu tháng:")
df_date.orderBy("pickup_date").show(5)

# Gold – by_vendor
df_vendor = spark.read.parquet(f"{BASE}/gold/aggregated_by_vendor/by_vendor")
print("🚕 So sánh Vendor:")
df_vendor.select("vendor_name", "total_trips", "total_revenue", "avg_fare", "avg_tip_pct").show()

# Gold – by_payment
df_pay = spark.read.parquet(f"{BASE}/gold/aggregated_by_vendor/by_payment")
print("💳 Phương thức thanh toán:")
df_pay.orderBy("total_trips", ascending=False).show()

print("=" * 60)
print("✅ Kiểm tra hoàn tất!\n")
spark.stop()
