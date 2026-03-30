"""
Cấu hình đường dẫn HDFS – tự động detect Docker hay local.
Import file này thay vì hardcode hdfs://localhost:9000 trong từng script.
"""
import os

# Nếu chạy trong Docker, biến môi trường HDFS_NAMENODE=namenode
# Nếu chạy local (WSL), mặc định là localhost
_namenode = os.environ.get("HDFS_NAMENODE", "localhost")
HDFS_BASE = f"hdfs://{_namenode}:9000/nyc_taxi"

# Các layer
BRONZE = f"{HDFS_BASE}/bronze/raw/yellow_tripdata_2026-01.parquet"
SILVER = f"{HDFS_BASE}/silver/cleaned/yellow_taxi_cleaned_2026_01"
GOLD_TIME   = f"{HDFS_BASE}/gold/aggregated_by_time"
GOLD_VENDOR = f"{HDFS_BASE}/gold/aggregated_by_vendor"
