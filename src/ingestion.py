"""
============================================================
NHÓM 4 – TV1 TUẦN 1
FILE: src/ingestion.py
MỤC ĐÍCH: Tải NYC Taxi Parquet về local → đẩy lên HDFS Bronze layer
CHẠY:     python3 src/ingestion.py
============================================================
"""

from typing import List
import os
import sys
import logging
import requests
from pathlib import Path

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("ingestion.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)

# ── Cấu hình ──────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))
from config import HDFS_BASE, BRONZE

DATASET_URL   = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet"
FILENAME      = "yellow_tripdata_2026-01.parquet"
LOCAL_RAW_DIR = Path("/app/data/raw")
LOCAL_FILE    = LOCAL_RAW_DIR / FILENAME

HDFS_BRONZE   = f"{HDFS_BASE}/bronze/raw"
HDFS_FILE     = f"{HDFS_BRONZE}/{FILENAME}"


# ── Download ──────────────────────────────────────────────────

def download_parquet(url: str, dest: Path) -> bool:
    if dest.exists():
        size_mb = dest.stat().st_size / 1_048_576
        log.info(f"File đã tồn tại local ({size_mb:.1f} MB) – bỏ qua tải về: {dest}")
        return True

    dest.parent.mkdir(parents=True, exist_ok=True)
    log.info(f"Bắt đầu tải: {url}")

    try:
        with requests.get(url, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            downloaded = 0
            chunk_size = 1024 * 1024

            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total:
                            pct = downloaded / total * 100
                            print(f"\r  Tiến độ: {pct:.1f}%  ({downloaded/1_048_576:.0f}/{total/1_048_576:.0f} MB)", end="")
            print()

        size_mb = dest.stat().st_size / 1_048_576
        log.info(f"Tải xong: {dest}  ({size_mb:.1f} MB)")
        return True

    except Exception as e:
        log.error(f"Lỗi khi tải: {e}")
        if dest.exists():
            dest.unlink()
        return False


# ── Upload lên HDFS qua PySpark ───────────────────────────────

def upload_to_hdfs(local_path: Path, hdfs_path: str) -> bool:
    log.info(f"Đang upload lên HDFS: {local_path} → {hdfs_path}")
    try:
        from pyspark.sql import SparkSession
        spark = (
            SparkSession.builder
            .appName("NYC_Taxi_Ingestion")
            .master("local[2]")
            .config("spark.driver.memory", "2g")
            .config("spark.hadoop.fs.defaultFS", f"hdfs://namenode:9000")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")

        # Đọc local (dùng file:// để tránh bị resolve thành HDFS)
        df = spark.read.parquet(f"file://{local_path}")
        row_count = df.count()

        df.write.mode("overwrite").parquet(hdfs_path)

        log.info(f"Upload thành công: {row_count:,} dòng → {hdfs_path}")

        # Xác nhận
        df_check = spark.read.parquet(hdfs_path)
        log.info(f"Xác nhận HDFS: {df_check.count():,} dòng, {len(df_check.columns)} cột")
        print(f"\n  Schema:")
        df_check.printSchema()
        print(f"\n  5 dòng đầu:")
        df_check.show(5, truncate=True)

        spark.stop()
        return True

    except Exception as e:
        log.error(f"Lỗi upload HDFS: {e}")
        return False


# ── Main ──────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("  NYC TAXI INGESTION – Bronze Layer")
    log.info("  Dataset: yellow_tripdata_2026-01.parquet")
    log.info("=" * 60)

    # 1. Tải file về local
    log.info("[1/2] Tải dữ liệu về local...")
    ok = download_parquet(DATASET_URL, LOCAL_FILE)
    if not ok:
        log.error("Tải dữ liệu thất bại.")
        sys.exit(1)

    # 2. Upload lên HDFS Bronze
    log.info("[2/2] Upload lên HDFS Bronze layer...")
    ok = upload_to_hdfs(LOCAL_FILE, HDFS_FILE)
    if not ok:
        log.error("Upload HDFS thất bại.")
        sys.exit(1)

    log.info("=" * 60)
    log.info("✅ Ingestion hoàn tất!")
    log.info(f"   Bronze HDFS : {HDFS_FILE}")
    log.info(f"   Local cache : {LOCAL_FILE}")
    log.info("")
    log.info("   Bước tiếp theo:")
    log.info("     docker exec spark python3 src/01_eda.py")
    log.info("     docker exec spark python3 src/02_data_cleaning.py")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
