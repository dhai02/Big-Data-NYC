"""
============================================================
NHÓM 4 – TV1 TUẦN 1
FILE: src/ingestion.py
MỤC ĐÍCH: Tải NYC Taxi Parquet về local → đẩy lên HDFS Bronze layer
CHẠY:     python src/ingestion.py
============================================================
"""

import os
import sys
import logging
import subprocess
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
import sys
sys.path.insert(0, os.path.dirname(__file__))
from config import HDFS_BASE

DATASET_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet"
)
FILENAME        = "yellow_tripdata_2026-01.parquet"
LOCAL_RAW_DIR   = Path("data/raw")
LOCAL_FILE      = LOCAL_RAW_DIR / FILENAME

# HDFS – Bronze layer (dữ liệu gốc, không chỉnh sửa)
HDFS_BRONZE     = f"{HDFS_BASE}/bronze/raw"
HDFS_FILE       = f"{HDFS_BRONZE}/{FILENAME}"

# ── Helpers ───────────────────────────────────────────────────

def run_hdfs(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Chạy lệnh hdfs dfs và trả về kết quả."""
    cmd = ["hdfs", "dfs"] + args
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def hdfs_file_exists(path: str) -> bool:
    result = run_hdfs(["-test", "-e", path], check=False)
    return result.returncode == 0


def hdfs_mkdir(path: str) -> None:
    run_hdfs(["-mkdir", "-p", path])
    log.info(f"HDFS mkdir: {path}")


def download_parquet(url: str, dest: Path) -> bool:
    """Tải file Parquet về local với progress log."""
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
            chunk_size = 1024 * 1024  # 1 MB

            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total:
                            pct = downloaded / total * 100
                            print(f"\r  Tiến độ: {pct:.1f}%  ({downloaded/1_048_576:.0f}/{total/1_048_576:.0f} MB)", end="")
            print()  # newline sau progress

        size_mb = dest.stat().st_size / 1_048_576
        log.info(f"Tải xong: {dest}  ({size_mb:.1f} MB)")
        return True

    except Exception as e:
        log.error(f"Lỗi khi tải: {e}")
        if dest.exists():
            dest.unlink()  # Xóa file lỗi
        return False


def upload_to_hdfs(local_path: Path, hdfs_path: str) -> bool:
    """Upload file lên HDFS Bronze layer."""
    if hdfs_file_exists(hdfs_path):
        # Kiểm tra kích thước để xác nhận file không bị corrupt
        result = run_hdfs(["-du", "-h", hdfs_path], check=False)
        log.info(f"File đã có trên HDFS – bỏ qua upload: {hdfs_path}")
        log.info(f"  Kích thước HDFS: {result.stdout.strip()}")
        return True

    log.info(f"Đang upload lên HDFS: {local_path} → {hdfs_path}")
    try:
        result = run_hdfs(["-put", str(local_path), hdfs_path])
        log.info(f"Upload thành công: {hdfs_path}")
        return True
    except subprocess.CalledProcessError as e:
        log.error(f"Lỗi upload HDFS: {e.stderr}")
        return False


def verify_hdfs(hdfs_path: str) -> None:
    """In thông tin file trên HDFS để xác nhận."""
    log.info("── Xác nhận file trên HDFS ──────────────────────")
    result = run_hdfs(["-ls", "-h", os.path.dirname(hdfs_path)], check=False)
    print(result.stdout)

    result = run_hdfs(["-du", "-h", hdfs_path], check=False)
    log.info(f"Dung lượng: {result.stdout.strip()}")

    # Đọc nhanh bằng PySpark để xác nhận schema
    log.info("── Xác nhận schema bằng PySpark ─────────────────")
    try:
        from pyspark.sql import SparkSession
        spark = (
            SparkSession.builder
            .appName("NYC_Taxi_Verify_Ingestion")
            .master("local[2]")
            .config("spark.driver.memory", "2g")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")

        df = spark.read.parquet(hdfs_path)
        row_count = df.count()

        print(f"\n  Tổng số dòng : {row_count:,}")
        print(f"  Số cột       : {len(df.columns)}")
        print(f"  Cột          : {df.columns}\n")
        df.printSchema()
        print("\n  5 dòng đầu:")
        df.show(5, truncate=True)

        spark.stop()
        log.info(f"Xác nhận thành công – {row_count:,} dòng trên HDFS Bronze")
    except Exception as e:
        log.warning(f"Không thể xác nhận bằng PySpark (có thể chưa cài): {e}")


# ── Main ──────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("  NYC TAXI INGESTION – Bronze Layer")
    log.info("  Dataset: yellow_tripdata_2026-01.parquet")
    log.info("=" * 60)

    # 1. Đảm bảo thư mục HDFS tồn tại
    log.info("[1/3] Chuẩn bị thư mục HDFS Bronze...")
    hdfs_mkdir(HDFS_BRONZE)

    # 2. Tải file về local
    log.info("[2/3] Tải dữ liệu về local...")
    ok = download_parquet(DATASET_URL, LOCAL_FILE)
    if not ok:
        log.error("Tải dữ liệu thất bại. Dừng lại.")
        sys.exit(1)

    # 3. Upload lên HDFS Bronze
    log.info("[3/3] Upload lên HDFS Bronze layer...")
    ok = upload_to_hdfs(LOCAL_FILE, HDFS_FILE)
    if not ok:
        log.error("Upload HDFS thất bại. Dừng lại.")
        sys.exit(1)

    # 4. Xác nhận
    verify_hdfs(HDFS_FILE)

    log.info("=" * 60)
    log.info("✅ Ingestion hoàn tất!")
    log.info(f"   Bronze HDFS : {HDFS_FILE}")
    log.info(f"   Local cache : {LOCAL_FILE}")
    log.info("")
    log.info("   Bước tiếp theo:")
    log.info("     python src/01_eda.py          # EDA từ Bronze")
    log.info("     python src/02_data_cleaning.py # Bronze → Silver")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
