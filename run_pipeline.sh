#!/bin/bash
# ============================================================
# NHÓM 4 – Chạy toàn bộ pipeline một lệnh
# Sử dụng: bash run_pipeline.sh
# ============================================================

set -e

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; NC='\033[0m'

log_step() { echo -e "\n${BLUE}[STEP $1]${NC} $2"; }
log_ok()   { echo -e "${GREEN}✓${NC} $1"; }
log_err()  { echo -e "${RED}✗ LỖI:${NC} $1"; exit 1; }

echo ""
echo "══════════════════════════════════════════════"
echo "  NYC TAXI BIG DATA PIPELINE – 01/2026"
echo "  Bronze → Silver → Gold"
echo "══════════════════════════════════════════════"

# Load biến môi trường Hadoop/Spark
export HADOOP_HOME="$HOME/hadoop"
export SPARK_HOME="$HOME/spark"
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
export PATH="$PATH:$JAVA_HOME/bin:$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin"

# Kích hoạt virtual environment
if [ -f "$HOME/bigdata-env/bin/activate" ]; then
    source "$HOME/bigdata-env/bin/activate"
    log_ok "Virtual environment đã kích hoạt: $(which python3)"
else
    log_err "Chưa tạo virtual environment. Chạy: bash setup/install_hadoop_spark.sh"
fi

# Đảm bảo các package đã cài trong venv
pip install --quiet requests pyarrow pyspark==3.5.1 pandas matplotlib seaborn 2>/dev/null || true

# Kiểm tra HDFS đang chạy
if ! jps | grep -q NameNode; then
    echo -e "${YELLOW}[WARN]${NC} HDFS chưa chạy – đang khởi động..."
    start-dfs.sh
    sleep 5
fi

log_step "1/5" "Ingestion – Tải data → HDFS Bronze"
python src/ingestion.py || log_err "Ingestion thất bại"
log_ok "Bronze layer sẵn sàng"

log_step "2/5" "EDA – Khám phá dữ liệu Bronze"
python src/01_eda.py || log_err "EDA thất bại"
log_ok "EDA hoàn tất"

log_step "3/5" "Cleaning – Bronze → Silver"
python src/02_data_cleaning.py || log_err "Cleaning thất bại"
log_ok "Silver layer sẵn sàng"

log_step "4/5" "Aggregation – Silver → Gold (time)"
python src/03_aggregation_time.py || log_err "Aggregation time thất bại"
log_ok "Gold (time) sẵn sàng"

log_step "4/5" "Aggregation – Silver → Gold (vendor)"
python src/04_aggregation_vendor.py || log_err "Aggregation vendor thất bại"
log_ok "Gold (vendor) sẵn sàng"

log_step "5/5" "Verify – Kiểm tra kết quả"
python src/05_verify_results.py || log_err "Verify thất bại"

echo ""
echo "══════════════════════════════════════════════"
echo -e "${GREEN}✅ Pipeline hoàn tất!${NC}"
echo ""
echo "  Cấu trúc HDFS:"
hdfs dfs -ls -R /nyc_taxi/ 2>/dev/null || true
echo ""
echo "  Dung lượng:"
hdfs dfs -du -h /nyc_taxi/ 2>/dev/null || true
echo ""
echo "  Web UI:"
echo "    HDFS NameNode : http://localhost:9870"
echo "    Spark UI      : http://localhost:4040"
echo "══════════════════════════════════════════════"
