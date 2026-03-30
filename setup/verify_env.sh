#!/bin/bash
# ============================================================
# NHÓM 4 – Script kiểm tra môi trường (TV2 có thể dùng)
# Sử dụng: bash setup/verify_env.sh
# ============================================================

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

PASS=0; FAIL=0

check() {
    local label="$1"; local cmd="$2"
    if eval "$cmd" &>/dev/null; then
        echo -e "  ${GREEN}✓${NC}  $label"
        ((PASS++))
    else
        echo -e "  ${RED}✗${NC}  $label"
        ((FAIL++))
    fi
}

echo ""
echo "══════════════════════════════════════"
echo "  KIỂM TRA MÔI TRƯỜNG – NHÓM 4"
echo "══════════════════════════════════════"
echo ""

echo "[ Phần mềm ]"
check "Java 11"          "java -version 2>&1 | grep -q '11'"
check "Hadoop 3.3.6"     "hadoop version 2>/dev/null | grep -q '3.3.6'"
check "Spark 3.5.1"      "spark-submit --version 2>&1 | grep -q '3.5.1'"
check "Python 3.10+"     "python3 -c 'import sys; assert sys.version_info >= (3,10)'"
check "PySpark"          "python3 -c 'import pyspark'"
check "PyArrow"          "python3 -c 'import pyarrow'"
check "Pandas"           "python3 -c 'import pandas'"

echo ""
echo "[ HDFS ]"
check "NameNode running" "jps | grep -q NameNode"
check "DataNode running" "jps | grep -q DataNode"
check "HDFS accessible"  "hdfs dfs -ls / &>/dev/null"
check "Bronze layer"     "hdfs dfs -test -d /nyc_taxi/bronze/raw"
check "Silver layer"     "hdfs dfs -test -d /nyc_taxi/silver/cleaned"
check "Gold layer"       "hdfs dfs -test -d /nyc_taxi/gold"

echo ""
echo "[ Dataset ]"
check "Raw parquet trên HDFS" \
    "hdfs dfs -test -e /nyc_taxi/bronze/raw/yellow_tripdata_2026-01.parquet"

echo ""
echo "══════════════════════════════════════"
echo -e "  Kết quả: ${GREEN}${PASS} pass${NC} / ${RED}${FAIL} fail${NC}"
echo "══════════════════════════════════════"
echo ""

if [ $FAIL -gt 0 ]; then
    echo "  Gợi ý: chạy lại  bash setup/install_hadoop_spark.sh"
    echo "         hoặc xem  docs/troubleshooting.md"
    echo ""
    exit 1
fi
