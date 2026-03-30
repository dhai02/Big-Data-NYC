#!/bin/bash
# ============================================================
# NHÓM 4 – TV1 TUẦN 1
# Script cài đặt tự động: Java 11 + Hadoop 3.3.6 + Spark 3.5.1
# Chạy trên Ubuntu 22.04 LTS / WSL2
# Sử dụng: bash setup/install_hadoop_spark.sh
# ============================================================

set -e  # Dừng ngay nếu có lỗi

# ── Màu sắc terminal ──────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; NC='\033[0m'

log_info()    { echo -e "${GREEN}[INFO]${NC}  $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; }
log_section() { echo -e "\n${BLUE}══════════════════════════════════════${NC}"; echo -e "${BLUE}  $1${NC}"; echo -e "${BLUE}══════════════════════════════════════${NC}"; }

# ── Biến cấu hình ─────────────────────────────────────────────
HADOOP_VERSION="3.3.6"
SPARK_VERSION="4.1.1"
JAVA_HOME_PATH="/usr/lib/jvm/java-11-openjdk-amd64"
HADOOP_INSTALL_DIR="$HOME/hadoop"
SPARK_INSTALL_DIR="$HOME/spark"
VENV_DIR="$HOME/bigdata-env"

# ── BƯỚC 1: Cập nhật hệ thống & cài Java 11 ──────────────────
log_section "BƯỚC 1: Cài đặt Java 11"

sudo apt-get update -qq
sudo apt-get install -y openjdk-11-jdk wget curl ssh pdsh python3-pip python3-venv -qq

java -version 2>&1 | head -1
log_info "Java 11 đã cài đặt thành công"

# ── BƯỚC 2: Cấu hình SSH localhost (Hadoop yêu cầu) ──────────
log_section "BƯỚC 2: Cấu hình SSH"

if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa -q
    log_info "Đã tạo SSH key"
fi

cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys 2>/dev/null || true
chmod 0600 ~/.ssh/authorized_keys
# Thêm localhost vào known_hosts để tránh hỏi xác nhận
ssh-keyscan -H localhost >> ~/.ssh/known_hosts 2>/dev/null || true
ssh-keyscan -H 0.0.0.0   >> ~/.ssh/known_hosts 2>/dev/null || true
log_info "SSH localhost đã cấu hình"

# ── BƯỚC 3: Cài đặt Hadoop 3.3.6 ─────────────────────────────
log_section "BƯỚC 3: Cài đặt Apache Hadoop $HADOOP_VERSION"

if [ -d "$HADOOP_INSTALL_DIR" ]; then
    log_warn "Hadoop đã tồn tại tại $HADOOP_INSTALL_DIR – bỏ qua"
else
    HADOOP_TGZ="hadoop-${HADOOP_VERSION}.tar.gz"

    # Tìm file .tar.gz đã tải thủ công (ưu tiên theo thứ tự)
    HADOOP_TGZ_PATH=""
    for candidate in \
        "$(dirname "$0")/../${HADOOP_TGZ}" \
        "$HOME/${HADOOP_TGZ}" \
        "/tmp/${HADOOP_TGZ}"
    do
        if [ -f "$candidate" ]; then
            HADOOP_TGZ_PATH="$(realpath "$candidate")"
            break
        fi
    done

    if [ -n "$HADOOP_TGZ_PATH" ]; then
        log_info "Dùng file có sẵn: $HADOOP_TGZ_PATH"
    else
        # Không tìm thấy → tải về
        HADOOP_URL="https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_TGZ}"
        log_info "Không tìm thấy file local – đang tải từ $HADOOP_URL ..."
        wget -q --show-progress -O "/tmp/${HADOOP_TGZ}" "$HADOOP_URL"
        HADOOP_TGZ_PATH="/tmp/${HADOOP_TGZ}"
    fi

    log_info "Đang giải nén Hadoop..."
    tar -xzf "$HADOOP_TGZ_PATH" -C "$HOME"
    mv "$HOME/hadoop-${HADOOP_VERSION}" "$HADOOP_INSTALL_DIR"
    log_info "Hadoop đã giải nén tại $HADOOP_INSTALL_DIR"
fi

# Cấu hình hadoop-env.sh
sed -i "s|# export JAVA_HOME=.*|export JAVA_HOME=${JAVA_HOME_PATH}|" \
    "$HADOOP_INSTALL_DIR/etc/hadoop/hadoop-env.sh"

# core-site.xml
cat > "$HADOOP_INSTALL_DIR/etc/hadoop/core-site.xml" << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/tmp/hadoopdata/tmp</value>
    </property>
</configuration>
EOF

# hdfs-site.xml
cat > "$HADOOP_INSTALL_DIR/etc/hadoop/hdfs-site.xml" << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///tmp/hadoopdata/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///tmp/hadoopdata/datanode</value>
    </property>
    <!-- Bật Web UI trên tất cả interfaces -->
    <property>
        <name>dfs.namenode.http-address</name>
        <value>0.0.0.0:9870</value>
    </property>
    <property>
        <name>dfs.datanode.http.address</name>
        <value>0.0.0.0:9864</value>
    </property>
</configuration>
EOF

# mapred-site.xml
cat > "$HADOOP_INSTALL_DIR/etc/hadoop/mapred-site.xml" << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
</configuration>
EOF

# yarn-site.xml
cat > "$HADOOP_INSTALL_DIR/etc/hadoop/yarn-site.xml" << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.resourcemanager.webapp.address</name>
        <value>0.0.0.0:8088</value>
    </property>
</configuration>
EOF

log_info "Hadoop đã cấu hình xong"

# ── BƯỚC 4: Cài đặt Spark 3.5.1 ──────────────────────────────
log_section "BƯỚC 4: Cài đặt Apache Spark $SPARK_VERSION"

if [ -d "$SPARK_INSTALL_DIR" ]; then
    log_warn "Spark đã tồn tại tại $SPARK_INSTALL_DIR – bỏ qua"
else
    SPARK_TGZ="spark-${SPARK_VERSION}-bin-hadoop3.tgz"

    # Tìm file .tgz đã tải thủ công (ưu tiên theo thứ tự)
    SPARK_TGZ_PATH=""
    for candidate in \
        "$(dirname "$0")/../${SPARK_TGZ}" \
        "$HOME/${SPARK_TGZ}" \
        "/tmp/${SPARK_TGZ}"
    do
        if [ -f "$candidate" ]; then
            SPARK_TGZ_PATH="$(realpath "$candidate")"
            break
        fi
    done

    if [ -n "$SPARK_TGZ_PATH" ]; then
        log_info "Dùng file có sẵn: $SPARK_TGZ_PATH"
    else
        # Dùng mirror gần (Asia) để tải nhanh hơn
        SPARK_URL="https://mirror.downloadvn.com/apache/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"
        SPARK_URL_FALLBACK="https://downloads.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"
        log_info "Đang tải Spark từ mirror Asia: $SPARK_URL ..."
        wget -q --show-progress -O "/tmp/${SPARK_TGZ}" "$SPARK_URL" || {
            log_warn "Mirror lỗi – thử server chính..."
            wget -q --show-progress -O "/tmp/${SPARK_TGZ}" "$SPARK_URL_FALLBACK"
        }
        SPARK_TGZ_PATH="/tmp/${SPARK_TGZ}"
    fi

    log_info "Đang giải nén Spark..."
    tar -xzf "$SPARK_TGZ_PATH" -C "$HOME"
    mv "$HOME/spark-${SPARK_VERSION}-bin-hadoop3" "$SPARK_INSTALL_DIR"
    log_info "Spark đã giải nén tại $SPARK_INSTALL_DIR"
fi

# spark-env.sh
cp "$SPARK_INSTALL_DIR/conf/spark-env.sh.template" \
   "$SPARK_INSTALL_DIR/conf/spark-env.sh" 2>/dev/null || true

cat >> "$SPARK_INSTALL_DIR/conf/spark-env.sh" << EOF
export JAVA_HOME=${JAVA_HOME_PATH}
export HADOOP_CONF_DIR=${HADOOP_INSTALL_DIR}/etc/hadoop
export SPARK_DRIVER_MEMORY=4g
EOF

# spark-defaults.conf
cat > "$SPARK_INSTALL_DIR/conf/spark-defaults.conf" << 'EOF'
spark.master                     local[*]
spark.driver.memory              4g
spark.sql.adaptive.enabled       true
spark.sql.shuffle.partitions     8
spark.ui.port                    4040
EOF

log_info "Spark đã cấu hình xong"

# ── BƯỚC 5: Biến môi trường ~/.bashrc ─────────────────────────
log_section "BƯỚC 5: Cấu hình biến môi trường"

BASHRC_MARKER="# === NYC TAXI BIGDATA ENV ==="

if grep -q "$BASHRC_MARKER" ~/.bashrc; then
    log_warn "Biến môi trường đã tồn tại trong ~/.bashrc – bỏ qua"
else
    cat >> ~/.bashrc << EOF

${BASHRC_MARKER}
export JAVA_HOME=${JAVA_HOME_PATH}
export HADOOP_HOME=${HADOOP_INSTALL_DIR}
export HADOOP_INSTALL=\$HADOOP_HOME
export HADOOP_MAPRED_HOME=\$HADOOP_HOME
export HADOOP_COMMON_HOME=\$HADOOP_HOME
export HADOOP_HDFS_HOME=\$HADOOP_HOME
export YARN_HOME=\$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=\$HADOOP_HOME/lib/native
export SPARK_HOME=${SPARK_INSTALL_DIR}
export PYSPARK_PYTHON=python3
export PATH=\$PATH:\$JAVA_HOME/bin:\$HADOOP_HOME/sbin:\$HADOOP_HOME/bin:\$SPARK_HOME/bin:\$SPARK_HOME/sbin
EOF
    log_info "Đã thêm biến môi trường vào ~/.bashrc"
fi

# Load ngay cho session hiện tại
export JAVA_HOME=$JAVA_HOME_PATH
export HADOOP_HOME=$HADOOP_INSTALL_DIR
export SPARK_HOME=$SPARK_INSTALL_DIR
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin

# ── BƯỚC 6: Python virtual environment ────────────────────────
log_section "BƯỚC 6: Cài đặt Python environment"

if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
    log_info "Đã tạo virtual environment tại $VENV_DIR"
fi

source "$VENV_DIR/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet -r "$(dirname "$0")/../requirements.txt"
log_info "Đã cài đặt tất cả Python packages"

# ── BƯỚC 7: Format & khởi động HDFS ──────────────────────────
log_section "BƯỚC 7: Khởi động HDFS"

mkdir -p /tmp/hadoopdata/tmp /tmp/hadoopdata/namenode /tmp/hadoopdata/datanode

# Format NameNode (chỉ khi chưa format)
if [ ! -d "/tmp/hadoopdata/namenode/current" ]; then
    log_info "Format NameNode lần đầu..."
    "$HADOOP_HOME/bin/hdfs" namenode -format -force -nonInteractive
    log_info "Format NameNode hoàn tất"
else
    log_warn "NameNode đã được format – bỏ qua"
fi

# Khởi động HDFS
"$HADOOP_HOME/sbin/start-dfs.sh"
sleep 5

# Tạo cấu trúc thư mục HDFS (Bronze / Silver / Gold)
log_info "Tạo cấu trúc thư mục HDFS (Bronze → Silver → Gold)..."
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /nyc_taxi/bronze/raw
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /nyc_taxi/silver/cleaned
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /nyc_taxi/gold/aggregated_by_time
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /nyc_taxi/gold/aggregated_by_vendor
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /nyc_taxi/logs

log_info "Cấu trúc HDFS đã tạo:"
"$HADOOP_HOME/bin/hdfs" dfs -ls -R /nyc_taxi/

# ── BƯỚC 8: Kiểm tra tổng thể ─────────────────────────────────
log_section "BƯỚC 8: Kiểm tra môi trường"

echo ""
echo "  Java:    $(java -version 2>&1 | head -1)"
echo "  Hadoop:  $($HADOOP_HOME/bin/hadoop version 2>/dev/null | head -1)"
echo "  Spark:   $($SPARK_HOME/bin/spark-submit --version 2>&1 | grep 'version' | head -1)"
echo "  Python:  $(python3 --version)"
echo ""
echo "  Processes (jps):"
jps
echo ""

log_section "✅ CÀI ĐẶT HOÀN TẤT"
echo ""
echo "  HDFS Web UI:  http://localhost:9870"
echo "  Spark UI:     http://localhost:4040  (khi có job đang chạy)"
echo ""
echo "  Bước tiếp theo:"
echo "    source ~/.bashrc"
echo "    source ~/bigdata-env/bin/activate"
echo "    python src/ingestion.py"
echo ""
