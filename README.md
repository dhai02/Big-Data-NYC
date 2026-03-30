# NYC Taxi Big Data Pipeline — January 2026
Group 4 · Apache Spark + HDFS · Bronze → Silver → Gold

---

## Run with Docker (recommended)

The easiest way — no need to install Java, Hadoop, or Spark manually.

**Requirements:** [Docker Desktop](https://www.docker.com/products/docker-desktop/)

```bash
git clone https://github.com/dhai02/Big-Data-NYC.git
cd Big-Data-NYC

# Start HDFS + Spark containers
docker-compose up -d

# Wait ~20 seconds for HDFS to be ready, then run the pipeline
docker exec spark python src/ingestion.py        # download data → HDFS Bronze
docker exec spark python src/02_data_cleaning.py # Bronze → Silver
docker exec spark python src/03_aggregation_time.py    # Silver → Gold
docker exec spark python src/04_aggregation_vendor.py
```

Open Jupyter to explore data interactively:
```
http://localhost:8888
```

Stop everything:
```bash
docker-compose down
```

---

## HDFS structure

```
/nyc_taxi/
├── bronze/raw/                 ← raw parquet, untouched
├── silver/cleaned/             ← cleaned + enriched columns
└── gold/
    ├── aggregated_by_time/
    │   ├── by_hour/
    │   ├── by_date/
    │   ├── by_dayofweek/
    │   └── by_dow_hour/
    └── aggregated_by_vendor/
        ├── by_vendor/
        ├── by_payment/
        ├── by_pickup_location_top20/
        └── by_route_top50/
```

Browse files at `http://localhost:9870` → Utilities → Browse the file system.

---

## Reading data from HDFS (inside the spark container)

```bash
docker exec -it spark python3
```

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Analysis") \
    .master("local[*]") \
    .getOrCreate()

# Silver – cleaned data, best starting point
df = spark.read.parquet(
    "hdfs://namenode:9000/nyc_taxi/silver/cleaned/yellow_taxi_cleaned_2026_01"
)
df.show(5)
df.printSchema()

# Gold – aggregated by hour
df_hour = spark.read.parquet(
    "hdfs://namenode:9000/nyc_taxi/gold/aggregated_by_time/by_hour"
)
df_hour.show()
```

Silver layer extra columns:

| Column | Description |
|---|---|
| `trip_duration_min` | Trip duration in minutes |
| `pickup_hour` | Hour of pickup (0–23) |
| `pickup_date` | Date of pickup |
| `pickup_dayofweek` | Day of week (1=Sun, 7=Sat) |
| `tip_percentage` | Tip as % of fare |
| `speed_mph` | Average speed in mph |

---

## Copy data from HDFS to local

```bash
# Copy Silver to your local machine
docker exec spark hdfs dfs -get \
    /nyc_taxi/silver/cleaned/yellow_taxi_cleaned_2026_01 /app/data/

# The /app/data folder is mounted to ./data on your host machine
```

---

## Web UIs

| UI | URL |
|---|---|
| HDFS NameNode (browse files) | http://localhost:9870 |
| Jupyter Notebook | http://localhost:8888 |
| Spark UI (during jobs) | http://localhost:4040 |

---

## Run individual steps

```bash
docker exec spark python src/ingestion.py              # Bronze
docker exec spark python src/01_eda.py                 # EDA
docker exec spark python src/02_data_cleaning.py       # Silver
docker exec spark python src/03_aggregation_time.py    # Gold (time)
docker exec spark python src/04_aggregation_vendor.py  # Gold (vendor)
docker exec spark python src/05_verify_results.py      # verify
```

---

## Manual setup (WSL2, without Docker)

<details>
<summary>Click to expand</summary>

Requires Ubuntu 22.04 on WSL2, Java 17, 8GB RAM.

```bash
bash setup/install_hadoop_spark.sh
source ~/.bashrc
source ~/bigdata-env/bin/activate
bash run_pipeline.sh
```

Start HDFS every new terminal session:
```bash
start-dfs.sh
```

Troubleshooting:
- `hdfs: Unknown command: dfs` → `export PATH=~/hadoop/bin:~/hadoop/sbin:$PATH`
- HDFS won't start → `stop-dfs.sh && rm -rf /tmp/hadoopdata/namenode/* /tmp/hadoopdata/datanode/* && hdfs namenode -format -force && start-dfs.sh`
- Java version error → `sudo apt install openjdk-17-jdk -y && export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`

</details>
