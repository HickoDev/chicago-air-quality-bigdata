# Setup Commands Used

This file lists the exact commands used to set up and test the project in the Docker TP environment.

## 1. Docker Cluster Setup

```powershell
docker network create --driver=bridge hadoop

docker run -itd --net=hadoop -p 9870:9870 -p 8088:8088 -p 7077:7077 -p 16010:16010 --name hadoop-master --hostname hadoop-master liliasfaxi/hadoop-cluster:latest

docker run -itd -p 8040:8042 --net=hadoop --name hadoop-worker1 --hostname hadoop-worker1 liliasfaxi/hadoop-cluster:latest

docker run -itd -p 8041:8042 --net=hadoop --name hadoop-worker2 --hostname hadoop-worker2 liliasfaxi/hadoop-cluster:latest

docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

## 2. Start Hadoop

```powershell
docker exec hadoop-master bash -lc "./start-hadoop.sh"
docker exec hadoop-master bash -lc "jps"
```

## 3. Generate the Sample Dataset

```powershell
python data\sample_generation.py --input "C:\Users\user\Downloads\Open_Air_Chicago_Individual_Measurements.csv" --output data\open_air_chicago_sample.csv --sample-size 100000
```

## 4. Copy Project Assets into `hadoop-master`

```powershell
docker cp mapreduce\chicago-air-mapreduce hadoop-master:/root/chicago-air-mapreduce
docker cp spark\chicago_spark_analysis.py hadoop-master:/root/chicago_spark_analysis.py
docker cp data\open_air_chicago_sample.csv hadoop-master:/root/open_air_chicago.csv
docker cp docker\build_mapreduce_in_container.sh hadoop-master:/root/build_mapreduce_in_container.sh
docker cp docker\load_sample_to_hdfs.sh hadoop-master:/root/load_sample_to_hdfs.sh
```

## 5. Build the MapReduce JAR in the Container

```powershell
docker exec hadoop-master bash -lc "chmod +x /root/build_mapreduce_in_container.sh /root/load_sample_to_hdfs.sh && /root/build_mapreduce_in_container.sh"
```

Helper script content:

```bash
#!/bin/bash
set -euo pipefail

PROJECT_DIR="/root/chicago-air-mapreduce"
BUILD_DIR="${PROJECT_DIR}/build"
CLASSES_DIR="${BUILD_DIR}/classes"
OUTPUT_JAR="/root/chicago-mapreduce.jar"

cd "${PROJECT_DIR}"
rm -rf "${BUILD_DIR}"
mkdir -p "${CLASSES_DIR}"

javac -cp "$(hadoop classpath --glob)" -d "${CLASSES_DIR}" $(find src/main/java -name "*.java")
jar cvf "${OUTPUT_JAR}" -C "${CLASSES_DIR}" .
```

## 6. Load the Sample to HDFS

```powershell
docker exec hadoop-master bash -lc "/root/load_sample_to_hdfs.sh && hdfs dfs -ls /chicago/input"
```

Helper script content:

```bash
#!/bin/bash
set -euo pipefail

hdfs dfs -mkdir -p /chicago/input
hdfs dfs -rm -f /chicago/input/open_air_chicago.csv || true
hdfs dfs -put /root/open_air_chicago.csv /chicago/input/
```

## 7. Run the Three MapReduce Jobs

```powershell
docker exec hadoop-master bash -lc "hdfs dfs -rm -r -f /chicago/output/avg_pm25_by_day >/dev/null 2>&1 || true; hadoop jar /root/chicago-mapreduce.jar hadoop.mapreduce.chicago.AveragePM25ByDay /chicago/input/open_air_chicago.csv /chicago/output/avg_pm25_by_day"

docker exec hadoop-master bash -lc "hdfs dfs -rm -r -f /chicago/output/avg_no2_by_sensor >/dev/null 2>&1 || true; hadoop jar /root/chicago-mapreduce.jar hadoop.mapreduce.chicago.AverageNO2BySensor /chicago/input/open_air_chicago.csv /chicago/output/avg_no2_by_sensor"

docker exec hadoop-master bash -lc "hdfs dfs -rm -r -f /chicago/output/pm25_threshold >/dev/null 2>&1 || true; hadoop jar /root/chicago-mapreduce.jar hadoop.mapreduce.chicago.PM25ThresholdBySensor /chicago/input/open_air_chicago.csv /chicago/output/pm25_threshold"
```

## 8. Verify MapReduce Outputs

```powershell
docker exec hadoop-master bash -lc "hdfs dfs -cat /chicago/output/avg_pm25_by_day/part-r-00000 | head"
docker exec hadoop-master bash -lc "hdfs dfs -cat /chicago/output/avg_no2_by_sensor/part-r-00000 | head"
docker exec hadoop-master bash -lc "hdfs dfs -cat /chicago/output/pm25_threshold/part-r-00000 | head"
```

## 9. Run Spark on YARN

```powershell
docker exec hadoop-master bash -lc "hdfs dfs -rm -r -f /chicago/spark_results >/dev/null 2>&1 || true; spark-submit --master yarn --deploy-mode client /root/chicago_spark_analysis.py"
```

## 10. Verify Spark Outputs

```powershell
docker exec hadoop-master bash -lc "hdfs dfs -ls /chicago/spark_results"
docker exec hadoop-master bash -lc "hdfs dfs -cat /chicago/spark_results/top_pm25_sensors/part* | head"
```

## 11. Prepare HBase CSV Locally

```powershell
python hbase\prepare_hbase_csv.py --input data\open_air_chicago_sample.csv --output data\chicago_hbase_clean.csv
```

## 12. Copy HBase Assets into `hadoop-master`

```powershell
docker cp data\chicago_hbase_clean.csv hadoop-master:/root/chicago_hbase_clean.csv
docker cp hbase\create_table.hbase hadoop-master:/root/create_table.hbase
docker cp hbase\import_tsv_command.sh hadoop-master:/root/import_tsv_command.sh
docker cp hbase\import_tsv_command_local.sh hadoop-master:/root/import_tsv_command_local.sh
```

## 13. Start HBase

```powershell
docker exec hadoop-master bash -lc "start-hbase.sh"
```

## 14. Import into HBase

The standard YARN-backed `ImportTsv` command failed in this image during MRAppMaster startup, so the tested command used local MapReduce mode:

```powershell
docker exec hadoop-master bash -lc "chmod +x /root/import_tsv_command_local.sh && cd /root && ./import_tsv_command_local.sh"
```

Equivalent direct command:

```powershell
docker exec hadoop-master bash -lc "hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dmapreduce.framework.name=local -Dimporttsv.separator=',' -Dimporttsv.columns=HBASE_ROW_KEY,m:pm25,m:no2,l:latitude,l:longitude,t:date,t:hour chicago_air_quality /chicago/hbase_input/chicago_hbase_clean.csv"
```

## 15. Verify HBase Rows

```powershell
docker exec hadoop-master bash -lc "printf \"scan 'chicago_air_quality', {LIMIT => 10}\nexit\n\" | hbase shell -n"
```

## 16. Export Spark Results from HDFS

```powershell
docker exec hadoop-master bash -lc "rm -rf /tmp/avg_pm25_by_day /tmp/avg_no2_by_sensor /tmp/pm25_exceedances /tmp/sensor_map_data && hdfs dfs -get /chicago/spark_results/avg_pm25_by_day /tmp/avg_pm25_by_day && hdfs dfs -get /chicago/spark_results/avg_no2_by_sensor /tmp/avg_no2_by_sensor && hdfs dfs -get /chicago/spark_results/pm25_exceedances /tmp/pm25_exceedances && hdfs dfs -get /chicago/spark_results/sensor_map_data /tmp/sensor_map_data"
```

## 17. Copy Spark Results Back to Windows

```powershell
docker cp hadoop-master:/tmp/avg_pm25_by_day .\visualization\local_results\avg_pm25_by_day
docker cp hadoop-master:/tmp/avg_no2_by_sensor .\visualization\local_results\avg_no2_by_sensor
docker cp hadoop-master:/tmp/pm25_exceedances .\visualization\local_results\pm25_exceedances
docker cp hadoop-master:/tmp/sensor_map_data .\visualization\local_results\sensor_map_data
```

## 18. Install Visualization Dependencies on Windows

```powershell
python -m pip install pandas matplotlib plotly
```

## 19. Generate the Final Visualizations

```powershell
python visualization\visualize_results.py
```

## Useful Web Interfaces

- NameNode: `http://localhost:9870`
- YARN: `http://localhost:8088`
- HBase UI: `http://localhost:16010`
