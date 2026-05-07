# Unified Pipeline Setup Commands

This file lists the commands used to run the final one-pipeline version of the project.

Pipeline:

```text
CSV -> Kafka -> Spark Structured Streaming -> HDFS -> MapReduce -> HBase -> Dashboard
```

## 1. Start Hadoop Containers

```powershell
docker start hadoop-master hadoop-worker1 hadoop-worker2
docker exec hadoop-master bash -lc "./start-hadoop.sh"
docker exec hadoop-master bash -lc "jps"
```

Useful UIs:

- NameNode: `http://localhost:9870`
- YARN: `http://localhost:8088`
- HBase UI: `http://localhost:16010`

## 2. Start Kafka

```powershell
docker compose -f streaming/docker-compose.kafka.yml up -d
docker exec -it kafka kafka-topics --create --if-not-exists --bootstrap-server kafka:29092 --replication-factor 1 --partitions 3 --topic air_quality_stream
docker exec -it kafka kafka-topics --list --bootstrap-server kafka:29092
```

## 3. Copy Spark Streaming Script to Hadoop

```powershell
docker cp streaming\spark_streaming_consumer.py hadoop-master:/root/spark_streaming_consumer.py
```

## 4. Run Spark Structured Streaming

Run inside `hadoop-master`:

```bash
spark-submit \
--master yarn \
--deploy-mode client \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
/root/spark_streaming_consumer.py \
--bootstrap-server kafka:29092 \
--topic air_quality_stream \
--output-path /chicago/streaming/bronze/air_quality_events \
--checkpoint-path /chicago/checkpoints/air_quality_stream
```

Keep this process running while the Kafka producer sends events.

## 5. Run the Kafka Producer

Install dependencies:

```powershell
python -m pip install -r streaming\requirements.txt
```

Send events:

```powershell
python streaming\kafka_producer_simulator.py --csv "%USERPROFILE%\Downloads\Open_Air_Chicago_Individual_Measurements.csv" --topic air_quality_stream --bootstrap-server localhost:9092 --delay 0.5 --limit 100
```

Optional topic check:

```powershell
python streaming\kafka_consumer_test.py --from-beginning
```

## 6. Verify HDFS Streaming Output

```powershell
docker exec hadoop-master bash -lc "hdfs dfs -ls /chicago/streaming/bronze/air_quality_events"
docker exec hadoop-master bash -lc "hdfs dfs -cat /chicago/streaming/bronze/air_quality_events/part* | head"
```

## 7. Build MapReduce JAR

Local Maven option:

```powershell
cd mapreduce\chicago-air-mapreduce
mvn clean package
cd ..\..
docker cp mapreduce\chicago-air-mapreduce\target\chicago-mapreduce.jar hadoop-master:/root/chicago-mapreduce.jar
```

Container build option:

```powershell
docker cp mapreduce\chicago-air-mapreduce hadoop-master:/root/chicago-air-mapreduce
docker cp docker\build_mapreduce_in_container.sh hadoop-master:/root/build_mapreduce_in_container.sh
docker exec hadoop-master bash -lc "chmod +x /root/build_mapreduce_in_container.sh && /root/build_mapreduce_in_container.sh"
```

## 8. Run MapReduce on Streamed HDFS Data

```powershell
docker exec hadoop-master bash -lc "hdfs dfs -rm -r -f /chicago/output/avg_pm25_by_day; hadoop jar /root/chicago-mapreduce.jar hadoop.mapreduce.chicago.AveragePM25ByDay /chicago/streaming/bronze/air_quality_events /chicago/output/avg_pm25_by_day"

docker exec hadoop-master bash -lc "hdfs dfs -rm -r -f /chicago/output/avg_no2_by_sensor; hadoop jar /root/chicago-mapreduce.jar hadoop.mapreduce.chicago.AverageNO2BySensor /chicago/streaming/bronze/air_quality_events /chicago/output/avg_no2_by_sensor"

docker exec hadoop-master bash -lc "hdfs dfs -rm -r -f /chicago/output/pm25_exceedances; hadoop jar /root/chicago-mapreduce.jar hadoop.mapreduce.chicago.PM25ThresholdBySensor /chicago/streaming/bronze/air_quality_events /chicago/output/pm25_exceedances"
```

## 9. Verify MapReduce Outputs

```powershell
docker exec hadoop-master bash -lc "hdfs dfs -cat /chicago/output/avg_pm25_by_day/part-r-00000 | head"
docker exec hadoop-master bash -lc "hdfs dfs -cat /chicago/output/avg_no2_by_sensor/part-r-00000 | head"
docker exec hadoop-master bash -lc "hdfs dfs -cat /chicago/output/pm25_exceedances/part-r-00000 | head"
```

## 10. Prepare HBase Input from Streamed Events

```powershell
docker exec hadoop-master bash -lc "rm -rf /tmp/air_quality_events && hdfs dfs -get /chicago/streaming/bronze/air_quality_events /tmp/air_quality_events"
docker cp hadoop-master:/tmp/air_quality_events .\data\air_quality_events
python hbase\prepare_hbase_csv.py --input data\air_quality_events --output data\chicago_hbase_clean.csv
```

## 11. Import to HBase

```powershell
docker cp data\chicago_hbase_clean.csv hadoop-master:/root/chicago_hbase_clean.csv
docker cp hbase\create_table.hbase hadoop-master:/root/create_table.hbase
docker cp hbase\import_tsv_command_local.sh hadoop-master:/root/import_tsv_command_local.sh
docker exec hadoop-master bash -lc "start-hbase.sh"
docker exec hadoop-master bash -lc "chmod +x /root/import_tsv_command_local.sh && cd /root && ./import_tsv_command_local.sh"
```

Verify:

```powershell
docker exec hadoop-master bash -lc "printf \"scan 'chicago_air_quality', {LIMIT => 10}\nexit\n\" | hbase shell -n"
```

## 12. Run the Dashboard

```powershell
python -m pip install -r dashboard\requirements.txt
streamlit run dashboard\streamlit_dashboard.py -- --from-beginning
```

Open:

```text
http://localhost:8501
```

## 13. Static Visualizations

```powershell
docker exec hadoop-master bash -lc "rm -rf /tmp/avg_pm25_by_day /tmp/avg_no2_by_sensor /tmp/pm25_exceedances /tmp/air_quality_events && hdfs dfs -get /chicago/output/avg_pm25_by_day /tmp/avg_pm25_by_day && hdfs dfs -get /chicago/output/avg_no2_by_sensor /tmp/avg_no2_by_sensor && hdfs dfs -get /chicago/output/pm25_exceedances /tmp/pm25_exceedances && hdfs dfs -get /chicago/streaming/bronze/air_quality_events /tmp/air_quality_events"

docker cp hadoop-master:/tmp/avg_pm25_by_day .\visualization\local_results\avg_pm25_by_day
docker cp hadoop-master:/tmp/avg_no2_by_sensor .\visualization\local_results\avg_no2_by_sensor
docker cp hadoop-master:/tmp/pm25_exceedances .\visualization\local_results\pm25_exceedances
docker cp hadoop-master:/tmp/air_quality_events .\visualization\local_results\air_quality_events

python -m pip install pandas matplotlib plotly
python visualization\visualize_results.py
```
