# chicago-air-quality-bigdata

Academic Big Data project for Chicago air-quality measurements.

The final project is one unified pipeline:

```text
Open Air Chicago CSV
-> Python Kafka producer simulator
-> Kafka topic air_quality_stream
-> Spark Structured Streaming
-> HDFS data lake
-> Java MapReduce analytics
-> HBase serving table
-> Streamlit dashboard / static visualizations
```

Spark is used once, as the streaming processor. MapReduce is used for the batch analytics layer.

## Dataset

- Dataset: Open Air Chicago Individual Measurements
- Source: Chicago Data Portal
- Direct CSV URL: `https://data.cityofchicago.org/api/views/xfya-dxtq/rows.csv?accessType=DOWNLOAD`
- Local path example: `%USERPROFILE%\Downloads\Open_Air_Chicago_Individual_Measurements.csv`

Important observed columns:

- `datasourceid`: sensor identifier
- `time`: measurement timestamp
- `pm2_5ConcMassIndividual.value`: PM2.5
- `no2ConcIndividual.value`: NO2
- `temperatureAmbientIndividual.value`: temperature
- `relHumidAmbientIndividual.value`: humidity
- `latitude`, `longitude`: location

## Technology Roles

- Kafka ingests simulated live sensor events from the historical CSV.
- Spark Structured Streaming parses Kafka JSON messages and stores cleaned events in HDFS.
- HDFS is the central data lake used by downstream processing.
- Java MapReduce reads HDFS event files and computes pollution indicators.
- HBase stores selected measurements with a sensor/timestamp row key for fast lookup.
- Streamlit and Python visualizations present live and processed results.

## Repository Structure

```text
docker/          Docker and Hadoop commands
data/            Sample generation helpers, generated data ignored by Git
streaming/       Kafka producer, test consumer, Spark Structured Streaming consumer
mapreduce/       Java MapReduce jobs
hbase/           HBase schema and ImportTsv preparation
dashboard/       Streamlit dashboard
visualization/   Static report-ready plots
report/          Report command notes
```

## 1. Start Hadoop

Create the Docker network once:

```powershell
docker network create --driver=bridge hadoop
```

Create the cluster once:

```powershell
docker run -itd --net=hadoop -p 9870:9870 -p 8088:8088 -p 7077:7077 -p 16010:16010 --name hadoop-master --hostname hadoop-master liliasfaxi/hadoop-cluster:latest
docker run -itd -p 8040:8042 --net=hadoop --name hadoop-worker1 --hostname hadoop-worker1 liliasfaxi/hadoop-cluster:latest
docker run -itd -p 8041:8042 --net=hadoop --name hadoop-worker2 --hostname hadoop-worker2 liliasfaxi/hadoop-cluster:latest
```

Start existing containers later:

```powershell
docker start hadoop-master hadoop-worker1 hadoop-worker2
docker exec hadoop-master bash -lc "./start-hadoop.sh"
docker exec hadoop-master bash -lc "jps"
```

Useful UIs:

- Hadoop NameNode: http://localhost:9870
- YARN ResourceManager: http://localhost:8088
- HBase UI: http://localhost:16010

## 2. Start Kafka

```powershell
docker compose -f streaming/docker-compose.kafka.yml up -d
```

Create the topic:

```powershell
docker exec -it kafka kafka-topics --create --if-not-exists --bootstrap-server kafka:29092 --replication-factor 1 --partitions 3 --topic air_quality_stream
```

Verify:

```powershell
docker exec -it kafka kafka-topics --list --bootstrap-server kafka:29092
```

## 3. Run Spark Structured Streaming

Copy the Spark streaming consumer into the Hadoop master:

```powershell
docker cp streaming\spark_streaming_consumer.py hadoop-master:/root/spark_streaming_consumer.py
```

Run it inside `hadoop-master`:

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

This process stays running. Keep it open while the producer sends events.

## 4. Produce Simulated Live Events

Install host Python dependencies:

```powershell
python -m pip install -r streaming\requirements.txt
```

Send CSV rows to Kafka:

```powershell
python streaming\kafka_producer_simulator.py --csv "%USERPROFILE%\Downloads\Open_Air_Chicago_Individual_Measurements.csv" --topic air_quality_stream --bootstrap-server localhost:9092 --delay 0.5 --limit 100
```

Optional test consumer:

```powershell
python streaming\kafka_consumer_test.py --from-beginning
```

After enough rows are sent, stop Spark Streaming with `Ctrl+C`.

Verify the HDFS data lake:

```powershell
docker exec hadoop-master bash -lc "hdfs dfs -ls /chicago/streaming/bronze/air_quality_events"
docker exec hadoop-master bash -lc "hdfs dfs -cat /chicago/streaming/bronze/air_quality_events/part* | head"
```

## 5. Build MapReduce

If Maven is installed locally:

```powershell
cd mapreduce\chicago-air-mapreduce
mvn clean package
cd ..\..
docker cp mapreduce\chicago-air-mapreduce\target\chicago-mapreduce.jar hadoop-master:/root/chicago-mapreduce.jar
```

If Maven is not installed, build inside `hadoop-master` with the helper script:

```powershell
docker cp mapreduce\chicago-air-mapreduce hadoop-master:/root/chicago-air-mapreduce
docker cp docker\build_mapreduce_in_container.sh hadoop-master:/root/build_mapreduce_in_container.sh
docker exec hadoop-master bash -lc "chmod +x /root/build_mapreduce_in_container.sh && /root/build_mapreduce_in_container.sh"
```

## 6. Run MapReduce on Streamed HDFS Data

```powershell
docker exec hadoop-master bash -lc "hdfs dfs -rm -r -f /chicago/output/avg_pm25_by_day; hadoop jar /root/chicago-mapreduce.jar hadoop.mapreduce.chicago.AveragePM25ByDay /chicago/streaming/bronze/air_quality_events /chicago/output/avg_pm25_by_day"

docker exec hadoop-master bash -lc "hdfs dfs -rm -r -f /chicago/output/avg_no2_by_sensor; hadoop jar /root/chicago-mapreduce.jar hadoop.mapreduce.chicago.AverageNO2BySensor /chicago/streaming/bronze/air_quality_events /chicago/output/avg_no2_by_sensor"

docker exec hadoop-master bash -lc "hdfs dfs -rm -r -f /chicago/output/pm25_exceedances; hadoop jar /root/chicago-mapreduce.jar hadoop.mapreduce.chicago.PM25ThresholdBySensor /chicago/streaming/bronze/air_quality_events /chicago/output/pm25_exceedances"
```

Check results:

```powershell
docker exec hadoop-master bash -lc "hdfs dfs -cat /chicago/output/avg_pm25_by_day/part-r-00000 | head"
docker exec hadoop-master bash -lc "hdfs dfs -cat /chicago/output/avg_no2_by_sensor/part-r-00000 | head"
docker exec hadoop-master bash -lc "hdfs dfs -cat /chicago/output/pm25_exceedances/part-r-00000 | head"
```

## 7. Load HBase

Export streamed HDFS events to Windows:

```powershell
docker exec hadoop-master bash -lc "rm -rf /tmp/air_quality_events && hdfs dfs -get /chicago/streaming/bronze/air_quality_events /tmp/air_quality_events"
docker cp hadoop-master:/tmp/air_quality_events .\data\air_quality_events
```

Prepare ImportTsv input:

```powershell
python hbase\prepare_hbase_csv.py --input data\air_quality_events --output data\chicago_hbase_clean.csv
```

Copy and import:

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

## 8. Dashboard

Install dashboard dependencies:

```powershell
python -m pip install -r dashboard\requirements.txt
```

Run:

```powershell
streamlit run dashboard\streamlit_dashboard.py -- --from-beginning
```

Open:

```text
http://localhost:8501
```

The dashboard shows:

- live Kafka events
- live PM2.5 and NO2 summaries
- PM2.5 exceedances
- sensor map
- exported MapReduce result charts

## 9. Static Visualizations

Export MapReduce outputs:

```powershell
docker exec hadoop-master bash -lc "rm -rf /tmp/avg_pm25_by_day /tmp/avg_no2_by_sensor /tmp/pm25_exceedances /tmp/air_quality_events && hdfs dfs -get /chicago/output/avg_pm25_by_day /tmp/avg_pm25_by_day && hdfs dfs -get /chicago/output/avg_no2_by_sensor /tmp/avg_no2_by_sensor && hdfs dfs -get /chicago/output/pm25_exceedances /tmp/pm25_exceedances && hdfs dfs -get /chicago/streaming/bronze/air_quality_events /tmp/air_quality_events"

docker cp hadoop-master:/tmp/avg_pm25_by_day .\visualization\local_results\avg_pm25_by_day
docker cp hadoop-master:/tmp/avg_no2_by_sensor .\visualization\local_results\avg_no2_by_sensor
docker cp hadoop-master:/tmp/pm25_exceedances .\visualization\local_results\pm25_exceedances
docker cp hadoop-master:/tmp/air_quality_events .\visualization\local_results\air_quality_events
```

Generate files:

```powershell
python -m pip install pandas matplotlib plotly
python visualization\visualize_results.py
```

Outputs:

- `visualization/pm25_by_day.png`
- `visualization/no2_by_sensor_top10.png`
- `visualization/pm25_exceedances_top10.png`
- `visualization/pollution_sensor_map.html`

## Validation Checklist

- Kafka containers run and topic `air_quality_stream` exists.
- Producer sends JSON events from the CSV.
- Spark Structured Streaming consumes Kafka and writes JSON files to HDFS.
- MapReduce jobs read `/chicago/streaming/bronze/air_quality_events`.
- HBase table `chicago_air_quality` contains imported rows.
- Streamlit dashboard opens at `http://localhost:8501`.
- Static visualization files are generated locally.

## Notes

- The large CSV is intentionally excluded from Git.
- Generated CSV, JAR, PNG, HTML, local result folders, and caches are ignored.
- `spark/chicago_spark_analysis.py` remains as an optional legacy batch comparison script, but the main project pipeline uses Spark only for Structured Streaming.
