# Real-Time Streaming Layer with Kafka

## Objective

This module turns the historical Open Air Chicago CSV into simulated live sensor events.

In the unified project pipeline, Kafka and Spark Structured Streaming are not a separate side demo. They are the ingestion layer:

```text
Open Air Chicago CSV
-> Python Kafka Producer
-> Kafka topic air_quality_stream
-> Spark Structured Streaming
-> HDFS /chicago/streaming/bronze/air_quality_events
-> MapReduce / HBase / Dashboard
```

## Why We Simulate Real Time From a CSV

The source dataset is historical. The producer replays rows one by one with a delay so every CSV row behaves like a new live air-quality sensor event.

## Start Kafka

Kafka and ZooKeeper join the existing Docker network named `hadoop`, so Kafka can communicate with the Hadoop containers.

```powershell
docker compose -f streaming/docker-compose.kafka.yml up -d
```

Create the topic:

```powershell
docker exec -it kafka kafka-topics --create --if-not-exists --bootstrap-server kafka:29092 --replication-factor 1 --partitions 3 --topic air_quality_stream
```

List topics:

```powershell
docker exec -it kafka kafka-topics --list --bootstrap-server kafka:29092
```

## Run the Producer

Install dependencies:

```powershell
python -m pip install -r streaming\requirements.txt
```

Run:

```powershell
python streaming\kafka_producer_simulator.py ^
--csv "%USERPROFILE%\Downloads\Open_Air_Chicago_Individual_Measurements.csv" ^
--topic air_quality_stream ^
--bootstrap-server localhost:9092 ^
--delay 0.5 ^
--limit 100
```

The producer prints the detected CSV column mapping and every JSON message sent to Kafka.

## Test the Kafka Topic

Python consumer:

```powershell
python streaming\kafka_consumer_test.py --from-beginning
```

Kafka console consumer:

```powershell
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic air_quality_stream --from-beginning
```

## Run Spark Structured Streaming

Copy the consumer script to `hadoop-master`:

```powershell
docker cp streaming\spark_streaming_consumer.py hadoop-master:/root/spark_streaming_consumer.py
```

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

The script:

- reads events from Kafka
- parses JSON fields
- normalizes empty sensor IDs to `unknown_sensor`
- computes live console aggregations
- writes cleaned JSON events to HDFS by default

Disable HDFS writes only for debugging:

```bash
spark-submit \
--master yarn \
--deploy-mode client \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
/root/spark_streaming_consumer.py \
--bootstrap-server kafka:29092 \
--topic air_quality_stream \
--no-write-hdfs
```

## Verify HDFS Output

```bash
hdfs dfs -ls /chicago/streaming/bronze/air_quality_events
hdfs dfs -cat /chicago/streaming/bronze/air_quality_events/part* | head
```

The HDFS JSON files are the input for the Java MapReduce jobs and HBase preparation.

## Dashboard

Use the Streamlit dashboard for a visual view of the live Kafka stream:

```powershell
python -m pip install -r dashboard\requirements.txt
streamlit run dashboard\streamlit_dashboard.py -- --from-beginning
```

Open:

```text
http://localhost:8501
```

## Troubleshooting

- If Kafka cannot start, verify that the external Docker network exists with `docker network ls`.
- If the `hadoop` network is missing, create it with `docker network create --driver=bridge hadoop`.
- If the producer cannot connect, confirm that Kafka exposes `localhost:9092` with `docker ps`.
- If Spark cannot read Kafka, include `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0`.
- If the dashboard shows no messages, produce new events or run it with `--from-beginning`.
