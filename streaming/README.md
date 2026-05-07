# Real-Time Streaming Extension with Kafka

## Objective

This module adds a Kafka-based streaming simulation to the Chicago air quality project. It reads historical CSV rows one by one and publishes each row as if it were a new live sensor event.

## Architecture

Open Air Chicago CSV
-> Python Kafka Producer
-> Kafka topic `air_quality_stream`
-> Kafka Consumer / Spark Structured Streaming
-> HDFS or HBase
-> Visualization

## Why We Simulate Real Time From a CSV

The Open Air Chicago file is historical, not a live feed. For a Big Data course project, the CSV can still be used to demonstrate streaming concepts by replaying rows with a delay. Each row becomes one live event, which lets Kafka and Spark Structured Streaming process the data continuously.

## Start Kafka

The Kafka and ZooKeeper containers join the existing external Docker network named `hadoop`, so the Hadoop containers and Kafka containers can communicate.

```powershell
docker compose -f streaming/docker-compose.kafka.yml up -d
```

Check containers:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

## Create Kafka Topic

```powershell
docker exec -it kafka kafka-topics --create --bootstrap-server kafka:29092 --replication-factor 1 --partitions 3 --topic air_quality_stream
```

List topics:

```powershell
docker exec -it kafka kafka-topics --list --bootstrap-server kafka:29092
```

## Run Producer

Install Python dependencies on Windows:

```powershell
python -m pip install -r streaming\requirements.txt
```

Run the producer:

```powershell
python streaming/kafka_producer_simulator.py ^
--csv "%USERPROFILE%\Downloads\Open_Air_Chicago_Individual_Measurements.csv" ^
--topic air_quality_stream ^
--bootstrap-server localhost:9092 ^
--delay 0.5 ^
--limit 100
```

Short demo:

```powershell
python streaming/kafka_producer_simulator.py --csv "%USERPROFILE%\Downloads\Open_Air_Chicago_Individual_Measurements.csv" --limit 20
```

The producer prints the detected column mapping and every sent JSON message.

## Run Consumer

Run the Python validation consumer:

```powershell
python streaming/kafka_consumer_test.py
```

To read old messages from the beginning:

```powershell
python streaming/kafka_consumer_test.py --from-beginning
```

Consume directly from the Kafka container:

```powershell
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic air_quality_stream --from-beginning
```

Expected result:

```text
partition=0 offset=12 key=DX123 value={'event_time': '...', 'ingestion_time': '...', 'sensor_id': 'DX123', ...}
```

## Optional Spark Structured Streaming

The Spark script reads the Kafka topic and writes real-time aggregations to the console:

- average PM2.5 by sensor
- average NO2 by sensor
- count of PM2.5 exceedances where `pm25 > 35`

Inside Docker, use Kafka's internal listener:

```bash
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
/root/spark_streaming_consumer.py \
--bootstrap-server kafka:29092 \
--topic air_quality_stream
```

From a local Spark installation on the host, use:

```powershell
spark-submit ^
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 ^
streaming/spark_streaming_consumer.py ^
--bootstrap-server localhost:9092 ^
--topic air_quality_stream
```

Optional HDFS raw event output:

```bash
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
/root/spark_streaming_consumer.py \
--bootstrap-server kafka:29092 \
--topic air_quality_stream \
--write-hdfs \
--output-path /chicago/streaming_results/air_quality \
--checkpoint-path /chicago/checkpoints/air_quality_stream
```

Writing directly to HBase can be added later with Spark `foreachBatch` and an HBase client.

## How This Connects to the Existing Hadoop/Spark/HBase Project

The existing project is a batch pipeline:

CSV -> HDFS -> MapReduce -> Spark -> HBase -> visualizations

The streaming extension complements it by replaying the same source data as live sensor events:

CSV -> Kafka -> streaming consumer -> HDFS or HBase

This demonstrates both batch processing and stream processing over the same environmental dataset.

## Troubleshooting

- If Kafka cannot start, verify that the external Docker network exists with `docker network ls`.
- If the `hadoop` network is missing, create it with `docker network create --driver=bridge hadoop`.
- If the producer cannot connect, confirm that Kafka exposes `localhost:9092` with `docker ps`.
- If Docker containers cannot resolve Kafka by name, ensure they are attached to the `hadoop` network.
- If Spark cannot read Kafka, include the package `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0`.
- If the consumer prints nothing, start the consumer first and then run the producer, or use `--from-beginning`.
- If the topic already exists, the create-topic command may report an error; continue with the list-topic command.
