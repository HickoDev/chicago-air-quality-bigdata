# Spark Usage

The main pipeline uses Spark only for Structured Streaming.

Main Spark script:

```text
streaming/spark_streaming_consumer.py
```

Role:

```text
Kafka topic air_quality_stream
-> Spark Structured Streaming
-> HDFS /chicago/streaming/bronze/air_quality_events
```

Run command inside `hadoop-master`:

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

The older `spark/chicago_spark_analysis.py` file is kept only as an optional comparison script. It is not part of the main final pipeline, because batch analytics are handled by Java MapReduce.
