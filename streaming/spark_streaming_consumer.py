from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, lit, when
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


DEFAULT_TOPIC = "air_quality_stream"
DEFAULT_BOOTSTRAP_SERVER = "kafka:29092"
DEFAULT_OUTPUT_PATH = "/chicago/streaming/bronze/air_quality_events"
DEFAULT_CHECKPOINT_PATH = "/chicago/checkpoints/air_quality_stream"


EVENT_SCHEMA = StructType(
    [
        StructField("event_time", StringType(), True),
        StructField("ingestion_time", StringType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("pm25", DoubleType(), True),
        StructField("no2", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Spark Structured Streaming ingestion for Kafka air-quality events."
    )
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic name.")
    parser.add_argument(
        "--bootstrap-server",
        default=DEFAULT_BOOTSTRAP_SERVER,
        help="Use kafka:29092 inside Docker or localhost:9092 on the host.",
    )
    parser.add_argument(
        "--output-path",
        default=DEFAULT_OUTPUT_PATH,
        help="HDFS data-lake path for parsed streaming events.",
    )
    parser.add_argument(
        "--checkpoint-path",
        default=DEFAULT_CHECKPOINT_PATH,
        help="Checkpoint path for Structured Streaming state.",
    )
    parser.add_argument(
        "--no-write-hdfs",
        action="store_true",
        help="Disable HDFS writes and only print console aggregations.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("ChicagoAirQualityKafkaStreaming")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_server)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_events = (
        kafka_df.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
        .withColumn(
            "sensor_id",
            when(col("sensor_id").isNull() | (col("sensor_id") == ""), lit("unknown_sensor"))
            .otherwise(col("sensor_id")),
        )
        .withColumn("pm25_exceedance", when(col("pm25") > 35.0, lit(1)).otherwise(lit(0)))
    )

    aggregations = (
        parsed_events.groupBy("sensor_id")
        .agg(
            {"pm25": "avg", "no2": "avg"}
        )
        .withColumnRenamed("avg(pm25)", "avg_pm25")
        .withColumnRenamed("avg(no2)", "avg_no2")
    )

    exceedances = (
        parsed_events.groupBy("sensor_id")
        .agg(
            count(when(col("pm25") > 35.0, True)).alias("pm25_exceedance_count")
        )
    )

    aggregation_query = (
        aggregations.writeStream.outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("checkpointLocation", f"{args.checkpoint_path}/aggregations")
        .start()
    )

    exceedance_query = (
        exceedances.writeStream.outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("checkpointLocation", f"{args.checkpoint_path}/exceedances")
        .start()
    )

    active_queries = [aggregation_query, exceedance_query]

    if not args.no_write_hdfs:
        # This HDFS path is the shared data-lake input for MapReduce and HBase preparation.
        # Direct HBase writes can be added later with foreachBatch and an HBase client.
        hdfs_query = (
            parsed_events.writeStream.outputMode("append")
            .format("json")
            .option("path", args.output_path)
            .option("checkpointLocation", f"{args.checkpoint_path}/raw_events")
            .start()
        )
        active_queries.append(hdfs_query)

    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in active_queries:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
