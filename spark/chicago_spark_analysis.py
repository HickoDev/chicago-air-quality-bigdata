from __future__ import annotations

import re
from typing import Dict, Iterable, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    desc,
    first,
    lit,
    to_date,
    to_timestamp,
)


INPUT_PATH = "hdfs:///chicago/input/open_air_chicago.csv"
OUTPUT_BASE = "/chicago/spark_results"

COLUMN_CANDIDATES = {
    "timestamp": [
        "time",
        "timestamp",
        "measurement_time",
        "measurementdate",
        "measurement_date",
        "date",
    ],
    "sensor_id": [
        "datasourceid",
        "sensor_id",
        "sensorid",
        "sensor_name",
        "node_id",
        "site_id",
    ],
    "sensor_name": [
        "sensor_name",
        "sensorname",
        "site_name",
    ],
    "pm25": [
        "pm2_5concmassindividual_value",
        "pm25",
        "pm2_5",
        "pm2_5_value",
        "pm2_5conc_value",
        "pm2_5_value",
    ],
    "no2": [
        "no2concindividual_value",
        "no2",
        "no2_value",
    ],
    "latitude": [
        "latitude",
        "lat",
    ],
    "longitude": [
        "longitude",
        "lon",
    ],
    "temperature": [
        "temperatureambientindividual_value",
        "temperature",
        "temp",
    ],
    "humidity": [
        "relhumidambientindividual_value",
        "humidity",
        "rel_humid",
    ],
}


def normalize_column_name(name: str) -> str:
    normalized = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "column"


def rename_columns(df: DataFrame) -> DataFrame:
    renamed_df = df
    used_names = set()

    for original_name in df.columns:
        normalized_name = normalize_column_name(original_name)
        candidate_name = normalized_name
        suffix = 2

        while candidate_name in used_names:
            candidate_name = f"{normalized_name}_{suffix}"
            suffix += 1

        if candidate_name != original_name:
            renamed_df = renamed_df.withColumnRenamed(original_name, candidate_name)

        used_names.add(candidate_name)

    return renamed_df


def resolve_column(columns: Iterable[str], logical_name: str) -> Optional[str]:
    available = list(columns)
    candidates = COLUMN_CANDIDATES[logical_name]

    for candidate in candidates:
        if candidate in available:
            return candidate

    for candidate in candidates:
        for column_name in available:
            if candidate in column_name:
                return column_name

    return None


def require_column(columns: Iterable[str], logical_name: str) -> str:
    resolved = resolve_column(columns, logical_name)
    if resolved is None:
        raise ValueError(
            f"Could not detect the '{logical_name}' column. "
            f"Available columns: {list(columns)}"
        )
    return resolved


def parse_event_timestamp(timestamp_column: str):
    return coalesce(
        to_timestamp(col(timestamp_column), "MM/dd/yyyy hh:mm:ss a"),
        to_timestamp(col(timestamp_column), "MM/dd/yyyy HH:mm:ss"),
        to_timestamp(col(timestamp_column), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col(timestamp_column), "yyyy-MM-dd'T'HH:mm:ss"),
        to_timestamp(col(timestamp_column)),
    )


def write_output(df: DataFrame, path: str) -> None:
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)
    print(f"Saved result to HDFS: {path}")


def main() -> None:
    spark = (
        SparkSession.builder.appName("ChicagoAirQualitySparkAnalysis")
        .getOrCreate()
    )

    try:
        raw_df = (
            spark.read.option("header", True)
            .option("multiLine", False)
            .option("escape", '"')
            .csv(INPUT_PATH)
        )

        print("Raw schema:")
        raw_df.printSchema()
        print("First five rows:")
        raw_df.show(5, truncate=False)

        normalized_df = rename_columns(raw_df)
        print("Normalized columns:")
        print(normalized_df.columns)

        timestamp_column = require_column(normalized_df.columns, "timestamp")
        sensor_id_column = require_column(normalized_df.columns, "sensor_id")
        pm25_column = require_column(normalized_df.columns, "pm25")
        no2_column = require_column(normalized_df.columns, "no2")
        latitude_column = resolve_column(normalized_df.columns, "latitude")
        longitude_column = resolve_column(normalized_df.columns, "longitude")
        sensor_name_column = resolve_column(normalized_df.columns, "sensor_name")

        print("Detected logical columns:")
        print(
            {
                "timestamp": timestamp_column,
                "sensor_id": sensor_id_column,
                "sensor_name": sensor_name_column,
                "pm25": pm25_column,
                "no2": no2_column,
                "latitude": latitude_column,
                "longitude": longitude_column,
            }
        )

        prepared_df = (
            normalized_df.withColumn("event_ts", parse_event_timestamp(timestamp_column))
            .withColumn("event_date", to_date(col("event_ts")))
            .withColumn("sensor_id", col(sensor_id_column))
            .withColumn("pm25", col(pm25_column).cast("double"))
            .withColumn("no2", col(no2_column).cast("double"))
        )

        if sensor_name_column:
            prepared_df = prepared_df.withColumn("sensor_name", col(sensor_name_column))
        else:
            prepared_df = prepared_df.withColumn("sensor_name", lit(None).cast("string"))

        if latitude_column:
            prepared_df = prepared_df.withColumn(
                "latitude", col(latitude_column).cast("double")
            )
        else:
            prepared_df = prepared_df.withColumn("latitude", lit(None).cast("double"))

        if longitude_column:
            prepared_df = prepared_df.withColumn(
                "longitude", col(longitude_column).cast("double")
            )
        else:
            prepared_df = prepared_df.withColumn("longitude", lit(None).cast("double"))

        clean_df = (
            prepared_df.filter(col("sensor_id").isNotNull())
            .filter(col("event_ts").isNotNull())
            .filter(col("pm25").isNotNull() | col("no2").isNotNull())
        )

        avg_pm25_by_day = (
            clean_df.filter(col("pm25").isNotNull())
            .groupBy("event_date")
            .agg(avg("pm25").alias("avg_pm25"))
            .orderBy("event_date")
        )

        avg_no2_by_sensor = (
            clean_df.filter(col("no2").isNotNull())
            .groupBy("sensor_id")
            .agg(avg("no2").alias("avg_no2"))
            .orderBy("sensor_id")
        )

        top_pm25_sensors = (
            clean_df.filter(col("pm25").isNotNull())
            .groupBy("sensor_id")
            .agg(avg("pm25").alias("avg_pm25"))
            .orderBy(desc("avg_pm25"))
            .limit(10)
        )

        pm25_exceedances = (
            clean_df.filter(col("pm25") > 35.0)
            .groupBy("sensor_id")
            .agg(count("*").alias("exceedance_count"))
            .orderBy(desc("exceedance_count"))
        )

        sensor_map_data = (
            clean_df.groupBy("sensor_id")
            .agg(
                first("sensor_name", ignorenulls=True).alias("sensor_name"),
                avg("pm25").alias("avg_pm25"),
                avg("no2").alias("avg_no2"),
                first("latitude", ignorenulls=True).alias("latitude"),
                first("longitude", ignorenulls=True).alias("longitude"),
            )
            .filter(col("latitude").isNotNull() & col("longitude").isNotNull())
            .orderBy("sensor_id")
        )

        write_output(avg_pm25_by_day, f"{OUTPUT_BASE}/avg_pm25_by_day")
        write_output(avg_no2_by_sensor, f"{OUTPUT_BASE}/avg_no2_by_sensor")
        write_output(top_pm25_sensors, f"{OUTPUT_BASE}/top_pm25_sensors")
        write_output(pm25_exceedances, f"{OUTPUT_BASE}/pm25_exceedances")
        write_output(sensor_map_data, f"{OUTPUT_BASE}/sensor_map_data")

        print("Spark analysis completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
