# Spark Analysis

This module runs the Chicago air quality analysis with Spark 3.5.0 on YARN.

## Input

- HDFS input file: `hdfs:///chicago/input/open_air_chicago.csv`

## Outputs

- `/chicago/spark_results/avg_pm25_by_day`
- `/chicago/spark_results/avg_no2_by_sensor`
- `/chicago/spark_results/top_pm25_sensors`
- `/chicago/spark_results/pm25_exceedances`
- `/chicago/spark_results/sensor_map_data`

## Run from `hadoop-master`

```bash
spark-submit --master yarn --deploy-mode client /root/chicago_spark_analysis.py
```

The script normalizes column names, detects the main fields used in the project, casts numeric columns, and writes compact CSV outputs back to HDFS.
