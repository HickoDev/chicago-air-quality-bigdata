# HBase Serving Layer

HBase stores selected air-quality measurements after the stream has been written to HDFS.

Pipeline position:

```text
Kafka -> Spark Structured Streaming -> HDFS -> HBase preparation -> ImportTsv -> HBase
```

## Table Design

Table:

```text
chicago_air_quality
```

Column families:

- `m`: measurements
- `l`: location
- `t`: time

Row key:

```text
sensorId_timestamp
```

Columns imported:

- `m:pm25`
- `m:no2`
- `l:latitude`
- `l:longitude`
- `t:date`
- `t:hour`

## Create the Table

The table definition is in `create_table.hbase`:

```text
create 'chicago_air_quality', 'm', 'l', 't'
```

## Prepare ImportTsv Input

The preparation script accepts either:

- the original/sample CSV
- an exported Spark Streaming JSON folder from HDFS

Recommended unified-pipeline input:

```powershell
docker exec hadoop-master bash -lc "rm -rf /tmp/air_quality_events && hdfs dfs -get /chicago/streaming/bronze/air_quality_events /tmp/air_quality_events"
docker cp hadoop-master:/tmp/air_quality_events .\data\air_quality_events
python hbase\prepare_hbase_csv.py --input data\air_quality_events --output data\chicago_hbase_clean.csv
```

Output:

```text
data/chicago_hbase_clean.csv
```

This file is headerless and ordered for ImportTsv:

```text
HBASE_ROW_KEY,pm25,no2,latitude,longitude,date,hour
```

## Import into HBase

Copy files:

```powershell
docker cp data\chicago_hbase_clean.csv hadoop-master:/root/chicago_hbase_clean.csv
docker cp hbase\create_table.hbase hadoop-master:/root/create_table.hbase
docker cp hbase\import_tsv_command_local.sh hadoop-master:/root/import_tsv_command_local.sh
```

Run:

```powershell
docker exec hadoop-master bash -lc "start-hbase.sh"
docker exec hadoop-master bash -lc "chmod +x /root/import_tsv_command_local.sh && cd /root && ./import_tsv_command_local.sh"
```

Verify:

```powershell
docker exec hadoop-master bash -lc "printf \"scan 'chicago_air_quality', {LIMIT => 10}\nexit\n\" | hbase shell -n"
```
