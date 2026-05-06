#!/bin/bash
set -e

hdfs dfs -mkdir -p /chicago/hbase_input
hdfs dfs -put /root/chicago_hbase_clean.csv /chicago/hbase_input/

hbase shell < create_table.hbase

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
-Dimporttsv.separator=',' \
-Dimporttsv.columns=HBASE_ROW_KEY,m:pm25,m:no2,l:latitude,l:longitude,t:date,t:hour \
chicago_air_quality /chicago/hbase_input/chicago_hbase_clean.csv

# Verification command to run inside the HBase shell:
# scan 'chicago_air_quality', {LIMIT => 10}
