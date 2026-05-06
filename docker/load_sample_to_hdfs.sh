#!/bin/bash
set -euo pipefail

hdfs dfs -mkdir -p /chicago/input
hdfs dfs -rm -f /chicago/input/open_air_chicago.csv || true
hdfs dfs -put /root/open_air_chicago.csv /chicago/input/

echo "Loaded /root/open_air_chicago.csv to hdfs:///chicago/input/open_air_chicago.csv"
