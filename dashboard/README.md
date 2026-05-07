# Dashboard

This folder contains a Streamlit dashboard for the unified Chicago air-quality pipeline.

The dashboard has two views:

- Live Kafka events from `air_quality_stream`
- Exported MapReduce results copied from HDFS to `dashboard/local_results`

## Install

```powershell
python -m pip install -r dashboard\requirements.txt
```

## Start the Dashboard

```powershell
streamlit run dashboard\streamlit_dashboard.py -- --from-beginning
```

Open:

```text
http://localhost:8501
```

## Live Kafka View

Start Kafka and produce events:

```powershell
docker compose -f streaming/docker-compose.kafka.yml up -d

python streaming\kafka_producer_simulator.py --csv "%USERPROFILE%\Downloads\Open_Air_Chicago_Individual_Measurements.csv" --limit 100 --delay 0.5
```

Then use the dashboard's **Live Kafka Events** tab and click **Poll Kafka now**.

## MapReduce Results View

After MapReduce finishes, export the result folders from HDFS inside `hadoop-master`:

```bash
hdfs dfs -get /chicago/output/avg_pm25_by_day /tmp/avg_pm25_by_day
hdfs dfs -get /chicago/output/avg_no2_by_sensor /tmp/avg_no2_by_sensor
hdfs dfs -get /chicago/output/pm25_exceedances /tmp/pm25_exceedances
```

Copy them to Windows:

```powershell
docker cp hadoop-master:/tmp/avg_pm25_by_day .\dashboard\local_results\avg_pm25_by_day
docker cp hadoop-master:/tmp/avg_no2_by_sensor .\dashboard\local_results\avg_no2_by_sensor
docker cp hadoop-master:/tmp/pm25_exceedances .\dashboard\local_results\pm25_exceedances
```

Refresh the **MapReduce Results** tab to view the charts.
