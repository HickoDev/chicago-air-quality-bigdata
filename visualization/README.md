# Visualization

This folder converts exported Spark result folders into report-ready figures.

## Expected local input folders

Place the copied Spark folders under `visualization/local_results/`:

- `avg_pm25_by_day`
- `avg_no2_by_sensor`
- `pm25_exceedances`
- `sensor_map_data`

## Example copy workflow

Inside `hadoop-master`:

```bash
hdfs dfs -get /chicago/spark_results/avg_pm25_by_day /tmp/avg_pm25_by_day
hdfs dfs -get /chicago/spark_results/avg_no2_by_sensor /tmp/avg_no2_by_sensor
hdfs dfs -get /chicago/spark_results/pm25_exceedances /tmp/pm25_exceedances
hdfs dfs -get /chicago/spark_results/sensor_map_data /tmp/sensor_map_data
```

On the Windows host:

```powershell
docker cp hadoop-master:/tmp/avg_pm25_by_day .\visualization\local_results\avg_pm25_by_day
docker cp hadoop-master:/tmp/avg_no2_by_sensor .\visualization\local_results\avg_no2_by_sensor
docker cp hadoop-master:/tmp/pm25_exceedances .\visualization\local_results\pm25_exceedances
docker cp hadoop-master:/tmp/sensor_map_data .\visualization\local_results\sensor_map_data
```

## Run

```powershell
python visualization\visualize_results.py
```

## Generated outputs

- `pm25_by_day.png`
- `no2_by_sensor_top10.png`
- `pm25_exceedances_top10.png`
- `pollution_sensor_map.html`
