# Visualization

This folder converts exported MapReduce result folders into report-ready figures.

The dashboard in `dashboard/` is the recommended interactive view. This folder is kept for static PNG/HTML outputs that can be inserted into the final report.

## Expected Local Input Folders

Place the copied HDFS folders under `visualization/local_results/`:

- `avg_pm25_by_day`
- `avg_no2_by_sensor`
- `pm25_exceedances`
- optional: `air_quality_events` for the sensor map

## Example Copy Workflow

Inside `hadoop-master`:

```bash
hdfs dfs -get /chicago/output/avg_pm25_by_day /tmp/avg_pm25_by_day
hdfs dfs -get /chicago/output/avg_no2_by_sensor /tmp/avg_no2_by_sensor
hdfs dfs -get /chicago/output/pm25_exceedances /tmp/pm25_exceedances
hdfs dfs -get /chicago/streaming/bronze/air_quality_events /tmp/air_quality_events
```

On the Windows host:

```powershell
docker cp hadoop-master:/tmp/avg_pm25_by_day .\visualization\local_results\avg_pm25_by_day
docker cp hadoop-master:/tmp/avg_no2_by_sensor .\visualization\local_results\avg_no2_by_sensor
docker cp hadoop-master:/tmp/pm25_exceedances .\visualization\local_results\pm25_exceedances
docker cp hadoop-master:/tmp/air_quality_events .\visualization\local_results\air_quality_events
```

## Run

```powershell
python visualization\visualize_results.py
```

## Generated Outputs

- `pm25_by_day.png`
- `no2_by_sensor_top10.png`
- `pm25_exceedances_top10.png`
- `pollution_sensor_map.html`
