# chicago-air-quality-bigdata

Academic Big Data project based on Chicago air quality measurements. The repository implements a full pipeline:

`CSV -> HDFS -> MapReduce -> Spark -> HBase -> visualizations -> report-ready outputs`

## 1. Project Objective

The objective is to build an end-to-end Big Data workflow for environmental measurements collected in Chicago. The project uses the professor's TP environment with Docker Desktop and the image `liliasfaxi/hadoop-cluster:latest`.

Main goals:

- store the CSV dataset in HDFS
- run Java MapReduce jobs with Maven-built artifacts
- run Spark analysis on YARN
- prepare and load selected data into HBase
- generate charts and a map for the final report

## 2. Dataset Used

- Dataset name: Open Air Chicago Individual Measurements
- Portal page: Chicago Data Portal
- Direct CSV URL: `https://data.cityofchicago.org/api/views/xfya-dxtq/rows.csv?accessType=DOWNLOAD`
- Example local file path:
  `<PATH_TO_DOWNLOADED_CSV>\Open_Air_Chicago_Individual_Measurements.csv`

Current important columns observed in the downloaded file:

- `datasourceid`: sensor identifier
- `time`: measurement timestamp
- `no2ConcIndividual.value`: NO2 measurement
- `pm2_5ConcMassIndividual.value`: PM2.5 measurement
- `temperatureAmbientIndividual.value`: ambient temperature
- `relHumidAmbientIndividual.value`: ambient humidity
- `latitude`, `longitude`: coordinates

If the portal schema changes later, update the column constants in the Java jobs and the candidate lists in the Spark and HBase scripts.

## 3. Technical Environment

- Windows host with Docker Desktop
- Docker image: `liliasfaxi/hadoop-cluster:latest`
- Hadoop cluster containers:
  - `hadoop-master`
  - `hadoop-worker1`
  - `hadoop-worker2`
- HDFS
- YARN
- Java 8
- Hadoop dependencies: 3.3.6
- Spark: 3.5.0
- HBase: 2.5.8
- Python for data preparation and visualization

## 4. Repository Structure

```text
chicago-air-quality-bigdata/
|
|- README.md
|- docker/
|  \- commands.md
|- data/
|  |- README.md
|  \- sample_generation.py
|- mapreduce/
|  \- chicago-air-mapreduce/
|     |- pom.xml
|     \- src/main/java/hadoop/mapreduce/chicago/
|        |- AveragePM25ByDay.java
|        |- AverageNO2BySensor.java
|        \- PM25ThresholdBySensor.java
|- spark/
|  |- chicago_spark_analysis.py
|  \- README.md
|- hbase/
|  |- create_table.hbase
|  |- import_tsv_command.sh
|  |- prepare_hbase_csv.py
|  \- README.md
|- visualization/
|  |- visualize_results.py
|  \- README.md
\- report/
   \- report_plan.md
```

## 5. Environment Setup

### Pull the required image

```powershell
docker pull liliasfaxi/hadoop-cluster:latest
```

### Create the Docker network

```powershell
docker network create --driver=bridge hadoop
```

### Start the Hadoop cluster containers

```powershell
docker run -itd --net=hadoop -p 9870:9870 -p 8088:8088 -p 7077:7077 -p 16010:16010 --name hadoop-master --hostname hadoop-master liliasfaxi/hadoop-cluster:latest

docker run -itd -p 8040:8042 --net=hadoop --name hadoop-worker1 --hostname hadoop-worker1 liliasfaxi/hadoop-cluster:latest

docker run -itd -p 8041:8042 --net=hadoop --name hadoop-worker2 --hostname hadoop-worker2 liliasfaxi/hadoop-cluster:latest
```

### Start existing containers later

```powershell
docker start hadoop-master hadoop-worker1 hadoop-worker2
```

### Enter the master container

```powershell
docker exec -it hadoop-master bash
```

### Start Hadoop services

```bash
./start-hadoop.sh
```

### Validate the services

```bash
jps
```

Expected Hadoop web interfaces:

- Hadoop NameNode: http://localhost:9870
- YARN ResourceManager: http://localhost:8088
- HBase UI: http://localhost:16010

## 6. Data Preparation

### Download the dataset

Download the CSV from the portal or use the direct link:

```text
https://data.cityofchicago.org/api/views/xfya-dxtq/rows.csv?accessType=DOWNLOAD
```

### Create a sample file for local tests

The sampling script reads the large CSV safely and creates a `100000`-row sample.

```powershell
python data\sample_generation.py --input "<PATH_TO_DOWNLOADED_CSV>\Open_Air_Chicago_Individual_Measurements.csv"
```

Output:

- `data/open_air_chicago_sample.csv`

The script also prints the detected column names and a preview of source rows.

## 7. Upload the Dataset to HDFS

### Copy the raw CSV into `hadoop-master`

```powershell
docker cp "<PATH_TO_DOWNLOADED_CSV>\Open_Air_Chicago_Individual_Measurements.csv" hadoop-master:/root/open_air_chicago.csv
```

### Inside `hadoop-master`, create the HDFS input folder

```bash
hdfs dfs -mkdir -p /chicago/input
```

### Upload the dataset from `/root` to HDFS

```bash
hdfs dfs -put /root/open_air_chicago.csv /chicago/input/
```

### Optional validation

```bash
hdfs dfs -ls /chicago/input
hdfs dfs -head /chicago/input/open_air_chicago.csv
```

## 8. Build the MapReduce JAR

The Maven project is configured for Java 8 and Hadoop 3.3.6, and uses the Maven Assembly Plugin to create a `jar-with-dependencies`.

### Build locally on Windows

```powershell
cd mapreduce\chicago-air-mapreduce
mvn clean package
```

Expected artifact:

- `mapreduce/chicago-air-mapreduce/target/chicago-mapreduce.jar`

### Copy the JAR to `hadoop-master`

```powershell
docker cp mapreduce\chicago-air-mapreduce\target\chicago-mapreduce.jar hadoop-master:/root/chicago-mapreduce.jar
```

## 9. Run MapReduce Jobs

Enter the master container if needed:

```powershell
docker exec -it hadoop-master bash
```

### Job 1: Average PM2.5 by day

```bash
hadoop jar /root/chicago-mapreduce.jar hadoop.mapreduce.chicago.AveragePM25ByDay /chicago/input/open_air_chicago.csv /chicago/output/avg_pm25_by_day
```

Expected output format:

```text
date<TAB>average_pm25
```

### Job 2: Average NO2 by sensor

```bash
hadoop jar /root/chicago-mapreduce.jar hadoop.mapreduce.chicago.AverageNO2BySensor /chicago/input/open_air_chicago.csv /chicago/output/avg_no2_by_sensor
```

Expected output format:

```text
sensor_id<TAB>average_no2
```

### Job 3: PM2.5 threshold exceedances by sensor

Threshold used:

- `PM2.5 > 35.0`

Run:

```bash
hadoop jar /root/chicago-mapreduce.jar hadoop.mapreduce.chicago.PM25ThresholdBySensor /chicago/input/open_air_chicago.csv /chicago/output/pm25_threshold
```

Expected output format:

```text
sensor_id<TAB>exceedance_count
```

### Show output in HDFS

```bash
hdfs dfs -cat /chicago/output/avg_pm25_by_day/part-r-00000 | head
```

Useful output checks:

```bash
hdfs dfs -cat /chicago/output/avg_no2_by_sensor/part-r-00000 | head
hdfs dfs -cat /chicago/output/pm25_threshold/part-r-00000 | head
```

## 10. Run Spark Analysis

### Copy the Spark script to `hadoop-master`

```powershell
docker cp spark\chicago_spark_analysis.py hadoop-master:/root/chicago_spark_analysis.py
```

### Run Spark on YARN

```bash
spark-submit --master yarn --deploy-mode client /root/chicago_spark_analysis.py
```

The script:

- prints the raw schema
- shows the first 5 rows
- normalizes column names
- detects timestamp, sensor, PM2.5, NO2, latitude, and longitude columns
- removes unusable rows
- computes the required aggregations
- saves results into HDFS

Expected HDFS outputs:

- `/chicago/spark_results/avg_pm25_by_day`
- `/chicago/spark_results/avg_no2_by_sensor`
- `/chicago/spark_results/top_pm25_sensors`
- `/chicago/spark_results/pm25_exceedances`
- `/chicago/spark_results/sensor_map_data`

Optional checks:

```bash
hdfs dfs -ls /chicago/spark_results
hdfs dfs -cat /chicago/spark_results/avg_pm25_by_day/part* | head
```

## 11. Start HBase and Create the Table

### Start HBase in `hadoop-master`

```bash
start-hbase.sh
hbase shell
```

### Create the table

The repository includes `hbase/create_table.hbase`:

```text
create 'chicago_air_quality', 'm', 'l', 't'
```

Families:

- `m`: measurements
- `l`: location
- `t`: time

Planned logical qualifiers:

- `m:pm25`
- `m:no2`
- `m:temperature`
- `m:humidity`
- `l:latitude`
- `l:longitude`
- `t:date`
- `t:hour`

## 12. Prepare and Import Data to HBase

### Prepare the HBase CSV locally

The HBase helper creates a headerless CSV whose first column is the row key.

```powershell
python hbase\prepare_hbase_csv.py --input data\open_air_chicago_sample.csv
```

Output:

- `data/chicago_hbase_clean.csv`

Produced column order:

```text
HBASE_ROW_KEY,pm25,no2,latitude,longitude,date,hour
```

The row key format is:

```text
sensorId_timestamp
```

### Copy HBase assets to the container

```powershell
docker cp data\chicago_hbase_clean.csv hadoop-master:/root/chicago_hbase_clean.csv
docker cp hbase\create_table.hbase hadoop-master:/root/create_table.hbase
docker cp hbase\import_tsv_command.sh hadoop-master:/root/import_tsv_command.sh
```

### Run the import inside `hadoop-master`

```bash
start-hbase.sh
cd /root
chmod +x import_tsv_command.sh
./import_tsv_command.sh
```

The import script executes:

```bash
hdfs dfs -mkdir -p /chicago/hbase_input
hdfs dfs -put /root/chicago_hbase_clean.csv /chicago/hbase_input/

hbase shell < create_table.hbase

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
-Dimporttsv.separator=',' \
-Dimporttsv.columns=HBASE_ROW_KEY,m:pm25,m:no2,l:latitude,l:longitude,t:date,t:hour \
chicago_air_quality /chicago/hbase_input/chicago_hbase_clean.csv
```

### Verify in HBase

```bash
scan 'chicago_air_quality', {LIMIT => 10}
```

## 13. Create Visualizations

### Export Spark result folders from HDFS to the container filesystem

Inside `hadoop-master`:

```bash
hdfs dfs -get /chicago/spark_results/avg_pm25_by_day /tmp/avg_pm25_by_day
hdfs dfs -get /chicago/spark_results/avg_no2_by_sensor /tmp/avg_no2_by_sensor
hdfs dfs -get /chicago/spark_results/pm25_exceedances /tmp/pm25_exceedances
hdfs dfs -get /chicago/spark_results/sensor_map_data /tmp/sensor_map_data
```

### Copy those folders from Docker to the Windows host

```powershell
docker cp hadoop-master:/tmp/avg_pm25_by_day .\visualization\local_results\avg_pm25_by_day
docker cp hadoop-master:/tmp/avg_no2_by_sensor .\visualization\local_results\avg_no2_by_sensor
docker cp hadoop-master:/tmp/pm25_exceedances .\visualization\local_results\pm25_exceedances
docker cp hadoop-master:/tmp/sensor_map_data .\visualization\local_results\sensor_map_data
```

### Generate the final plots

```powershell
python visualization\visualize_results.py
```

Generated files:

- `visualization/pm25_by_day.png`
- `visualization/no2_by_sensor_top10.png`
- `visualization/pm25_exceedances_top10.png`
- `visualization/pollution_sensor_map.html`

## 14. Validation Checklist

Use this checklist before submission:

- Docker containers start correctly: `hadoop-master`, `hadoop-worker1`, `hadoop-worker2`
- `./start-hadoop.sh` runs without errors
- `jps` shows NameNode, DataNode, ResourceManager, NodeManager, and related Hadoop services
- HDFS contains `/chicago/input/open_air_chicago.csv`
- Maven builds `target/chicago-mapreduce.jar`
- The three MapReduce jobs complete successfully on Hadoop
- HDFS output folders exist under `/chicago/output/`
- `spark-submit` finishes and writes all Spark result folders under `/chicago/spark_results/`
- `start-hbase.sh` launches HBase services
- HBase table `chicago_air_quality` is created
- `ImportTsv` loads data successfully
- `scan 'chicago_air_quality', {LIMIT => 10}` returns rows
- Visualization files are generated locally
- Screenshots of Hadoop UI, YARN UI, HBase UI, HDFS folders, MapReduce outputs, Spark outputs, and final graphs are captured for the report

## Notes

- The full raw CSV is intentionally excluded from Git.
- Generated CSV files, JAR files, plots, HTML outputs, and Python cache files are ignored by `.gitignore`.
- The Java jobs use fixed column indexes documented at the top of each class because split-aware header parsing is not reliable in basic MapReduce CSV processing.
