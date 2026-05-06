# HBase Layer

This folder contains the assets required to load Chicago air quality measurements into HBase 2.5.8.

## Files

- `create_table.hbase`: creates the `chicago_air_quality` table with column families `m`, `l`, and `t`
- `prepare_hbase_csv.py`: converts the source CSV into a headerless ImportTsv-friendly file
- `import_tsv_command.sh`: imports the prepared CSV from HDFS into HBase

## Expected row design

- Row key: `sensorId_timestamp`
- Measurement family: `m`
- Location family: `l`
- Time family: `t`

## Typical workflow

```powershell
python hbase\prepare_hbase_csv.py --input data\open_air_chicago_sample.csv
docker cp data\chicago_hbase_clean.csv hadoop-master:/root/chicago_hbase_clean.csv
docker cp hbase\create_table.hbase hadoop-master:/root/create_table.hbase
docker cp hbase\import_tsv_command.sh hadoop-master:/root/import_tsv_command.sh
```

Then, inside `hadoop-master`:

```bash
start-hbase.sh
chmod +x /root/import_tsv_command.sh
cd /root
./import_tsv_command.sh
```
