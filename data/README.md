# Data Folder

This folder stores helper assets for preparing the Chicago air quality dataset.

## Source dataset

- Dataset: Open Air Chicago Individual Measurements
- Direct CSV URL: `https://data.cityofchicago.org/api/views/xfya-dxtq/rows.csv?accessType=DOWNLOAD`
- Example local dataset path:
  `<PATH_TO_DOWNLOADED_CSV>\Open_Air_Chicago_Individual_Measurements.csv`

## Notes

- Do not commit the full raw CSV file to Git.
- Use `sample_generation.py` to produce a manageable sample for local testing.
- Inspect the raw CSV header before running MapReduce or HBase imports if the data portal schema changes.
