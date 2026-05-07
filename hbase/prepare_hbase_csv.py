from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "chicago_hbase_clean.csv"
DEFAULT_INPUT_CANDIDATES = [
    PROJECT_ROOT / "data" / "open_air_chicago_sample.csv",
    PROJECT_ROOT / "data" / "open_air_chicago.csv",
]

COLUMN_CANDIDATES = {
    "timestamp": [
        "time",
        "timestamp",
        "measurement_time",
        "measurementdate",
        "measurement_date",
        "date",
    ],
    "sensor_id": [
        "datasourceid",
        "sensor_id",
        "sensorid",
        "node_id",
        "site_id",
    ],
    "pm25": [
        "pm2_5concmassindividual_value",
        "pm25",
        "pm2_5",
        "pm2_5_value",
    ],
    "no2": [
        "no2concindividual_value",
        "no2",
        "no2_value",
    ],
    "latitude": [
        "latitude",
        "lat",
    ],
    "longitude": [
        "longitude",
        "lon",
    ],
}

TIMESTAMP_PATTERNS = [
    "%m/%d/%Y %I:%M:%S %p",
    "%m/%d/%Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare a headerless CSV for HBase ImportTsv from CSV or streamed JSON events."
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="Input CSV file or exported Spark Streaming JSON folder. Defaults to the sample file when available.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output CSV for HBase ImportTsv.",
    )
    return parser.parse_args()


def normalize_column_name(name: str) -> str:
    normalized = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


def resolve_input_path(explicit_path: Optional[Path]) -> Path:
    if explicit_path:
        if not explicit_path.exists():
            raise FileNotFoundError(f"Input path not found: {explicit_path}")
        return explicit_path

    for candidate in DEFAULT_INPUT_CANDIDATES:
        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        "No input CSV found. Provide --input or create data/open_air_chicago_sample.csv."
    )


def resolve_column(columns: Iterable[str], logical_name: str) -> Optional[str]:
    available = list(columns)
    candidates = COLUMN_CANDIDATES[logical_name]

    for candidate in candidates:
        if candidate in available:
            return candidate

    for candidate in candidates:
        for column_name in available:
            if candidate in column_name:
                return column_name

    return None


def require_column(columns: Iterable[str], logical_name: str) -> str:
    resolved = resolve_column(columns, logical_name)
    if resolved is None:
        raise ValueError(
            f"Missing required column '{logical_name}'. Available columns: {list(columns)}"
        )
    return resolved


def parse_timestamp(value: str) -> Optional[datetime]:
    cleaned = (value or "").strip()
    if not cleaned:
        return None

    for pattern in TIMESTAMP_PATTERNS:
        try:
            return datetime.strptime(cleaned, pattern)
        except ValueError:
            continue

    return None


def clean_value(value: Optional[object]) -> str:
    cleaned = "" if value is None else str(value).strip()
    if cleaned.lower() == "null":
        return ""
    return cleaned


def build_row_key(sensor_id: str, timestamp_value: datetime) -> str:
    safe_sensor = re.sub(r"[^0-9A-Za-z_-]+", "_", sensor_id.strip())
    return f"{safe_sensor}_{timestamp_value.strftime('%Y%m%dT%H%M%S')}"


def iter_input_files(input_path: Path) -> Iterator[Path]:
    if input_path.is_file():
        yield input_path
        return

    for file_path in sorted(input_path.rglob("*")):
        if any(part.startswith(("_", ".")) for part in file_path.relative_to(input_path).parts):
            continue
        if file_path.is_file() and file_path.name.startswith("part"):
            yield file_path


def iter_csv_records(input_path: Path) -> Iterator[Dict[str, str]]:
    with input_path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise ValueError("Input CSV does not contain a header row.")

        normalized_to_original: Dict[str, str] = {
            normalize_column_name(field_name): field_name
            for field_name in reader.fieldnames
        }

        timestamp_column = normalized_to_original[
            require_column(normalized_to_original.keys(), "timestamp")
        ]
        sensor_column = normalized_to_original[
            require_column(normalized_to_original.keys(), "sensor_id")
        ]
        pm25_column = normalized_to_original[
            require_column(normalized_to_original.keys(), "pm25")
        ]
        no2_column = normalized_to_original[
            require_column(normalized_to_original.keys(), "no2")
        ]
        latitude_column = normalized_to_original[
            require_column(normalized_to_original.keys(), "latitude")
        ]
        longitude_column = normalized_to_original[
            require_column(normalized_to_original.keys(), "longitude")
        ]

        for row in reader:
            yield {
                "timestamp": clean_value(row.get(timestamp_column)),
                "sensor_id": clean_value(row.get(sensor_column)),
                "pm25": clean_value(row.get(pm25_column)),
                "no2": clean_value(row.get(no2_column)),
                "latitude": clean_value(row.get(latitude_column)),
                "longitude": clean_value(row.get(longitude_column)),
            }


def iter_json_records(input_path: Path) -> Iterator[Dict[str, str]]:
    for file_path in iter_input_files(input_path):
        with file_path.open("r", encoding="utf-8") as source:
            for line_number, line in enumerate(source, start=1):
                cleaned = line.strip()
                if not cleaned:
                    continue

                try:
                    row = json.loads(cleaned)
                except json.JSONDecodeError as exception:
                    raise ValueError(
                        f"Invalid JSON in {file_path} at line {line_number}: {exception}"
                    ) from exception

                yield {
                    "timestamp": clean_value(row.get("event_time") or row.get("timestamp")),
                    "sensor_id": clean_value(row.get("sensor_id")),
                    "pm25": clean_value(row.get("pm25")),
                    "no2": clean_value(row.get("no2")),
                    "latitude": clean_value(row.get("latitude")),
                    "longitude": clean_value(row.get("longitude")),
                }


def iter_normalized_records(input_path: Path) -> Iterator[Dict[str, str]]:
    if input_path.is_dir():
        yield from iter_json_records(input_path)
        return

    with input_path.open("r", encoding="utf-8-sig") as source:
        first_non_empty = ""
        for line in source:
            first_non_empty = line.strip()
            if first_non_empty:
                break

    if first_non_empty.startswith("{"):
        yield from iter_json_records(input_path)
    else:
        yield from iter_csv_records(input_path)


def prepare_hbase_csv(input_path: Path, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    row_count = 0
    skipped_count = 0

    with output_path.open("w", encoding="utf-8", newline="") as target:
        writer = csv.writer(target)

        for row in iter_normalized_records(input_path):
            sensor_id = clean_value(row.get("sensor_id"))
            timestamp = parse_timestamp(clean_value(row.get("timestamp")))

            if not sensor_id or timestamp is None:
                skipped_count += 1
                continue

            row_key = build_row_key(sensor_id, timestamp)
            writer.writerow(
                [
                    row_key,
                    clean_value(row.get("pm25")),
                    clean_value(row.get("no2")),
                    clean_value(row.get("latitude")),
                    clean_value(row.get("longitude")),
                    timestamp.strftime("%Y-%m-%d"),
                    timestamp.strftime("%H"),
                ]
            )
            row_count += 1

    print(f"Input path: {input_path}")
    print(f"Output file: {output_path}")
    print(f"Rows written: {row_count:,}")
    print(f"Rows skipped (missing sensor or timestamp): {skipped_count:,}")


def main() -> None:
    args = parse_args()
    input_path = resolve_input_path(args.input)
    prepare_hbase_csv(input_path, args.output)


if __name__ == "__main__":
    main()
