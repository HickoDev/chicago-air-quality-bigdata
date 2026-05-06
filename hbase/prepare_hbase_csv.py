from __future__ import annotations

import argparse
import csv
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Optional


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
        description="Prepare a headerless CSV for HBase ImportTsv."
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="Input CSV file. Defaults to the sample file when available.",
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
            raise FileNotFoundError(f"Input CSV not found: {explicit_path}")
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


def clean_value(value: Optional[str]) -> str:
    cleaned = (value or "").strip()
    if cleaned.lower() == "null":
        return ""
    return cleaned


def build_row_key(sensor_id: str, timestamp_value: datetime) -> str:
    safe_sensor = re.sub(r"[^0-9A-Za-z_-]+", "_", sensor_id.strip())
    return f"{safe_sensor}_{timestamp_value.strftime('%Y%m%dT%H%M%S')}"


def prepare_hbase_csv(input_path: Path, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

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

        row_count = 0
        skipped_count = 0

        with output_path.open("w", encoding="utf-8", newline="") as target:
            writer = csv.writer(target)

            for row in reader:
                sensor_id = clean_value(row.get(sensor_column))
                timestamp = parse_timestamp(clean_value(row.get(timestamp_column)))

                if not sensor_id or timestamp is None:
                    skipped_count += 1
                    continue

                row_key = build_row_key(sensor_id, timestamp)
                writer.writerow(
                    [
                        row_key,
                        clean_value(row.get(pm25_column)),
                        clean_value(row.get(no2_column)),
                        clean_value(row.get(latitude_column)),
                        clean_value(row.get(longitude_column)),
                        timestamp.strftime("%Y-%m-%d"),
                        timestamp.strftime("%H"),
                    ]
                )
                row_count += 1

    print(f"Input file: {input_path}")
    print(f"Output file: {output_path}")
    print(f"Rows written: {row_count:,}")
    print(f"Rows skipped (missing sensor or timestamp): {skipped_count:,}")


def main() -> None:
    args = parse_args()
    input_path = resolve_input_path(args.input)
    prepare_hbase_csv(input_path, args.output)


if __name__ == "__main__":
    main()
