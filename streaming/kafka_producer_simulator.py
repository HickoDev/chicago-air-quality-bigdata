from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional

from kafka import KafkaProducer


DEFAULT_TOPIC = "air_quality_stream"
DEFAULT_BOOTSTRAP_SERVER = "localhost:9092"

COLUMN_CANDIDATES = {
    "event_time": [
        "timestamp",
        "measurement_time",
        "measurement time",
        "measurement_date",
        "measurement date",
        "date",
        "time",
    ],
    "sensor_id": [
        "sensor_id",
        "sensor id",
        "site_id",
        "site id",
        "node_id",
        "node id",
        "device_id",
        "device id",
        "datasourceid",
    ],
    "pm25": [
        "pm25",
        "pm2.5",
        "pm2_5",
        "pm2.5 value",
        "pm2_5 value",
        "pm2_5concmassindividual.value",
        "pm2_5concmassindividual_value",
    ],
    "no2": [
        "no2",
        "no2 value",
        "no2.value",
        "no2concindividual.value",
        "no2concindividual_value",
    ],
    "temperature": [
        "temperature",
        "temp",
        "temperatureambientindividual.value",
        "temperatureambientindividual_value",
    ],
    "humidity": [
        "humidity",
        "relative_humidity",
        "relative humidity",
        "relhumidambientindividual.value",
        "relhumidambientindividual_value",
    ],
    "latitude": [
        "latitude",
        "lat",
    ],
    "longitude": [
        "longitude",
        "lon",
        "lng",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Simulate real-time Chicago air-quality events from a CSV file."
    )
    parser.add_argument("--csv", required=True, type=Path, help="Input CSV file path.")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic name.")
    parser.add_argument(
        "--bootstrap-server",
        default=DEFAULT_BOOTSTRAP_SERVER,
        help="Kafka bootstrap server, for example localhost:9092.",
    )
    parser.add_argument(
        "--delay",
        default=0.5,
        type=float,
        help="Delay in seconds between emitted events.",
    )
    parser.add_argument(
        "--limit",
        default=100,
        type=int,
        help="Maximum number of rows to send. Use 0 for no limit.",
    )
    return parser.parse_args()


def normalize_column_name(value: str) -> str:
    return "".join(character.lower() for character in value if character.isalnum())


def build_lookup(fieldnames: Iterable[str]) -> Dict[str, str]:
    return {normalize_column_name(field): field for field in fieldnames}


def resolve_column(fieldnames: Iterable[str], logical_name: str) -> Optional[str]:
    fields = list(fieldnames)
    lookup = build_lookup(fields)

    for candidate in COLUMN_CANDIDATES[logical_name]:
        normalized = normalize_column_name(candidate)
        if normalized in lookup:
            return lookup[normalized]

    for candidate in COLUMN_CANDIDATES[logical_name]:
        normalized_candidate = normalize_column_name(candidate)
        for field in fields:
            normalized_field = normalize_column_name(field)
            if normalized_candidate in normalized_field:
                return field

    return None


def detect_columns(fieldnames: Iterable[str]) -> Dict[str, Optional[str]]:
    fields = list(fieldnames)
    return {
        logical_name: resolve_column(fields, logical_name)
        for logical_name in COLUMN_CANDIDATES
    }


def clean_string(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None

    cleaned = value.strip()
    if not cleaned or cleaned.lower() in {"null", "none", "nan"}:
        return None
    return cleaned


def clean_float(value: Optional[str]) -> Optional[float]:
    cleaned = clean_string(value)
    if cleaned is None:
        return None

    try:
        return float(cleaned)
    except ValueError:
        return None


def get_value(row: Dict[str, str], mapping: Dict[str, Optional[str]], logical_name: str) -> Optional[str]:
    column = mapping.get(logical_name)
    if column is None:
        return None
    return row.get(column)


def build_event(row: Dict[str, str], mapping: Dict[str, Optional[str]]) -> Dict[str, object]:
    sensor_id = clean_string(get_value(row, mapping, "sensor_id")) or "unknown_sensor"

    # Each historical CSV row is converted into a live event with a new ingestion timestamp.
    return {
        "event_time": clean_string(get_value(row, mapping, "event_time")),
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
        "sensor_id": sensor_id,
        "pm25": clean_float(get_value(row, mapping, "pm25")),
        "no2": clean_float(get_value(row, mapping, "no2")),
        "temperature": clean_float(get_value(row, mapping, "temperature")),
        "humidity": clean_float(get_value(row, mapping, "humidity")),
        "latitude": clean_float(get_value(row, mapping, "latitude")),
        "longitude": clean_float(get_value(row, mapping, "longitude")),
    }


def create_producer(bootstrap_server: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_server,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=3,
    )


def stream_csv(args: argparse.Namespace) -> None:
    if not args.csv.exists():
        raise FileNotFoundError(f"CSV file not found: {args.csv}")

    csv.field_size_limit(min(sys.maxsize, 2_147_483_647))

    with args.csv.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise ValueError("CSV file does not contain a header row.")

        mapping = detect_columns(reader.fieldnames)
        print("Detected column mapping:")
        for logical_name, column_name in mapping.items():
            print(f"  {logical_name}: {column_name or '<missing>'}")

        producer = create_producer(args.bootstrap_server)
        sent_count = 0

        try:
            for row in reader:
                if args.limit > 0 and sent_count >= args.limit:
                    break

                event = build_event(row, mapping)
                producer.send(args.topic, key=str(event["sensor_id"]), value=event)
                sent_count += 1
                print(f"Sent message {sent_count}: {event}")

                if args.delay > 0:
                    time.sleep(args.delay)
        finally:
            producer.flush()
            producer.close()

    print(f"Finished sending {sent_count} messages to topic '{args.topic}'.")


def main() -> None:
    args = parse_args()
    stream_csv(args)


if __name__ == "__main__":
    main()
