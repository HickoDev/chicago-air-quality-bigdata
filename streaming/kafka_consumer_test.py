from __future__ import annotations

import argparse
import json

from kafka import KafkaConsumer


DEFAULT_TOPIC = "air_quality_stream"
DEFAULT_BOOTSTRAP_SERVER = "localhost:9092"
DEFAULT_GROUP_ID = "air-quality-test-consumer"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume simulated air-quality events from Kafka for validation."
    )
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic name.")
    parser.add_argument(
        "--bootstrap-server",
        default=DEFAULT_BOOTSTRAP_SERVER,
        help="Kafka bootstrap server, for example localhost:9092.",
    )
    parser.add_argument(
        "--group-id",
        default=DEFAULT_GROUP_ID,
        help="Kafka consumer group ID.",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Read from the beginning of the topic instead of only new messages.",
    )
    return parser.parse_args()


def create_consumer(args: argparse.Namespace) -> KafkaConsumer:
    offset_reset = "earliest" if args.from_beginning else "latest"
    return KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_server,
        group_id=args.group_id,
        auto_offset_reset=offset_reset,
        enable_auto_commit=True,
        key_deserializer=lambda value: value.decode("utf-8") if value else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def main() -> None:
    args = parse_args()
    consumer = create_consumer(args)
    print(
        f"Listening to topic '{args.topic}' on {args.bootstrap_server}. "
        "Press Ctrl+C to stop."
    )

    try:
        for message in consumer:
            print(
                "partition={partition} offset={offset} key={key} value={value}".format(
                    partition=message.partition,
                    offset=message.offset,
                    key=message.key,
                    value=message.value,
                )
            )
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
