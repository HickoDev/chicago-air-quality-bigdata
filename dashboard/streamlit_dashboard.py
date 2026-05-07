from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd
import plotly.express as px
import streamlit as st
from kafka import KafkaConsumer
from kafka.errors import KafkaError


DEFAULT_TOPIC = "air_quality_stream"
DEFAULT_BOOTSTRAP_SERVER = "localhost:9092"
DEFAULT_RESULTS_DIR = Path(__file__).resolve().parent / "local_results"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dashboard for the Chicago air-quality pipeline.")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic to inspect.")
    parser.add_argument(
        "--bootstrap-server",
        default=DEFAULT_BOOTSTRAP_SERVER,
        help="Use localhost:9092 on Windows or kafka:29092 inside Docker.",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Replay existing topic messages instead of only waiting for new messages.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=500,
        help="Maximum Kafka messages to poll per refresh.",
    )
    parser.add_argument(
        "--poll-timeout-ms",
        type=int,
        default=3000,
        help="Kafka poll timeout per dashboard refresh.",
    )
    parser.add_argument(
        "--auto-refresh-seconds",
        type=float,
        default=0.0,
        help="Automatically refresh the dashboard. Use 0 to disable.",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=DEFAULT_RESULTS_DIR,
        help="Local folder containing exported MapReduce result folders.",
    )
    return parser.parse_args()


def parse_message_value(raw_value: bytes) -> Dict[str, object]:
    try:
        return json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {"raw_value": raw_value.decode("utf-8", errors="replace")}


def poll_kafka(args: argparse.Namespace) -> List[Dict[str, object]]:
    offset_policy = "earliest" if args.from_beginning else "latest"
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_server,
        auto_offset_reset=offset_policy,
        enable_auto_commit=False,
        consumer_timeout_ms=args.poll_timeout_ms,
    )

    events: List[Dict[str, object]] = []
    try:
        for message in consumer:
            event = parse_message_value(message.value)
            event["_topic"] = message.topic
            event["_partition"] = message.partition
            event["_offset"] = message.offset
            events.append(event)
            if len(events) >= args.max_messages:
                break
    finally:
        consumer.close()

    return events


def add_events_to_session(events: Iterable[Dict[str, object]]) -> None:
    if "events" not in st.session_state:
        st.session_state.events = []
    if "seen_offsets" not in st.session_state:
        st.session_state.seen_offsets = set()

    for event in events:
        key = (event.get("_topic"), event.get("_partition"), event.get("_offset"))
        if key in st.session_state.seen_offsets:
            continue
        st.session_state.seen_offsets.add(key)
        st.session_state.events.append(event)


def events_to_dataframe() -> pd.DataFrame:
    events = st.session_state.get("events", [])
    if not events:
        return pd.DataFrame()

    df = pd.DataFrame(events)
    for column in ["pm25", "no2", "temperature", "humidity", "latitude", "longitude"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def render_live_tab(args: argparse.Namespace) -> None:
    st.subheader("Live Kafka Stream")
    st.caption(
        "This view reads the same Kafka topic consumed by Spark Structured Streaming. "
        "Use it to prove that simulated sensor events are arriving."
    )

    col_a, col_b, col_c = st.columns(3)
    col_a.metric("Kafka topic", args.topic)
    col_b.metric("Bootstrap server", args.bootstrap_server)
    col_c.metric("Mode", "from beginning" if args.from_beginning else "live only")

    should_poll = st.button("Poll Kafka now", type="primary")
    if should_poll or "events" not in st.session_state:
        try:
            add_events_to_session(poll_kafka(args))
        except KafkaError as exception:
            st.error(f"Kafka connection failed: {exception}")
            return

    df = events_to_dataframe()
    if df.empty:
        st.info("No Kafka messages available yet. Start the producer, then poll again.")
        return

    metric_a, metric_b, metric_c, metric_d = st.columns(4)
    metric_a.metric("Events loaded", f"{len(df):,}")
    metric_b.metric("Sensors", f"{df.get('sensor_id', pd.Series(dtype=str)).nunique():,}")
    metric_c.metric("Average PM2.5", format_optional_number(df.get("pm25", pd.Series(dtype=float)).mean()))
    metric_d.metric("PM2.5 exceedances", f"{int((df.get('pm25', pd.Series(dtype=float)) > 35.0).sum()):,}")

    st.dataframe(df.tail(50), use_container_width=True)

    if {"sensor_id", "pm25"}.issubset(df.columns):
        top_pm25 = (
            df.dropna(subset=["pm25"])
            .groupby("sensor_id", as_index=False)["pm25"]
            .mean()
            .sort_values("pm25", ascending=False)
            .head(10)
        )
        st.plotly_chart(
            px.bar(top_pm25, x="sensor_id", y="pm25", title="Top Sensors by Live Average PM2.5"),
            use_container_width=True,
        )

    if {"sensor_id", "pm25"}.issubset(df.columns):
        exceedances = (
            df.assign(exceedance=(df["pm25"] > 35.0).astype(int))
            .groupby("sensor_id", as_index=False)["exceedance"]
            .sum()
            .sort_values("exceedance", ascending=False)
            .head(10)
        )
        st.plotly_chart(
            px.bar(exceedances, x="sensor_id", y="exceedance", title="Live PM2.5 Exceedances"),
            use_container_width=True,
        )

    if {"latitude", "longitude"}.issubset(df.columns):
        map_df = df.dropna(subset=["latitude", "longitude"])
        if not map_df.empty:
            st.plotly_chart(
                px.scatter_mapbox(
                    map_df,
                    lat="latitude",
                    lon="longitude",
                    color="pm25" if "pm25" in map_df.columns else None,
                    hover_name="sensor_id" if "sensor_id" in map_df.columns else None,
                    zoom=9,
                    height=520,
                    mapbox_style="open-street-map",
                    title="Live Sensor Events Map",
                ),
                use_container_width=True,
            )


def format_optional_number(value: float) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{value:.2f}"


def part_files(folder: Path) -> List[Path]:
    if not folder.exists():
        return []
    return sorted(path for path in folder.iterdir() if path.is_file() and path.name.startswith("part"))


def load_mapreduce_output(results_dir: Path, folder_name: str, columns: List[str]) -> pd.DataFrame:
    folder = results_dir / folder_name
    files = part_files(folder)
    if not files:
        return pd.DataFrame(columns=columns)
    frames = [pd.read_csv(file_path, sep="\t", header=None, names=columns) for file_path in files]
    return pd.concat(frames, ignore_index=True)


def render_batch_tab(args: argparse.Namespace) -> None:
    st.subheader("MapReduce Analytics")
    st.caption(
        "This view reads local copies of HDFS MapReduce output folders. "
        "Export them into dashboard/local_results before opening this tab."
    )

    st.code(
        "docker cp hadoop-master:/tmp/avg_pm25_by_day .\\dashboard\\local_results\\avg_pm25_by_day\n"
        "docker cp hadoop-master:/tmp/avg_no2_by_sensor .\\dashboard\\local_results\\avg_no2_by_sensor\n"
        "docker cp hadoop-master:/tmp/pm25_exceedances .\\dashboard\\local_results\\pm25_exceedances",
        language="powershell",
    )

    pm25_by_day = load_mapreduce_output(args.results_dir, "avg_pm25_by_day", ["event_date", "avg_pm25"])
    no2_by_sensor = load_mapreduce_output(args.results_dir, "avg_no2_by_sensor", ["sensor_id", "avg_no2"])
    exceedances = load_mapreduce_output(args.results_dir, "pm25_exceedances", ["sensor_id", "exceedance_count"])

    if pm25_by_day.empty and no2_by_sensor.empty and exceedances.empty:
        st.info(f"No exported MapReduce results found under {args.results_dir}.")
        return

    if not pm25_by_day.empty:
        pm25_by_day["event_date"] = pd.to_datetime(pm25_by_day["event_date"], errors="coerce")
        pm25_by_day["avg_pm25"] = pd.to_numeric(pm25_by_day["avg_pm25"], errors="coerce")
        st.plotly_chart(
            px.line(pm25_by_day.sort_values("event_date"), x="event_date", y="avg_pm25", title="Average PM2.5 by Day"),
            use_container_width=True,
        )

    if not no2_by_sensor.empty:
        no2_by_sensor["avg_no2"] = pd.to_numeric(no2_by_sensor["avg_no2"], errors="coerce")
        top_no2 = no2_by_sensor.sort_values("avg_no2", ascending=False).head(10)
        st.plotly_chart(
            px.bar(top_no2, x="sensor_id", y="avg_no2", title="Top 10 Sensors by Average NO2"),
            use_container_width=True,
        )

    if not exceedances.empty:
        exceedances["exceedance_count"] = pd.to_numeric(exceedances["exceedance_count"], errors="coerce")
        top_exceedances = exceedances.sort_values("exceedance_count", ascending=False).head(10)
        st.plotly_chart(
            px.bar(
                top_exceedances,
                x="sensor_id",
                y="exceedance_count",
                title="Top 10 Sensors by PM2.5 Exceedances",
            ),
            use_container_width=True,
        )


def main() -> None:
    args = parse_args()
    st.set_page_config(page_title="Chicago Air Quality Pipeline", layout="wide")
    st.title("Chicago Air Quality Big Data Pipeline")
    st.write(
        "CSV simulator -> Kafka -> Spark Structured Streaming -> HDFS -> MapReduce -> HBase -> Dashboard"
    )

    live_tab, batch_tab = st.tabs(["Live Kafka Events", "MapReduce Results"])
    with live_tab:
        render_live_tab(args)
    with batch_tab:
        render_batch_tab(args)

    if args.auto_refresh_seconds > 0:
        time.sleep(args.auto_refresh_seconds)
        st.rerun()


if __name__ == "__main__":
    main()
