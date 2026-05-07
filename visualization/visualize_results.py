from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List

import matplotlib
import pandas as pd
import plotly.express as px


matplotlib.use("Agg")
import matplotlib.pyplot as plt


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_RESULTS_DIR = SCRIPT_DIR / "local_results"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create report-ready charts from exported MapReduce outputs."
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=DEFAULT_RESULTS_DIR,
        help="Local folder containing exported MapReduce output folders.",
    )
    return parser.parse_args()


def part_files(folder_path: Path) -> List[Path]:
    if not folder_path.exists():
        return []
    return sorted(
        path for path in folder_path.iterdir() if path.is_file() and path.name.startswith("part")
    )


def load_mapreduce_folder(results_dir: Path, folder_name: str, columns: List[str]) -> pd.DataFrame:
    folder_path = results_dir / folder_name
    files = part_files(folder_path)
    if not files:
        raise FileNotFoundError(f"No MapReduce part files found in {folder_path}")

    frames = [pd.read_csv(file_path, sep="\t", header=None, names=columns) for file_path in files]
    return pd.concat(frames, ignore_index=True)


def load_optional_stream_events(results_dir: Path) -> pd.DataFrame:
    folder_path = results_dir / "air_quality_events"
    files = part_files(folder_path)
    if not files:
        return pd.DataFrame()

    records = []
    for file_path in files:
        with file_path.open("r", encoding="utf-8") as source:
            for line in source:
                cleaned = line.strip()
                if cleaned:
                    records.append(json.loads(cleaned))

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    for column in ["pm25", "no2", "latitude", "longitude"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def save_pm25_by_day(results_dir: Path) -> None:
    df = load_mapreduce_folder(results_dir, "avg_pm25_by_day", ["event_date", "avg_pm25"])
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
    df["avg_pm25"] = pd.to_numeric(df["avg_pm25"], errors="coerce")
    df = df.dropna(subset=["event_date", "avg_pm25"]).sort_values("event_date")

    plt.figure(figsize=(12, 6))
    plt.plot(df["event_date"], df["avg_pm25"], color="#bc3908", linewidth=2.0)
    plt.title("Average PM2.5 by Day")
    plt.xlabel("Date")
    plt.ylabel("Average PM2.5")
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.savefig(SCRIPT_DIR / "pm25_by_day.png", dpi=200)
    plt.close()


def save_no2_top10(results_dir: Path) -> None:
    df = load_mapreduce_folder(results_dir, "avg_no2_by_sensor", ["sensor_id", "avg_no2"])
    df["avg_no2"] = pd.to_numeric(df["avg_no2"], errors="coerce")
    df = df.dropna(subset=["avg_no2"]).sort_values("avg_no2", ascending=False).head(10)

    plt.figure(figsize=(12, 6))
    plt.bar(df["sensor_id"], df["avg_no2"], color="#386641")
    plt.title("Top 10 Sensors by Average NO2")
    plt.xlabel("Sensor ID")
    plt.ylabel("Average NO2")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(SCRIPT_DIR / "no2_by_sensor_top10.png", dpi=200)
    plt.close()


def save_pm25_exceedances_top10(results_dir: Path) -> None:
    df = load_mapreduce_folder(results_dir, "pm25_exceedances", ["sensor_id", "exceedance_count"])
    df["exceedance_count"] = pd.to_numeric(df["exceedance_count"], errors="coerce")
    df = df.dropna(subset=["exceedance_count"]).sort_values("exceedance_count", ascending=False).head(10)

    plt.figure(figsize=(12, 6))
    plt.bar(df["sensor_id"], df["exceedance_count"], color="#6a040f")
    plt.title("Top 10 Sensors by PM2.5 Exceedances")
    plt.xlabel("Sensor ID")
    plt.ylabel("Exceedance Count")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(SCRIPT_DIR / "pm25_exceedances_top10.png", dpi=200)
    plt.close()


def save_pollution_map(results_dir: Path) -> None:
    events = load_optional_stream_events(results_dir)
    required_columns = {"sensor_id", "latitude", "longitude"}
    if events.empty or not required_columns.issubset(events.columns):
        print("Skipping pollution map: copy streamed event JSON files to local_results/air_quality_events first.")
        return

    aggregation = {
        "latitude": "first",
        "longitude": "first",
    }
    if "pm25" in events.columns:
        aggregation["pm25"] = "mean"
    if "no2" in events.columns:
        aggregation["no2"] = "mean"

    df = events.dropna(subset=["latitude", "longitude"]).groupby("sensor_id", as_index=False).agg(aggregation)
    rename_map = {}
    if "pm25" in df.columns:
        rename_map["pm25"] = "avg_pm25"
    if "no2" in df.columns:
        rename_map["no2"] = "avg_no2"
    df = df.rename(columns=rename_map)

    map_options = {
        "lat": "latitude",
        "lon": "longitude",
        "hover_name": "sensor_id",
        "hover_data": [column for column in ["avg_pm25", "avg_no2"] if column in df.columns],
        "zoom": 9,
        "height": 700,
        "title": "Chicago Sensors Colored by Average PM2.5",
    }
    if "avg_pm25" in df.columns:
        map_options["color"] = "avg_pm25"
        map_options["size"] = "avg_pm25"
        map_options["color_continuous_scale"] = "Turbo"

    figure = px.scatter_mapbox(df, **map_options)
    figure.update_layout(mapbox_style="open-street-map", margin={"r": 0, "t": 60, "l": 0, "b": 0})
    figure.write_html(SCRIPT_DIR / "pollution_sensor_map.html")


def main() -> None:
    args = parse_args()

    # Typical copy workflow after MapReduce finishes:
    # 1. Inside hadoop-master:
    #    hdfs dfs -get /chicago/output/avg_pm25_by_day /tmp/avg_pm25_by_day
    # 2. On the Windows host:
    #    docker cp hadoop-master:/tmp/avg_pm25_by_day .\visualization\local_results\avg_pm25_by_day
    #
    # Repeat for avg_no2_by_sensor and pm25_exceedances.
    # For the optional map, also copy /chicago/streaming/bronze/air_quality_events.
    save_pm25_by_day(args.results_dir)
    save_no2_top10(args.results_dir)
    save_pm25_exceedances_top10(args.results_dir)
    save_pollution_map(args.results_dir)
    print("Visualization files created in the visualization folder.")


if __name__ == "__main__":
    main()
