from __future__ import annotations

import argparse
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
        description="Create report-ready charts from exported Spark CSV outputs."
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=DEFAULT_RESULTS_DIR,
        help="Local folder containing exported Spark result folders.",
    )
    return parser.parse_args()


def load_result_folder(results_dir: Path, folder_name: str) -> pd.DataFrame:
    folder_path = results_dir / folder_name
    if not folder_path.exists():
        raise FileNotFoundError(f"Missing results folder: {folder_path}")

    part_files: List[Path] = sorted(
        path for path in folder_path.iterdir() if path.is_file() and path.name.startswith("part")
    )
    if not part_files:
        raise FileNotFoundError(f"No Spark part files found in {folder_path}")

    frames = [pd.read_csv(file_path) for file_path in part_files]
    return pd.concat(frames, ignore_index=True)


def save_pm25_by_day(results_dir: Path) -> None:
    df = load_result_folder(results_dir, "avg_pm25_by_day")
    df["event_date"] = pd.to_datetime(df["event_date"])
    df = df.sort_values("event_date")

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
    df = load_result_folder(results_dir, "avg_no2_by_sensor")
    df = df.sort_values("avg_no2", ascending=False).head(10)

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
    df = load_result_folder(results_dir, "pm25_exceedances")
    df = df.sort_values("exceedance_count", ascending=False).head(10)

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
    df = load_result_folder(results_dir, "sensor_map_data")
    required_columns = {"latitude", "longitude", "sensor_id"}
    if not required_columns.issubset(df.columns):
        raise ValueError(
            f"sensor_map_data must contain {sorted(required_columns)}. Got: {list(df.columns)}"
        )

    if "avg_pm25" not in df.columns:
        raise ValueError("sensor_map_data must contain the avg_pm25 column.")

    hover_columns = [column for column in ["sensor_name", "avg_pm25", "avg_no2"] if column in df.columns]
    figure = px.scatter_mapbox(
        df,
        lat="latitude",
        lon="longitude",
        hover_name="sensor_id",
        hover_data=hover_columns,
        color="avg_pm25",
        size="avg_pm25",
        zoom=9,
        height=700,
        color_continuous_scale="Turbo",
        title="Chicago Sensors Colored by Average PM2.5",
    )
    figure.update_layout(mapbox_style="open-street-map", margin={"r": 0, "t": 60, "l": 0, "b": 0})
    figure.write_html(SCRIPT_DIR / "pollution_sensor_map.html")


def main() -> None:
    args = parse_args()

    # Typical copy workflow after Spark finishes:
    # 1. Inside hadoop-master:
    #    hdfs dfs -get /chicago/spark_results/avg_pm25_by_day /tmp/avg_pm25_by_day
    # 2. On the Windows host:
    #    docker cp hadoop-master:/tmp/avg_pm25_by_day .\visualization\local_results\avg_pm25_by_day
    #
    # Repeat the same pattern for avg_no2_by_sensor, pm25_exceedances, and sensor_map_data.
    save_pm25_by_day(args.results_dir)
    save_no2_top10(args.results_dir)
    save_pm25_exceedances_top10(args.results_dir)
    save_pollution_map(args.results_dir)
    print("Visualization files created in the visualization folder.")


if __name__ == "__main__":
    main()
