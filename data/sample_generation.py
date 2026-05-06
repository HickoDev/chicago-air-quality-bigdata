from __future__ import annotations

import argparse
import csv
import random
import sys
from pathlib import Path
from typing import List


DEFAULT_INPUT = Path(
    r"C:\Users\user\Downloads\Open_Air_Chicago_Individual_Measurements.csv"
)
DEFAULT_OUTPUT = Path(__file__).resolve().parent / "open_air_chicago_sample.csv"
DEFAULT_SAMPLE_SIZE = 100_000
DEFAULT_SEED = 42


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a manageable sample from the Chicago air quality CSV."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Path to the downloaded Chicago CSV file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Path for the sampled CSV output.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help="Number of rows to keep in the output sample.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="Random seed used by reservoir sampling.",
    )
    return parser.parse_args()


def validate_paths(input_path: Path, output_path: Path) -> None:
    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)


def sample_csv(
    input_path: Path, output_path: Path, sample_size: int, seed: int
) -> None:
    if sample_size <= 0:
        raise ValueError("sample_size must be greater than zero")

    csv.field_size_limit(min(sys.maxsize, 2_147_483_647))
    rng = random.Random(seed)
    preview_rows: List[List[str]] = []
    sample_rows: List[List[str]] = []

    # If the portal schema changes, inspect the header and update downstream
    # scripts that rely on columns such as time, datasourceid,
    # pm2_5ConcMassIndividual.value, and no2ConcIndividual.value.
    with input_path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        header = next(reader)

        for row_index, row in enumerate(reader):
            if not any(cell.strip() for cell in row):
                continue

            if len(preview_rows) < 5:
                preview_rows.append(row)

            if len(sample_rows) < sample_size:
                sample_rows.append(row)
            else:
                replacement_index = rng.randint(0, row_index)
                if replacement_index < sample_size:
                    sample_rows[replacement_index] = row

            if row_index and row_index % 100_000 == 0:
                print(f"Processed {row_index:,} rows...")

    with output_path.open("w", encoding="utf-8", newline="") as target:
        writer = csv.writer(target)
        writer.writerow(header)
        writer.writerows(sample_rows)

    print("Columns:")
    for index, column in enumerate(header):
        print(f"  {index:02d}: {column}")

    print("\nFirst rows from the source file:")
    for row in preview_rows:
        print(row)

    print(
        f"\nSaved {len(sample_rows):,} rows to {output_path} using reservoir sampling."
    )


def main() -> None:
    args = parse_args()
    validate_paths(args.input, args.output)
    sample_csv(args.input, args.output, args.sample_size, args.seed)


if __name__ == "__main__":
    main()
