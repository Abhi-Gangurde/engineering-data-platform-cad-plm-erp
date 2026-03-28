"""
split_cad_metadata.py
---------------------
Reads cad_metadata_1M.csv, groups rows by the month of `created_date`,
and writes one file per month named:
    cad_metadata_YYYY_MM_01.csv   (first day of the month, monthly snapshot convention)

Usage:
    python split_cad_metadata.py \
        --input  /path/to/cad_metadata_1M.csv \
        --output /path/to/output_folder

Defaults (if no args passed):
    --input  ./cad_metadata_1M.csv
    --output ./cad_metadata_splits/
"""

import os
import argparse
import pandas as pd


# ── CLI args ──────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Split cad_metadata CSV into monthly snapshot files.")
parser.add_argument(
    "--input",
    default="cad_metadata_1M.csv",
    help="Path to the source CSV file (default: cad_metadata_1M.csv)",
)
parser.add_argument(
    "--output",
    default="cad_metadata_splits",
    help="Destination folder for monthly files (default: ./cad_metadata_splits/)",
)
args = parser.parse_args()

INPUT_FILE  = args.input
OUTPUT_DIR  = args.output

# ── Setup ─────────────────────────────────────────────────────────────────────
os.makedirs(OUTPUT_DIR, exist_ok=True)
print(f"Reading  : {INPUT_FILE}")
print(f"Output   : {OUTPUT_DIR}")
print("-" * 50)

# ── Load ──────────────────────────────────────────────────────────────────────
df = pd.read_csv(INPUT_FILE, parse_dates=["created_date"])

if "created_date" not in df.columns:
    raise ValueError("Column 'created_date' not found in the CSV.")

# ── Derive year + month period ────────────────────────────────────────────────
df["_year"]  = df["created_date"].dt.year
df["_month"] = df["created_date"].dt.month

# ── Split & write ─────────────────────────────────────────────────────────────
groups = df.groupby(["_year", "_month"])
total_files = 0

for (year, month), group in groups:
    # Drop helper columns before saving
    chunk = group.drop(columns=["_year", "_month"])

    # Filename convention: cad_metadata_YYYY_MM_01.csv
    filename = f"cad_metadata_{year}_{month:02d}_01.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)

    chunk.to_csv(filepath, index=False)
    print(f"  ✓ {filename}  ({len(chunk):>10,} rows)")
    total_files += 1

print("-" * 50)
print(f"Done. {total_files} files written to '{OUTPUT_DIR}'.")