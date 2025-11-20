"""
merge_batches.py

Wrapper script to merge all batch Excel files inside bing_batches/
into a single final_merged.xlsx and final_merged.csv.

- Automatically detects batch_*.xlsx files
- Sorts them in correct numeric order
- Merges all rows
- Removes duplicates (based on company name + address)
- Converts list-like text columns back to clean strings
"""

import os
import re
import pandas as pd

BATCH_FOLDER = "bing_batches"
OUTPUT_CSV = "final_merged.csv"
OUTPUT_XLSX = "final_merged.xlsx"

def natural_sort_key(s):
    """Sort filenames like batch_1, batch_2, … batch_10 correctly"""
    return [int(text) if text.isdigit() else text.lower()
            for text in re.split(r'(\d+)', s)]

def merge_batches():
    if not os.path.exists(BATCH_FOLDER):
        print(f"[ERROR] Folder not found: {BATCH_FOLDER}")
        return

    files = [f for f in os.listdir(BATCH_FOLDER) if f.endswith(".xlsx")]
    if not files:
        print("[ERROR] No batch_*.xlsx files found!")
        return

    # Sort batches in numeric sequence
    files = sorted(files, key=natural_sort_key)

    print(f"Found {len(files)} batch files. Merging...")

    df_list = []
    for file in files:
        path = os.path.join(BATCH_FOLDER, file)
        print(f" → Reading {path}")
        df = pd.read_excel(path)
        df_list.append(df)

    # Combine everything
    merged = pd.concat(df_list, ignore_index=True)

    # Clean duplicated entries
    before = len(merged)
    merged.drop_duplicates(subset=["name", "address"], keep="first", inplace=True)
    after = len(merged)

    print(f"Removed {before - after} duplicate rows.")
    print(f"Total rows after merge: {after}")

    # Clean column types (aggregator_links and emails are comma-separated)
    for col in ["aggregator_links", "emails"]:
        if col in merged.columns:
            merged[col] = merged[col].fillna("").astype(str)
            # Remove brackets if text appears like "['a','b']"
            merged[col] = merged[col].str.replace(r"[\[\]']", "", regex=True)

    # Save CSV + Excel
    merged.to_csv(OUTPUT_CSV, index=False)
    merged.to_excel(OUTPUT_XLSX, index=False)

    print(f"\n✔ Saved final merged CSV:  {OUTPUT_CSV}")
    print(f"✔ Saved final merged Excel: {OUTPUT_XLSX}")

if __name__ == "__main__":
    merge_batches()
