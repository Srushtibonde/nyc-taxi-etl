import pandas as pd
import os
def find_file():
    files = os.listdir("data")
    print(f"Files in data folder: {files}")
    
    for file in files:
        if file.endswith(".parquet"):
            print(f"Found parquet file: {file}")
            return "data/" + file
    
    print("ERROR: No parquet file found in data folder!")
    return None
def load_file(filepath):
    print(f"\nLoading: {filepath}")
    
    size_mb = os.path.getsize(filepath) / 1024 / 1024
    print(f"File size: {size_mb:.1f} MB")
    
    df = pd.read_parquet(filepath)
    
    print(f"Rows loaded: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    
    return df
def profile(df):
    print("\n=== DATA QUALITY REPORT ===")
    print(f"Total rows        : {len(df):,}")
    print(f"Total columns     : {len(df.columns)}")
    print(f"Negative fares    : {(df['fare_amount'] < 0).sum():,}")
    print(f"Zero distances    : {(df['trip_distance'] == 0).sum():,}")
    print(f"Null passengers   : {df['passenger_count'].isnull().sum():,}")
    print(f"Duplicate rows    : {df.duplicated().sum():,}")
    print(f"Date range        : {df['tpep_pickup_datetime'].min()} to {df['tpep_pickup_datetime'].max()}")
def extract():
    print("=" * 45)
    print("EXTRACT LAYER STARTING")
    print("=" * 45)
    
    filepath = find_file()
    
    if filepath is None:
        return None
    
    df = load_file(filepath)
    profile(df)
    
    print("\n✅ Extract complete!")
    return df


# This runs when you type: python extract.py
if __name__ == "__main__":
    df = extract()