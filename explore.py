import pandas as pd

# Load the REAL data
df = pd.read_parquet("data/yellow_tripdata_2025-11.parquet")

print("=== SHAPE ===")
print(f"Rows   : {len(df):,}")
print(f"Columns: {len(df.columns)}")

print("\n=== COLUMN NAMES & DATA TYPES ===")
print(df.dtypes)

print("\n=== FIRST 3 ROWS ===")
print(df.head(3).to_string())

print("\n=== NULL VALUES PER COLUMN ===")
print(df.isnull().sum())

print("\n=== NEGATIVE FARES ===")
print(f"Count: {(df['fare_amount'] < 0).sum():,}")

print("\n=== NEGATIVE DISTANCES ===")
print(f"Count: {(df['trip_distance'] < 0).sum():,}")

print("\n=== BASIC STATS ===")
print(df[['fare_amount', 'trip_distance', 'passenger_count']].describe())