import sqlite3
import pandas as pd
import os
from datetime import datetime


# ── FUNCTION 1: GET CONNECTION ────────────────────────────────────
def get_connection():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect("data/nyc_taxi_warehouse.db")
    return conn


# ── FUNCTION 2: CREATE TABLES ─────────────────────────────────────
def create_tables(conn):
    print("Creating database tables...")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS fact_trips (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor_id             INTEGER,
            pickup_datetime       TEXT,
            dropoff_datetime      TEXT,
            pickup_date           TEXT,
            passenger_count       INTEGER,
            trip_distance         REAL,
            pickup_location_id    INTEGER,
            dropoff_location_id   INTEGER,
            pickup_zone           TEXT,
            dropoff_zone          TEXT,
            payment_type          INTEGER,
            payment_method        TEXT,
            fare_amount           REAL,
            tip_amount            REAL,
            tolls_amount          REAL,
            total_amount          REAL,
            congestion_surcharge  REAL,
            airport_fee           REAL,
            cbd_congestion_fee    REAL,
            trip_duration_mins    REAL,
            pickup_hour           INTEGER,
            pickup_day            TEXT,
            is_weekend            INTEGER,
            time_of_day           TEXT,
            fare_per_mile         REAL,
            is_airport_trip       INTEGER,
            loaded_at             TEXT
        );

        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at          TEXT,
            raw_records     INTEGER,
            clean_records   INTEGER,
            removed_records INTEGER,
            status          TEXT
        );
    """)

    # Clear existing data — prevents duplicates on re-run
    conn.execute("DELETE FROM fact_trips")
    conn.commit()
    print("✅ Tables ready: fact_trips, pipeline_runs")


# ── FUNCTION 3: LOAD DATA ─────────────────────────────────────────
def load_data(conn, df):
    print(f"\nLoading {len(df):,} records into fact_trips...")

    load_df = df[[
        'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
        'pickup_date',
        'passenger_count', 'trip_distance',
        'PULocationID', 'DOLocationID',
        'pickup_zone', 'dropoff_zone',
        'payment_type', 'payment_method',
        'fare_amount', 'tip_amount', 'tolls_amount', 'total_amount',
        'congestion_surcharge', 'Airport_fee', 'cbd_congestion_fee',
        'trip_duration_mins', 'pickup_hour', 'pickup_day',
        'is_weekend', 'time_of_day', 'fare_per_mile', 'is_airport_trip'
    ]].copy()

    load_df.columns = [
        'vendor_id', 'pickup_datetime', 'dropoff_datetime',
        'pickup_date',
        'passenger_count', 'trip_distance',
        'pickup_location_id', 'dropoff_location_id',
        'pickup_zone', 'dropoff_zone',
        'payment_type', 'payment_method',
        'fare_amount', 'tip_amount', 'tolls_amount', 'total_amount',
        'congestion_surcharge', 'airport_fee', 'cbd_congestion_fee',
        'trip_duration_mins', 'pickup_hour', 'pickup_day',
        'is_weekend', 'time_of_day', 'fare_per_mile', 'is_airport_trip'
    ]

    # Convert booleans to 0/1 for SQLite
    load_df['is_weekend'] = load_df['is_weekend'].astype(int)
    load_df['is_airport_trip'] = load_df['is_airport_trip'].astype(int)

    # Convert datetimes to strings for SQLite
    load_df['pickup_datetime'] = load_df['pickup_datetime'].astype(str)
    load_df['dropoff_datetime'] = load_df['dropoff_datetime'].astype(str)

    # Timestamp of when this was loaded
    load_df['loaded_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Write to database
    load_df.to_sql('fact_trips', conn, if_exists='append', index=False)

    # Verify
    count = pd.read_sql("SELECT COUNT(*) as total FROM fact_trips", conn)
    print(f"✅ fact_trips now contains: {count['total'][0]:,} records")


# ── FUNCTION 4: BUILD INSIGHTS ────────────────────────────────────
def build_insights(conn):
    print("\n--- Building business insights ---")

    revenue = pd.read_sql("""
        SELECT
            COUNT(*) as total_trips,
            ROUND(SUM(total_amount), 2) as total_revenue,
            ROUND(AVG(fare_amount), 2) as avg_fare,
            ROUND(AVG(trip_duration_mins), 1) as avg_duration_mins,
            ROUND(AVG(trip_distance), 2) as avg_distance_miles
        FROM fact_trips
    """, conn).iloc[0]

    print(f"\n  Total trips     : {int(revenue['total_trips']):,}")
    print(f"  Total revenue   : ${revenue['total_revenue']:,.2f}")
    print(f"  Avg fare        : ${revenue['avg_fare']:.2f}")
    print(f"  Avg duration    : {revenue['avg_duration_mins']} mins")
    print(f"  Avg distance    : {revenue['avg_distance_miles']} miles")

    airport = pd.read_sql("""
        SELECT
            is_airport_trip,
            COUNT(*) as trips,
            ROUND(AVG(fare_amount), 2) as avg_fare,
            ROUND(SUM(total_amount), 2) as total_revenue
        FROM fact_trips
        GROUP BY is_airport_trip
    """, conn)

    print(f"\n  AIRPORT vs CITY TRIPS:")
    for _, row in airport.iterrows():
        label = "Airport" if row['is_airport_trip'] == 1 else "City   "
        print(f"  {label} → {int(row['trips']):>10,} trips │ avg fare ${row['avg_fare']:.2f} │ revenue ${row['total_revenue']:,.2f}")

    peak = pd.read_sql("""
        SELECT pickup_hour, COUNT(*) as trips
        FROM fact_trips
        GROUP BY pickup_hour
        ORDER BY trips DESC
        LIMIT 3
    """, conn)

    print(f"\n  TOP 3 PEAK HOURS:")
    for _, row in peak.iterrows():
        print(f"  {int(row['pickup_hour']):02d}:00 → {int(row['trips']):,} trips")

    payments = pd.read_sql("""
        SELECT
            payment_method,
            COUNT(*) as trips,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_trips), 1) as pct
        FROM fact_trips
        GROUP BY payment_method
        ORDER BY trips DESC
    """, conn)

    print(f"\n  PAYMENT METHODS:")
    for _, row in payments.iterrows():
        print(f"  {str(row['payment_method']):<15} → {int(row['trips']):>10,} trips ({row['pct']}%)")

    zones = pd.read_sql("""
        SELECT pickup_zone, COUNT(*) as trips,
               ROUND(SUM(total_amount), 2) as revenue
        FROM fact_trips
        WHERE pickup_zone != 'Unknown Zone'
        GROUP BY pickup_zone
        ORDER BY revenue DESC
        LIMIT 5
    """, conn)

    print(f"\n  TOP 5 PICKUP ZONES:")
    for _, row in zones.iterrows():
        print(f"  {row['pickup_zone']:<35} {int(row['trips']):>7,} trips  ${row['revenue']:>12,.2f}")


# ── FUNCTION 5: LOG THE RUN ───────────────────────────────────────
def log_run(conn, raw_count, clean_count):
    removed = raw_count - clean_count
    conn.execute("""
        INSERT INTO pipeline_runs
        (run_at, raw_records, clean_records, removed_records, status)
        VALUES (?, ?, ?, ?, ?)
    """, (
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        raw_count,
        clean_count,
        removed,
        'SUCCESS'
    ))
    conn.commit()
    print(f"\n✅ Run logged: {raw_count:,} in → {clean_count:,} clean → {removed:,} removed")


# ── FUNCTION 6: MASTER LOAD ───────────────────────────────────────
def load(df, raw_count):
    print("=" * 45)
    print("LOAD LAYER STARTING")
    print("=" * 45)

    conn = get_connection()
    create_tables(conn)
    load_data(conn, df)
    build_insights(conn)
    log_run(conn, raw_count, len(df))
    conn.close()

    print("\n✅ Load complete!")
    print("Database saved to: data/nyc_taxi_warehouse.db")


# ── RUN STANDALONE ────────────────────────────────────────────────
if __name__ == "__main__":
    from extract import extract
    from transform import transform

    raw_df = extract()
    clean_df = transform(raw_df)
    load(clean_df, len(raw_df))