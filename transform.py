import pandas as pd


# ── FUNCTION 1: CLEAN ─────────────────────────────────────────────
def clean(df):
    print("=" * 45)
    print("TRANSFORM LAYER STARTING")
    print("=" * 45)

    n_start = len(df)
    print(f"Records coming in: {n_start:,}")

    # Fix 1: Remove rows with wrong year
    # We have Nov 2025 data but found timestamps from 2008
    before = len(df)
    df = df[df['tpep_pickup_datetime'].dt.year == 2025]
    removed = before - len(df)
    print(f"Removed {removed:,} rows with wrong year (expected 2025)")

    # Fix 2: Remove negative fares
    # Negative fares = billing reversals, not real trips
    before = len(df)
    df = df[df['fare_amount'] >= 0]
    removed = before - len(df)
    print(f"Removed {removed:,} rows with negative fares")

    # Fix 3: Remove zero distance trips
    # A taxi trip with 0 miles = meter on and off immediately
    before = len(df)
    df = df[df['trip_distance'] > 0]
    removed = before - len(df)
    print(f"Removed {removed:,} rows with zero distance")

    # Fix 4: Remove impossible distances
    # Max realistic NYC taxi trip = 100 miles
    # We found max = 256,000 miles (distance to the moon!)
    before = len(df)
    df = df[df['trip_distance'] <= 100]
    removed = before - len(df)
    print(f"Removed {removed:,} rows with distance over 100 miles")

    # Fix 5: Fill null passenger counts with 1
    # TLC says drivers skip this for solo rides
    # null almost always means 1 passenger
    df['passenger_count'] = df['passenger_count'].fillna(1)
    df['passenger_count'] = df['passenger_count'].replace(0, 1)
    df['passenger_count'] = df['passenger_count'].astype(int)
    print(f"Fixed null/zero passenger counts → set to 1")

    n_end = len(df)
    total_removed = n_start - n_end
    print(f"\nTotal removed    : {total_removed:,} ({total_removed/n_start*100:.1f}%)")
    print(f"Records remaining: {n_end:,}")

    return df


# ── FUNCTION 2: ENRICH ────────────────────────────────────────────
def enrich(df):
    print("\n--- Enriching data ---")

    # Trip duration in minutes
    df['trip_duration_mins'] = (
        (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime'])
        .dt.total_seconds() / 60
    ).round(2)

    # Remove trips longer than 4 hours (meter errors)
    # Remove trips with negative duration (dropoff before pickup)
    before = len(df)
    df = df[df['trip_duration_mins'] <= 240]
    df = df[df['trip_duration_mins'] > 0]
    print(f"Removed {before - len(df):,} trips with invalid duration")

    # Hour of day the trip started (0-23)
    df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour

    # Day of week (Monday, Tuesday etc)
    df['pickup_day'] = df['tpep_pickup_datetime'].dt.day_name()

    # Is it a weekend? (dayofweek: 0=Monday, 5=Saturday, 6=Sunday)
    df['is_weekend'] = df['tpep_pickup_datetime'].dt.dayofweek >= 5

    # Time of day bucket
    def time_of_day(hour):
        if 5 <= hour < 12:
            return 'Morning'
        elif 12 <= hour < 17:
            return 'Afternoon'
        elif 17 <= hour < 21:
            return 'Evening'
        else:
            return 'Night'

    df['time_of_day'] = df['pickup_hour'].apply(time_of_day)

    # Fare per mile — key profitability metric
    df['fare_per_mile'] = (
        df['fare_amount'] / df['trip_distance']
    ).round(2)

    # Is this an airport trip?
    # Zone 132 = JFK, 138 = LaGuardia, 1 = Newark
    airport_zones = [1, 132, 138]
    df['is_airport_trip'] = (
        df['PULocationID'].isin(airport_zones) |
        df['DOLocationID'].isin(airport_zones)
    )

    # Payment label — makes analysis readable
    payment_map = {
        1: 'Credit Card',
        2: 'Cash',
        3: 'No Charge',
        4: 'Dispute',
        5: 'Unknown'
    }
    df['payment_method'] = df['payment_type'].map(payment_map)

    print(f"Added 8 new columns")
    print(f"Final record count: {len(df):,}")

    return df


# ── FUNCTION 3: MASTER TRANSFORM ──────────────────────────────────
def transform(df):
    df = clean(df)
    df = enrich(df)
    print("\n✅ Transform complete!")
    return df


# ── RUN STANDALONE ────────────────────────────────────────────────
if __name__ == "__main__":
    from extract import extract

    raw_df = extract()
    clean_df = transform(raw_df)

    print("\nSample of transformed data:")
    print(clean_df[[
        'fare_amount',
        'trip_duration_mins',
        'pickup_hour',
        'time_of_day',
        'is_airport_trip',
        'payment_method'
    ]].head(5).to_string())