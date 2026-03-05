import pandas as pd


# ── FUNCTION 1: CLEAN ─────────────────────────────────────────────
def clean(df):
    print("=" * 45)
    print("TRANSFORM LAYER STARTING")
    print("=" * 45)

    n_start = len(df)
    print(f"Records coming in: {n_start:,}")

    # Fix 1: Remove rows with wrong year
    before = len(df)
    df = df[df['tpep_pickup_datetime'].dt.year == 2025]
    print(f"Removed {before - len(df):,} rows with wrong year (expected 2025)")

    # Fix 2: Remove negative fares
    before = len(df)
    df = df[df['fare_amount'] >= 0]
    print(f"Removed {before - len(df):,} rows with negative fares")

    # Fix 3: Remove zero distance trips
    before = len(df)
    df = df[df['trip_distance'] > 0]
    print(f"Removed {before - len(df):,} rows with zero distance")

    # Fix 4: Remove impossible distances
    before = len(df)
    df = df[df['trip_distance'] <= 100]
    print(f"Removed {before - len(df):,} rows with distance over 100 miles")

    # Fix 5: Fill null passenger counts with 1
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

    # Remove invalid durations
    before = len(df)
    df = df[df['trip_duration_mins'] <= 240]
    df = df[df['trip_duration_mins'] > 0]
    print(f"Removed {before - len(df):,} trips with invalid duration")

    # Date only (for daily aggregations)
    df['pickup_date'] = df['tpep_pickup_datetime'].dt.date.astype(str)

    # Hour of day
    df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour

    # Day of week
    df['pickup_day'] = df['tpep_pickup_datetime'].dt.day_name()

    # Weekend flag
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

    # Fare per mile
    df['fare_per_mile'] = (
        df['fare_amount'] / df['trip_distance']
    ).round(2)

    # Airport trip flag
    airport_zones = [1, 132, 138]
    df['is_airport_trip'] = (
        df['PULocationID'].isin(airport_zones) |
        df['DOLocationID'].isin(airport_zones)
    )

    # Payment label
    payment_map = {
        1: 'Credit Card',
        2: 'Cash',
        3: 'No Charge',
        4: 'Dispute',
        5: 'Unknown'
    }
    df['payment_method'] = df['payment_type'].map(payment_map)

    # Zone lookup
    ZONE_LOOKUP = {
        1:"EWR/Newark Airport", 2:"Jamaica Bay", 4:"Alphabet City",
        7:"Astoria", 8:"Astoria Park", 12:"Battery Park",
        13:"Battery Park City", 14:"Bay Ridge", 17:"Bedford",
        24:"Bloomingdale", 25:"Boerum Hill", 33:"Brooklyn Heights",
        36:"Bushwick North", 37:"Bushwick South", 41:"Central Harlem",
        42:"Central Harlem North", 43:"Central Park", 48:"Clinton Hill",
        61:"Crown Heights North", 68:"East Chelsea", 74:"East Elmhurst",
        79:"East Village", 87:"Financial District North",
        88:"Financial District South", 100:"Garment District",
        113:"Hell's Kitchen", 114:"Highbridge", 125:"Jackson Heights",
        126:"Jamaica", 128:"JFK Airport", 132:"JFK Airport",
        138:"LaGuardia Airport", 140:"Lenox Hill East",
        141:"Lenox Hill West", 142:"Lincoln Square East",
        143:"Lincoln Square West", 144:"Little Italy/NoLiTa",
        148:"Long Island City", 152:"Manhattan Valley",
        158:"Meatpacking/West Village", 161:"Midtown Center",
        162:"Midtown East", 163:"Midtown North", 164:"Midtown South",
        166:"Morningside Heights", 186:"Penn Station/Madison Sq West",
        209:"Seaport", 224:"Steinway", 229:"Sutton Place/Turtle Bay North",
        230:"Times Sq/Theatre District", 231:"TriBeCa/Civic Center",
        233:"UN/Turtle Bay South", 234:"Union Square",
        236:"Upper East Side North", 237:"Upper East Side South",
        238:"Upper West Side North", 239:"Upper West Side South",
        243:"Washington Heights North", 249:"West Village",
        261:"Woodside", 262:"Yorkville East", 263:"Yorkville West",
    }

    df['pickup_zone'] = df['PULocationID'].map(ZONE_LOOKUP).fillna('Unknown Zone')
    df['dropoff_zone'] = df['DOLocationID'].map(ZONE_LOOKUP).fillna('Unknown Zone')

    print(f"Added 10 new columns")
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
        'fare_amount', 'trip_duration_mins',
        'pickup_hour', 'time_of_day',
        'is_airport_trip', 'payment_method',
        'pickup_zone', 'pickup_date'
    ]].head(5).to_string())