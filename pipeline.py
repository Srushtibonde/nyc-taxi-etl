import time
from datetime import datetime
from extract import extract
from transform import transform
from load import load


def print_banner():
    print("""
╔══════════════════════════════════════════════════╗
║       NYC TAXI ETL PIPELINE  🚕                  ║
║       Source : NYC TLC Yellow Taxi Records       ║
║       Period : January 2025                     ║
║       Built  : Python + Pandas + SQLite          ║
╚══════════════════════════════════════════════════╝
    """)


def run_pipeline():
    print_banner()

    start_time = time.time()
    print(f"Pipeline started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # ── STEP 1: EXTRACT ──────────────────────────────
    print("STEP 1 of 3 → EXTRACT")
    raw_df = extract()
    raw_count = len(raw_df)
    print()

    # ── STEP 2: TRANSFORM ────────────────────────────
    print("STEP 2 of 3 → TRANSFORM")
    clean_df = transform(raw_df)
    print()

    # ── STEP 3: LOAD ─────────────────────────────────
    print("STEP 3 of 3 → LOAD")
    load(clean_df, raw_count)
    print()

    # ── DONE ─────────────────────────────────────────
    elapsed = round(time.time() - start_time, 1)
    print("=" * 50)
    print(f"  ✅ PIPELINE COMPLETE")
    print(f"  ⏱  Total time     : {elapsed} seconds")
    print(f"  📥 Raw records    : {raw_count:,}")
    print(f"  ✅ Clean records  : {len(clean_df):,}")
    print(f"  🗑  Removed        : {raw_count - len(clean_df):,} invalid records")
    print(f"  💾 Database       : data/nyc_taxi_warehouse.db")
    print(f"  📁 Project folder : nyc_taxi_etl/")
    print("=" * 50)


if __name__ == "__main__":
    run_pipeline()