\# 🚕 NYC Taxi ETL Pipeline



An end-to-end data engineering pipeline processing real NYC TLC 

Yellow Taxi trip data — extracting, cleaning, transforming and 

loading 4.1M records into a structured analytical warehouse.



---



\## 📌 What Problem Does This Solve?



NYC TLC publishes monthly taxi trip data (~4M rows/month).

The raw data arrives with serious quality issues:



\- 395,093 negative fare amounts (billing reversals)

\- 1,014,740 null passenger counts from a non-standard vendor

\- Timestamps dating back to 2008 in a November 2025 dataset  

\- Trip distances up to 256,000 miles (distance to the moon)



Without a reliable pipeline, these corrupt records silently 

pollute revenue reporting and pricing decisions.



---



\## 📐 Architecture

```

Extract              Transform              Load

───────              ─────────              ────

Raw parquet     →    Clean 537K       →    SQLite warehouse

4,181,444 rows       invalid records       3,644,032 rows

67.8 MB file         Enrich 8 cols         fact\_trips table

Profile quality      Fix nulls             pipeline\_runs log

```



---



\## 🔍 Key Findings — November 2025 Data



| Metric | Value |

|--------|-------|

| Raw records ingested | 4,181,444 |

| Invalid records removed | 537,412 (12.8%) |

| Clean records loaded | 3,644,032 |

| Total revenue in dataset | $103,715,278.96 |

| Average fare | $19.55 |

| Average trip duration | 18.3 mins |

| Airport trips % of total trips | 8.9% |

| Airport trips % of total revenue | 24.6% |

| Airport avg fare vs city avg fare | $57.43 vs $15.86 (3.6x higher) |

| Peak hour | 18:00 — 242,776 trips |



\*\*Business insight:\*\* Airport routes represent less than 9% of 

trips but generate 24.6% of total revenue at 3.6x higher average 

fare. A fleet operator optimising dispatch toward JFK/LaGuardia 

during peak hours could significantly increase per-vehicle revenue 

without adding vehicles.



---



\## 🛠️ Tech Stack



| Tool | Purpose |

|------|---------|

| Python 3.x | Core language |

| Pandas | Data transformation |

| SQLite | Local data warehouse |

| Pyarrow | Parquet file reading |



---



\## 🚀 How to Run

```bash

\# 1. Clone the repo

git clone https://github.com/Srushtibonde/nyc-taxi-etl

cd nyc-taxi-etl



\# 2. Create virtual environment

python -m venv venv

venv\\Scripts\\activate        # Windows

source venv/bin/activate     # Mac/Linux



\# 3. Install dependencies

pip install pandas pyarrow



\# 4. Download real data

\# Go to: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

\# Download Yellow Taxi November 2025 parquet → place in data/ folder



\# 5. Run the pipeline

python pipeline.py

```



---



\## 📁 Project Structure

```

nyc\_taxi\_etl/

├── extract.py       # Load \& profile raw parquet data

├── transform.py     # Clean + enrich data

├── load.py          # Load to warehouse + business insights

├── pipeline.py      # Orchestrator — runs all 3 steps

├── explore.py       # Initial data exploration

├── requirements.txt

└── data/            # ← not in repo, create locally

&nbsp;   └── yellow\_tripdata\_2025-11.parquet

```



---



\## 🧠 Engineering Decisions



\*\*Why delete and reload instead of append?\*\*  

Running the pipeline twice with append doubles records and 

corrupts all revenue figures. I discovered this bug during 

development — fixed by adding DELETE before each load. 

Full refresh guarantees exactly one copy of data per run.



\*\*Why drop negative fares instead of imputing?\*\*  

Negative fares are billing reversals with semantic meaning.

Replacing with mean/median would corrupt revenue analysis.



\*\*Why fill null passenger\_count with 1 instead of dropping?\*\*  

TLC documentation states drivers frequently skip this field 

for solo rides. 1 is the correct default — we recover 

1,014,740 rows instead of losing them.



\*\*Why SQLite and not PostgreSQL?\*\*  

Zero setup — anyone can clone and run in 2 minutes.

Upgrading to PostgreSQL = change one line in get\_connection().



---



\## 🚨 Known Issues \& Next Steps



\- VendorID 7 (new 2025 vendor) uses unmapped payment codes → 

&nbsp; shows as `nan` in payment analysis. Needs investigation.

\- \[ ] Schedule with Apache Airflow

\- \[ ] Upgrade to PostgreSQL

\- \[ ] Add dbt transformation layer

\- \[ ] Add Great Expectations data quality tests

\- \[ ] Load taxi zone lookup for zone name mapping



---



\## 📚 Data Source



\[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

— public dataset updated monthly, used widely in data 

engineering benchmarks and academic research.



---



\## 👤 Author



\*\*Srushtibonde\*\*  

\[GitHub](https://github.com/Srushtibonde)

