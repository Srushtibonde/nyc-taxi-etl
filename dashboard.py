import streamlit as st
import pandas as pd
import plotly.express as px
import sqlite3


# ── PAGE CONFIG ───────────────────────────────────────────
st.set_page_config(
    page_title="NYC Taxi Analytics",
    page_icon="🚕",
    layout="wide"
)


# ── LOAD DATA ─────────────────────────────────────────────
@st.cache_data
def load_data():
    conn = sqlite3.connect("data/nyc_taxi_warehouse.db")

    hourly = pd.read_sql("""
        SELECT
            pickup_hour,
            COUNT(*) as trips,
            ROUND(AVG(fare_amount), 2) as avg_fare,
            ROUND(AVG(trip_duration_mins), 1) as avg_duration
        FROM fact_trips
        GROUP BY pickup_hour
        ORDER BY pickup_hour
    """, conn)

    airport = pd.read_sql("""
        SELECT
            CASE WHEN is_airport_trip = 1
                 THEN 'Airport' ELSE 'City' END as trip_type,
            COUNT(*) as trips,
            ROUND(AVG(fare_amount), 2) as avg_fare,
            ROUND(SUM(total_amount), 2) as revenue
        FROM fact_trips
        GROUP BY is_airport_trip
    """, conn)

    payments = pd.read_sql("""
        SELECT
            payment_method,
            COUNT(*) as trips,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_trips), 1) as pct
        FROM fact_trips
        WHERE payment_method != 'nan'
        AND payment_method IS NOT NULL
        GROUP BY payment_method
        ORDER BY trips DESC
    """, conn)

    zones = pd.read_sql("""
        SELECT
            pickup_zone,
            COUNT(*) as trips,
            ROUND(SUM(total_amount), 2) as revenue,
            ROUND(AVG(fare_amount), 2) as avg_fare
        FROM fact_trips
        WHERE pickup_zone != 'Unknown Zone'
        GROUP BY pickup_zone
        ORDER BY revenue DESC
        LIMIT 10
    """, conn)

    daily = pd.read_sql("""
        SELECT
            pickup_date,
            COUNT(*) as trips,
            ROUND(SUM(total_amount), 2) as revenue,
            ROUND(AVG(fare_amount), 2) as avg_fare
        FROM fact_trips
        GROUP BY pickup_date
        ORDER BY pickup_date
    """, conn)

    time_of_day = pd.read_sql("""
        SELECT
            time_of_day,
            COUNT(*) as trips,
            ROUND(AVG(fare_amount), 2) as avg_fare
        FROM fact_trips
        GROUP BY time_of_day
        ORDER BY trips DESC
    """, conn)

    conn.close()
    return hourly, airport, payments, zones, daily, time_of_day


hourly, airport, payments, zones, daily, time_of_day = load_data()


# ── HEADER ────────────────────────────────────────────────
st.title("🚕 NYC Taxi Analytics Dashboard")
st.markdown(
    "**Source:** NYC TLC Yellow Taxi Records — November 2025 &nbsp;|&nbsp; "
    "**Pipeline:** Python · Pandas · SQLite &nbsp;|&nbsp; "
    "**[GitHub](https://github.com/Srushtibonde/nyc-taxi-etl)**"
)
st.divider()


# ── KPI CARDS ─────────────────────────────────────────────
col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Total Trips", "3,644,032")
col2.metric("Total Revenue", "$103.7M")
col3.metric("Average Fare", "$19.55")
col4.metric("Avg Trip Duration", "18.3 mins")
col5.metric(
    "Airport Revenue Share", "24.6%",
    delta="Only 8.9% of trips"
)

st.divider()


# ── ROW 1: HOURLY TRIPS + AIRPORT VS CITY ─────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("🕐 Trips by Hour of Day")
    fig1 = px.bar(
        hourly,
        x="pickup_hour",
        y="trips",
        color="avg_fare",
        color_continuous_scale="Oranges",
        labels={
            "pickup_hour": "Hour of Day",
            "trips": "Number of Trips",
            "avg_fare": "Avg Fare ($)"
        },
        title="Peak demand: 17:00–18:00 evening rush hour"
    )
    fig1.update_layout(showlegend=False)
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.subheader("✈️ Airport vs City — Revenue Split")
    airport['revenue_pct'] = (
        airport['revenue'] / airport['revenue'].sum() * 100
    ).round(1)
    airport['trips_pct'] = (
        airport['trips'] / airport['trips'].sum() * 100
    ).round(1)

    fig2 = px.pie(
        airport,
        values="revenue",
        names="trip_type",
        color="trip_type",
        color_discrete_map={"Airport": "#FF6B35", "City": "#2E86AB"},
        title="Airport = 8.9% of trips → 24.6% of revenue"
    )
    fig2.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig2, use_container_width=True)

st.divider()


# ── ROW 2: PAYMENT METHODS + TOP ZONES ────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("💳 Payment Methods")
    fig3 = px.bar(
        payments,
        x="pct",
        y="payment_method",
        orientation="h",
        color="pct",
        color_continuous_scale="Blues",
        labels={
            "pct": "% of Trips",
            "payment_method": ""
        },
        title="72% of passengers pay by credit card"
    )
    fig3.update_layout(
        showlegend=False,
        yaxis={'categoryorder': 'total ascending'}
    )
    st.plotly_chart(fig3, use_container_width=True)

with col2:
    st.subheader("📍 Top 10 Pickup Zones by Revenue")
    fig4 = px.bar(
        zones.sort_values("revenue"),
        x="revenue",
        y="pickup_zone",
        orientation="h",
        color="avg_fare",
        color_continuous_scale="Greens",
        labels={
            "revenue": "Total Revenue ($)",
            "pickup_zone": ""
        },
        title="JFK Airport and Midtown dominate revenue"
    )
    fig4.update_layout(showlegend=False)
    st.plotly_chart(fig4, use_container_width=True)

st.divider()


# ── ROW 3: DAILY REVENUE TREND ────────────────────────────
st.subheader("📈 Daily Revenue Trend — November 2025")
fig5 = px.line(
    daily,
    x="pickup_date",
    y="revenue",
    labels={
        "pickup_date": "Date",
        "revenue": "Daily Revenue ($)"
    },
    title="Daily revenue across November 2025"
)
fig5.update_traces(line_color="#FF6B35", line_width=2)
fig5.update_layout(hovermode="x unified")
st.plotly_chart(fig5, use_container_width=True)

st.divider()


# ── ROW 4: TIME OF DAY ────────────────────────────────────
st.subheader("🌙 Trips by Time of Day")
col1, col2 = st.columns([1, 2])

with col1:
    order = ["Morning", "Afternoon", "Evening", "Night"]
    time_of_day["time_of_day"] = pd.Categorical(
        time_of_day["time_of_day"], categories=order, ordered=True
    )
    time_of_day = time_of_day.sort_values("time_of_day")

    fig6 = px.bar(
        time_of_day,
        x="time_of_day",
        y="trips",
        color="avg_fare",
        color_continuous_scale="Purples",
        labels={
            "time_of_day": "Time of Day",
            "trips": "Number of Trips",
            "avg_fare": "Avg Fare ($)"
        },
        title="Night trips have highest avg fare"
    )
    st.plotly_chart(fig6, use_container_width=True)

with col2:
    st.divider()
    st.markdown("### 💡 Key Business Findings")

    st.info("""
    **✈️ Airport Revenue Opportunity**

    Airport trips are only **8.9% of volume** but generate
    **24.6% of total revenue** at **3.6x higher average fare**.

    Fleet operators should prioritise JFK/LaGuardia 
    dispatch during peak hours to maximise revenue.
    """)

    st.warning("""
    **🕐 Peak Hour Concentration**

    **17:00–18:00** is the busiest window with 242,776 trips.
    Demand drops 40% after 22:00.

    Driver supply should match this demand curve
    to minimise passenger wait times and maximise utilisation.
    """)

    st.success("""
    **💳 Digital Payment Dominance**

    **72.1%** of trips paid by credit card.
    Only **9.5%** cash payments remain.

    Contactless infrastructure is now the primary
    revenue channel — cash handling costs can be reduced.
    """)

st.divider()


# ── FOOTER ────────────────────────────────────────────────
st.caption(
    "Built by Srushtibonde · "
    "Data: NYC TLC Open Data · "
    "Pipeline: github.com/Srushtibonde/nyc-taxi-etl"
)