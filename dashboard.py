import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from filter import apply_filters, compute_average_by_hour

# ======= CONFIGURATION =======
st.set_page_config(page_title="IESO Market Dashboard", layout="wide")
st.title("📊 IESO Market Dashboard")

# ======= LOAD DATA (cached) =======
@st.cache_data(ttl=3600)
def load_dataset(url):
    df = pd.read_csv(url, compression='gzip', parse_dates=["Date"])
    df.sort_values('Date', ascending=True, inplace=True)
    return df

dataset_option = st.sidebar.selectbox("⚡ Available Datasets", [
    "Energy - Hourly",
    "Operating Reserve - Hourly",
    "Energy - 5-min intervals",
    "Operating Reserve - 5-min intervals"
])

dataset_urls = {
    "Energy - Hourly": "https://storage.googleapis.com/ieso_monitoring_market_data/energy/processed/energy_historical_hourly.csv.gz",
    "Operating Reserve - Hourly": "https://storage.googleapis.com/ieso_monitoring_market_data/operating_reserve/processed/OR_historical_hourly.csv.gz",
    "Energy - 5-min intervals": "https://storage.googleapis.com/ieso_monitoring_market_data/energy/processed/energy_historical_interval.csv.gz",
    "Operating Reserve - 5-min intervals": "https://storage.googleapis.com/ieso_monitoring_market_data/operating_reserve/processed/OR_historical_interval.csv.gz"
}

hourly_bool = "5-min" not in dataset_option
df = load_dataset(dataset_urls[dataset_option])

# ======= SIDEBAR FILTERS =======
with st.sidebar:
    st.header("🔍 Filters")

    st.subheader("📍 Location Settings")
    all_locations = df["Pricing Location"].unique().tolist()
    selected_locations = st.multiselect("Pricing Location", all_locations, default=[])

    st.subheader("📅 Date Settings")
    min_date, max_date = df["Date"].min(), df["Date"].max()
    date_range = st.date_input("Date range", [min_date, max_date])

    include_avg = st.checkbox("Include average across selected dates", value=True)

    if len(date_range) == 2:
        start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    else:
        start_date = end_date = pd.to_datetime(date_range[0])

    st.subheader("📑 Dispatch Settings")
    all_dispatch = ['Day_Ahead', 'Pre_Dispatch', 'Real_Time']
    dispatch_type = st.multiselect("Select dispatch type", all_dispatch, default=all_dispatch)

    st.subheader("💲 Price Settings")
    if "Energy" in dataset_option:
        price_type = st.selectbox("Price Type:", ["LMP", "Energy Loss Price", "Energy Congestion Price"])
    else:
        price_type = st.selectbox("Price Type:", [
            "LMP 10S", "Congestion Price 10S",
            "LMP 10N", "Congestion Price 10N",
            "LMP 30R", "Congestion Price 30R"
        ])

# ======= UTILS =======
def get_column_set(prefix):
    return f"{price_type}_{prefix}"

def melt_df(df_to_melt):
    value_vars = [
        get_column_set(suffix)
        for suffix in dispatch_type
        if get_column_set(suffix) in df_to_melt.columns
    ]
    if not value_vars:
        return pd.DataFrame()

    melted = df_to_melt.melt(
        id_vars=["Pricing Location", "Date", "Delivery Hour"] + (["Interval"] if "Interval" in df_to_melt.columns else []),
        value_vars=value_vars,
        var_name="Market",
        value_name="Price"
    )
    melted["Market"] = melted["Market"].str.extract(r"_(Day_Ahead|Pre_Dispatch|Real_Time)")
    return melted

# ======= FILTER DATA =======
if selected_locations:
    df_filtered = apply_filters(df, locations=selected_locations, dispatch_type=dispatch_type, start_date=start_date, end_date=end_date, hourly_bool=hourly_bool)
    df_filtered = df_filtered.sort_values(by=["Date", "Delivery Hour"])
    df_melted = melt_df(df_filtered)
else:
    df_filtered = pd.DataFrame()
    df_melted = pd.DataFrame()

# ======= AVERAGE CALCULATION =======
df_avg_melted = None
df_avg = pd.DataFrame()
if include_avg:
    df_avg = compute_average_by_hour(df, dispatch_type=dispatch_type,start_date=start_date, end_date=end_date, hourly_bool=hourly_bool)
    df_avg = df_avg.sort_values(by=["Date", "Delivery Hour"])
    df_avg_melted = melt_df(df_avg)

# ======= Handle Interval Timestamp =======
def prepare_time_axis(df_plot):
    df_plot["timestamp"] = pd.to_datetime(df_plot["Date"]) + pd.to_timedelta(df_plot["Delivery Hour"], unit="h")

    if "Interval" in df_plot.columns:
        df_plot["timestamp"] += pd.to_timedelta((df_plot["Interval"] - 1) * 5, unit="min")

    return df_plot


# ======= PLOT =======
st.subheader(f"📈 {price_type} Price Evolution")
fig = go.Figure()

# ➕ Filtered data
if not df_melted.empty:
    df_melted = prepare_time_axis(df_melted)

    for market in df_melted["Market"].unique():
        for location in df_melted["Pricing Location"].unique():
            subset = df_melted[
                (df_melted["Market"] == market) &
                (df_melted["Pricing Location"] == location)
            ].sort_values("timestamp")

            fig.add_trace(go.Scatter(
                x=subset["timestamp"],
                y=subset["Price"],
                mode="lines+markers",
                name=f"{location} – {market}",
                line=dict(dash="solid")
            ))

# ➕ Average data
if df_avg_melted is not None and not df_avg_melted.empty:
    df_avg_melted = prepare_time_axis(df_avg_melted)

    for market in df_avg_melted["Market"].unique():
        subset = df_avg_melted[
            df_avg_melted["Market"] == market
        ].sort_values("timestamp")

        fig.add_trace(go.Scatter(
            x=subset["timestamp"],
            y=subset["Price"],
            mode="lines+markers",
            name=f"Average – {market}",
            line=dict(dash="dot", width=3)
        ))

fig.update_layout(
    title=f"{price_type} per {'interval' if not hourly_bool else 'hour'} across markets",
    xaxis_title="Timestamp",
    yaxis_title="Price ($/MWh)",
    legend_title="Zone / Market",
    template="plotly_white",
    hovermode="x unified"
)

st.plotly_chart(fig, use_container_width=True)



# ======= DATA PREVIEW =======
if not df_filtered.empty:
    with st.expander("🔎 Overview of Filtered Data"):
        st.dataframe(df_filtered.style.format(precision=2), use_container_width=True)

if not df_avg.empty:
    with st.expander("📖 Overview of the average"):
        st.dataframe(df_avg.style.format(precision=2), use_container_width=True)





