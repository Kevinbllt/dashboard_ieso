import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
from filter import apply_filters, compute_average_by_hour, sort_and_extract_top, build_statistics


def check_password():
    """Simple password protection"""
    def password_entered():
        if st.session_state["password"] == st.secrets["auth"]["password"]:
            st.session_state["authenticated"] = True
            del st.session_state["password"]  # Don’t store password
        else:
            st.session_state["authenticated"] = False

    if "authenticated" not in st.session_state:
        st.title("🔒 IESO Market App – Login")
        st.text_input("Enter password", type="password", on_change=password_entered, key="password")
        st.stop()
    elif not st.session_state["authenticated"]:
        st.title("🔒 IESO Market App – Login")
        st.error("Incorrect password. Try again.")
        st.text_input("Enter password", type="password", on_change=password_entered, key="password")
        st.stop()

check_password()



st.set_page_config(page_title="IESO Market App", layout="wide")

# ======= PAGE SELECTOR =======
page = st.sidebar.radio("📄 Select a page", ["📊 Market Dashboard", "📈 Statistics"])

@st.cache_data(ttl=3600)
def load_dataset(url):
    df = pd.read_csv(url, compression='gzip', parse_dates=["Date"])
    df.sort_values('Date', ascending=True, inplace=True)
    return df

dataset_urls = {
    "Energy - Hourly": "https://storage.googleapis.com/ieso_monitoring_market_data/energy/processed/energy_historical_hourly.csv.gz",
    "Operating Reserve - Hourly": "https://storage.googleapis.com/ieso_monitoring_market_data/operating_reserve/processed/OR_historical_hourly.csv.gz",
    "Energy - 5-min intervals": "https://storage.googleapis.com/ieso_monitoring_market_data/energy/processed/energy_historical_interval.csv.gz",
    "Operating Reserve - 5-min intervals": "https://storage.googleapis.com/ieso_monitoring_market_data/operating_reserve/processed/OR_historical_interval.csv.gz"
}

price_labels = {
    "LMP": "LMP",
    "Energy Loss Price": "Energy Loss Price",
    "Energy Congestion Price": "Energy Congestion Price",
    "spread_4h_Real_Time": "Spread 4h – Real-Time",
    "spread_4h_Day_Ahead": "Spread 4h – Day-Ahead",
    "spread_4h_Pre_Dispatch": "Spread 4h – Pre-Dispatch",
    "spread_Day_Ahead_vs_Real_Time": "Spread – DA vs RT"
}

custom_price_columns = [
    "spread_4h_Real_Time",
    "spread_4h_Day_Ahead",
    "spread_4h_Pre_Dispatch",
    "spread_Day_Ahead_vs_Real_Time"
]

if page == "📊 Market Dashboard":
    st.title("📊 IESO Market Dashboard")
    dataset_option = st.sidebar.selectbox("⚡ Available Datasets", list(dataset_urls.keys()))
    hourly_bool = "5-min" not in dataset_option
    df = load_dataset(dataset_urls[dataset_option])

    with st.sidebar:
        st.header("🔍 Filters")
        all_locations = df["Pricing Location"].unique().tolist()
        selected_locations = st.multiselect("Pricing Location", all_locations, default=[])
        min_date, max_date = df["Date"].min(), df["Date"].max()
        date_range = st.date_input("Date range", [min_date, max_date])
        include_avg = st.checkbox("Include average across selected dates", value=True)
        start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[-1])
        all_dispatch = ['Day_Ahead', 'Pre_Dispatch', 'Real_Time']
        dispatch_type = st.multiselect("Select dispatch type", all_dispatch, default=all_dispatch)
        st.subheader("💲 Price Settings")
        if dataset_option.startswith("Energy"):
            base_energy_price_options = ["LMP", "Energy Loss Price", "Energy Congestion Price"]
            extra_price_options = [col for col in custom_price_columns if col in df.columns]
            available_price_types = base_energy_price_options + extra_price_options
            display_options = [price_labels.get(p, p) for p in available_price_types]
            label_to_price_type = {price_labels.get(p, p): p for p in available_price_types}
            selected_label = st.selectbox("Price Type:", display_options)
            price_type = label_to_price_type[selected_label]
        else:
            available_price_types = ["LMP 10S", "Congestion Price 10S","LMP 10N", "Congestion Price 10N","LMP 30R", "Congestion Price 30R"]
            price_type = st.selectbox("Price Type:", available_price_types)

    def get_column_set(prefix):
        return price_type if price_type in custom_price_columns else f"{price_type}_{prefix}"

    def melt_df(df_to_melt):
        value_vars = [get_column_set(suffix) for suffix in dispatch_type if get_column_set(suffix) in df_to_melt.columns]
        if not value_vars:
            return pd.DataFrame()
        melted = df_to_melt.melt(id_vars=["Pricing Location", "Date", "Delivery Hour"] + (["Interval"] if "Interval" in df_to_melt.columns else []),
                                 value_vars=value_vars, var_name="Market", value_name="Price")
        if price_type in custom_price_columns:
            melted["Market"] = price_type
        else:
            melted["Market"] = melted["Market"].str.extract(r"_(Day_Ahead|Pre_Dispatch|Real_Time)")
        return melted

    if selected_locations:
        df_filtered = apply_filters(df, locations=selected_locations, dispatch_type=dispatch_type, start_date=start_date, end_date=end_date, hourly_bool=hourly_bool)
        df_filtered = df_filtered.sort_values(by=["Date", "Delivery Hour"])
        df_melted = melt_df(df_filtered)
    else:
        df_filtered = pd.DataFrame()
        df_melted = pd.DataFrame()

    df_avg_melted = None
    df_avg = pd.DataFrame()
    if include_avg:
        df_avg = compute_average_by_hour(df, dispatch_type=dispatch_type, start_date=start_date, end_date=end_date, hourly_bool=hourly_bool)
        df_avg = df_avg.sort_values(by=["Date", "Delivery Hour"])
        df_avg_melted = melt_df(df_avg)

    def prepare_time_axis(df_plot):
        df_plot["timestamp"] = pd.to_datetime(df_plot["Date"]) + pd.to_timedelta(df_plot["Delivery Hour"], unit="h")
        if "Interval" in df_plot.columns:
            df_plot["timestamp"] += pd.to_timedelta((df_plot["Interval"] - 1) * 5, unit="min")
        return df_plot

    st.subheader(f"📈 {price_labels.get(price_type, price_type)} Price Evolution")
    fig = go.Figure()

    if not df_melted.empty:
        df_melted = prepare_time_axis(df_melted)
        for market in df_melted["Market"].unique():
            for location in df_melted["Pricing Location"].unique():
                subset = df_melted[(df_melted["Market"] == market) & (df_melted["Pricing Location"] == location)].sort_values("timestamp")
                fig.add_trace(go.Scatter(x=subset["timestamp"], y=subset["Price"], mode="lines+markers", name=f"{location} – {market}", line=dict(dash="solid")))

    if df_avg_melted is not None and not df_avg_melted.empty:
        df_avg_melted = prepare_time_axis(df_avg_melted)
        for market in df_avg_melted["Market"].unique():
            subset = df_avg_melted[df_avg_melted["Market"] == market].sort_values("timestamp")
            fig.add_trace(go.Scatter(x=subset["timestamp"], y=subset["Price"], mode="lines+markers", name=f"Average – {market}", line=dict(dash="dot", width=3)))

    fig.update_layout(title=f"{price_labels.get(price_type, price_type)} per {'interval' if not hourly_bool else 'hour'} across markets",
                      xaxis_title="Timestamp", yaxis_title="Price ($/MWh)", legend_title="Zone / Market", template="plotly_white",
                      hovermode="x unified", height=600)
    st.plotly_chart(fig, use_container_width=True)

    if not df_filtered.empty:
        with st.expander("🔎 Overview of Filtered Data"):
            st.dataframe(df_filtered.style.format(precision=2), use_container_width=True)

    if not df_avg.empty:
        with st.expander("📖 Overview of the average"):
            st.dataframe(df_avg.style.format(precision=2), use_container_width=True)

    #st.markdown("""<div style='text-align: center; color: grey; margin-top: 50px; font-size: 0.9em;'>🕑 This dashboard is updated every day at <b>10am Toronto time</b></div>""", unsafe_allow_html=True)

elif page == "📈 Statistics":
    st.title("📈 Statistics Summary")
    df = load_dataset(dataset_urls["Energy - Hourly"])
    min_date, max_date = df["Date"].min(), df["Date"].max()
    date_range = st.sidebar.date_input("Date range", [min_date, max_date])
    start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[-1])
    df_filtered = df[(df["Date"] >= start_date) & (df["Date"] <= end_date)]

    if df_filtered.empty:
        st.warning("No data in selected date range.")
        st.stop()

    stats_dict = build_statistics(df_filtered)

    col1, col2 = st.columns(2)
    with col1:
        mode = st.selectbox("Choose type", ["Low", "High"])
    with col2:
        metric = st.selectbox("Choose metric", [
            "LMP_Day_Ahead",
            "LMP_Real_Time",
            "spread_4h_Day_Ahead",
            "spread_4h_Real_Time",
            "spread_Day_Ahead_vs_Real_Time"
        ])

    key = f"{mode.lower()}_{metric}"
    df_to_plot = stats_dict.get(key)

    ascending = True if mode == "Low" else False
    df_sorted = df_to_plot.sort_values(by=metric, ascending=ascending)
    category_order = df_sorted["Pricing Location"].tolist()
    values = df_sorted[metric]
    colors = values[::-1] if mode == "Low" else values

    fig = go.Figure(go.Bar(
        x=values,
        y=df_sorted["Pricing Location"],
        orientation="h",
        text=[f"{v:.2f}" for v in values],
        textposition="inside",
        insidetextanchor="start",
        marker=dict(
            color=colors,
            colorscale='Blues' if mode == "Low" else 'Reds',
            reversescale=False,
            colorbar=dict(title=metric)
        )
    ))

    fig.update_layout(
        xaxis_title=metric,
        yaxis_title="",
        yaxis=dict(
            categoryorder="array",
            categoryarray=category_order[::-1]
        ),
        height=450,
        showlegend=False,
        margin=dict(l=40, r=20, t=20, b=40),
        plot_bgcolor="rgba(0,0,0,0)"
    )

    st.plotly_chart(fig, use_container_width=True)





st.markdown("""
    <div style='text-align: center; color: grey; margin-top: 50px; font-size: 0.9em;'>
        🕑 This dashboard is updated every day at <b>10am Toronto time</b>
    </div>
""", unsafe_allow_html=True)
