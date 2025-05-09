# src/filter.py

import pandas as pd


def apply_filters(df: pd.DataFrame, 
                  locations: list = None, 
                  dispatch_type: list=None,
                  date: pd.Timestamp = None, 
                  start_date: pd.Timestamp = None, 
                  end_date: pd.Timestamp = None,
                  hourly_bool = True) -> pd.DataFrame:
    """
    Filters the DataFrame based on pricing locations, specific date, or date range.

    Parameters:
        df (pd.DataFrame): Input DataFrame with at least 'Pricing Location' and 'Date' columns.
        locations (list, optional): List of locations to keep. If None, no location filtering.
        date (pd.Timestamp, optional): Single date to filter on.
        start_date (pd.Timestamp, optional): Start of date range (inclusive).
        end_date (pd.Timestamp, optional): End of date range (inclusive).

    Returns:
        pd.DataFrame: Filtered DataFrame.
    """
    df_filtered = df.copy()

    if locations:
        df_filtered = df_filtered[df_filtered["Pricing Location"].isin(locations)]

    if date:
        df_filtered = df_filtered[df_filtered["Date"] == pd.to_datetime(date)]

    if start_date and end_date:
        df_filtered = df_filtered[
            (df_filtered["Date"] >= pd.to_datetime(start_date)) &
            (df_filtered["Date"] <= pd.to_datetime(end_date))
        ]

    # Handle column selection by dispatch type and hourly/interval mode
    if dispatch_type:
        col_to_keep_list = ['Date', 'Pricing Location', 'Delivery Hour']
        if not hourly_bool:
            col_to_keep_list.append('Interval')

        # Add only columns that match the selected dispatch suffixes
        for dispatch in dispatch_type:
            for col in df.columns:
                if col.endswith(dispatch):
                    col_to_keep_list.append(col)

        df_filtered = df_filtered[col_to_keep_list]

    return df_filtered





def compute_average_by_hour(
    df: pd.DataFrame,
    group_cols=["Date", "Delivery Hour"],
    dispatch_type: list = None,
    date: pd.Timestamp = None, 
    start_date: pd.Timestamp = None, 
    end_date: pd.Timestamp = None,
    hourly_bool = True
) -> pd.DataFrame:
    
    df_filtered = df.copy()
    
    if date:
        df_filtered = df_filtered[df_filtered["Date"] == pd.to_datetime(date)]

    if start_date and end_date:
        df_filtered = df_filtered[
            (df_filtered["Date"] >= pd.to_datetime(start_date)) &
            (df_filtered["Date"] <= pd.to_datetime(end_date))
        ]

    if hourly_bool == True:
        df_avg = (
            df_filtered
            .groupby(group_cols, as_index=False)
            .mean(numeric_only=True)
        )
    elif hourly_bool == False:
        group_cols = group_cols + ['Interval']
        df_avg = (
            df_filtered
            .groupby(group_cols, as_index=False)
            .mean(numeric_only=True)
        )

    # Keep only selected dispatch columns + group cols
    if dispatch_type:
        col_to_keep_list = group_cols.copy()
        for dispatch in dispatch_type:
            for col in df_avg.columns:
                if col.endswith(dispatch):
                    col_to_keep_list.append(col)
        df_avg = df_avg[col_to_keep_list]

    # Add synthetic location for plotting purposes
    df_avg["Pricing Location"] = "Average"

    return df_avg



def sort_and_extract_top(df, sort_col='LMP_Day_Ahead', ascending=True, top_n=5):
    df_grouped = (
        df.groupby('Pricing Location')
          .mean(numeric_only=True)
          .reset_index()
          .sort_values(by=sort_col, ascending=ascending)
    )

    return df_grouped[['Pricing Location', sort_col]].head(top_n)

def build_statistics (df):

    metrics = [
        "LMP_Day_Ahead",
        "LMP_Real_Time",
        "spread_4h_Day_Ahead",
        "spread_4h_Real_Time",
        "spread_Day_Ahead_vs_Real_Time"
    ]

    dict_top = {}

    for metric in metrics:
        dict_top[f"low_{metric}"] = sort_and_extract_top(
            df,  sort_col=metric, ascending=True, top_n=5
        )
        dict_top[f"high_{metric}"] = sort_and_extract_top(
            df, sort_col=metric, ascending=False, top_n=5
        )

    return dict_top
