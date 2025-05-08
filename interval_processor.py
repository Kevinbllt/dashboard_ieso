# src/hourly_processor.py

import pandas as pd
import numpy as np
import os 

def expand_to_intervals(df_hourly):
    """
    Expand hourly dataframe to 5-minute intervals (12 rows per hour) with Interval 1 to 12.
    """
    repeated_df = df_hourly.loc[df_hourly.index.repeat(12)].copy()
    repeated_df["Interval"] = np.tile(range(1, 13), len(df_hourly))
    return repeated_df.reset_index(drop=True)





def create_interval_energy_data (energy_day_ahead_df:pd.DataFrame, energy_pre_dispatch_df:pd.DataFrame, energy_real_time_df:pd.DataFrame) -> pd.DataFrame:
    """
    From raw energy daily files from Day Ahead, Pre-Dispatch and Real-Time, create a unique and interval daily file for energy data.

    Parameters:
        energy_day_ahead_df (pd.DataFrame): raw energy Day-Ahead df
        energy_pre_dispatch_df (pd.DataFrame): raw energy Pre-Dispatch df
        energy_real_time_df (pd.DataFrame): raw energy Real-Time df (interval format)

    Returns:
        pd.DataFrame, with the 3 dfs merged at a interval granularity for a specific date for energy data.
    """

    print("🧑‍🍳 Start creating interval energy data")

    col_to_rename = ['LMP', 'Energy Loss Price', 'Energy Congestion Price']

    # Start by converting the hourly dfs into 5-mninutes intervals dfs
    energy_day_ahead_df = energy_day_ahead_df.rename(columns={col: f"{col}_Day_Ahead" for col in col_to_rename})
    energy_day_ahead_interval_df = expand_to_intervals(energy_day_ahead_df)
    energy_day_ahead_interval_df["Date"] = pd.to_datetime(energy_day_ahead_interval_df["Date"])

    energy_pre_dispatch_df = energy_pre_dispatch_df.rename(columns={col: f"{col}_Pre_Dispatch" for col in col_to_rename})
    energy_pre_dispatch_interval_df = expand_to_intervals(energy_pre_dispatch_df)
    energy_pre_dispatch_interval_df["Date"] = pd.to_datetime(energy_pre_dispatch_interval_df["Date"])

    energy_real_time_df = energy_real_time_df.rename(columns={col: f"{col}_Real_Time" for col in col_to_rename})
    energy_real_time_df["Date"] = pd.to_datetime(energy_real_time_df["Date"])                                  
                                       

    interval_energy_df1 = pd.merge(
        energy_day_ahead_interval_df,
        energy_pre_dispatch_interval_df,
        on = ['Pricing Location','Date','Delivery Hour','Interval'],
        how = 'inner'
    )

    interval_energy_df2 = pd.merge(
    interval_energy_df1,
    energy_real_time_df,
    on = ['Pricing Location','Date','Delivery Hour','Interval'],
    how = 'inner'
    )

    print("✅ Finished the creation of interval energy data")

    return interval_energy_df2



def create_interval_OR_data (or_day_ahead_df:pd.DataFrame, or_pre_dispatch_df:pd.DataFrame, or_real_time_df:pd.DataFrame) -> pd.DataFrame:
    """
    From raw Operating Reserve daily files from Day Ahead, Pre-Dispatch and Real-Time, create a unique and interval daily file for OR data.

    Parameters:
        energy_day_ahead_df (pd.DataFrame): raw OR Day-Ahead df
        energy_pre_dispatch_df (pd.DataFrame): raw OR Pre-Dispatch df
        energy_real_time_df (pd.DataFrame): raw OR Real-Time df (interval format)

    Returns:
        pd.DataFrame, with the 3 dfs merged at a interval granularity for a specific date for OR data.
    """

    print("🧑‍🍳 Start creating interval OR data")

    col_to_rename = ['LMP 10S', 'Congestion Price 10S', 'LMP 10N', 'Congestion Price 10N','LMP 30R', 'Congestion Price 30R']

    # Start by converting the 5-minutes interval df into an hourly df.

    # Start by converting the hourly dfs into 5-mninutes intervals dfs
    or_day_ahead_interval_df = (expand_to_intervals(or_day_ahead_df)
                                    .rename(columns={col: f"{col}_Day_Ahead" for col in col_to_rename})
                                    .assign(Date = lambda df: pd.to_datetime(df['Date']))
                                    )


    or_pre_dispatch_interval_df = (expand_to_intervals(or_pre_dispatch_df)
                                       .rename(columns={col: f"{col}_Pre_Dispatch" for col in col_to_rename})
                                       .assign(Date = lambda df: pd.to_datetime(df['Date']))
                                    )

    or_real_time_df = (or_real_time_df
                                       .rename(columns={col: f"{col}_Real_Time" for col in col_to_rename})
                                       .assign(Date = lambda df: pd.to_datetime(df['Date']))                          
                           )   
    
    if 'DELIVERY_HOUR' in or_pre_dispatch_interval_df.columns:
        or_pre_dispatch_interval_df.rename(columns={'DELIVERY_HOUR':'Delivery Hour'}, inplace=True)

    inteval_or_df1 = pd.merge(
        or_day_ahead_interval_df,
        or_pre_dispatch_interval_df,
        on = ['Pricing Location','Date','Delivery Hour','Interval'],
        how = 'inner'
    )

    interval_or_df2 = pd.merge(
    inteval_or_df1,
    or_real_time_df,
    on = ['Pricing Location','Date','Delivery Hour','Interval'],
    how = 'inner'
    )

    print("✅ Finished the creation of interval OR data")

    return interval_or_df2





def add_interval_energy_data (interval_energy_df:pd.DataFrame, date_str:str):
    """
    Appends new interval energy data to the historical energy dataset if the specified date is not already present.

    This function checks whether the provided date (as a string) already exists in the historical energy file. 
    If the date is not found, the new data is appended and the updated historical file is saved, replacing the old version.
    If the date already exists in the file, no action is taken.

    Parameters:
        interval_energy_df (pd.DataFrame): A cleaned DataFrame containing interval energy data for a specific date.
        date_str (str): The date (in 'YYYYMMDD' format) corresponding to the new data to be added.

    Returns:
        None. The function saves the updated dataset to the existing historical CSV file.
    """

    energy_historical_df = pd.read_csv(r"C:\Users\kboulliat\Desktop\Dev\Monitoring_IESO_Data_project\data\energy\processed\energy_interval_historical.csv")
    energy_historical_columns = energy_historical_df.columns.tolist()

    target_date = pd.to_datetime(date_str).normalize()
    interval_energy_df["Date"] = pd.to_datetime(interval_energy_df["Date"]).dt.normalize()

    if target_date not in interval_energy_df["Date"].unique():
        print("⚙️ Start adding the new data to the historical energy file")
        interval_energy_df = interval_energy_df[energy_historical_columns] # Make sure we have the columns in the same order
        new_historical_df = pd.concat([energy_historical_df, interval_energy_df], axis=0)

        # Save the new df
        folder = "data/energy/processed"
        os.makedirs(folder, exist_ok=True)
        output_path = os.path.join(folder, f"energy_interval_historical.csv")
        new_historical_df.to_csv(output_path, index=False)

    else:
        print("❓ The date is already in the historical file")





def add_interval_OR_data (interval_or_df:pd.DataFrame, date_str:str):
    """
    Appends new interval Operating Reserve data to the historical OR dataset if the specified date is not already present.

    This function checks whether the provided date (as a string) already exists in the historical energy file. 
    If the date is not found, the new data is appended and the updated historical file is saved, replacing the old version.
    If the date already exists in the file, no action is taken.

    Parameters:
        interval_or_df (pd.DataFrame): A cleaned DataFrame containing interval OR data for a specific date.
        date_str (str): The date (in 'YYYYMMDD' format) corresponding to the new data to be added.

    Returns:
        None. The function saves the updated dataset to the existing historical CSV file.
    """

    or_historical_df = pd.read_csv(r"C:\Users\kboulliat\Desktop\Dev\Monitoring_IESO_Data_project\data\operating_reserve\processed\operating_reserve_interval_historical.csv")
    or_historical_columns = or_historical_df.columns.tolist()

    target_date = pd.to_datetime(date_str).normalize()
    interval_or_df["Date"] = pd.to_datetime(interval_or_df["Date"]).dt.normalize()

    if target_date not in interval_or_df["Date"].unique():
        print("⚙️ Start adding the new data to the historical OR file")
        hourly_energy_df = hourly_energy_df[or_historical_columns] # Make sure we have the columns in the same order
        new_historical_df = pd.concat([or_historical_df, interval_or_df], axis=0)

        # Save the new df
        folder = "data/operating_reserve/processed"
        os.makedirs(folder, exist_ok=True)
        output_path = os.path.join(folder, f"operating_reserve_interval_historical.csv")
        new_historical_df.to_csv(output_path, index=False)

    else:
        print("❓ The date is already in the historical file")




if __name__ == "__main__":
    day_ahead_df = pd.read_csv(r'C:\Users\kboulliat\Desktop\Dev\Monitoring_IESO_Data_project\data\energy\day_ahead\energy_day_ahead_20250503.csv')
    pre_dispatch_df = pd.read_csv(r'C:\Users\kboulliat\Desktop\Dev\Monitoring_IESO_Data_project\data\energy\predispatch\energy_predispatch_20250503.csv')
    real_time_df = pd.read_csv(r'C:\Users\kboulliat\Desktop\Dev\Monitoring_IESO_Data_project\data\energy\real_time\energy_real_time_20250503.csv')

    df = create_interval_energy_data(day_ahead_df, pre_dispatch_df, real_time_df)

    folder = "data/energy/processed"
    os.makedirs(folder, exist_ok=True)
    output_path = os.path.join(folder, f"energy_interval_historical.csv")
    #df.to_csv(output_path, index=False)

    add_interval_energy_data(df, '20250503')