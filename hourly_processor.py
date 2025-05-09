# src/hourly_processor.py

import pandas as pd
import os 
from google.cloud import storage
import io


def create_hourly_energy_data (energy_day_ahead_df:pd.DataFrame, energy_pre_dispatch_df:pd.DataFrame, energy_real_time_df:pd.DataFrame) -> pd.DataFrame:
    """
    From raw energy daily files from Day Ahead, Pre-Dispatch and Real-Time, create a unique and hourly daily file for energy data.

    Parameters:
        energy_day_ahead_df (pd.DataFrame): raw energy Day-Ahead df
        energy_pre_dispatch_df (pd.DataFrame): raw energy Pre-Dispatch df
        energy_real_time_df (pd.DataFrame): raw energy Real-Time df (interval format)

    Returns:
        pd.DataFrame, with the 3 dfs merged at a hourly granularity for a specific date for energy data.
    """

    print("🧑‍🍳 Start creating hourly energy data")

    col_to_rename = ['LMP', 'Energy Loss Price', 'Energy Congestion Price']

    # Start by converting the 5-minutes interval df into an hourly df.
    energy_real_time_hourly_df = (energy_real_time_df
                                  .groupby(['Pricing Location','Date','Delivery Hour'])
                                  .mean()
                                  .reset_index()
                                  .drop(columns='Interval')
                                  .rename(columns={col: f"{col}_Real_Time" for col in col_to_rename})
                                  .assign(Date = lambda df: pd.to_datetime(df['Date']))
                                )
    

    energy_day_ahead_df = (energy_day_ahead_df
                           .rename(columns={col: f"{col}_Day_Ahead" for col in col_to_rename})
                           .assign(Date = lambda df: pd.to_datetime(df['Date']))
                           )

    energy_pre_dispatch_df = (energy_pre_dispatch_df
                              .rename(columns={col: f"{col}_Pre_Dispatch" for col in col_to_rename})
                              .assign(Date = lambda df: pd.to_datetime(df['Date']))
                              )

    hourly_energy_df1 = pd.merge(
        energy_day_ahead_df,
        energy_pre_dispatch_df,
        on = ['Pricing Location','Date','Delivery Hour'],
        how = 'inner'
    )

    hourly_energy_df2 = pd.merge(
    hourly_energy_df1,
    energy_real_time_hourly_df,
    on = ['Pricing Location','Date','Delivery Hour'],
    how = 'inner'
    )

    print("✅ Finished the creation of hourly energy data")

    return hourly_energy_df2



def create_hourly_OR_data (or_day_ahead_df:pd.DataFrame, or_pre_dispatch_df:pd.DataFrame, or_real_time_df:pd.DataFrame) -> pd.DataFrame:
    """
    From raw Operating Reserve daily files from Day Ahead, Pre-Dispatch and Real-Time, create a unique and hourly daily file for OR data.

    Parameters:
        energy_day_ahead_df (pd.DataFrame): raw OR Day-Ahead df
        energy_pre_dispatch_df (pd.DataFrame): raw OR Pre-Dispatch df
        energy_real_time_df (pd.DataFrame): raw OR Real-Time df (interval format)

    Returns:
        pd.DataFrame, with the 3 dfs merged at a hourly granularity for a specific date for OR data.
    """

    print("🧑‍🍳 Start creating hourly OR data")

    col_to_rename = ['LMP 10S', 'Congestion Price 10S', 'LMP 10N', 'Congestion Price 10N','LMP 30R', 'Congestion Price 30R']

    # Start by converting the 5-minutes interval df into an hourly df.

    or_real_time_hourly_df = (or_real_time_df
                                  .groupby(['Pricing Location','Date','Delivery Hour'])
                                  .mean()
                                  .reset_index()
                                  .drop(columns='Interval')
                                  .rename(columns={col: f"{col}_Real_Time" for col in col_to_rename})
                                  .assign(Date = lambda df: pd.to_datetime(df['Date']))
                                )
    

    or_day_ahead_df = (or_day_ahead_df
                           .rename(columns={col: f"{col}_Day_Ahead" for col in col_to_rename})
                           .assign(Date = lambda df: pd.to_datetime(df['Date']))
                           )

    or_pre_dispatch_df = (or_pre_dispatch_df
                              .rename(columns={col: f"{col}_Pre_Dispatch" for col in col_to_rename})
                              .assign(Date = lambda df: pd.to_datetime(df['Date']))
                              )
    
    if 'DELIVERY_HOUR' in or_pre_dispatch_df.columns:
        or_pre_dispatch_df.rename(columns={'DELIVERY_HOUR':'Delivery Hour'}, inplace=True)

    hourly_or_df1 = pd.merge(
        or_day_ahead_df,
        or_pre_dispatch_df,
        on = ['Pricing Location','Date','Delivery Hour'],
        how = 'inner'
    )

    hourly_or_df2 = pd.merge(
    hourly_or_df1,
    or_real_time_hourly_df,
    on = ['Pricing Location','Date','Delivery Hour'],
    how = 'inner'
    )

    print("✅ Finished the creation of hourly OR data")

    return hourly_or_df2



def write_df_to_gcs(df, blob_path):
    buffer = io.BytesIO()
    df.to_csv(buffer, index=False, compression="gzip")
    buffer.seek(0)

    storage_client = storage.Client()
    bucket = storage_client.bucket("ieso_monitoring_market_data")
    blob = bucket.blob(blob_path)

    blob.upload_from_file(buffer, content_type="application/gzip")

    print(f"✅ Fichier mis à jour dans GCS : gs://ieso_monitoring_market_data/{blob_path}")



def add_hourly_energy_data (hourly_energy_df:pd.DataFrame, date_str:str):
    """
    Appends new hourly energy data to the historical energy dataset if the specified date is not already present.

    This function checks whether the provided date (as a string) already exists in the historical energy file. 
    If the date is not found, the new data is appended and the updated historical file is saved, replacing the old version.
    If the date already exists in the file, no action is taken.

    Parameters:
        hourly_energy_df (pd.DataFrame): A cleaned DataFrame containing hourly energy data for a specific date.
        date_str (str): The date (in 'YYYYMMDD' format) corresponding to the new data to be added.

    Returns:
        None. The function saves the updated dataset to the existing historical CSV file.
    """

    energy_url = "https://storage.googleapis.com/ieso_monitoring_market_data/energy/processed/energy_historical_hourly.csv.gz"
    energy_historical_df = pd.read_csv(energy_url, compression='gzip' , parse_dates=["Date"])
    energy_historical_columns = energy_historical_df.columns.tolist()

    target_date = pd.to_datetime(date_str).normalize()
    hourly_energy_df["Date"] = pd.to_datetime(hourly_energy_df["Date"]).dt.normalize()
    energy_historical_df["Date"] = pd.to_datetime(energy_historical_df["Date"]).dt.normalize()

    if target_date.date() not in energy_historical_df["Date"].dt.date.unique():
        print("⚙️ Start adding the new data to the historical energy file")
        hourly_energy_df = hourly_energy_df[energy_historical_columns]  # enforce column order
        new_historical_df = pd.concat([energy_historical_df, hourly_energy_df], axis=0)
        new_historical_df.sort_values('Date', ascending=True)

        write_df_to_gcs(new_historical_df, "energy/processed/energy_historical_hourly.csv.gz")
        print("✅ Energy historical hourly data updated into GCP")


    else:
        print("❓ The date is already in the GCP historical hourly file")





def add_hourly_OR_data (hourly_or_df:pd.DataFrame, date_str:str):
    """
    Appends new hourly Operating Reserve data to the historical OR dataset if the specified date is not already present.

    This function checks whether the provided date (as a string) already exists in the historical energy file. 
    If the date is not found, the new data is appended and the updated historical file is saved, replacing the old version.
    If the date already exists in the file, no action is taken.

    Parameters:
        hourly_or_df (pd.DataFrame): A cleaned DataFrame containing hourly OR data for a specific date.
        date_str (str): The date (in 'YYYYMMDD' format) corresponding to the new data to be added.

    Returns:
        None. The function saves the updated dataset to the existing historical CSV file.
    """
    or_url = "https://storage.googleapis.com/ieso_monitoring_market_data/operating_reserve/processed/OR_historical_hourly.csv.gz"
    or_historical_df = pd.read_csv(or_url, compression='gzip',parse_dates=["Date"])
    or_historical_columns = or_historical_df.columns.tolist()

    # Normalisation des dates
    target_date = pd.to_datetime(date_str).normalize()
    hourly_or_df["Date"] = pd.to_datetime(hourly_or_df["Date"]).dt.normalize()
    or_historical_df["Date"] = pd.to_datetime(or_historical_df["Date"]).dt.normalize()  # 🔥 important !

    # Check sur l’historique, pas sur le nouveau
    if target_date.date() not in or_historical_df["Date"].dt.date.unique():
        print("⚙️ Start adding the new data to the historical OR file")

        hourly_or_df = hourly_or_df[or_historical_columns]  # 🔁 pas hourly_energy_df ici
        new_historical_df = pd.concat([or_historical_df, hourly_or_df], axis=0)
        new_historical_df.sort_values('Date', ascending=True)

        write_df_to_gcs(new_historical_df, "operating_reserve/processed/OR_historical_hourly.csv.gz")

        print("✅ OR historical hourly data updated into GCP")
    else:
        print("❓ The date is already in the historical GCP OR file")



#if __name__ == "__main__":
    #day_ahead_url = "https://storage.googleapis.com/ieso_monitoring_market_data/operating_reserve/day_ahead/OR_day_ahead_20250504.csv.gz"
    #day_ahead_df = pd.read_csv(day_ahead_url, compression='gzip',parse_dates=["Date"])
     
    #pre_dispatch_url = "https://storage.googleapis.com/ieso_monitoring_market_data/operating_reserve/pre_disptach/OR_pre_dispatch_20250504.csv.gz"
    #pre_dispatch_df = pd.read_csv(pre_dispatch_url, compression='gzip',parse_dates=["Date"])

    #real_time_url = "https://storage.googleapis.com/ieso_monitoring_market_data/operating_reserve/real_time/OR_real_time_20250504.csv.gz"
    #real_time_df = pd.read_csv(real_time_url, compression='gzip',parse_dates=["Date"])

    #df = create_hourly_OR_data(day_ahead_df, pre_dispatch_df, real_time_df )

    #folder = "data/operating_reserve/processed"
    #os.makedirs(folder, exist_ok=True)
    #output_path = os.path.join(folder, f"OR_historical_hourly.csv.gz")
    #df.to_csv(output_path, index=False, compression="gzip")


    #storage_client = storage.Client()
    #bucket = storage_client.bucket("ieso_monitoring_market_data")
    #blob_path = f"operating_reserve/processed/OR_historical_hourly.csv.gz"
    #blob = bucket.blob(blob_path)
    #blob.upload_from_filename(output_path)
 
    #df=create_hourly_OR_data(day_ahead_df, pre_dispatch_df, real_time_df )

    #folder = "data/operating_reserve/processed"
    #os.makedirs(folder, exist_ok=True)
    #output_path = os.path.join(folder, f"operating_reserve_hourly_historical.csv")
    #df.to_csv(output_path, index=False)

    #add_hourly_OR_data(df, '20250504')

    #energy_url = "https://storage.googleapis.com/ieso_monitoring_market_data/energy/processed/energy_hourly2_historical.csv.gz"
    #print(pd.read_csv(energy_url, compression='gzip' , parse_dates=["Date"]))
