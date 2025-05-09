# src/parser.py

import requests
import os
import pandas as pd
from io import StringIO
from pathlib import Path
import time
from google.cloud import storage
import urllib3




### Day-Ahead Data ###


def fetch_day_ahead_energy_lmp(date_str: str, sleep_sec=5) -> pd.DataFrame:
    """
    Fetch energy Day-Ahead data from the IESO website, save locally (gzip), upload to GCS, and return the DataFrame.
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    base_url = "https://reports-public.ieso.ca/public/DAHourlyEnergyLMP/"
    filename = f"PUB_DAHourlyEnergyLMP_{date_str}.csv"
    full_url = base_url + filename

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    print(f"📥 Fetching Energy Day-Ahead data for date: {date_str}")
    response = requests.get(full_url, headers=headers, verify=False)
    time.sleep(sleep_sec)

    if response.status_code == 200:
        df = pd.read_csv(StringIO(response.text), skiprows=1)
        df['Date'] = pd.to_datetime(date_str, format='%Y%m%d')

        # Local save
        folder = "data/energy/day_ahead"
        os.makedirs(folder, exist_ok=True)
        local_path = os.path.join(folder, f"energy_day_ahead_{date_str}.csv.gz")

        try:
            df.to_csv(local_path, index=False, compression="gzip")
            print(f"✅ Energy Day-Ahead file saved locally: {local_path}")

        except Exception as e:
            print(f"❌ Failed to save Energy Day-Ahead file locally: {e}")

        # Upload to GCS
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket("ieso_monitoring_market_data")
            blob_path = f"energy/day_ahead/energy_day_ahead_{date_str}.csv.gz"
            blob = bucket.blob(blob_path)

            blob.upload_from_filename(local_path)
            print(f"✅ Energy Day-Ahead file uploaded to GCS: {blob.public_url}")

        except Exception as e:
            print(f"❌ Failed to upload Energy Day-Ahead file to GCS: {e}")

    else:
        raise ConnectionError(f"❌ Failed to download file {filename}: HTTP {response.status_code}")





def fetch_day_ahead_OR_lmp(date_str: str, sleep_sec=5) -> pd.DataFrame:
    """
    Fetch Operating Reserve Day-Ahead data from the IESO website, save locally (gzip), upload to GCS, and return the DataFrame. 

    Parameters:
        date_str (str): Date (format YYYYMMDD)
        sleep_sec (int): Time in seconds between 2 fetchs

    Returns:
        pd.DataFrame with Operating Reserve LMPs and Congestion Prices per Class for all pricing locations and all hours of the dispatch day (the next day).
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    base_url = "https://reports-public.ieso.ca/public/DAHourlyORLMP/"
    filename = f"PUB_DAHourlyORLMP_{date_str}.csv"
    full_url = base_url + filename

    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    } #Avoir being suspicious when scrapping

    print(f"📥 Fetching Operating Reserve Day-Ahead data for date: {date_str}")
    response = requests.get(full_url, headers=headers, verify=False)
    time.sleep(sleep_sec) # Avoid being suspicious

    if response.status_code == 200:
        predisp_or_lmp_df = pd.read_csv(StringIO(response.text), skiprows=1) # The first line is not usefull for us
        predisp_or_lmp_df['Date'] = pd.to_datetime(date_str , format='%Y%m%d')

        # Local save
        folder = "data/operating_reserve/day_ahead"
        os.makedirs(folder, exist_ok=True)
        local_path = os.path.join(folder, f"OR_day_ahead_{date_str}.csv.gz")

        try:
            predisp_or_lmp_df.to_csv(local_path, index=False, compression="gzip")
            print(f"✅ Operating Reserve Day-Ahead file saved into folder {folder}")

        except:
            print("❌ Failed to save locally the Operating Reserve Day-Ahead file")
        
        # Upload to GCS
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket("ieso_monitoring_market_data")
            blob_path = f"operating_reserve/day_ahead/OR_day_ahead_{date_str}.csv.gz"
            blob = bucket.blob(blob_path)

            blob.upload_from_filename(local_path)
            print(f"✅ Operating Reserve Day-Ahead file uploaded to GCS: {blob.public_url}")
        
        except Exception as e:
            print(f"❌ Failed to upload to GCS: {e}")     

    else:
        print(f"⚠️ File not found for date {date_str}: HTTP {response.status_code}")
        return pd.DataFrame()  # Return empty DataFrame instead of crashing








### Pre-Dispatch Data ###


def fetch_predisp_energy_lmp(date_str: str, sleep_sec=5) -> pd.DataFrame:
    """
    Fetch energy predispatch data from the IESO website, save locally (gzip), upload to GCS, and return the DataFrame.

    Parameters:
        date_str (str): Date (format YYYYMMDD)
        sleep_sec (int): Time in seconds between 2 fetchs

    Returns:
        pd.DataFrame with Pre_Dispatch energy LMPs, Energy loss price and Energy congestion price for all pricing locations and all hours of the dispatch day (the next day).
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    base_url = "https://reports-public.ieso.ca/public/PredispHourlyEnergyLMP/"
    filename = f"PUB_PredispHourlyEnergyLMP_{date_str}_v5.csv"
    full_url = base_url + filename

    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    } #Avoir being suspicious when scrapping

    print(f"📥 Fetching Energy Pre-Dispatch data for date: {date_str}")
    response = requests.get(full_url, headers=headers, verify=False)
    time.sleep(sleep_sec) # Avoid being suspicious

    if response.status_code == 200:
        predisp_or_lmp_df = pd.read_csv(StringIO(response.text), skiprows=1) # The first line is not usefull for us
        predisp_or_lmp_df['Date'] = pd.to_datetime(date_str , format='%Y%m%d')

        # Save CSV
        folder = "data/energy/predispatch"
        os.makedirs(folder, exist_ok=True)
        output_path = os.path.join(folder, f"energy_predispatch_{date_str}.csv.gz")

        try:
            predisp_or_lmp_df.to_csv(output_path, index=False, compression="gzip")
            print(f"✅ Energy Pre-Dispatch file saved into folder {folder}")
        except:
            print("❌ Failed to save locally Energy Pre-Dispatch the file")

        # Upload to GCS
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket("ieso_monitoring_market_data")
            blob_path = f"energy/pre_dispatch/energy_pre_dispatch_{date_str}.csv.gz"
            blob = bucket.blob(blob_path)

            blob.upload_from_filename(output_path)
            print(f"✅ Energy Pre-Dispatch file uploaded to GCS: {blob.public_url}")

        except Exception as e:
            print(f"❌ Failed to upload Energy Pre-Dispatch file to GCS: {e}")
            
    else:
        print(f"⚠️ File not found for date {date_str}: HTTP {response.status_code}")
        return pd.DataFrame()  # Return empty DataFrame instead of crashing





def fetch_predisp_OR_lmp(date_str: str, sleep_sec=5) -> pd.DataFrame:
    """
    Fetch Operating Reserve Pre-Dispatch data from the IESO website, save locally (gzip), upload to GCS, and return the DataFrame.

    Parameters:
        date_str (str): Date (format YYYYMMDD)
        sleep_sec (int): Time in seconds between 2 fetchs

    Returns:
        pd.DataFrame with Real-Time Operating Reserve LMPs, Congestion Prices per Class for all pricing location and all hour of the dispatch day (the next day).
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    base_url = "https://reports-public.ieso.ca/public/PredispHourlyORLMP/"
    filename = f"PUB_PredispHourlyORLMP_{date_str}.csv"
    full_url = base_url + filename

    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    } #Avoir being suspicious when scrapping

    print(f"📥 Fetching Operating Reserve Pre-Dispatch data for date: {date_str}")
    response = requests.get(full_url, headers=headers, verify=False)
    time.sleep(sleep_sec) # Avoid being suspicious

    if response.status_code == 200:
        predisp_or_lmp_df = pd.read_csv(StringIO(response.text), skiprows=1) # The first line is not usefull for us
        predisp_or_lmp_df['Date'] = pd.to_datetime(date_str , format='%Y%m%d')

        # Save CSV
        folder = "data/operating_reserve/predispatch"
        os.makedirs(folder, exist_ok=True)
        output_path = os.path.join(folder, f"OR_predispatch_{date_str}.csv.gz")

        try:
            predisp_or_lmp_df.to_csv(output_path, index=False, compression='gzip')
            print(f"✅ Pre-Dispatch Operating Reserve file saved into folder {folder}")
        except:
            print("❌ Failed to save locally Pre-Dispatch Operating Reserve file")

        # Upload to GCS
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket("ieso_monitoring_market_data")
            blob_path = f"operating_reserve/pre_disptach/OR_pre_dispatch_{date_str}.csv.gz"
            blob = bucket.blob(blob_path)

            blob.upload_from_filename(output_path)
            print(f"✅ Pre-Dispatch Operating Reserve file uploaded to GCS: {blob.public_url}")

        except Exception as e:
            print(f"❌ Failed to upload Pre-Dispatch Operating Reserve file to GCS: {e}")

    else:
        print(f"⚠️ File not found for date {date_str}: HTTP {response.status_code}")
        return pd.DataFrame()  # Return empty DataFrame instead of crashing
    




### Real-Time data ###


def fetch_real_time_energy_lmp(date_str: str, sleep_sec=5) -> pd.DataFrame:
    """
    Fetch Real-Time energy data from the IESO website, save locally (gzip), upload to GCS, and return the DataFrame.

    Parameters:
        date_str (str): Date (format YYYYMMDD)
        sleep_sec (int): Time in seconds between 2 fetchs

    Returns:
        pd.DataFrame with Real-Time energy LMPs, Energy loss price and Energy congestion price for all pricing locations and all hours of the dispatch day (the current day).
    """ 
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    base_url = "https://reports-public.ieso.ca/public/RealtimeEnergyLMP/"
    hours = [f"{i:02d}" for i in range(1, 24)]
    df_list = []

    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    } #Avoir being suspicious when scrapping

    for h in hours:
        filename = f"PUB_RealtimeEnergyLMP_{date_str}{h}.csv"
        full_url = base_url + filename

        print(f"📥 Fetching Energy Real-Time data for date: {date_str} and hour {h}") 
        response = requests.get(full_url, headers=headers, verify=False)
        time.sleep(sleep_sec) # Avoid being suspicious

        if response.status_code == 200:
            h_real_time_lmp_df = pd.read_csv(StringIO(response.text), skiprows=1) # The first line is not usefull for us
            h_real_time_lmp_df['Date'] = pd.to_datetime(date_str , format='%Y%m%d')
            df_list.append(h_real_time_lmp_df)

        else:
            raise ConnectionError(f"Failed to download file {filename}: HTTP {response.status_code}")

    # Concat all the hourly df into a single one
    real_time_lmp_df = pd.concat(df_list, axis=0, ignore_index=True)

    # Save CSV
    folder = "data/energy/real_time"
    os.makedirs(folder, exist_ok=True)
    output_path = os.path.join(folder, f"energy_real_time_{date_str}.csv.gz")

    try:
        real_time_lmp_df.to_csv(output_path, index=False, compression="gzip")
        print(f"✅ Real-Time energy file saved into folder {folder}")
    except:
        print("❌ Failed to save locally the Real-Time energy file")

        # Upload to GCS
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket("ieso_monitoring_market_data")
        blob_path = f"energy/real_time/energy_real_time_{date_str}.csv.gz"
        blob = bucket.blob(blob_path)

        blob.upload_from_filename(output_path)
        print(f"✅ Real-Time energy file uploaded to GCS: {blob.public_url}")
    
    except Exception as e:
        print(f"❌ Failed to upload Real-Time energy file to GCS: {e}")   



def fetch_real_time_OR_lmp(date_str: str, sleep_sec=5) -> pd.DataFrame:
    """
    Fetch Real-Time Operating Reserve data from the IESO website, save locally (gzip), upload to GCS, and return the DataFrame.

    Parameters:
        date_str (str): Date (format YYYYMMDD)
        sleep_sec (int): Time in seconds between 2 fetchs

    Returns:
        pd.DataFrame with Real-Time Operating Reserve LMPs, Congestion Prices per Class for all pricing location and all hour of the dispatch day
    """ 
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    base_url = "https://reports-public.ieso.ca/public/RealtimeORLMP/"
    hours = [f"{i:02d}" for i in range(1, 24)]
    df_list = []

    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    } #Avoir being suspicious when scrapping

    for h in hours:
        filename = f"PUB_RealtimeORLMP_{date_str}{h}.csv"
        full_url = base_url + filename

        print(f"📥 Fetching Real-Time Operating Reserve data for date: {date_str} and hour {h}") 
        response = requests.get(full_url, headers=headers, verify=False)
        time.sleep(sleep_sec) # Avoid being suspicious

        if response.status_code == 200:
            h_real_time_or_lmp_df = pd.read_csv(StringIO(response.text), skiprows=1) # The first line is not usefull for us
            h_real_time_or_lmp_df['Date'] = pd.to_datetime(date_str , format='%Y%m%d')
            df_list.append(h_real_time_or_lmp_df)

        else:
            raise ConnectionError(f"Failed to download file {filename}: HTTP {response.status_code}")

    # Concat all the hourly df into a single one
    real_time_or_lmp_df = pd.concat(df_list, axis=0, ignore_index=True)

    # Save CSV
    folder = "data/operating_reserve/real_time"
    os.makedirs(folder, exist_ok=True)
    output_path = os.path.join(folder, f"OR_real_time_{date_str}.csv.gz")

    try:
        real_time_or_lmp_df.to_csv(output_path, index=False, compression="gzip")
        print(f"✅ Real-Time Operating Reserve file saved into folder {folder}")
    except:
        print("❌ Failed to locally save the Real-Time Operating Reserve file")

    # Upload to GCS
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket("ieso_monitoring_market_data")
        blob_path = f"operating_reserve/real_time/OR_real_time_{date_str}.csv.gz"
        blob = bucket.blob(blob_path)

        blob.upload_from_filename(output_path)
        print(f"✅ Real-Time Operating Reserve file uploaded to GCS: {blob.public_url}")
    
    except Exception as e:
        print(f"❌ Failed to upload Real-Time Operating Reserve file to GCS: {e}")   




#if __name__ == "__main__":
    #fetch_day_ahead_energy_lmp("20250504")
    #time.sleep(20)
    #fetch_day_ahead_OR_lmp("20250504")
    #time.sleep(20)
    #fetch_predisp_energy_lmp("20250504")
    #time.sleep(20)
    #fetch_predisp_OR_lmp("20250504")
    #time.sleep(20)
    #fetch_real_time_energy_lmp("20250504")
    #time.sleep(20)
    #fetch_real_time_OR_lmp("20250504")
    


