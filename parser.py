# src/parser.py

import requests
import os
import pandas as pd
from io import StringIO
from pathlib import Path
import time
import urllib3





### Day-Ahead Data ###

def fetch_day_ahead_energy_lmp(date_str: str, sleep_sec=5) -> pd.DataFrame:
    """
    Fetch energy Day-Ahead data from the IESO website and return a DataFrame of the file for a specific date (format:YYYYMMDD)

    Parameters:
        date_str (str): Date (format YYYYMMDD)
        sleep_sec (int): Time in seconds between 2 fetchs

    Returns:
        pd.DataFrame with Day-Ahead energy LMPs, Energy loss price and Energy congestion price for all pricing locations and all hours of the dispatch day (the next day).
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    base_url = "https://reports-public.ieso.ca/public/DAHourlyEnergyLMP/"
    filename = f"PUB_DAHourlyEnergyLMP_{date_str}.csv"
    full_url = base_url + filename

    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    } #Avoir being suspicious when scrapping

    print(f"📥 Fetching Energy Day-Ahead data for date: {date_str}")
    response = requests.get(full_url, headers=headers, verify=False)
    time.sleep(sleep_sec) # Avoid being suspicious

    if response.status_code == 200:
        predisp_or_lmp_df = pd.read_csv(StringIO(response.text), skiprows=1) # The first line is not usefull for us
        predisp_or_lmp_df['Date'] = pd.to_datetime(date_str , format='%Y%m%d')

        # Save CSV
        folder = "data/energy/day_ahead"
        os.makedirs(folder, exist_ok=True)
        output_path = os.path.join(folder, f"energy_day_ahead_{date_str}.csv")

        try:
            predisp_or_lmp_df.to_csv(output_path, index=False)
            print(f"✅ File saved into folder {folder}")
        except:
            print("❌ Failed to save the file")

    else:
        raise ConnectionError(f"Failed to download file {filename}: HTTP {response.status_code}")





def fetch_day_ahead_OR_lmp(date_str: str, sleep_sec=5) -> pd.DataFrame:
    """
    Fetch Operating Reserve Day-Ahead data from the IESO website and return a DataFrame of the file for a specific date (format:YYYYMMDD)

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

        # Save CSV
        folder = "data/operating_reserve/day_ahead"
        os.makedirs(folder, exist_ok=True)
        output_path = os.path.join(folder, f"OR_day_ahead_{date_str}.csv")

        try:
            predisp_or_lmp_df.to_csv(output_path, index=False)
            print(f"✅ File saved into folder {folder}")
        except:
            print("❌ Failed to save the file")

    else:
        print(f"⚠️ File not found for date {date_str}: HTTP {response.status_code}")
        return pd.DataFrame()  # Return empty DataFrame instead of crashing








### Pre-Dispatch Data ###


def fetch_predisp_energy_lmp(date_str: str, sleep_sec=5) -> pd.DataFrame:
    """
    Fetch energy predispatch data from the IESO website and return a DataFrame of the file for a specific date (format:YYYYMMDD)

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
        output_path = os.path.join(folder, f"energy_predispatch_{date_str}.csv")

        try:
            predisp_or_lmp_df.to_csv(output_path, index=False)
            print(f"✅ File saved into folder {folder}")
        except:
            print("❌ Failed to save the file")

    else:
        print(f"⚠️ File not found for date {date_str}: HTTP {response.status_code}")
        return pd.DataFrame()  # Return empty DataFrame instead of crashing





def fetch_predisp_OR_lmp(date_str: str, sleep_sec=5) -> pd.DataFrame:
    """
    Fetch Operating Reserve Pre-Dispatch data from the IESO website and return a DataFrame of the file for a specific date (format:YYYYMMDD)

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
        output_path = os.path.join(folder, f"OR_predispatch_{date_str}.csv")

        try:
            predisp_or_lmp_df.to_csv(output_path, index=False)
            print(f"✅ File saved into folder {folder}")
        except:
            print("❌ Failed to save the file")

    else:
        print(f"⚠️ File not found for date {date_str}: HTTP {response.status_code}")
        return pd.DataFrame()  # Return empty DataFrame instead of crashing
    




### Real-Time data ###


def fetch_real_time_energy_lmp(date_str: str, sleep_sec=5) -> pd.DataFrame:
    """
    Fetch Real-Time energy data from the IESO website and return a DataFrame of the file for a specific date (format:YYYYMMDD)

    Parameters:
        date_str (str): Date (format YYYYMMDD)
        sleep_sec (int): Time in seconds between 2 fetchs

    Returns:
        pd.DataFrame with Real-Time energy LMPs, Energy loss price and Energy congestion price for all pricing locations and all hours of the dispatch day (the current day).
    """ 
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    base_url = "https://reports-public.ieso.ca/public/RealtimeEnergyLMP/"
    hours = [f"{i:02d}" for i in range(4, 24)]
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
    output_path = os.path.join(folder, f"energy_real_time_{date_str}.csv")

    try:
        real_time_lmp_df.to_csv(output_path, index=False)
        print(f"✅ File saved into folder {folder}")
    except:
        print("❌ Failed to save the file")





def fetch_real_time_OR_lmp(date_str: str, sleep_sec=5) -> pd.DataFrame:
    """
    Fetch Real-Time Operating Reserve data from the IESO website and return a DataFrame of the file for a specific date (format:YYYYMMDD)

    Parameters:
        date_str (str): Date (format YYYYMMDD)
        sleep_sec (int): Time in seconds between 2 fetchs

    Returns:
        pd.DataFrame with Real-Time Operating Reserve LMPs, Congestion Prices per Class for all pricing location and all hour of the dispatch day
    """ 
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    base_url = "https://reports-public.ieso.ca/public/RealtimeORLMP/"
    hours = [f"{i:02d}" for i in range(4, 24)]
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
    output_path = os.path.join(folder, f"OR_real_time_{date_str}.csv")

    try:
        real_time_or_lmp_df.to_csv(output_path, index=False)
        print(f"✅ File saved into folder {folder}")
    except:
        print("❌ Failed to save the file")

    


