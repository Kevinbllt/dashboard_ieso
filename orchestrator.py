# src/orchestrator.py

import pandas as pd
import numpy as np
from datetime import date, timedelta
import time
import io
import os 

from parser import fetch_day_ahead_energy_lmp, fetch_day_ahead_OR_lmp, fetch_predisp_energy_lmp, fetch_predisp_OR_lmp, fetch_real_time_energy_lmp, fetch_real_time_OR_lmp
from hourly_processor import create_hourly_energy_data, create_hourly_OR_data, add_hourly_energy_data, add_hourly_OR_data
from interval_processor import expand_to_intervals, create_interval_energy_data, create_interval_OR_data, add_interval_energy_data, add_interval_OR_data



def import_daily_data (date_today_str:str):
    """
    Fetch and store raw energy and operating reserve data for a specific date for Day-Ahead, Pre-Dispatch and Real-Time.
    """
    print("🔄 Daily import of the IESO data")
    fetch_day_ahead_energy_lmp(date_today_str)
    print(f"✅ Fetched Day-Ahead energy data for the date: {date_today_str}")
    time.sleep(20) # Add 20 seconds between each fetch not too look suspicious
    fetch_day_ahead_OR_lmp(date_today_str)
    print(f"✅ Fetched Day-Ahead Operating Reserve data for the date: {date_today_str}")
    time.sleep(20)
    fetch_predisp_energy_lmp(date_today_str)
    print(f"✅ Fetched Pre-Dispatch energy data for the date: {date_today_str}")
    time.sleep(20)
    fetch_predisp_OR_lmp(date_today_str)
    print(f"✅ Fetched Pre-Dispatch Operating Reserve data for the date: {date_today_str}")
    time.sleep(20)
    fetch_real_time_energy_lmp(date_today_str)
    print(f"✅ Fetched Real_Time energy data for the date: {date_today_str}")
    time.sleep(20)
    fetch_real_time_OR_lmp(date_today_str)
    print(f"✅ Fetched Real_Time Operating Reserve data for the date: {date_today_str}")


def create_hourly_data (date_today_str:str):

    # Get energy data for the specific date
    print("⚙️ Start creating hourly data")

    energy_day_ahead_url = f"https://storage.googleapis.com/ieso_monitoring_market_data/energy/day_ahead/energy_day_ahead_{date_today_str}.csv.gz"
    energy_pre_dispatch_url = f"https://storage.googleapis.com/ieso_monitoring_market_data/energy/pre_dispatch/energy_pre_dispatch_{date_today_str}.csv.gz"
    energy_real_time_url = f"https://storage.googleapis.com/ieso_monitoring_market_data/energy/real_time/energy_real_time_{date_today_str}.csv.gz"

    energy_day_ahead_df = pd.read_csv(energy_day_ahead_url, compression='gzip',parse_dates=["Date"])
    energy_pre_dispatch_df = pd.read_csv(energy_pre_dispatch_url, compression='gzip',parse_dates=["Date"])
    energy_real_time_df = pd.read_csv(energy_real_time_url, compression='gzip',parse_dates=["Date"])

    or_day_ahead_url = f"https://storage.googleapis.com/ieso_monitoring_market_data/operating_reserve/day_ahead/OR_day_ahead_{date_today_str}.csv.gz"
    or_pre_dispatch_url = f"https://storage.googleapis.com/ieso_monitoring_market_data/operating_reserve/pre_disptach/OR_pre_dispatch_{date_today_str}.csv.gz"
    or_real_time_url = f"https://storage.googleapis.com/ieso_monitoring_market_data/operating_reserve/real_time/OR_real_time_{date_today_str}.csv.gz"


    or_day_ahead_df = pd.read_csv(or_day_ahead_url, compression='gzip',parse_dates=["Date"])
    or_pre_dispatch_df = pd.read_csv(or_pre_dispatch_url, compression='gzip',parse_dates=["Date"])
    or_real_time_df = pd.read_csv(or_real_time_url, compression='gzip',parse_dates=["Date"])  

    # Create hourly df from the previous data
    hourly_energy_df = create_hourly_energy_data(energy_day_ahead_df, energy_pre_dispatch_df, energy_real_time_df)
    hourly_or_df = create_hourly_OR_data(or_day_ahead_df, or_pre_dispatch_df, or_real_time_df)

    # Add these dfs to the historical and unique data files
    print(f"➕ Add date for the day {date_today_str} to the historical file")
    add_hourly_energy_data(hourly_energy_df, date_today_str)
    add_hourly_OR_data(hourly_or_df, date_today_str)

    print(f"✅ Hourly data created and added to the historical file for the date: {date_today_str}")


def create_interval_data (date_today_str:str):

    # Get energy data for the specific date
    energy_day_ahead_url = f"https://storage.googleapis.com/ieso_monitoring_market_data/energy/day_ahead/energy_day_ahead_{date_today_str}.csv.gz"
    energy_pre_dispatch_url = f"https://storage.googleapis.com/ieso_monitoring_market_data/energy/pre_dispatch/energy_pre_dispatch_{date_today_str}.csv.gz"
    energy_real_time_url = f"https://storage.googleapis.com/ieso_monitoring_market_data/energy/real_time/energy_real_time_{date_today_str}.csv.gz"

    energy_day_ahead_df = pd.read_csv(energy_day_ahead_url, compression='gzip',parse_dates=["Date"])
    energy_pre_dispatch_df = pd.read_csv(energy_pre_dispatch_url, compression='gzip',parse_dates=["Date"])
    energy_real_time_df = pd.read_csv(energy_real_time_url, compression='gzip',parse_dates=["Date"])

    or_day_ahead_url = f"https://storage.googleapis.com/ieso_monitoring_market_data/operating_reserve/day_ahead/OR_day_ahead_{date_today_str}.csv.gz"
    or_pre_dispatch_url = f"https://storage.googleapis.com/ieso_monitoring_market_data/operating_reserve/pre_disptach/OR_pre_dispatch_{date_today_str}.csv.gz"
    or_real_time_url = f"https://storage.googleapis.com/ieso_monitoring_market_data/operating_reserve/real_time/OR_real_time_{date_today_str}.csv.gz"


    or_day_ahead_df = pd.read_csv(or_day_ahead_url, compression='gzip',parse_dates=["Date"])
    or_pre_dispatch_df = pd.read_csv(or_pre_dispatch_url, compression='gzip',parse_dates=["Date"])
    or_real_time_df = pd.read_csv(or_real_time_url, compression='gzip',parse_dates=["Date"]) 

    # Create interval df from the previous data
    interval_energy_df = create_interval_energy_data(energy_day_ahead_df, energy_pre_dispatch_df, energy_real_time_df)
    interval_or_df = create_interval_OR_data(or_day_ahead_df, or_pre_dispatch_df, or_real_time_df)

    # Add these dfs to the historical and unique data files
    print(f"➕ Add date for the day {date_today_str} to the historical file")
    add_interval_energy_data(interval_energy_df, date_today_str)
    add_interval_OR_data(interval_or_df, date_today_str)

    print(f"✅ 5-min intervals data created and added to the historical file for the date: {date_today_str}")


def run_all_flow(date_today_str):

    print(f"Starting the flow for date: {date_today_str}")
    import_daily_data(date_today_str)
    create_hourly_data(date_today_str)
    create_interval_data(date_today_str)

    print('Finished the flow ✅')


if __name__ == "__main__":
    yesterday_date_str = (date.today() - timedelta(days=1)).strftime(format="%Y%m%d")
    run_all_flow(yesterday_date_str)



