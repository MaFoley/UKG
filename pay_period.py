import requests, json, csv, tomllib, os
import pandas as pd
from datetime import datetime
from functools import wraps
from typing import Union, Literal
import logging, sys
OUTPUT_FILE_PATH = './DataFiles'
logger = logging.getLogger('pay_period')
logger.level = logging.INFO
if not logger.hasHandlers():
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    sh, fh = logging.StreamHandler(sys.stdout),logging.FileHandler(f"{OUTPUT_FILE_PATH}/middleware.log")
    sh.setFormatter(formatter)
    sh.setLevel(logger.level)
    fh.setFormatter(formatter)
    sh.setLevel(logger.level)
    logger.addHandler(sh)
    logger.addHandler(fh)



payRuntoPayGroup = {'B':18,'W':43}

def find_period_by_date(
    df: pd.DataFrame,
    payRun: Literal['B' , 'W'],
    target_date: Union[str, pd.Timestamp]
) -> pd.DataFrame:
    """
    Finds the Period in the DataFrame that contains the target_date 
    (inclusive of start and end dates).

    Args:
        df: DataFrame with 'Period', 'startDate', and 'endDate' columns 
            (where dates are pandas datetimes).
        target_date: The date to check, provided as a date string 
                     (e.g., '2024-05-15') or a pandas Timestamp object.

    Returns:
        The Period (int) if exactly one period is found, otherwise None.
    """
    try:
        if isinstance(target_date, str):
            check_date = pd.to_datetime(target_date)
        else:
            check_date = target_date
        for col in df.columns:
            if 'date' in col.lower():
                try:
                    df[col] = pd.to_datetime(df[col])
                except:
                    print(f"could not convert column {col} to datetime.")
        
        # 2. Use boolean indexing for inclusive comparison (>= and <=)
        filtered_df = df[
            (df['Pay Run'] == payRun) &
            (df['Start Date'] <= check_date) & 
            (df['End Date'] >= check_date)
        ]

        if filtered_df.empty:
            logger.info(f"Date {check_date.date()} is not contained in any defined period.")
            return None
        elif len(filtered_df) > 1:
            # Unexpected result, but handles overlapping periods gracefully
            logger.info(f"Warning: Date {check_date.date()} found in multiple periods. Returning the first match.")
            return filtered_df
        else:
            return filtered_df

    except Exception as e:
        logger.error(f"An error occurred during date parsing or comparison: {e}")
        return None

if __name__ == "__main__":
    pay_period_file = "DataFiles/Pay_Period.csv"
    df = pd.read_csv(pay_period_file, skiprows=2)
    print(find_period_by_date(df,'W', "2025-09-26"))