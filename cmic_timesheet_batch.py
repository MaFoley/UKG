import pandas as pd
import numpy as np
from datetime import datetime

def transform_ukg_to_cmic_timesheets(df_time: pd.DataFrame) -> pd.DataFrame:
    """
    Inputs a DataFrame of UKG 'Time' joined with 'Employee', 'Paygroup', etc.
    Returns a DataFrame formatted for CMiC.
    """
    # 1. Date Conversions & Periods
    # Standardize WorkDate to datetime objects
    df_time['WorkDate_dt'] = pd.to_datetime(df_time['WorkDate'])
    
    # 2. Pay Run Mapping (Enforce HJRC/HJRU constraints)
    conditions = [
        (df_time['paygroup_name'].str.strip().str.upper() == 'HJRC'),
        (df_time['paygroup_name'].str.strip().str.upper() == 'HJRU')
    ]
    choices = ['B', 'W']
    df_time['TshPrnCode'] = np.select(conditions, choices, default='New Paygroup')

    # 3. Pay Period Calculation (Vectorized)
    period_end_date = pd.Timestamp(2024, 12, 29)
    # 14 days if 'B', else 7
    period_lengths = np.where(df_time['TshPrnCode'] == 'B', 14, 7)
    delta_days = (df_time['WorkDate_dt'] - period_end_date).dt.days
    df_time['TshPprPeriod'] = (delta_days // period_lengths) + 1
    
    # 4. Job/Project Type Logic (Overhead vs Job)
    # Check if project name ends in '0000' or contains 'PROJ'
    is_overhead = (
        df_time['project_name'].str.endswith('0000') | 
        df_time['project_name'].str.contains('PROJ') | 
        (df_time['project_name'] == 'Z')
    )
    
    # TshTypeCode: 'G' for Overhead, 'J' for Jobs
    df_time['TshTypeCode'] = np.where(is_overhead, 'G', 'J')
    
    # TshJobdeptwoId logic
    df_time['TshJobdeptwoId'] = np.where(
        is_overhead, 
        df_time['location_CMiC_Dept_ID'], 
        df_time['cmic_project_JobCode']
    )
    
    # 5. Column Formatting & Constraints
    cmic_df = pd.DataFrame({
        'TshPrnCode': df_time['TshPrnCode'],
        'TshDate': df_time['WorkDate_dt'].dt.strftime('%Y-%m-%d'),
        'TshPprYear': df_time['WorkDate_dt'].dt.year,
        'TshPprPeriod': df_time['TshPprPeriod'],
        'TshEmpNo': df_time['EmpId'].str[3:9], # Short code logic
        'TshNormalHours': df_time['RegHr'].fillna(0),
        'TshCompCode': df_time['paygroup_name'].str[:3],
        'TshCatexpId': 'L',
        'TshOtHours': df_time['Overt1'].fillna(0),
        'TshDotHours': df_time['Overt2'].fillna(0),
        'TshUserField5': df_time['Id']
    })
    
    return cmic_df

# Convert transformed DataFrame to list of dicts
cmic_df = transform_ukg_to_cmic_timesheets(df_time)
payload = cmic_df.to_dict(orient='records')

# Use SQLAlchemy Core for high-performance batch insert
from sqlalchemy import insert
from models import CMiC_Timesheet_Entry
import sqlalchemy
engine =sqlalchemy.create_engine("sqlite:///DataFiles/utm.db", echo=False)
session = sqlalchemy.Session(engine)
session.execute(insert(CMiC_Timesheet_Entry), payload)
session.commit()