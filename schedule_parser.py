# airflow_report_generator/schedule_parser.py

import re
from typing import List, Union
import pandas as pd
import numpy as np
from datetime import datetime

def timedelta_calculator(cron_value: str) -> List[Union[str, int]]:
    """
    Generates the needed time delta fields (report_type, hours, minutes)
    based on a cron_value string (e.g., '0 3 * * *' or '30 8 * * 1').

    Returns:
        [report_type, time_delta_hour, time_delta_minutes]
    """
    if not cron_value:
        raise ValueError("cron_value cannot be empty.")
    
    trimmed_cron_value = re.sub(r'\s+', '', cron_value)

    report_type = None
    time_delta_hour = 0
    time_delta_minutes = 0

    # Company standard start time (3 AM PST in your example)
    standard_start_time = 3
    parsed_cron_value = cron_value.split()

    # Daily Reports (if ends with * * * typically)
    if trimmed_cron_value.endswith("***"):
        report_type = 'Daily'
        # parse hour/min
        if len(parsed_cron_value) >= 2:
            hour_val = int(parsed_cron_value[1]) - standard_start_time
            time_delta_hour = hour_val
            minute_val = parsed_cron_value[0]
            time_delta_minutes = int(minute_val) if minute_val.isdigit() else 0

    # Weekly Reports (if last character isn't '*')
    elif trimmed_cron_value[-1] != '*':
        report_type = 'Weekly'
        # This logic can be simplified or corrected to suit your needs
        # For now, it just tries to interpret the day-of-week from the last field
        # ...
        # (Same logic from your monolith)
        # Example placeholder
        weekday_str = parsed_cron_value[-1]
        if weekday_str.isdigit():
            # e.g., 1 = Monday, 2 = Tuesday, ...
            # Adjust to your standard_start_time
            hour_val = (int(weekday_str) * 24) - (int(parsed_cron_value[1]) - standard_start_time)
            time_delta_hour = hour_val * -1
        else:
            # If it's sun, mon, etc. - your original logic
            pass
        
        minute_val = parsed_cron_value[0]
        time_delta_minutes = int(minute_val) if minute_val.isdigit() else 0

    # Monthly / or Specific Day of Month
    else:
        # your original approach:
        # if there's a day-of-month in the third field, etc.
        # ...
        report_type = 'Monthly'
        # just an example fallback
        time_delta_hour = 0
        time_delta_minutes = 0

    if report_type is None:
        raise ValueError("Invalid cron_value or cannot parse it.")
    
    return [report_type, time_delta_hour, time_delta_minutes]

def discover_cron(df_and_column: pd.Series) -> str:
    """
    Attempts to auto-generate a cron schedule from the ticket body or the 'period_and_time' column.

    Returns:
        A cron string, e.g. '0 8 * * *'.
    """
    df_lower = df_and_column.str.lower().drop_duplicates()
    
    # Defaults
    minute_value = 0
    hour_value = 4  # 4 AM by default
    week_value = 1  # Monday by default
    
    # Attempt to detect time from strings like "10:30 am", "2 pm"
    time_match = df_lower.str.extract(r'(\d+):(\d+)').dropna()
    if not time_match.empty:
        minute_value = int(time_match.iloc[0, 1])
    
    am_pm_match = df_lower.str.extract(r'(\d+):?(?:\d{{2}})?\s*(am|pm)').dropna()
    if not am_pm_match.empty:
        hour = int(am_pm_match.iloc[0, 0])
        if am_pm_match.iloc[0, 1].lower() == 'pm' and hour < 12:
            hour_value = hour + 12
        else:
            hour_value = hour
    
    # Timezone adjustments (PST is your base)
    if 'ct' in df_lower.iloc[0] or 'cst' in df_lower.iloc[0]:
        hour_value -= 2
    elif 'et' in df_lower.iloc[0] or 'est' in df_lower.iloc[0] or 'edt' in df_lower.iloc[0]:
        hour_value -= 3
    elif 'mt' in df_lower.iloc[0] or 'mst' in df_lower.iloc[0]:
        hour_value -= 1

    hour_value %= 24

    # Detect weekday
    weekday_map = {
        'monday': 1,
        'tuesday': 2,
        'wednesday': 3,
        'thursday': 4,
        'friday': 5,
        'saturday': 6,
        'sunday': 0
    }
    for weekday, value in weekday_map.items():
        if weekday in df_lower.iloc[0]:
            week_value = value
            break

    # If phrase "daily" is found, we skip the day-of-week
    if 'daily' in df_lower.iloc[0] or ' day' in df_lower.iloc[0]:
        cron_value = f'{minute_value} {hour_value} * * *'
    else:
        cron_value = f'{minute_value} {hour_value} * * {week_value}'

    return cron_value

def discover_timespan(timespan_column: pd.Series) -> Union[str, int, None]:
    """
    Determines the correct timespan based on the input string.
    e.g., "Last 7 Days" -> 7, "Present 01/15 -> Now" -> 'YYYY-MM-DD', 'MTD'/'toStartOfMonth', etc.

    Returns:
        str or int (number of days). None if unparsed.
    """
    date_value = np.nan
    days_back = np.nan

    if timespan_column.empty:
        return None

    today = datetime.today()

    lower_str = timespan_column.drop_duplicates().str.lower()
    value_str = lower_str.iloc[0]

    if 'present' in value_str:
        # e.g. "Present 03/10" -> parse that date
        match = re.search(r'(\d{1,2}/\d{1,2})', value_str)
        if match:
            mm_dd = match.group(1).split('/')
            month = int(mm_dd[0]) if len(mm_dd[0]) == 2 else int(mm_dd[0])
            day = int(mm_dd[1]) if len(mm_dd[1]) == 2 else int(mm_dd[1])
            year = today.year
            date_value = f'{year:04d}-{month:02d}-{day:02d}'
    elif 'mtd' in value_str or 'month to date' in value_str:
        date_value = 'toStartOfMonth(today())'
    elif 'ytd' in value_str or 'year to date' in value_str:
        date_value = 'toStartOfYear(today())'
    elif 'prior day' in value_str or 'previous day' in value_str:
        days_back = 1
    elif 'ongoing' in value_str or 'starting' in value_str:
        # parse start date from string
        numbers = re.findall(r'\d+', value_str)
        if len(numbers) == 0:
            year, month, day = today.year, today.month, today.day
        elif len(numbers) == 1:
            month = int(numbers[0])
            year, day = today.year, today.day
        elif len(numbers) == 2:
            month, day = int(numbers[0]), int(numbers[1])
            year = today.year
        else:
            month, day, year = int(numbers[0]), int(numbers[1]), int(numbers[2])
            if year < 100:
                year += 2000
        try:
            parsed_date = datetime(year, month, day)
            date_value = parsed_date.strftime('%Y-%m-%d')
        except ValueError:
            pass
    else:
        # e.g. "Last X Days"
        match = re.search(r'(\d+)\s*day', value_str)
        if match:
            days_back = int(match.group(1))

    if pd.notna(date_value):
        return str(date_value)
    elif pd.notna(days_back):
        return int(days_back)
    else:
        return None

