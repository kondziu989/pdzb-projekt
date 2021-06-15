from datetime import datetime
import pandas as pd

def get_date_dims(date_str, time_str):
    date_time = date_str + " " + time_str
    date = datetime.strptime(date_str, "%Y-%m-%d")
    pandas_date = pd.Timestamp(date)
    semester = 2 if date.month > 6 else 1
    return f'{date};{date.year};{semester};{pandas_date.quarter};{date.month}'