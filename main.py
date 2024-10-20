import requests
import pandas as pd 
from datetime import datetime, timedelta

class Date:

    def __init__(self, year: int, month: int, day: int):
        self.year = year
        self.month = month
        self.day = day

    def to_string(self):
        return f"{self.year}-{self.month:02d}-{self.day:02d}"

    def yesterday(self):
        today = datetime(self.year, self.month, self.day)
        yesterday = today - timedelta(days=1)
        return Date.from_datetime(yesterday)

    def tomorrow(self):
        today = datetime(self.year, self.month, self.day)
        tomorrow = today + timedelta(days=1)
        return Date.from_datetime(tomorrow)

    @classmethod
    def from_string(cls, date_string: str):
        year, month, day = date_string.split("-")
        return cls(int(year),int(month),int(day))

    @classmethod
    def from_datetime(cls, date_time: datetime):
        return cls(date_time.year, date_time.month, date_time.day)


def fetch_data_from_api_for_date_string(date: str):
    '''
    Fetch BMRS Imbalance data from Elexon Insights API for a given settlement date.

    Parameters:
    date : str
        The settlement date for which to fetch the data, formatted as 'yyyy-mm-dd'.

    Returns:
    pd.DataFrame or None
        A Pandas DataFrame containing the system price data for the specified 
        date if the request was successful; otherwise, returns None.
    '''
    response = requests.get(f"https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices/{date}?format=json")

    if response.status_code == 200:
        data = response.json()['data']
        data = pd.DataFrame(data)
        return data
    else:
        print("Error: ", response.status_code)
        return None
    
def fetch_data_from_api_for_date(date: Date):
    '''
    Fetch BMRS Imbalance data from Elexon Insights API for a given settlement date.

    Parameters:
    date : Date
        The settlement date for which to fetch the data as Date class.

    Returns:
    pd.DataFrame or None
        A Pandas DataFrame containing the system price data for the specified 
        date if the request was successful; otherwise, returns None.
    '''

    date_string = date.to_string()
    return fetch_data_from_api_for_date_string(date_string)


def generate_expected_start_times(date: str):
    '''
    Generate series of expected settlement period start times for given date.

    Parameters:
    date : str
        The settlement date for which to generate the series, formatted as 'yyyy-mm-dd'.

    Returns:
    pd.DatetimeIndex
        A Pandas DatetimeIndex containing series of timeslots at half hour intervals.
    '''
    start = f'{date} 00:00:00'  
    end = f'{date} 23:30:00'    
    interval = '0.5H'                     

    date_series = pd.date_range(start = start, end = end, freq = interval)

    return date_series


def transform_date_columns_to_datetime(df):

    date_columns = ['settlementDate', 'startTime', 'createdDateTime']
    df[date_columns] = df[date_columns].apply(pd.to_datetime)
    return df


date_string = "2024-04-01"
columns_of_interest = ['settlementDate', 'settlementPeriod', 'startTime', 'createdDateTime', 'systemSellPrice', 'systemBuyPrice', 'netImbalanceVolume']
df = fetch_data_from_api_for_date_string(date_string)
filtered_data = df[columns_of_interest]
filtered_data = transform_date_columns_to_datetime(filtered_data)


expected_start_times = generate_expected_start_times(date_string)

filtered_data = filtered_data[filtered_data['startTime'].isin(expected_start_times)]

missing_times = expected_start_times[~expected_start_times.isin(filtered_data['startTime'])]

if len(missing_times) > 0 :
    date = Date.from_string(date_string)
    yesterday = date.yesterday()

    yesterday_df = fetch_data_from_api_for_date(yesterday)
    yesterday_df = yesterday_df[columns_of_interest]
    yesterday_df = transform_date_columns_to_datetime(yesterday_df)

    yesterday_df = yesterday_df[yesterday_df['startTime'].isin(missing_times)]

    tomorrow = date.tomorrow()
    tomorrow_df = fetch_data_from_api_for_date(tomorrow)
    tomorrow_df = tomorrow_df[columns_of_interest]
    tomorrow_df = transform_date_columns_to_datetime(tomorrow_df)

    tomorrow_df = tomorrow_df[tomorrow_df['startTime'].isin(missing_times)]


combined_df = pd.concat([yesterday_df, filtered_data, tomorrow_df], axis = 0)

