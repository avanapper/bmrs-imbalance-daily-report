import requests
import pandas as pd 
from datetime import datetime, timedelta


def fetch_data_from_api_for_date(date):
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
    

def generate_expected_start_times(date):
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


date = "2024-04-01"
df = fetch_data_from_api_for_date(date)
filtered_data = df[['settlementDate', 'settlementPeriod', 'startTime', 'createdDateTime', 'systemSellPrice', 'systemBuyPrice', 'netImbalanceVolume']]

# Some settlementDates are missing settlementPeriods and/or contain settlementPeriods from the previous date
# 1. Remove entries where startTime doesn't match the settlementDate -- done
# 2. Bring in the missing settlement Periods

filtered_data['startTime'] = pd.to_datetime(filtered_data['startTime'])
filtered_data['createdDateTime'] = pd.to_datetime(filtered_data['createdDateTime'])
filtered_data['settlementDate'] = pd.to_datetime(filtered_data['settlementDate'])

expected_start_times = generate_expected_start_times(date)

filtered_data = filtered_data[filtered_data['startTime'].isin(expected_start_times)]

missing_times = expected_start_times[~expected_start_times.isin(filtered_data['startTime'])]

if len(missing_times) > 0 :
    date_Date = datetime.strptime(date, "%Y-%m-%d")
    yesterday = date_Date - timedelta(days=1)


