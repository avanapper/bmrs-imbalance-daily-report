import requests
import pandas as pd 
from datetime import datetime 


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
    
date = "2024-04-01"
df = fetch_data_from_api_for_date(date)
filtered_data = df[['settlementDate', 'settlementPeriod', 'startTime', 'createdDateTime', 'systemSellPrice', 'systemBuyPrice', 'netImbalanceVolume']]



# Some settlementDates are missing settlementPeriods and/or contain settlementPeriods from the previous date
# 1. Remove entries where startTime doesn't match the settlementDate
# 2. Bring in the misisng settlement Periods


filtered_data['startTime'] = pd.to_datetime(filtered_data['startTime'])
filtered_data['createdDateTime'] = pd.to_datetime(filtered_data['createdDateTime'])
filtered_data['settlementDate'] = pd.to_datetime(filtered_data['settlementDate']).dt.date
filtered_data = filtered_data[filtered_data['settlementDate']  == filtered_data['startTime'].dt.date]

print(filtered_data)