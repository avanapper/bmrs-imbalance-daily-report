import requests
import pandas as pd 
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np

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
    interval = '0.5h'                     

    date_series = pd.date_range(start = start, end = end, freq = interval, tz='UTC')

    return date_series



def transform_date_columns_to_datetime(df):
    date_columns = ['settlementDate', 'startTime', 'createdDateTime']
    df[date_columns] = df[date_columns].apply(pd.to_datetime)
    return df


def fetch_and_transform_data_for_date_string(date_string : str):
    """
    Fetch data from an API for a specific date and transform the relevant columns.

    This function retrieves data using the provided date string (in format yyyy-MM-dd), 
    filters the DataFrame to include only columns of interest, 
    and converts the specific date columns to datetime format.

    Parameters:
    date_string: str 
        The date string used to fetch data from the API, format yyyy-MM-dd

    Returns:
    pandas.DataFrame: 
        A DataFrame containing the filtered and transformed data, 
        with relevant date columns converted to datetime format.
    """
    
    df = fetch_data_from_api_for_date_string(date_string)

    columns_of_interest = ['settlementDate', 'settlementPeriod', 'startTime', 'createdDateTime', 'systemSellPrice', 'systemBuyPrice', 'netImbalanceVolume']
    filtered_data = df[columns_of_interest]

    transformed_data = transform_date_columns_to_datetime(filtered_data)
    
    return transformed_data


date_string = "2024-08-01"
df = fetch_and_transform_data_for_date_string(date_string)

expected_start_times = generate_expected_start_times(date_string)
filtered_data = df[df['startTime'].isin(expected_start_times)]

missing_times = expected_start_times[~expected_start_times.isin(df['startTime'])]

if len(missing_times) > 0 :
    date = Date.from_string(date_string)
    yesterday = date.yesterday()

    yesterday_df = fetch_and_transform_data_for_date_string(yesterday.to_string())

    yesterday_df = yesterday_df[yesterday_df['startTime'].isin(missing_times)]

    tomorrow = date.tomorrow()
    tomorrow_df = fetch_and_transform_data_for_date_string(tomorrow.to_string())

    tomorrow_df = tomorrow_df[tomorrow_df['startTime'].isin(missing_times)]


combined_df = pd.concat([yesterday_df, filtered_data, tomorrow_df], axis = 0)


combined_df['Time'] = combined_df.apply(lambda row: f"{row['startTime'].hour:02d}:{row['startTime'].minute:02d}", axis = 1)
combined_df = combined_df.reset_index(drop = True)
# print(combined_df)
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(20, 15)) 
ax1.plot(combined_df['Time'], combined_df['systemSellPrice'], label='systemSellPrice', marker='x') 
ax1.set_title(f"System Sell Price on {date_string}")
ax1.set_ylabel("£/MWh")
ax1.set_xlabel("Settlement Period Start Time")
for label in ax1.get_xticklabels():
    label.set_rotation(45)
ax2.plot(combined_df['Time'], combined_df['systemBuyPrice'], label='systemBuyPrice', marker='x') 
ax2.set_title(f"System Buy Price on {date_string}")
ax2.set_ylabel("£/MWh")
ax2.set_xlabel("Settlement Period Start Time")
for label in ax2.get_xticklabels():
    label.set_rotation(45)
fig.tight_layout() 
plt.show()

# Total daily imbalance cost 
# NIV > 0: system is short
combined_df['ImbalanceCost'] = np.where(combined_df['netImbalanceVolume'] > 0, combined_df['netImbalanceVolume'] * combined_df['systemSellPrice'], combined_df['netImbalanceVolume'] * combined_df['systemBuyPrice'])
total_imbalance_cost = combined_df['ImbalanceCost'].sum()
print(f"Total Daily Imbalance Cost = {total_imbalance_cost}")

# Print hour containing half hour slot with highest net imbalance volume
highest_imbalance_vol_time = combined_df['startTime'].iloc[combined_df['netImbalanceVolume'].idxmax()]
highest_imbalance_vol_hour = highest_imbalance_vol_time.hour
print(highest_imbalance_vol_hour)

# Print hour with highest sum of net imabalnce volumes (over the 2 half hour slots within the hour)
combined_df['Hour'] = combined_df['startTime'].dt.hour
grouped_df = combined_df.groupby('Hour')['netImbalanceVolume'].sum()

print(grouped_df.idxmax())