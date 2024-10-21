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
    pandas.DataFrame or None: 
        A DataFrame containing the filtered and transformed data, 
        with relevant date columns converted to datetime format.
    """
    
    df = fetch_data_from_api_for_date_string(date_string)
    
    if df is not None and df.shape[0] > 0:
        columns_of_interest = ['settlementDate', 'settlementPeriod', 'startTime', 'createdDateTime', 'systemSellPrice', 'systemBuyPrice', 'netImbalanceVolume']
        filtered_data = df[columns_of_interest]

        transformed_data = transform_date_columns_to_datetime(filtered_data)

        return transformed_data
    
    return None


def add_missing_settlement_periods(settlement_date : str, settlement_date_df : pd.DataFrame, missing_settlement_times):
    """
    Fetch data for missing settlement periods by checking previous and next settlement dates in API.
    Combine with data saved under correct settlement date.

    This function retrieves data for the previous and following days, 
    and checks if any of the periods belong to the required settlment date. 
    It combines the DataFrames into a single DataFrame for further analysis.

    Parameters:
    settlement_date : str 
        A string representing the settlement date in the format 'YYYY-MM-DD' 
        to fetch the missing data for. 

    settlement_date_df : pd.DataFrame
        A DataFrame containing the non missing data for the settlement date, 
        which will be included in the combined DataFrame.

    missing_settlement_times :
        Used to filter the data for previous and next settlment dates,
        to entries that belong to the specified settlement_date

    Returns:
    pandas.DataFrame: 
        A DataFrame containing the combined data for the settlement date. 
        Note it is not guaranteed that all settlement periods were found
    """
    date = Date.from_string(date_string)

    yesterday_df = fetch_and_transform_data_for_date_string(date.yesterday().to_string())

    misplaced_periods_yesterday = yesterday_df[yesterday_df['startTime'].isin(missing_settlement_times)]

    tomorrow_df = fetch_and_transform_data_for_date_string(date.tomorrow().to_string())
    
    if tomorrow_df is not None and df.shape[0] > 0:
        misplaced_periods_tomorrow = tomorrow_df[tomorrow_df['startTime'].isin(missing_settlement_times)]

        if misplaced_periods_yesterday.shape[0] > 0:
            combined_df = pd.concat([misplaced_periods_yesterday, settlement_date_df, misplaced_periods_tomorrow], axis = 0)
        else:
            combined_df = pd.concat([settlement_date_df, misplaced_periods_tomorrow], axis = 0)

    elif misplaced_periods_yesterday.shape[0] > 0:
        combined_df = pd.concat([misplaced_periods_yesterday, settlement_date_df], axis = 0)
    else:
        combined_df = settlement_date_df
    
    return combined_df


def clean_data(date_string, df):
    """
    Clean and filter settlement data based on expected start times for a given date.

    This function generates expected settlement period start times for the specified date, 
    filters the provided DataFrame to include only entries where the 
    'startTime' matches the expected values. If there are missing settlement periods, 
    it adds these to the filtered DataFrame. 
    The function will also print a warning if there are still missing 
    settlement periods after attempting to add them.

    Parameters:
    date_string : str
        A string representing the date in the format 'yyyy-MM-dd' 
        for which to clean and filter the settlement data.
    df : pd.DataFrame 
        A DataFrame containing the settlement data that needs to be cleaned.

    Returns:
    pandas.DataFrame: 
        A DataFrame containing only the filtered settlement data 
        that matches the expected start times, with missing periods 
        added if necessary.

    """

    expected_start_times = generate_expected_start_times(date_string)
    filtered_data = df[df['startTime'].isin(expected_start_times)]

    missing_times = expected_start_times[~expected_start_times.isin(df['startTime'])]

    if len(missing_times) > 0 :
        filtered_data = add_missing_settlement_periods(date_string, filtered_data, missing_times)

        missing_times = expected_start_times[~expected_start_times.isin(df['startTime'])]
        if len(missing_times) > 0:
            print("Settlement date is missing settlement periods.")

    return filtered_data





date_string = "2024-03-31"
df = fetch_and_transform_data_for_date_string(date_string)
# combined_df = df

combined_df = clean_data(date_string, df)

combined_df['Time'] = combined_df.apply(lambda row: f"{row['startTime'].hour:02d}:{row['startTime'].minute:02d}", axis = 1)
# NIV > 0: system is short
combined_df['ImbalanceCost'] = np.where(combined_df['netImbalanceVolume'] > 0, combined_df['netImbalanceVolume'] * combined_df['systemSellPrice'], combined_df['netImbalanceVolume'] * combined_df['systemBuyPrice'])
combined_df = combined_df.reset_index(drop = True)

# System Sell price and System Buy price are equal
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(20, 10)) 
ax1.plot(combined_df['Time'], combined_df['systemSellPrice'], label='systemSellPrice', marker='x') 
ax1.set_title(f"System Sell Price on {date_string}")
ax1.set_ylabel("£/MWh")
ax1.set_xlabel("Settlement Period Start Time")
for label in ax1.get_xticklabels():
    label.set_rotation(45)
ax2.plot(combined_df['Time'], combined_df['ImbalanceCost'], label='ImbalanceCost', marker='x') 
ax2.set_title(f"Imbalance Cost on {date_string}")
ax2.set_ylabel("£")
ax2.set_xlabel("Settlement Period Start Time")
for label in ax2.get_xticklabels():
    label.set_rotation(45)

fig.tight_layout() 
plt.show()


# Total daily imbalance cost 
total_imbalance_cost = combined_df['ImbalanceCost'].sum()
print(f"Total Daily Imbalance Cost = {total_imbalance_cost}")


combined_df['absNetImbalanceVolume'] = combined_df['netImbalanceVolume'].apply(abs)


# Print hour containing half hour slot with highest absolute imbalance imbalance volume
highest_imbalance_vol_time = combined_df['startTime'].iloc[combined_df['absNetImbalanceVolume'].idxmax()]
highest_imbalance_vol_hour = highest_imbalance_vol_time.hour
print(f"Hour with highest absolute imbalance volumes (containing half hour slot with highest absolute imbalance imbalance volume): {highest_imbalance_vol_hour}")
# Print hour with highest sum of absolute imbalance volumes (over the 2 half hour slots within the hour)
combined_df['Hour'] = combined_df['startTime'].dt.hour
grouped_df = combined_df.groupby('Hour')['absNetImbalanceVolume'].sum()

print(f"Hour with highest absolute imbalance volumes (sum over the 2 half hour settlement periods): {grouped_df.idxmax()}")