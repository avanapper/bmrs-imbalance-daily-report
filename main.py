import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests

from typing import Iterable
from datetime import datetime, timedelta

class Date:

    def __init__(self, year: int, month: int, day: int) -> None:
        self.year = year
        self.month = month
        self.day = day

    def to_string(self) -> str:
        return f"{self.year}-{self.month:02d}-{self.day:02d}"

    def yesterday(self) -> 'Date':
        today = datetime(self.year, self.month, self.day)
        yesterday = today - timedelta(days=1)
        return Date.from_datetime(yesterday)

    def tomorrow(self) -> 'Date':
        today = datetime(self.year, self.month, self.day)
        tomorrow = today + timedelta(days=1)
        return Date.from_datetime(tomorrow)

    @classmethod
    def from_string(cls, date_string: str) -> 'Date':
        year, month, day = date_string.split("-")
        return cls(int(year), int(month), int(day))

    @classmethod
    def from_datetime(cls, date_time: datetime) -> 'Date':
        return cls(date_time.year, date_time.month, date_time.day)


def fetch_data_from_api_for_date_string(date: str) -> pd.DataFrame | None:
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
    

def fetch_data_from_api_for_date(date: Date) -> pd.DataFrame | None:
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

    return fetch_data_from_api_for_date_string(date.to_string())


def generate_expected_start_times(date: str) -> pd.DatetimeIndex:
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

    date_series = pd.date_range(start=start, end=end, freq=interval, tz='UTC')

    return date_series


def transform_date_columns_to_datetime(df : pd.DataFrame) -> pd.DataFrame:
    date_columns = ['settlementDate', 'startTime', 'createdDateTime']
    df[date_columns] = df[date_columns].apply(pd.to_datetime)
    return df


def fetch_and_transform_data_for_date_string(date_string : str) -> pd.DataFrame | None:
    """
    Fetch data from an API for a specific date and transform the relevant columns.

    This function retrieves data using the provided date string (in format yyyy-mm-dd), 
    filters the DataFrame to include only columns of interest, 
    and converts the specific date columns to datetime format.

    Parameters:
    date_string: str 
        The date string used to fetch data from the API, format yyyy-mm-dd

    Returns:
    pd.DataFrame or None: 
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


def add_missing_settlement_periods(settlement_date : str, settlement_date_df : pd.DataFrame, missing_settlement_times : pd.DatetimeIndex) -> pd.DataFrame:
    """
    Fetch data for missing settlement periods by checking previous and next settlement dates in API.
    Combine with data saved under correct settlement date.

    This function retrieves data for the previous and following days, 
    and checks if any of the periods belong to the required settlement date. 
    It combines the DataFrames into a single DataFrame for further analysis.

    Parameters:
    settlement_date : str 
        A string representing the settlement date in the format 'yyyy-mm-dd' 
        to fetch the missing data for. 

    settlement_date_df : pd.DataFrame
        A DataFrame containing the non missing data for the settlement date, 
        which will be included in the combined DataFrame.

    missing_settlement_times : pd.DatetimeIndex
        Used to filter the data for previous and next settlement dates,
        to entries that belong to the specified settlement_date

    Returns:
    pd.DataFrame: 
        A DataFrame containing the combined data for the settlement date. 
        Note: it is not guaranteed that all settlement periods were found
    """
    date = Date.from_string(settlement_date)

    yesterday_df = fetch_and_transform_data_for_date_string(date.yesterday().to_string())

    misplaced_periods_yesterday = yesterday_df[yesterday_df['startTime'].isin(missing_settlement_times)]

    tomorrow_df = fetch_and_transform_data_for_date_string(date.tomorrow().to_string())
    
    if tomorrow_df is not None and tomorrow_df.shape[0] > 0:
        misplaced_periods_tomorrow = tomorrow_df[tomorrow_df['startTime'].isin(missing_settlement_times)]

        if misplaced_periods_yesterday.shape[0] > 0:
            combined_df = pd.concat([misplaced_periods_yesterday, settlement_date_df, misplaced_periods_tomorrow], axis=0)
        else:
            combined_df = pd.concat([settlement_date_df, misplaced_periods_tomorrow], axis=0)

    elif misplaced_periods_yesterday.shape[0] > 0:
        combined_df = pd.concat([misplaced_periods_yesterday, settlement_date_df], axis=0)
    else:
        combined_df = settlement_date_df
    
    return combined_df


def clean_data(date_string : str, df : pd.DataFrame) -> pd.DataFrame:
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
        A string representing the date in the format 'yyyy-mm-dd' 
        for which to clean and filter the settlement data.
    df : pd.DataFrame 
        A DataFrame containing the settlement data that needs to be cleaned.

    Returns:
    pd.DataFrame: 
        A DataFrame containing only the filtered settlement data 
        that matches the expected start times, with missing periods 
        added if necessary.

    """

    expected_start_times = generate_expected_start_times(date_string)
    filtered_data = df[df['startTime'].isin(expected_start_times)]

    missing_times = expected_start_times[~expected_start_times.isin(df['startTime'])]

    if len(missing_times) > 0 :
        filtered_data = add_missing_settlement_periods(date_string, filtered_data, missing_times)

        missing_times = expected_start_times[~expected_start_times.isin(filtered_data['startTime'])]
        if len(missing_times) > 0:
            print("Settlement date is missing settlement periods.")
            print(missing_times)

    return filtered_data


def generate_price_and_imbalance_cost_plots_from_dataframe(settlement_date : str, df : pd.DataFrame) -> None:
    return generate_price_and_imbalance_cost_plots(settlement_date, df['Time'], df['systemSellPrice'], df['ImbalanceCost'])

def generate_price_and_imbalance_cost_plots(settlement_date : str, time: Iterable[float], sell_price: Iterable[float], imbalance_cost: Iterable[float],) -> None:
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(20, 10)) 
    ax1.plot(time, sell_price, label='System Price', marker='x') 
    ax1.set_title(f"System Price on {settlement_date}")
    ax1.set_ylabel("£/MWh")
    ax1.set_xlabel("Settlement Period Start Time")
    for label in ax1.get_xticklabels():
        label.set_rotation(45)
    ax2.plot(time, imbalance_cost, label='ImbalanceCost', marker='x') 
    ax2.set_title(f"Imbalance Cost on {settlement_date}")
    ax2.set_ylabel("£")
    ax2.set_xlabel("Settlement Period Start Time")
    for label in ax2.get_xticklabels():
        label.set_rotation(45)

    fig.tight_layout() 
    plt.show()


def generate_max_abs_imbalance_volume_hour(df : pd.DataFrame) -> str:
    df['Hour'] = df['startTime'].dt.hour
    grouped_df = df.groupby('Hour')['absNetImbalanceVolume'].sum()

    if (grouped_df.idxmax() < 12):
        am_pm = "am"
    else:
        am_pm = "pm"

    max_hour = grouped_df.idxmax() % 12
    if max_hour == 0:
        max_hour = 12

    return(f"Hour with highest absolute imbalance volumes (sum over the 2 half hour settlement periods): {max_hour}{am_pm} (UTC)",
            f"with absolute imbalance volume of {np.round(grouped_df.max(), 2)} MWh")

def output_report_and_plots_for_date(date: str) -> None:
    df = fetch_and_transform_data_for_date_string(date)

    combined_df = clean_data(date, df)

    combined_df['Time'] = combined_df.apply(lambda row: f"{row['startTime'].hour:02d}:{row['startTime'].minute:02d}", axis = 1)

    # NIV = Net Imbalance Volume
    # NIV > 0 means system is short
    combined_df['ImbalanceCost'] = np.where(combined_df['netImbalanceVolume'] > 0, combined_df['netImbalanceVolume'] * combined_df['systemSellPrice'], combined_df['netImbalanceVolume'] * combined_df['systemBuyPrice'])
    combined_df = combined_df.reset_index(drop = True)

    total_imbalance_cost = combined_df['ImbalanceCost'].sum()
    print(f"Total Daily Imbalance Cost = £{np.round(total_imbalance_cost, 2)}")

    combined_df['absNetImbalanceVolume'] = combined_df['netImbalanceVolume'].apply(abs)

    # Print hour containing half hour slot with highest absolute imbalance volume
    highest_imbalance_vol_time = combined_df['startTime'].iloc[combined_df['absNetImbalanceVolume'].idxmax()]
    highest_imbalance_vol_hour = highest_imbalance_vol_time.hour
    print(f"Hour with highest absolute imbalance volumes (containing half hour slot with highest absolute imbalance imbalance volume): {highest_imbalance_vol_hour}")

    
    # Print hour with highest sum of absolute imbalance volumes (over the 2 half hour slots within the hour)
    print(generate_max_abs_imbalance_volume_hour(combined_df))


    generate_price_and_imbalance_cost_plots_from_dataframe(date, combined_df)


if __name__ == "__main__":
    date_string = "2024-03-31"
    output_report_and_plots_for_date(date_string)