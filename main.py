import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests

from typing import Iterable
from datetime import datetime, timedelta

pd.options.mode.copy_on_write = True

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

## Data retrieval and processing functions

def fetch_data_from_api_for_date_string(date: str) -> pd.DataFrame | None:
    '''
    Fetch BMRS Imbalance data from Elexon Insights API for a given settlement date.

    Parameters:
    date : str
        The settlement date for which to fetch the data, formatted as 'yyyy-mm-dd'.
        The settlement date is in local time, i.e Greenwich Mean Time (GMT) or British Summer Time (BST).

    Returns:
    pd.DataFrame or None
        A Pandas DataFrame containing the system price data for the specified 
        date if the request was successful; otherwise, returns None. 
        Columns containing times are in Coordinated Universal Time (UTC).
    '''
    response = requests.get(f"https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices/{date}?format=json")
    
    if response.status_code == 200:
        data = response.json()['data']
        data = pd.DataFrame(data)
        if data.shape[0] > 0:
            return data
        else:
            print(f"Error: no data for date: {date}")
            return None
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


def transform_data_from_api(df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Transform the fetched data from the API by filtering and converting columns.

    This function filters the provided DataFrame to include only the columns of interest
    related to imbalance data and transforms specific date columns to datetime format.

    Parameters:
    df : pd.DataFrame
        A DataFrame containing the raw data fetched from the API. It must contain columns
        'settlementDate', 'settlementPeriod', 'startTime', 'createdDateTime',
        'systemSellPrice', 'systemBuyPrice', and 'netImbalanceVolume'.

    Returns:
    pd.DataFrame or None:
        A DataFrame containing the filtered and transformed data with relevant date columns
        converted to datetime format. Returns None if the input DataFrame is empty or invalid.
    """
    columns_of_interest = ['settlementDate', 'settlementPeriod', 'startTime', 'createdDateTime', 'systemSellPrice', 'systemBuyPrice', 'netImbalanceVolume']
    filtered_data = df[columns_of_interest]

    transformed_data = transform_date_columns_to_datetime(filtered_data)

    return transformed_data


def fetch_and_transform_data_for_date_string(date_string : str) -> pd.DataFrame | None:
    """
    Fetch data from an API for a specific date and transform the relevant columns.

    This function retrieves data using the provided date string (formatted as 'yyyy-mm-dd'),
    filters the DataFrame to include only the specified columns of interest, 
    and converts relevant date columns to datetime format.

    Parameters:
    date_string : str
        The date string used to fetch data from the API, formatted as 'yyyy-mm-dd'.

    Returns:
    pd.DataFrame or None:
        A DataFrame containing the filtered and transformed data with relevant date columns
        converted to datetime format if the data was successfully retrieved; otherwise, returns None.
    """
    
    df = fetch_data_from_api_for_date_string(date_string)

    if df is not None:
        transformed_data = transform_data_from_api(df)
        return transformed_data
    
    return None


def transform_date_columns_to_datetime(df : pd.DataFrame) -> pd.DataFrame:
    date_columns = ['settlementDate', 'startTime', 'createdDateTime']
    df[date_columns] = df[date_columns].apply(pd.to_datetime)
    return df


def switch_timezone_to_utc(date_string : str, df : pd.DataFrame) -> pd.DataFrame:
    """
    Adjusts the input dataframe's times to UTC and filters out missing settlement periods.

    Elexon API settlementDate parameter is based on local time (e.g. British Summer Time (BST) during Summer),
    this function retrieves data based on UTC settlement date (rather than local) 
    by fetching data for the previous and next day (if possible), 
    and combining them with the current date's data (filtering out periods that don't match the UTC date).

    Parameters
    date_string : str
        The date string in "yyyy-mm-dd" format representing the settlement date.
    df : pd.DataFrame
        The input dataframe containing imbalance data with a "startTime" column.

    Returns
    pd.DataFrame
        A dataframe filtered to include only expected settlement periods, with any missing periods added if possible.
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
        to fetch the missing data for, this is in local timezone. 

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

    if tomorrow_df is not None:
        
        misplaced_periods_tomorrow = tomorrow_df[tomorrow_df['startTime'].isin(missing_settlement_times)]

        if misplaced_periods_yesterday.shape[0] > 0 :
            combined_df = pd.concat([misplaced_periods_yesterday, settlement_date_df, misplaced_periods_tomorrow], axis=0)
        else:
            combined_df = pd.concat([settlement_date_df, misplaced_periods_tomorrow], axis=0)

    elif misplaced_periods_yesterday.shape[0] > 0:
        combined_df = pd.concat([misplaced_periods_yesterday, settlement_date_df], axis=0)
    else:
        combined_df = settlement_date_df
    
    return combined_df



## Plotting functions
def generate_price_and_imbalance_cost_plots_from_dataframe(settlement_date : str, df : pd.DataFrame) -> None:
    df['Time'] = df.apply(lambda row: f"{row['startTime'].hour:02d}:{row['startTime'].minute:02d}", axis = 1)

    return generate_price_and_imbalance_cost_plots(settlement_date, df['Time'], df['systemSellPrice'], df['ImbalanceCost'])


def generate_price_and_imbalance_cost_plots(settlement_date : str, time: Iterable[float], sell_price: Iterable[float], imbalance_cost: Iterable[float],) -> None:
    """
    Generates and displays two plots: one for the system price and another for the imbalance cost
    for a specified settlement date. Each plot includes annotations for the maximum values.

    Parameters:
    settlement_date : str
        A string representing the date of the settlement in the format 'yyyy-mm-dd'.
    
    time : Iterable[float]
        An iterable containing the time points (e.g., timestamps) 
        for the x-axis of the plots. This represents the settlement period start times in UTC.
    
    sell_price : Iterable[float]
        An iterable containing the system sell prices corresponding to each time point. 
        These values should be in £/MWh. 
    
    imbalance_cost : Iterable[float]
        An iterable containing the imbalance costs corresponding to each time point. 
        These values should be in £.

    Returns:
    None
        This function does not return a value. It displays the generated plots directly.

    """
    max_price = max(sell_price)
    max_price_time = time[sell_price.idxmax()]

    max_imbalance_cost = max(imbalance_cost)
    max_imbalance_cost_time = time[imbalance_cost.idxmax()]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(20, 10)) 
    ax1.plot(time, sell_price, label='System Price', marker='x') 
    ax1.set_title(f"System Price on {settlement_date}")
    ax1.set_ylabel("£/MWh")
    ax1.set_xlabel("Settlement Period Start Time (UTC)")
    ax1.grid(True, linestyle='--', alpha=0.7)
    ax1.annotate(f'Max: £{np.round(max_price, 2):,.2f}', xy=(max_price_time, max_price), xytext=(max_price_time, max_price * 1.1),
                 arrowprops=dict(facecolor='black', arrowstyle='->'), fontsize=10, color='b')
    for label in ax1.get_xticklabels():
        label.set_rotation(45)

    ax2.plot(time, imbalance_cost, label='ImbalanceCost', marker='x') 
    ax2.set_title(f"Imbalance Cost on {settlement_date}")
    ax2.set_ylabel("£")
    ax2.set_xlabel("Settlement Period Start Time (UTC)")
    ax2.grid(True, linestyle='--', alpha=0.7)
    for label in ax2.get_xticklabels():
        label.set_rotation(45)
    ax2.annotate(f'Max: £{np.round(max_imbalance_cost, 2):,.2f}', xy=(max_imbalance_cost_time, max_imbalance_cost), xytext=(max_imbalance_cost_time, max_imbalance_cost * 1.1),
            arrowprops=dict(facecolor='black', arrowstyle='->'), fontsize=10, color='b')

    fig.tight_layout() 
    plt.show()



## Calculation functions

def generate_max_net_abs_imbalance_volume_hour(df : pd.DataFrame) -> tuple:
    """
    Identifies the hour (UTC) with the highest absolute imbalance volumes from the given DataFrame.

    This function extracts the hour from the 'startTime' column, calculates the absolute values of the
    'netImbalanceVolume' column, and groups the DataFrame by hour to sum the absolute imbalance volumes.
    It then identifies the hour with the highest imbalance volume and returns both the hour and the volume.

    Parameters:
    df : pd.DataFrame
        A Pandas DataFrame containing the imbalance data, where:
        - 'startTime' column contains datetime objects representing the start time of each settlement period.
        - 'netImbalanceVolume' column contains the net imbalance volume (in MWh) for each period.

    Returns:
    tuple
        A tuple containing:
        - int: The hour (0-23) (UTC) with the highest absolute imbalance volume.
        - float: The maximum absolute imbalance volume (in MWh).

    Note: If local timezone is BST, the UTC hours follow the order from 23, 0, 1, ..., 22, not 0-23
    """

    df['Hour'] = df['startTime'].dt.hour
    df['absNetImbalanceVolume'] = df['netImbalanceVolume'].apply(abs)
    grouped_df = df.groupby('Hour')['absNetImbalanceVolume'].sum()
    return(grouped_df.idxmax(), grouped_df.max())


def generate_max_abs_imbalance_volume_period_hour() -> tuple:
    """
    Identifies the hour (UTC) containing the settlement period of the highest absolute imbalance volume from the given DataFrame.

    Parameters:
    df : pd.DataFrame
        A Pandas DataFrame containing the imbalance data, where:
        - 'startTime' column contains datetime objects representing the start time of each settlement period.
        - 'netImbalanceVolume' column contains the net imbalance volume (in MWh) for each period.

    Returns:
    tuple
        A tuple containing:
        - int: The hour (0-23) (UTC) with the highest absolute imbalance volume.
        - float: The maximum absolute imbalance volume (in MWh).

    Note: If local timezone is BST, the UTC hours follow the order from 23, 0, 1, ..., 22, not 0-23
    """
    df['absNetImbalanceVolume'] = df['netImbalanceVolume'].apply(abs)
    df = df.reset_index(drop = True)

    max_abs_imbalance_vol_time = df['startTime'].iloc[df['absNetImbalanceVolume'].idxmax()]
    max_abs_imbalance_vol_hour = max_abs_imbalance_vol_time.hour
    max_abs_vol = df['absNetImbalanceVolume'].max()

    return(max_abs_imbalance_vol_hour, max_abs_vol)


def calculate_total_imbalance_cost(df : pd.DataFrame) -> float:
    """
    Calculates the total imbalance cost based on the net imbalance volume and system prices.

    The function computes the imbalance cost for each entry in the DataFrame by determining 
    whether the net imbalance volume (NIV) is positive or negative. If the NIV is positive, 
    the cost is calculated using the system sell price; if it is negative, the cost uses 
    the system buy price. The function returns the sum of all calculated costs.

    Parameters:
    df : pd.DataFrame
        A DataFrame containing the following columns:
        - 'netImbalanceVolume' (float): The net imbalance volume for the settlement period.
        - 'systemSellPrice' (float): The system sell price per MWh.
        - 'systemBuyPrice' (float): The system buy price per MWh.
    
    Returns:
    float
        The total imbalance cost calculated in £, representing the overall cost incurred 
        based on the net imbalance volumes and corresponding prices.

    Notes:
    - A positive net imbalance volume indicates that the system is short, meaning 
      there is more demand than supply, leading to costs calculated using the 
      system sell price.
    - A negative net imbalance volume indicates that the system is long, meaning 
      there is more supply than demand, leading to costs calculated using the 
      system buy price.
    """
    df['ImbalanceCost'] = np.where(df['netImbalanceVolume'] > 0, df['netImbalanceVolume'] * df['systemSellPrice'], df['netImbalanceVolume'] * df['systemBuyPrice'])

    total_imbalance_cost = df['ImbalanceCost'].sum()

    return total_imbalance_cost



## Reporting Functions


def report_max_net_abs_imbalance_volume_hour(df : pd.DataFrame) -> None:
    """
    Reports the hour with the highest absolute net imbalance volume in a 12-hour AM/PM format (UTC), and the volume.

    Parameters:
    df : pd.DataFrame
        A Pandas DataFrame containing the imbalance data, where:
        - 'startTime' column contains datetime objects representing the start time of each settlement period.
        - 'netImbalanceVolume' column contains the net imbalance volume (in MWh) for each period.

    Returns:
    None
        This function prints the hour with the highest absolute imbalance volume and the volume and does not return any value.
    """
    max_hour, max_val = generate_max_net_abs_imbalance_volume_hour(df)

    if (max_hour < 12):
        am_pm = "am"
    else:
        am_pm = "pm"

    max_hour = max_hour % 12
    if max_hour == 0:
        max_hour = 12

    print(f"Hour with highest absolute imbalance volumes: {max_hour}{am_pm} (UTC),",
            f"with net absolute imbalance volume of {np.round(max_val, 2)} MWh")


def report_total_imbalance_cost(df : pd.DataFrame) -> None:

    print(f"Total Daily Imbalance Cost = £{np.round(calculate_total_imbalance_cost(df), 2):,.2f}")


def output_report_and_plots_for_date(date: str, use_local_timezone: bool) -> None:
    """
    Fetches, transforms, and reports imbalance data for a specified date, generating plots for system prices 
    and imbalance costs. The function can switch between local timezone and UTC for the date the data is obtained for.

    Parameters:
    date : str
        A string representing the date in the format 'yyyy-mm-dd' for which the report and plots 
        are to be generated.
    
    use_local_timezone : bool
        A boolean flag indicating whether to use the local timezone for the data. If set to True, 
        the data will be processed in the local timezone; if False, the data will be converted to 
        UTC before further processing.

    Returns:
    None
        This function does not return a value. It directly outputs the report and displays 
        the generated plots.

    """
        
    df = fetch_and_transform_data_for_date_string(date)

    if not use_local_timezone:
        df = switch_timezone_to_utc(date, df)
        
    report_total_imbalance_cost(df)

    report_max_net_abs_imbalance_volume_hour(df)

    generate_price_and_imbalance_cost_plots_from_dataframe(date, df)



if __name__ == "__main__":
    date_string = "2024-01-01"
    output_report_and_plots_for_date(date_string, use_local_timezone=True)