import pytest
from main import fetch_data_from_api_for_date_string 
from main import generate_max_net_abs_imbalance_volume_hour
from main import calculate_total_imbalance_cost 
from main import transform_data_from_api
import pandas as pd
import numpy as np

def test_fetch_data_from_api_for_date_string_invalid_string():
    assert fetch_data_from_api_for_date_string("01-01-2024") is None

    assert fetch_data_from_api_for_date_string("hello") is None

    assert fetch_data_from_api_for_date_string("2024-02-30") is None

    assert fetch_data_from_api_for_date_string("2050-01-01") is None


def test_fetch_data_from_api_for_date_string_valid_string():
 
    assert fetch_data_from_api_for_date_string("2024-02-01") is not None

    assert fetch_data_from_api_for_date_string("2020-02-01") is not None


def test_generate_max_net_abs_imbalance_volume_hour():
    # Test Case 1: Standard Case
    data = {
        'startTime': pd.to_datetime([
            '2024-10-14T00:00:00Z', 
            '2024-10-14T01:00:00Z', 
            '2024-10-14T01:30:00Z', 
            '2024-10-14T02:00:00Z'
        ]),
        'netImbalanceVolume': [5, -3, 4, 2]
    }
    df = pd.DataFrame(data)
    hour, max_volume = generate_max_net_abs_imbalance_volume_hour(df)
    assert hour == 1  # Hour with the highest absolute imbalance
    assert max_volume == 7  # Absolute max: |-3| + |4| = 7


    # Test Case 2: Single Hour
    data = {
        'startTime': pd.to_datetime([
            '2024-10-14T04:00:00Z'
        ]),
        'netImbalanceVolume': [2]
    }
    df = pd.DataFrame(data)
    hour, max_volume = generate_max_net_abs_imbalance_volume_hour(df)
    assert hour == 4
    assert max_volume == 2


    # Test Case 3: Negative Values
    data = {
        'startTime': pd.to_datetime([
            '2024-10-14T00:00:00Z',
            '2024-10-14T00:30:00Z',
            '2024-10-14T01:00:00Z',
            '2024-10-14T01:30:00Z'
        ]),
        'netImbalanceVolume': [-1, -2, -3, -4]
    }
    df = pd.DataFrame(data)
    hour, max_volume = generate_max_net_abs_imbalance_volume_hour(df)
    assert hour == 1  # Hour 1 has the highest absolute volume of 7
    assert max_volume == 7  # |-3| + |-4| = 7

    # # Test Case 4: Empty DataFrame
    # df = pd.DataFrame(columns=['startTime', 'netImbalanceVolume'])
    # hour, max_volume = generate_max_net_abs_imbalance_volume_hour(df)
    # print(hour)
    # # assert hour is None  
    # # assert max_volume is None 


def test_calculate_total_imbalance_cost():
    # Test Case 1: Standard Case
    data = {
        'netImbalanceVolume': [10, -5, 15],
        'systemSellPrice': [100, 200, 150],
        'systemBuyPrice': [80, 180, 120]
    }

    df = pd.DataFrame(data)
    total_cost = calculate_total_imbalance_cost(df)
    expected_cost = (10 * 100) + (-5 * 180) + (15 * 150) 
    assert np.isclose(total_cost, expected_cost), f"Expected {expected_cost}, got {total_cost}"

    # Test Case 2: All Positive Values
    data = {
        'netImbalanceVolume': [10, 5, 15],
        'systemSellPrice': [100, 200, 150],
        'systemBuyPrice': [80, 180, 120]
    }
    df = pd.DataFrame(data)
    total_cost = calculate_total_imbalance_cost(df)
    expected_cost = (10 * 100) + (5 * 200) + (15 * 150)
    assert np.isclose(total_cost, expected_cost), f"Expected {expected_cost}, got {total_cost}"

    # Test Case 3: All Negative Values
    data = {
        'netImbalanceVolume': [-10, -5, -15],
        'systemSellPrice': [100, 200, 150],
        'systemBuyPrice': [80, 180, 120]
    }
    df = pd.DataFrame(data)
    total_cost = calculate_total_imbalance_cost(df)
    expected_cost = (-10 * 80) + (-5 * 180) + (-15 * 120)
    assert np.isclose(total_cost, expected_cost), f"Expected {expected_cost}, got {total_cost}"


def test_transform_data_from_api_success():
    # Sample data to be returned from the mock API call
    sample_data = pd.DataFrame({
        'settlementDate': ['2024-10-14'],
        'settlementPeriod': [1],
        'startTime': ['2024-10-14T23:00:00Z'],
        'createdDateTime': ['2024-10-14T00:00:00Z'],
        'systemSellPrice': [50.0],
        'systemBuyPrice': [45.0],
        'netImbalanceVolume': [10.0],
        'unneededColumn': [10.0]
    })

    expected_data = pd.DataFrame({
        'settlementDate': pd.to_datetime(['2024-10-14']),
        'settlementPeriod': [1],
        'startTime': pd.to_datetime(['2024-10-14T23:00:00Z']),
        'createdDateTime': pd.to_datetime(['2024-10-14T00:00:00Z']),
        'systemSellPrice': [50.0],
        'systemBuyPrice': [45.0],
        'netImbalanceVolume': [10.0]
    })

    pd.testing.assert_frame_equal(transform_data_from_api(sample_data), expected_data)


def test_transform_data_from_api_missing_columns():
    # Sample data with missing columns
    sample_data = {
        'settlementDate': ['2024-10-14'],
        'settlementPeriod': [1],
        'startTime': ['2024-10-14T23:00:00Z'],
        # 'createdDateTime' column is missing
        'systemSellPrice': [50.0],
        'systemBuyPrice': [45.0],
        'netImbalanceVolume': [10.0]
    }

    sample_df = pd.DataFrame(sample_data)

    with pytest.raises(KeyError):
        transform_data_from_api(sample_df)

