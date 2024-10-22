# BMRS Imbalance Daily Report

This repository contains a Python script for fetching and analyzing electricity imbalance data from the **Elexon BMRS API**. 
It provides functions to retrieve, process, and visualize imbalance data, such as system prices and net imbalance volumes for a given date.

## Features

- **Data Fetching**: Retrieve imbalance data from the Elexon BMRS API for a specific date.
- **Data Transformation**: Convert and clean data, focusing on key columns such as system prices and imbalance volumes.
- **Plot Generation**: Generate visual plots for system prices and total imbalance costs.
- **Reporting**: Identify the hours with the highest absolute imbalance volumes and calculate total imbalance costs.

## Usage
Specify required date in the main.py file and run.
Tests are included in test_pytest.py.

## Acknowledgement
Contains BMRS data © Elexon Limited copyright and database right [2024] [license link] (https://www.elexon.co.uk/operations-settlement/bsc-central-services/balancing-mechanism-reporting-agent/copyright-licence-bmrs-data/)
