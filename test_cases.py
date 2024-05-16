# Amulya Chennaboyena
# Code written on May 15 th
# Tested until method def process_to_RO(data: Dict[str, pd.DataFrame]) -> List[RO]:
# Testing: Write test cases to validate that each part of the pipeline works as intended.

import os
import pandas as pd
import xml.etree.ElementTree as ET
from typing import List, Dict
from typing import Dict
import pandas as pd
import tempfile


#testcases

#testing reading files from directory
    # Create a temporary directory with XML files

   #created data_test directory for testing data added file1.xml and file2.xml
def test_read_files_from_dir():
    # Create a temporary directory with XML files

    dir_path = '/Users/amulyachennaboyena/Downloads/data/task/test_data/'
    xml_contents = test_read_files_from_dir(dir_path)

    # Write some XML content to the files
    with open(file1_path, 'w') as file1:
        file1.write(
            '<event><order_id>101</order_id><date_time>2023-08-10T12:00:00</date_time><status>Completed</status><cost>75.00</cost></event>')
    with open(file2_path, 'w') as file2:
        file2.write(
            '<event><order_id>102</order_id><date_time>2023-08-11T09:00:00</date_time><status>In Progress</status><cost>85.00</cost></event>')

    # Call the function
    xml_contents = read_files_from_dir(temp_dir)

    # Assert the results
    assert len(xml_contents) == 2
    assert '<event><order_id>101</order_id><date_time>2023-08-10T12:00:00</date_time><status>Completed</status><cost>75.00</cost></event>' in xml_contents
    assert '<event><order_id>102</order_id><date_time>2023-08-11T09:00:00</date_time><status>In Progress</status><cost>85.00</cost></event>' in xml_contents


test_read_files_from_dir()


def test_parse_xml():
    # Sample XML content
    xml_content = [
        '<event><order_id>101</order_id><date_time>2023-08-10T12:00:00</date_time><status>Completed</status><cost>75.00</cost></event>',
        '<event><order_id>102</order_id><date_time>2023-08-11T09:00:00</date_time><status>In Progress</status><cost>85.00</cost></event>']

    # Call the function
    df = parse_xml(xml_content)

    # Assert the results
    assert len(df) == 2
    assert df.iloc[0]['order_id'] == '101'
    assert df.iloc[1]['order_id'] == '102'


test_parse_xml()


def test_window_by_datetime():
    # Create a DataFrame with sample data
    df = pd.DataFrame({'order_id': ['101', '102', '103'],
                       'date_time': pd.to_datetime(
                           ['2023-08-10T12:00:00', '2023-08-11T09:00:00', '2023-08-12T10:00:00']),
                       'status': ['Completed', 'In Progress', 'Received'],
                       'cost': [75.00, 85.00, 90.00]})

    # Call the function
    windowed_data = window_by_datetime(df, '1D')

    # Assert the results
    assert len(windowed_data) == 3
    for window_start, window_data in windowed_data.items():
        assert isinstance(window_start, str)
        assert isinstance(window_data, pd.DataFrame)


test_window_by_datetime()


def test_process_to_RO():
    # Sample windowed data
    windowed_data = {'2023-08-10': pd.DataFrame({'order_id': ['101'], 'date_time': ['2023-08-10T12:00:00'],
                                                 'status': ['Completed'], 'cost': [75.00]}),
                     '2023-08-11': pd.DataFrame({'order_id': ['102'], 'date_time': ['2023-08-11T09:00:00'],
                                                 'status': ['In Progress'], 'cost': [85.00]})}

    # Call the function
    ro_list = process_to_RO(windowed_data)

    # Assert the results
    assert len(ro_list) == 2
    assert ro_list[0].order_id == '101'
    assert ro_list[1].order_id == '102'


test_process_to_RO()
