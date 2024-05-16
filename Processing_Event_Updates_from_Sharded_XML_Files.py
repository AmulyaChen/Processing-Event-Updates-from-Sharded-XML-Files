# Amulya Chennaboyena
# Code written on May 15 th
# Tested until method def process_to_RO(data: Dict[str, pd.DataFrame]) -> List[RO]:
#
import os
import pandas as pd
import xml.etree.ElementTree as ET
from typing import List, Dict
from typing import Dict
import pandas as pd


# Define the RO class
class RO:
    def __init__(self, order_id, date_time, status, cost, technician, part_names, part_quantities):
        self.order_id = order_id
        self.date_time = date_time
        self.status = status
        self.cost = cost
        self.technician = technician
        self.part_names = part_names
        self.part_quantities = part_quantities

#faced multiple issues like the xml file has a mistake <repair_parts> tag is miss-spelled as <pair parts> in shard_13.xml so modified the xml file
 #faced issue once changing the xml to List[str] the new line characters are still added as \n so deleted the \n characters with replace method
 #and checked the output multiple times by printing the data to make sure parse xml has no issues
 
 #Reading from Directory: Create a function read_files_from_dir(dir: str) -> List[str] that reads all the XML files from a specified directory and prefix (folder). 
 #The function should return a list of XML contents as strings.

def read_files_from_dir(dir_path: str) -> List[str]:
    xml_contents = []
    for file_name in os.listdir(dir_path):
        if file_name.endswith('.xml'):
            file_path = os.path.join(dir_path, file_name)
            with open(file_path, 'r') as file:
                xml_content = file.read()
                xml_contents.append(xml_content)
    return xml_contents

# adding
def clean_xml_content(xml_content):
    cleaned_content = xml_content.replace("  ", "").replace("\n", "").replace("  ", "")
    return cleaned_content

# Example usage:
dir_path = '/Users/amulyachennaboyena/Downloads/data'
xml_contents = read_files_from_dir(dir_path)

# Clean up all XML content strings
cleaned_xml_contents = [clean_xml_content(xml_content) for xml_content in xml_contents]

# Print the cleaned XML content
#for idx, xml_content in enumerate(cleaned_xml_contents, start=1):
#print(cleaned_xml_contents)

#print(cleaned_xml_contents)


# Parsing XML Files: Create a function parse_xml(files: List[str]) -> pd.DataFrame that takes the XML content
# and parses them into a DataFrame. The XML files contain the following structure:

def parse_xml(files: List[str]) -> pd.DataFrame:
    data = []
    for xml_string in files:
        root = ET.fromstring(xml_string)
        order_id = root.find('order_id').text
        date_time = root.find('date_time').text
        status = root.find('status').text
        cost = float(root.find('cost').text)
        technician = root.find('.//technician').text
        parts = root.findall('.//part')
        part_names = [part.attrib['name'] for part in parts]
        part_quantities = [int(part.attrib['quantity']) for part in parts]

        data.append({
            'order_id': order_id,
            'date_time': date_time,
            'status': status,
            'cost': cost,
            'technician': technician,
            'part_names': part_names,
            'part_quantities': part_quantities
        })

    df = pd.DataFrame(data)
    return df

df = parse_xml(cleaned_xml_contents)

# Windowing by Date_Time to Get Latest Event: Implement a function window_by_datetime(data: pd.DataFrame, window: str) -> Dict[str, pd.DataFrame]
# that takes the DataFrame and a window parameter (e.g., '1D' for 1 day) and groups the data by the specified window based on the date_time column
# to get the latest event for the correct updates.
# The function should return a dictionary with keys as window identifiers and values as DataFrames for each window.

def window_by_datetime(data: pd.DataFrame, window: str) -> Dict[str, pd.DataFrame]:
    # Convert date_time column to datetime type
    data['date_time'] = pd.to_datetime(data['date_time'])

    # Group by window
    grouped = data.groupby(pd.Grouper(key='date_time', freq=window))

    # Initialize dictionary to store results
    windowed_data = {}

    # Iterate over groups and get all events for each window
    for window_start, group in grouped:
        windowed_data[str(window_start)] = group

    return windowed_data




# Assuming 'df' is the DataFrame containing the data
# '1D' represents 1 day window
windowed_data = window_by_datetime(df, '1D')
# Print the windowed data
#for window_start, window_data in windowed_data.items():
 #print(f"Window Start: {window_start}\n{window_data}\n")


#Processing into Structured RO Format: Write a function process_to_RO(data: Dict[str, pd.DataFrame]) -> List[RO]
# that takes the windowed data and transforms it into a structured RO format, defining the RO class as needed.

def process_to_RO(data: Dict[str, pd.DataFrame]) -> List[RO]:
    ro_list = []
    for window_start, windowed_data in data.items():
        # Iterate over each row in the windowed DataFrame
        for _, row in windowed_data.iterrows():
            order_id = row['order_id']
            date_time = row['date_time']
            status = row['status']
            cost = row['cost']
            technician = row['technician']
            part_names = row['part_names']
            part_quantities = row['part_quantities']

            # Create a new RO object and append it to the list
            ro = RO(order_id, date_time, status, cost, technician, part_names, part_quantities)
            ro_list.append(ro)

    # Print all the values
    for ro in ro_list:
        print("Order ID:", ro.order_id)
        print("Date Time:", ro.date_time)
        print("Status:", ro.status)
        print("Cost:", ro.cost)
        print("Technician:", ro.technician)
        print("Part Names:", ro.part_names)
        print("Part Quantities:", ro.part_quantities)
        print()  # Add a blank line for readability

    return ro_list

# Process the windowed_data into RO objects
ro_objects = process_to_RO(windowed_data)

#Integration: Combine these functions into a single pipeline script that reads from a specified directory, parses the XML files,
# windows the data by date_time,
#and processes them into the structured RO format, and then writes the output to a SQLite database.

def write_to_database(ro_objects: List[RO], db_path: str):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS RO
                 (order_id TEXT, date_time TEXT, status TEXT, cost REAL, technician TEXT, part_names TEXT, part_quantities TEXT)''')
    for ro in ro_objects:
        c.execute("INSERT INTO RO VALUES (?, ?, ?, ?, ?, ?, ?)",
                  (ro.order_id, ro.date_time, ro.status, ro.cost, ro.technician,
                   ','.join(ro.part_names), ','.join(ro.part_quantities)))
    conn.commit()
    conn.close()

# List of RO objects obtained from process_to_RO method
ro_objects = [Order_ID,Date_Time,Status,Cost,Technician,Part_Names,Part_Quantities]
db_path = ''  # Path to SQLite database file
write_to_database(ro_objects, db_path)



