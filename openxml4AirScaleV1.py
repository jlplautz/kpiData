import os
from datetime import datetime
import xml.etree.ElementTree as ET
import pandas as pd


directory_path = "/Userdata/proj2025/kpidata/kpi_files"

# Get a sorted list of files in the directory
files = sorted(os.listdir(directory_path))

def process_kpiFiles(file_path):

    # Parse the XML file
    tree = ET.parse(file_path)
    # Get the root element of the XML tree
    root = tree.getroot()


    # Initialize a dictionary to store data for each measurementType
    data_by_measurementType = {}

    # Iterate through the XML tree and extract the desired data
    for PMSetup in root.iter('PMSetup'):
        # Extract common fields for the row
        createAt = PMSetup.attrib.get('startTime')

        for PMMOResult in PMSetup.iter('PMMOResult'):
            manageObject = None
            measurementType = None

            for child in PMMOResult:
                if child.tag == 'MO' and child.attrib.get('dimension') == 'network_element':
                    for subchild in child:
                        if subchild.tag == 'DN':
                            original = subchild.text
                            manageObject = original.replace("PLMN-PLMN/", "")  # Replace spaces with underscores
                elif child.tag == 'NE-WBTS_1.0':
                    measurementType = child.attrib.get('measurementType')

                    # Initialize the data structure for this measurementType if not already done
                    if measurementType not in data_by_measurementType:
                        data_by_measurementType[measurementType] = []

                    # Create a dictionary to store KPI values for this row
                    row = {
                        'createAt': createAt,
                        'manageObject': manageObject
                    }

                    # Add KPI values as columns
                    has_kpi_value = False  # Track if there is any kpiValue > 0
                    for subchild in child:
                        if int(subchild.text) > 0 or int(subchild.text) < 0:
                            row[subchild.tag] = int(subchild.text)
                            has_kpi_value = True

                    # Append the row to the corresponding measurementType
                    if has_kpi_value:
                        data_by_measurementType[measurementType].append(row)

    # Append or create CSV files for each measurementType
    for measurementType, rows in data_by_measurementType.items():
        if rows:  # Only proceed if there are rows with kpiValue > 0
            df_new = pd.DataFrame(rows)

            # Check if the CSV file for this measurementType already exists
            filename = f"{measurementType}.csv"
            if os.path.exists(filename):
                # If the file exists, append the new data
                df_existing = pd.read_csv(filename)
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                df_combined.to_csv(filename, index=False)
                # print(f"Appended data to {filename}")
            else:
                # If the file does not exist, create a new one
                df_new.to_csv(filename, index=False)
                # print(f"Created new file: {filename}")


# Loop through all files in the directory
for file_name in files:
    print("Processing file:", file_name)
    file_path = os.path.join(directory_path, file_name)
    if os.path.isfile(file_path):  # Ensure it's a file
        process_kpiFiles(file_path)