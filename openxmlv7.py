import os
import xml.etree.ElementTree as ET
import psycopg2
import shutil

"""
docker run --name kpiairscale -e POSTGRES_USER=Solis -e POSTGRES_PASSWORD=Solis2025 -e POSTGRES_DB=kpiairscale -p 5433:5432 -d postgres:11
docker run --name kpidata -e POSTGRES_USER=Solis -e POSTGRES_PASSWORD=Solis2025 -e POSTGRES_DB=kpidata -v pgdata:/var/lib/postgresql/data -p 5432:5432 -d postgres:11

"""

# Directory containing KPI files
directory_path = "/Userdata/proj2025/kpidata/kpi_files"
destination_path = "/Userdata/proj2025/kpidata/kpi_files_read"

# PostgreSQL connection config
db_config = {
    "dbname": "kpiAirScale",
    "user": "Solis",
    "password": "Solis2025",
    "host": "localhost",
    "port": 5433
}

def create_table_if_not_exists(measurementType, kpi_columns):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    columns = ', '.join([f'"{col}" INTEGER' for col in kpi_columns])
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS "{measurementType}" (
        id SERIAL PRIMARY KEY,
        create_at TIMESTAMP,
        manage_object TEXT,
        {columns}
    );
    '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()
    
def insert_into_table(measurementType, data, kpi_columns):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    columns = ', '.join([f'"{col}"' for col in kpi_columns])
    placeholders = ', '.join(['%s'] * (len(kpi_columns) + 2))  # +2 for create_at and manage_object
    insert_query = f'''
    INSERT INTO "{measurementType}" (create_at, manage_object, {columns})
    VALUES ({placeholders})
    '''
    cursor.executemany(insert_query, data)
    conn.commit()
    cursor.close()
    conn.close()


def process_kpi_file(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    print(f"Processing file: {file_path}")

    # Initialize a dictionary to store data for each measurementType    
    for PMSetup in root.iter('PMSetup'):
        createAt = PMSetup.attrib.get('startTime')
        for PMMOResult in PMSetup.iter('PMMOResult'):
            manageObject = None
            for child in PMMOResult:
                if child.tag == 'MO' and child.attrib.get('dimension') == 'network_element':
                    for subchild in child:
                        if subchild.tag == 'DN':
                            original = subchild.text
                            manageObject = original.replace("PLMN-PLMN/", "")  # Replace spaces with underscores
                elif child.tag == 'NE-WBTS_1.0':
                    measurementType = child.attrib.get('measurementType')
                    kpi_dict = {}
                    for subchild in child:
                        try:
                            value = int(subchild.text)
                        except (TypeError, ValueError):
                            continue
                        # if value != 0:
                        kpi_dict[subchild.tag] = value

                    if kpi_dict:  # Only proceed if there are non-zero kpiValues
                        kpi_columns = sorted(kpi_dict.keys())
                        create_table_if_not_exists(measurementType, kpi_columns)
                        row = [createAt, manageObject] + [kpi_dict.get(col) for col in kpi_columns]
                        insert_into_table(measurementType, [row], kpi_columns)
    
    # Move the file to the read directory
    dst_path = os.path.join(destination_path, os.path.basename(file_path))
    shutil.move(file_path, dst_path)  # Move the file to the read directory

# Loop through all files in the directory
files = sorted(os.listdir(directory_path))
for file_name in files:
    file_path = os.path.join(directory_path, file_name)
    if os.path.isfile(file_path):  # Ensure it's a file
        process_kpi_file(file_path)