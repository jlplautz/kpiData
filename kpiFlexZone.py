from datetime import datetime
import paramiko
import os
import gzip
import psycopg2
import pytz
import shutil
import xml.etree.ElementTree as ET

"""
Example how can we create a dickr container with postgresql
docker run --name kpiFlexiZone -e POSTGRES_USER=Solis -e POSTGRES_PASSWORD=Solis2025 -e POSTGRES_DB=kpiFlexiZone -d -p 5434:5432 -v pgdata:/var/openkpi/postgresql/kpiFlexiZone postgres:11
"""


# List of radios with their connection details
radios = [
    # {"server_ip": "10.1.1.2"  ,"username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/stats/iOms/"},
    # {"server_ip": "10.1.101.2","username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/stats/iOms/"},
    # {"server_ip": "10.1.16.12","username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/stats/iOms/"},
    # {"server_ip": "10.1.16.31","username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/stats/iOms/"},
    # {"server_ip": "10.1.16.32","username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/stats/iOms/"},
    # {"server_ip": "10.1.30.2" ,"username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/stats/iOms/"},
    # {"server_ip": "10.1.35.2" ,"username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/stats/iOms/"},
    # {"server_ip": "10.1.36.10","username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/stats/iOms/"},
    # {"server_ip": "10.1.60.2" ,"username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/stats/iOms/"},
    {"server_ip": "10.1.5.2"  ,"username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/stats/iOms/"},
]

# Local directory where files will be saved
# dir_zip = "/var/openkpi/fzm_zip"
# dir_files = "var/openkpi/fzm_files"
# dir_files_read = "/var/openkpi/fzm_files_read"
dir_zip = r"/Userdata/proj2025/kpiData/fzm_zip"
dir_files = r"/Userdata/proj2025/kpiData/fzm_files"
dir_files_read = r"/Userdata/proj2025/kpiData/fzm_files_read"
os.makedirs(dir_zip, exist_ok=True)

# PostgreSQL connection config
db_config = {
    "dbname": "kpiFlexiZone",
    "user": "Solis",
    "password": "Solis2025",
    "host": "localhost",
    "port": 5434
}

def get_current_quarter():
    """
    Determine the current quarter of the hour (e.g., 15:00, 15:15, 15:30, 15:45).
    """
    utc = pytz.utc
    now = datetime.now(utc)
    print(now)
    minute = now.minute
    if 0 <= minute < 15:
        return f"{now.hour:02d}:00"
    elif 15 <= minute < 30:
        return f"{now.hour:02d}:15"
    elif 30 <= minute < 45:
        return f"{now.hour:02d}:30"
    else:
        return f"{now.hour:02d}:45"

def adjust_file_name(original_name):
    """
    Adjust the file name to change the extension from .raw to .xml.
    Example: "PM.BTS-414225.20250505.3000.ANY.raw.gz" -> "PM.BTS-414225.20250505.3000.xml"
    """
    if original_name.endswith(".raw.gz"):
        new_name = original_name.replace("3000.LTE.raw.gz", "3000.xml")
        return new_name, None  # No need to return a timestamp
    return original_name, None  # Return the original name if it doesn't end with .raw

def download_files():
# def download_and_rename_files(server_ip, username, password, remote_directory):
    for radio in radios:
        try:
            server_ip = radio["server_ip"]
            username = radio["username"]
            password = radio["password"]
            remote_directory = radio["remote_directory"]

            # Establish SFTP connection
            transport = paramiko.Transport((server_ip, 22))
            transport.connect(username=username, password=password)

            # Create the SFTP client
            sftp = paramiko.SFTPClient.from_transport(transport)

            # List files in the remote directory
            print(f"Connecting to server {server_ip}...")
            remote_files = sftp.listdir(remote_directory)

            # # Get the current quarter
            # current_quarter = get_current_quarter()
            # print(f"Current quarter: {current_quarter}")

        except Exception as e:
            print(f"Error during file transfer from {server_ip}: {e}")

        for file_name in remote_files:
            # Check if the file matches the quarterly KPI naming pattern
            if file_name.startswith("PM.BTS") and file_name.endswith("0000.LTE.raw.gz"):
                # Adjust the file name and extract the timestamp
                new_file_name, _ = adjust_file_name(file_name)

                # Define remote and local file paths
                remote_file_path = os.path.join(remote_directory, file_name)
                print(remote_directory)
                local_file_zip = os.path.join(dir_zip, new_file_name)
                print(dir_zip)
                local_file_path = os.path.join(dir_files, new_file_name)
                print(local_file_path)

                # Check if the file already exists locally
                if os.path.exists(local_file_zip):
                    print(f"File {new_file_name} already exists locally. Skipping download.")
                    continue

                # Download the file with the new name
                print(f"Downloading {file_name} as {new_file_name}...")
                sftp.get(remote_file_path, local_file_zip)
                print(f"File {new_file_name} downloaded successfully.")

                # Open the .gz file and write the uncompressed data to the output file
                with gzip.open(local_file_zip, 'rb') as f_in:
                    with open(local_file_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)

                try:
                    for filename in os.listdir(dir_zip):
                        file_path = os.path.join(dir_zip, filename)
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                            print(f'Removed: {file_path}')
                except PermissionError as e:
                    print(f'Error: {e}')
                except Exception as e:
                    print(f'An error occurred: {e}')
                    
        sftp.close()
        transport.close()
        print(f"All files from {server_ip} processed successfully.\n")



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
                if child.tag == 'MO':
                    for subchild in child:
                        if subchild.tag == 'localMoid':
                            original = subchild.text
                            manageObject = original.replace("DN:", "")  # Replace spaces with underscores
                elif child.tag == 'NE-WBTS_1.0':
                    measurementType = child.attrib.get('measurementType')
                    kpi_dict = {}
                    for subchild in child:
                        try:
                            value = int(subchild.text)
                            kpi_dict[subchild.tag] = value
                        except (TypeError, ValueError):
                            continue

                    if kpi_dict:  # Only proceed if there are non-zero kpiValues
                        kpi_columns = sorted(kpi_dict.keys())
                        create_table_if_not_exists(measurementType, kpi_columns)
                        row = [createAt, manageObject] + [kpi_dict.get(col) for col in kpi_columns]
                        insert_into_table(measurementType, [row], kpi_columns)
    
    # Move the file to the read directory
    dst_path = os.path.join(dir_files_read, os.path.basename(file_path))
    shutil.move(file_path, dst_path)  # Move the file to the read directory

# Loop through all files in the directory
def process_all_files():
    files = sorted(os.listdir(dir_files))
    print(files)
    for file_name in files:
        file_path = os.path.join(dir_files, file_name)
        if os.path.isfile(file_path):  # Ensure it's a file
            process_kpi_file(file_path)

# Main Execution
if __name__ == "__main__":
    download_files()
    process_all_files()

