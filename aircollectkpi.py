
import paramiko
import os
import re
import pytz
from datetime import datetime
import gzip
import shutil

# List of radios with their connection details
radios = [
    {"server_ip": "10.1.8.2",  "username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/"},
    # {"server_ip": "10.1.16.2", "username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/"},
    # {"server_ip": "10.1.16.21","username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/"},
    # {"server_ip": "10.1.38.2", "username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/"},
    # {"server_ip": "10.1.39.2", "username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/"},
    # {"server_ip": "10.1.42.2", "username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/"},
    # {"server_ip": "10.1.50.2", "username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/"},
    # {"server_ip": "10.1.52.2", "username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/"},
    # {"server_ip": "10.1.53.2", "username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/"},
]

# Local directory where files will be saved
local_directory = "/Userdata/proj2025/kpidata/kpi_files"
local_zip_directory = "/Userdata/proj2025/kpidata/kpi_zip"
# local_directory = r"/var/openkpi/kpi_files"
os.makedirs(local_directory, exist_ok=True)

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
    Example: "PM.BTS-100004.20250518.111500.ANY.raw.gz" -> "PM.BTS-100004.20250518.111500.gz"
    """
    if original_name.endswith(".ANY.raw.gz"):
        new_name = original_name.replace(".ANY.raw.gz", ".xml")
        return new_name, None  # No need to return a timestamp
    return original_name, None  # Return the original name if it doesn't end with .raw


def download_and_unzip_files(server_ip, username, password, remote_directory):
    try:
        # Establish SFTP connection
        transport = paramiko.Transport((server_ip, 22))
        transport.connect(username=username, password=password)

        # Create the SFTP client
        sftp = paramiko.SFTPClient.from_transport(transport)

        # List files in the remote directory
        print(f"Connecting to server {server_ip}...")
        remote_files = sftp.listdir(remote_directory)

        # Get the current quarter
        current_quarter = get_current_quarter()
        print(f"Current quarter: {current_quarter}")

        for file_name in remote_files:
            print(file_name, remote_directory)
            # Check if the file matches the quarterly KPI naming pattern
            if file_name.startswith("PM.") and file_name.endswith(".ANY.raw.gz"):
                # Adjust the file name and extract the timestamp
                new_file_name, _ = adjust_file_name(file_name)

                # Define remote and local file paths
                remote_file_path = os.path.join(remote_directory, file_name)
                print(remote_file_path)
                local_file_path = os.path.join(local_zip_directory, new_file_name)
                print(local_file_path)

                # Check if the file already exists locally
                if os.path.exists(local_file_path):
                    print(f"File {new_file_name} already exists locally. Skipping download.")
                    continue

                # Download the file with the new name
                print(f"Downloading {file_name} as {new_file_name}...")
                sftp.get(remote_file_path, local_file_path)
                print(f"File {new_file_name} downloaded successfully.")

                # Open the .gz file and write the uncompressed data to the output file
                with gzip.open(remote_file_path, 'rb') as f_in:
                    with open(local_file_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)

        # Close the SFTP connection
        sftp.close()
        transport.close()
        print(f"All files from {server_ip} processed successfully.\n")

    except Exception as e:
        print(f"Error during file transfer from {server_ip}: {e}")

# Iterate through all radios and download files
for radio in radios:
    download_and_unzip_files(
        server_ip=radio["server_ip"],
        username=radio["username"],
        password=radio["password"],
        remote_directory=radio["remote_directory"]
    )
