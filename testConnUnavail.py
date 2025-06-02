import paramiko
import socket
from time import sleep

# List of radios with their connection details
radios = [
    {"server_ip": "10.1.101.2","username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/stats/iOms/"},
    # {"server_ip": "10.1.101.2","username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/stats/iOms/"},
    {"server_ip": "10.1.60.2","username": "toor4nsn", "password": "oZPS0POrRieRtu","remote_directory": "/ram/stats/iOms/"},

]

def download_files():
    for radio in radios:
        max_retries = 1
        retry_delay = 5  # seconds between retries
        connected = False
        
        for attempt in range(max_retries):
            server_ip = radio["server_ip"]
            username = radio["username"]
            password = radio["password"]
            remote_directory = radio["remote_directory"]
            
            transport = None
            sftp = None
            
            try:
                print(f"Attempt {attempt + 1}/{max_retries} to connect to {server_ip}")
                
                # Establish SFTP connection with timeout
                transport = paramiko.Transport((server_ip, 22))
                transport.connect(username=username, password=password)
                
                # Create the SFTP client
                sftp = paramiko.SFTPClient.from_transport(transport)
                
                # List files in the remote directory
                print(f"Connected to {server_ip}")
                remote_files = sftp.listdir(remote_directory)
                
                # # Get the current quarter
                # current_quarter = get_current_quarter()
                # print(f"Current quarter: {current_quarter}")
                
                # Process your files here...
                
                connected = True
                break  # Success, exit retry loop
                
            except paramiko.SSHException as e:
                print(f"SSH error connecting to {server_ip}: {str(e)}")
            except socket.timeout:
                print(f"Connection timeout for {server_ip}")
            except socket.error as e:
                print(f"Socket error for {server_ip}: {str(e)}")
            except Exception as e:
                print(f"Unexpected error with {server_ip}: {str(e)}")
            finally:
                if sftp:
                    sftp.close()
                if transport:
                    transport.close()
            
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
        
        if not connected:
            print(f"Failed to connect to {server_ip} after {max_retries} attempts")
            continue  # Skip to next radio
            
        # Continue with file processing if connection was successful
        print(f"Successfully processed files from {server_ip}")

# Main Execution
if __name__ == "__main__":
    download_files()