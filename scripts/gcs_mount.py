import os
import subprocess
import tempfile
import time
import signal

# 3 STEP PROCESS TO INSTALL A GCS MOUNT:
"""
1. Install windows file system proxy 
====================================
Go to the following link:
    https://github.com/winfsp/winfsp/releases

Download
    winfsp-*.msi

Install
    winfsp-*.msi

2. Install Chocolatey
=====================
Open PowerShell as Administrator
Run the following command:
    Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

3. Install rclone
=================
Open PowerShell as Administrator
Run the following command:
    choco install rclone
"""

# CONFIGURATION
RCLONE_PATH = r"C:\ProgramData\chocolatey\bin\rclone.exe"
BUCKET_NAME = "stock_overflow_bucket"
# DEFINE THE SERVICE ACCOUNT KEY PATH (ENVIRONMENT VARIABLE, OUTSIDE THE WORKSPACE; SEE ingest/gcs_file_management.py)
SERVICE_ACCOUNT_KEY_PATH = os.environ.get("SO_GCS_KEY_PATH", "")
DRIVE_LETTER = "G:"

# FUNCTION: CHECK IF RCLONE IS INSTALLED
def check_rclone_installed():
    """
    Checks if rclone is installed and accessible at the specified path.

    Raises:
        RuntimeError: If rclone is not found at RCLONE_PATH or an error occurs when running it.
    """
    try:
        subprocess.run([RCLONE_PATH, "--version"], check=True, capture_output=True)
    except Exception as e:
        raise RuntimeError(f"rclone not found at {RCLONE_PATH}. Error: {e}")

# FUNCTION: CREATE RCLONE CONFIG
def create_rclone_config():
    """
    Creates a temporary rclone configuration file for mounting a Google Cloud Storage (GCS) bucket.

    The configuration uses the service account key for authentication and names the remote 'GCS_mount'.

    Returns:
        str: The file path to the generated rclone config file.
    """
    config_content = f"""
[GCS_mount]
type = google cloud storage
service_account_file = {SERVICE_ACCOUNT_KEY_PATH}
project_number =
"""
    config_path = os.path.join(tempfile.gettempdir(), "rclone_gcs.conf")
    with open(config_path, "w") as f:
        f.write(config_content)
    return config_path

# FUNCTION: MOUNT BUCKET
def mount_bucket(config_path):
    """
    Mounts the specified GCS bucket to the given drive letter using rclone with the provided config.

    Args:
        config_path (str): The path to the rclone config file.

    Returns:
        subprocess.Popen: The running rclone mount process.
    """
    print(f"Mounting bucket '{BUCKET_NAME}' to drive {DRIVE_LETTER} ...")
    cmd = [
        RCLONE_PATH,
        "mount",
        f"GCS_mount:{BUCKET_NAME}",
        DRIVE_LETTER,
        "--config", config_path,
        "--vfs-cache-mode", "writes",
        "--log-level", "INFO",
    ]
    process = subprocess.Popen(cmd)
    print("Bucket mounted successfully. Press Ctrl+C to unmount.")
    return process

# FUNCTION: MAIN
def main():
    # CHECK THE KEY PATH
    if not SERVICE_ACCOUNT_KEY_PATH or not os.path.isfile(SERVICE_ACCOUNT_KEY_PATH):
        raise SystemExit("Set the environment variable SO_GCS_KEY_PATH to the service account key file.")
    check_rclone_installed()
    config_path = create_rclone_config()
    process = mount_bucket(config_path)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nUnmounting...")
        # CTRL_BREAK_EVENT EXISTS ONLY ON WINDOWS
        if hasattr(signal, "CTRL_BREAK_EVENT"):
            process.send_signal(signal.CTRL_BREAK_EVENT)
        process.terminate()
        print("Unmounted successfully.")

# MAIN FUNCTION
if __name__ == "__main__":
    main()