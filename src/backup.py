from data import log_to_db

import shutil
import os
import datetime

# Get the backup folder path from environment variable
backup_folder_path = os.environ.get('BACKUP-FOLDER')
if backup_folder_path is None or not backup_folder_path:
    backup_folder_path = './backup'

# Create a backup folder if it doesn't exist
if not os.path.exists(backup_folder_path):
    os.makedirs(backup_folder_path)


def backup_folder(source_folder_path):
    if not os.path.exists(source_folder_path):
        log_to_db("BACKUP", "Folder=" + source_folder_path, "ERROR")
        raise ValueError("Source Folder does not exist")

    # Create a timestamp for the backup folder name
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    backup_folder_name = f"{os.path.basename(source_folder_path)}_{timestamp}"
    backup_path = os.path.join(backup_folder_path, backup_folder_name)

    # Copy the contents of the source folder to the backup folder
    shutil.copytree(source_folder_path, backup_path)

    log_to_db("BACKUP", "Folder=" + source_folder_path, "SUCCESS")
