import os
from datetime import datetime


def configure_folder_path(folder_path):
    folder_exists = os.path.exists(folder_path)
    if folder_exists:
        date_time = datetime.now()
        os.rename(folder_path, f"{folder_path}_archive_{date_time.date()}_{date_time.hour}:{date_time.minute}:{date_time.second}")
    os.makedirs(folder_path)

