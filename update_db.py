import requests
import json
import os
from datetime import datetime
from threading import Timer

# URL to download the JSON file
url = 'https://service.taipower.com.tw/data/opendata/apply/file/d006001/001.json'

# Directory to store the database files
db_dir = 'db_files'

# Ensure the directory exists
os.makedirs(db_dir, exist_ok=True)

# Global variables to store the current month's data and timestamps
current_month = None
db = []
timestamps = set()

def download_json(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error downloading JSON: {e}")
        return None

def get_db_filename(timestamp):
    date = datetime.strptime(timestamp, '%Y-%m-%d %H:%M')
    return os.path.join(db_dir, f'db_{date.strftime("%Y%m")}.json')

def load_local_db(db_filename):
    global db, timestamps
    try:
        if os.path.exists(db_filename):
            with open(db_filename, 'r', encoding='utf-8') as file:
                loaded_data = json.load(file)
                unique_data = []
                seen_timestamps = set()
                for record in loaded_data:
                    record_timestamp = record[""]
                    if record_timestamp not in seen_timestamps:
                        seen_timestamps.add(record_timestamp)
                        unique_data.append(record)
                # Sort the unique data by timestamp
                unique_data.sort(key=lambda x: datetime.strptime(x[""], '%Y-%m-%d %H:%M'))
                db = unique_data
                timestamps = seen_timestamps
                print("len db:" + str(len(db)))
        else:
            db = []
            timestamps = set()
    except Exception as e:
        print(f"Error loading local database: {e}")
        db = []
        timestamps = set()

def save_local_db(db_filename):
    global db
    try:
        with open(db_filename, 'w', encoding='utf-8') as file:
            json.dump(db, file, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Error saving local database: {e}")

def update_db(new_data):
    global db, timestamps
    new_timestamp = new_data[""]
    if new_timestamp in timestamps:
        return False  # Duplicate found, no update needed
    db.append(new_data)
    timestamps.add(new_timestamp)
    return True

def fetch_and_update():
    global current_month
    try:
        # Download the JSON data
        new_data = download_json(url)
        if new_data is None:
            return  # Skip this cycle if download failed

        # Determine the appropriate database file based on the timestamp
        new_timestamp = new_data[""]
        db_filename = get_db_filename(new_timestamp)

        # Check if we need to switch to a new month's file
        if current_month != db_filename:
            # Save the current month's data before switching
            if current_month is not None:
                save_local_db(current_month)
            # Load the new month's data
            load_local_db(db_filename)
            current_month = db_filename

        # Update the local database with new data
        if update_db(new_data):
            # Save the updated database back to the file
            save_local_db(current_month)

        print(f"Updated database with timestamp: {new_timestamp}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Schedule the function to run again after 5 minutes
        Timer(300, fetch_and_update).start()

if __name__ == '__main__':
    # Initialize the database with the current month's data
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
    current_month = get_db_filename(current_time)
    load_local_db(current_month)

    # Start the periodic update
    fetch_and_update()

