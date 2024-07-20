import pandas as pd
from datetime import datetime, timedelta
import requests
import time
import os
import io

# Define URLs for today's and yesterday's data
url_today = "https://www.taipower.com.tw/d006/loadGraph/loadGraph/data/loadfueltype.csv"
url_yesterday = "https://www.taipower.com.tw/d006/loadGraph/loadGraph/data/loadfueltype_1.csv"

# Define columns
columns = ['Time', '核能', '燃煤', '汽電共生', '民營燃煤', '燃氣', '民營燃氣', '燃油', '輕油', '水力', '風力', '太陽能', '其它再生', '儲能', '儲能負載']

# Define database file
db_file = 'db.csv'

# Function to download and process CSV files
def download_and_process_csv(url, date):
    try:
        response = requests.get(url, timeout=10)  # Set a timeout for the request
        response.encoding = 'big5'
        csv_content = response.text
        data = pd.read_csv(io.StringIO(csv_content), header=None)
        data.columns = columns
        data['Time'] = date + ' ' + data['Time']
        data['Time'] = pd.to_datetime(data['Time'], format='%Y-%m-%d %H:%M')
        return data
    except Exception as e:
        print(f"Error downloading or processing CSV from {url}: {e}")
        return pd.DataFrame(columns=columns)

# Initialize database
if os.path.exists(db_file):
    db_data = pd.read_csv(db_file, parse_dates=['Time'])
else:
    db_data = pd.DataFrame(columns=columns)

# Fetch yesterday's data
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
data_yesterday = download_and_process_csv(url_yesterday, yesterday)

# Append yesterday's data to the database
db_data = pd.concat([db_data, data_yesterday], ignore_index=True).drop_duplicates(subset=['Time'])

# Main loop to fetch today's data every 5 minutes
while True:
    try:
        now = datetime.now()
        # Check if the current time is between 00:00 and 00:30
        if now.time() >= datetime.strptime('00:00', '%H:%M').time() and now.time() <= datetime.strptime('00:30', '%H:%M').time():
            print("Pause data collection between 00:00 and 00:30")
            time.sleep(300)  # Sleep for 5 minutes before checking again
            continue

        today = now.strftime('%Y-%m-%d')
        data_today = download_and_process_csv(url_today, today)

        # Append today's data to the database
        db_data = pd.concat([db_data, data_today], ignore_index=True).drop_duplicates(subset=['Time'])

        # Sort the database by the Time column
        db_data.sort_values(by='Time', inplace=True)

        # Save the updated database as UTF-8 encoded CSV
        db_data.to_csv(db_file, index=False, encoding='utf-8')
    
    except Exception as e:
        print(f"Error during the main loop: {e}")

    # Sleep for 5 minutes
    time.sleep(300)

