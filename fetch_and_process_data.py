import requests
import psycopg2
import os
import pandas as pd
import json
import urllib.parse

# Fetch secrets from environment variables
FIREBASE_DATABASE_URL = os.getenv('FIREBASE_DATABASE_URL')
FIREBASE_DATABASE_SECRET = os.getenv('FIREBASE_DATABASE_SECRET')

# Function to get the latest timestamp from Firebase
def get_latest_timestamp():
    response = requests.get(f'{FIREBASE_DATABASE_URL}/Tanks/data.json?auth={FIREBASE_DATABASE_SECRET}')
    data = response.json()

    if data:
        timestamps = []
        for device_data in data.values():
            if isinstance(device_data, dict):
                timestamps.extend(device_data.keys())

        if timestamps:
            valid_timestamps = []
            for ts in timestamps:
                try:
                    start_time_str = ts.split(" - ")[0]
                    start_time = pd.to_datetime(start_time_str, format='%Y-%m-%dT%H:%M:%S')
                    valid_timestamps.append(start_time)
                except Exception as e:
                    print(f"Invalid timestamp format: {ts} - Error: {e}")

            if valid_timestamps:
                latest_timestamp = max(valid_timestamps)
                return latest_timestamp
    return None

# Function to fetch new data from PostgreSQL
def fetch_new_data(since_timestamp=None):
    pg_conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

    cursor = pg_conn.cursor()

    query = """
    SELECT
        d.devicename,
        dd.deviceid,
        s.sensordescription,
        sd.value,
        dd.devicetimestamp
    FROM
        sensordata sd
    JOIN
        sensors s ON s.sensorid = sd.sensorid
    JOIN
        devicedata dd ON sd.dataid = dd.dataid
    JOIN
        devices d ON d.deviceid = dd.deviceid
    WHERE
        dd.deviceid IN (10, 9, 8, 7)
    """
    if since_timestamp:
        query += f" AND dd.devicetimestamp > '{since_timestamp}'"

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    combined_df = pd.DataFrame(rows, columns=columns)

    combined_df['devicetimestamp'] = pd.to_datetime(combined_df['devicetimestamp']) + pd.Timedelta(hours=8)
    combined_df['minute_interval'] = combined_df['devicetimestamp'].dt.floor('T')

    pivot_df = combined_df.pivot_table(
        index=['devicename', 'deviceid', 'minute_interval'],
        columns='sensordescription',
        values='value',
        aggfunc='mean'
    ).reset_index()
    pivot_df.columns.name = None
    pivot_df.columns = [str(col) for col in pivot_df.columns]

    def replace_out_of_range(series, min_val, max_val):
        valid_series = series.copy()
        mask = (series < min_val) | (series > max_val)
        valid_series[mask] = pd.NA
        valid_series = valid_series.ffill().fillna(0)
        return valid_series

    if 'Soil - Temperature' in pivot_df.columns:
        pivot_df['Soil - Temperature'] = replace_out_of_range(pivot_df['Soil - Temperature'], 5, 40)

    if 'Soil - PH' in pivot_df.columns:
        pivot_df['Soil - PH'] = replace_out_of_range(pivot_df['Soil - PH'], 0, 14)

    data_dict = {}
    latest_data_dict = {}

    for _, row in pivot_df.iterrows():
        devicename = row['devicename']
        start_time = row['minute_interval']
        timestamp = start_time.strftime('%Y-%m-%dT%H:%M:%S')

        # Populate the latest data dict
        if devicename not in latest_data_dict:
            latest_data_dict[devicename] = {}
        latest_data_dict[devicename][timestamp] = row.drop(['devicename', 'deviceid', 'minute_interval']).to_dict()

        # Populate the aggregated data dict
        if devicename not in data_dict:
            data_dict[devicename] = {}
        data_dict[devicename][timestamp] = row.drop(['devicename', 'deviceid', 'minute_interval']).to_dict()

    return data_dict, latest_data_dict

# Function to push latest data to `Livedata`
def push_latest_data_to_firebase(data):
    for device_name, timestamps in data.items():
        for timestamp, values in timestamps.items():
            # URL encode the timestamp to handle special characters
            encoded_timestamp = urllib.parse.quote(timestamp)
            url = f'{FIREBASE_DATABASE_URL}/Livedata/{device_name}/{encoded_timestamp}.json?auth={FIREBASE_DATABASE_SECRET}'
            
            try:
                response = requests.put(url, json=values)
                response.raise_for_status()  # Raise an HTTPError for bad responses
                print(f"Successfully pushed latest data for {device_name} at {timestamp}")
            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error occurred for {device_name} at {timestamp}: {http_err}")
            except Exception as err:
                print(f"Other error occurred for {device_name} at {timestamp}: {err}")

# Function to push aggregated data to Firebase
def push_aggregated_data_to_firebase(data):
    for device_name, timestamps in data.items():
        for timestamp, values in timestamps.items():
            # URL encode the timestamp to handle special characters
            encoded_timestamp = urllib.parse.quote(timestamp)
            url = f'{FIREBASE_DATABASE_URL}/Tanks/data/{device_name}/{encoded_timestamp}.json?auth={FIREBASE_DATABASE_SECRET}'
            
            try:
                response = requests.put(url, json=values)
                response.raise_for_status()  # Raise an HTTPError for bad responses
                print(f"Successfully pushed aggregated data for {device_name} at {timestamp}")
            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error occurred for {device_name} at {timestamp}: {http_err}")
            except Exception as err:
                print(f"Other error occurred for {device_name} at {timestamp}: {err}")

# Main process
latest_timestamp = get_latest_timestamp()
if latest_timestamp:
    print(f"Latest timestamp from Firebase: {latest_timestamp}")
    new_data, latest_data = fetch_new_data(latest_timestamp)
else:
    print("No data found in Firebase or unable to fetch latest timestamp. Fetching all data.")
    new_data, latest_data = fetch_new_data()

if latest_data:
    push_latest_data_to_firebase(latest_data)  # Push latest data to Livedata

if new_data:
    push_aggregated_data_to_firebase(new_data)  # Push aggregated data to Tanks/data
