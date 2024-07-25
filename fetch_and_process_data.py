import requests
import psycopg2
import os
import pandas as pd
import json

FIREBASE_DATABASE_URL = os.getenv('FIREBASE_DATABASE_URL')
FIREBASE_DATABASE_SECRET = os.getenv('FIREBASE_DATABASE_SECRET')

# Function to get the latest timestamp from Firebase
def get_latest_timestamp():
    response = requests.get(f'{FIREBASE_DATABASE_URL}/Tanks/data.json?auth={FIREBASE_DATABASE_SECRET}')
	get_latest_timestamp():
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
    combined_df['hourly_interval'] = combined_df['devicetimestamp'].dt.floor('H')

    pivot_df = combined_df.pivot_table(
        index=['devicename', 'deviceid', 'hourly_interval'],
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
    for _, row in pivot_df.iterrows():
        devicename = row['devicename']
        start_time = row['hourly_interval']
        end_time = start_time + pd.Timedelta(hours=1)
        timestamp = f"{start_time.strftime('%Y-%m-%dT%H:%M:%S')} - {end_time.strftime('%H:%M:%S')}"
        if devicename not in data_dict:
            data_dict[devicename] = {}
        data_dict[devicename][timestamp] = row.drop(['devicename', 'deviceid', 'hourly_interval']).to_dict()

    return data_dict

# Function to push data to Firebase
def push_data_to_firebase(data):
    for device_name, timestamps in data.items():
        for timestamp, values in timestamps.items():
            url = f'{FIREBASE_DATABASE_URL}/Tanks/data/{device_name}/{timestamp}.json?auth={FIREBASE_DATABASE_SECRET}'
            response = requests.put(url, json=values)
            if response.status_code != 200:
                print(f"Failed to push data for {device_name} at {timestamp}: {response.content}")

# Main process
latest_timestamp = get_latest_timestamp()
if latest_timestamp:
	push_data_to_firebase(data):

if new_data:
    push_data_to_firebase(new_data)


