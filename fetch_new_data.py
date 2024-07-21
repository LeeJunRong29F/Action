import os
import psycopg2
import pandas as pd
import json
import requests

# Firebase configuration
FIREBASE_DATABASE_URL = os.getenv('FIREBASE_DATABASE_URL')
FIREBASE_DATABASE_SECRET = os.getenv('FIREBASE_DATABASE_SECRET')

def get_latest_timestamp():
    response = requests.get(f'{FIREBASE_DATABASE_URL}/Tanks/data.json?auth={FIREBASE_DATABASE_SECRET}')
    data = response.json()
    if data:
        # Flatten the nested dictionary to get all timestamps
        timestamps = []
        for device_data in data.values():
            timestamps.extend(device_data.keys())
        if timestamps:
            latest_timestamp = max(timestamps, key=lambda k: pd.to_datetime(k))
            return latest_timestamp
    return None

def fetch_all_data():
    # PostgreSQL connection details
    pg_conn = psycopg2.connect(
        dbname="smart_composting_api",
        user="npds_a",
        password="npds_A_rg6STCC-8LoXakAHDJerqtZNlRr5TtlvcxSrJFOa9rbdeLmf4THld-8LEaaXnjTCbCjGdl9evTGe2kxRA8vrbg",
        host="db.composting.tinkerthings.global",
        port="6969"
    )

    cursor = pg_conn.cursor()

    # SQL query to get all data
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
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    # Convert data to a pandas DataFrame
    combined_df = pd.DataFrame(rows, columns=columns)

    # Process and pivot the data as needed
    pivot_df = combined_df.pivot_table(
        index=['devicename', 'deviceid', 'devicetimestamp'],
        columns='sensordescription',
        values='value',
        aggfunc='first'
    ).reset_index()
    pivot_df.columns.name = None
    pivot_df.columns = [str(col) for col in pivot_df.columns]

    # Function to replace values outside range
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

    # Convert DataFrame to a dictionary
    data_dict = {}
    for _, row in pivot_df.iterrows():
        devicename = row['devicename']
        timestamp = row['devicetimestamp'].strftime('%Y-%m-%dT%H:%M:%S')
        if devicename not in data_dict:
            data_dict[devicename] = {}
        data_dict[devicename][timestamp] = row.drop(['devicename', 'deviceid', 'devicetimestamp']).to_dict()

    return data_dict

def push_to_firebase(data_dict):
    for devicename, timestamps in data_dict.items():
        for timestamp, values in timestamps.items():
            url = f'{FIREBASE_DATABASE_URL}/Tanks/data/{devicename}/{timestamp}.json?auth={FIREBASE_DATABASE_SECRET}'
            requests.put(url, json=values)

if __name__ == "__main__":
    latest_timestamp = get_latest_timestamp()
    if latest_timestamp:
        print(f'Latest timestamp in Firebase: {latest_timestamp}')
        new_data = fetch_new_data(latest_timestamp)
    else:
        print('No existing data in Firebase. Fetching all data.')
        new_data = fetch_all_data()
    
    push_to_firebase(new_data)
