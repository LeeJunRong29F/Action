import requests
import psycopg2
import os
import pandas as pd
import json

# Firebase configuration
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
    combined_df['hourly_interval'] = combined_df['devicetimestamp'].dt.floor('H')

    # Pivot the DataFrame to reshape it
    pivot_df = combined_df.pivot_table(
        index=['devicename', 'deviceid', 'devicetimestamp', 'hourly_interval'],
        columns='sensordescription',
        values='value',
        aggfunc='first'
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

    # Group by 'deviceid' and get the latest entry for each group
    latest_live_df = pivot_df.loc[pivot_df.groupby(['deviceid'])['devicetimestamp'].idxmax()]
    latest_live_df = latest_live_df.drop(columns=['hourly_interval'])  # Do not use inplace=True
    
    # Group by 'devicename', 'deviceid', and 'hourly_interval' and calculate the mean
    grouped_df = pivot_df.groupby(['devicename', 'deviceid', 'hourly_interval']).mean().reset_index()
    grouped_df = grouped_df.drop(columns=['devicetimestamp'])  # Do not use inplace=True


    # Prepare data_dict1 from grouped_df
    data_dict1 = {}
    for _, row in grouped_df.iterrows():
        devicename = row['devicename']
        start_time = row['hourly_interval']
        end_time = start_time + pd.Timedelta(hours=1)
        timestamp = f"{start_time.strftime('%Y-%m-%dT%H:%M:%S')} - {end_time.strftime('%H:%M:%S')}"
        if devicename not in data_dict1:
            data_dict1[devicename] = {}
        data_dict1[devicename][timestamp] = row.drop(['devicename', 'deviceid', 'hourly_interval']).to_dict()

    # Prepare data_dict2 from latest_live_df
    data_dict2 = {}
    for _, row in latest_live_df.iterrows():
        devicename = row['devicename']
        if devicename not in data_dict2:
            data_dict2[devicename] = {}
        data_dict2[devicename] = row.drop(['devicename', 'deviceid', 'devicetimestamp']).to_dict()

    return data_dict1, data_dict2

# Function to push data to Firebase
def push_data_to_firebase(data_dict1, data_dict2):
    def serialize_data(data):
        """Convert pd.Timestamp to ISO format string for JSON serialization."""
        serialized_data = {}
        for k, v in data.items():
            if isinstance(v, pd.Timestamp):
                serialized_data[k] = v.isoformat()
            elif isinstance(v, dict):
                serialized_data[k] = serialize_data(v)
            else:
                serialized_data[k] = v
        return serialized_data

    # Push data_dict1
    for device_name, timestamps in data_dict1.items():
        for timestamp, values in timestamps.items():
            url = f'{FIREBASE_DATABASE_URL}/Tanks/{device_name}/HourlyData/{timestamp}.json?auth={FIREBASE_DATABASE_SECRET}'
            values = serialize_data(values)  # Convert data to serializable format
            response = requests.put(url, json=values)
            if response.status_code != 200:
                print(f"Failed to push data for {device_name} at {timestamp}: {response.content}")

    # Push data_dict2
    for device_name, values in data_dict2.items():
        url = f'{FIREBASE_DATABASE_URL}/Tanks/{device_name}/LiveData.json?auth={FIREBASE_DATABASE_SECRET}'
        values = serialize_data(values)  # Convert data to serializable format
        response = requests.put(url, json=values)
        if response.status_code != 200:
            print(f"Failed to push latest data for {device_name}: {response.content}")

# Main process
def main():
    latest_timestamp = get_latest_timestamp()
    if latest_timestamp:
        print(f"Latest timestamp from Firebase: {latest_timestamp}")
        data_dict1, data_dict2 = fetch_new_data(latest_timestamp)
    else:
        print("No data found in Firebase or unable to fetch latest timestamp. Fetching all data.")
        data_dict1, data_dict2 = fetch_new_data()

    if data_dict1 or data_dict2:
        push_data_to_firebase(data_dict1, data_dict2)

if __name__ == "__main__":
    main()
