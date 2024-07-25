import requests
import psycopg2
import os
import pandas as pd
import json

# Firebase configuration
# Fetch secrets from environment variables
FIREBASE_DATABASE_URL = os.getenv('FIREBASE_DATABASE_URL')
FIREBASE_DATABASE_SECRET = os.getenv('FIREBASE_DATABASE_SECRET')


pg_conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

# Create a cursor object
cursor = pg_conn.cursor()

# Define the query
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

# Execute the query
cursor.execute(query)
rows = cursor.fetchall()

# Get column names from cursor description
columns = [desc[0] for desc in cursor.description]

# Create a DataFrame from the fetched data
combined_df = pd.DataFrame(rows, columns=columns)

# Convert devicetimestamp to datetime and add 8 hours
combined_df['devicetimestamp'] = pd.to_datetime(combined_df['devicetimestamp']) + pd.Timedelta(hours=8)

# Create a new column for hourly intervals
combined_df['hourly_interval'] = combined_df['devicetimestamp'].dt.floor('H')

# Sort the DataFrame by devicetimestamp in ascending order
combined_df = combined_df.sort_values(by='devicetimestamp', ascending=True)


# Pivot the DataFrame to reshape it
pivot_df = combined_df.pivot_table(
    index=['devicename', 'deviceid', 'devicetimestamp','hourly_interval'],
    columns='sensordescription',
    values='value',
    aggfunc='first'
).reset_index()


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




# Close the cursor and connection
cursor.close()
pg_conn.close()

# Group by 'deviceid' and get the latest entry for each group
latest_live_df = pivot_df.loc[pivot_df.groupby(['deviceid'])['devicetimestamp'].idxmax()]

# Group by 'devicename', 'deviceid', and 'hourly_interval' and calculate the mean
grouped_df = pivot_df.groupby(['devicename', 'deviceid', 'hourly_interval']).mean().reset_index()


data_dict = {}
for _, row in grouped_df.iterrows():
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

# Main process
latest_timestamp = get_latest_timestamp()
if latest_timestamp:
    print(f"Latest timestamp from Firebase: {latest_timestamp}")
    new_data = fetch_new_data(latest_timestamp)
else:
    print("No data found in Firebase or unable to fetch latest timestamp. Fetching all data.")
    new_data = fetch_new_data()

if new_data:
    push_data_to_firebase(new_data)


