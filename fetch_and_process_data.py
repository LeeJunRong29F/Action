import os
import psycopg2
import pandas as pd
import json

# PostgreSQL connection details
pg_conn = psycopg2.connect(
    dbname="smart_composting_api",
    user="npds_a",
    password="npds_A_rg6STCC-8LoXakAHDJerqtZNlRr5TtlvcxSrJFOa9rbdeLmf4THld-8LEaaXnjTCbCjGdl9evTGe2kxRA8vrbg",
    host="db.composting.tinkerthings.global",
    port="6969"
)

cursor = pg_conn.cursor()

# Query to join the tables
query = """
SELECT
    d.devicename,
    dd.deviceid,
    s.sensordescription,
    sd.value,
    dd.devicetimestamp,
    dd.dbtimestamp
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

# Pivot the data
pivot_df = combined_df.pivot_table(
    index=['devicename', 'deviceid', 'devicetimestamp'],
    columns='sensordescription',
    values='value',
    aggfunc='first'  # assuming one value per sensor per deviceid and timestamp
).reset_index()

# Flatten the columns
pivot_df.columns.name = None
pivot_df.columns = [str(col) for col in pivot_df.columns]

# Function to replace values outside range with the previous valid value within range
def replace_out_of_range(series, min_val, max_val):
    valid_series = series.copy()
    mask = (series < min_val) | (series > max_val)
    valid_series[mask] = pd.NA  # Replace out-of-range values with NA
    valid_series = valid_series.ffill().fillna(0)  # Forward fill NA values, then fill remaining NAs with 0
    return valid_series

# Apply the function to the temperature column
if 'Soil - Temperature' in pivot_df.columns:
    pivot_df['Soil - Temperature'] = replace_out_of_range(pivot_df['Soil - Temperature'], 5, 40)

# Apply the function to the PH column 
if 'Soil - PH' in pivot_df.columns:
    pivot_df['Soil - PH'] = replace_out_of_range(pivot_df['Soil - PH'], 0, 14)

# Format the DataFrame into a nested dictionary
data_dict = {}
for _, row in pivot_df.iterrows():
    devicename = row['devicename']
    timestamp = row['devicetimestamp'].strftime('%Y-%m-%dT%H:%M:%S')  # Convert Timestamp to string
    if devicename not in data_dict:
        data_dict[devicename] = {}
    data_dict[devicename][timestamp] = row.drop(['devicename', 'deviceid', 'devicetimestamp']).to_dict()

# Save the final DataFrame to a JSON file
with open('processed_data.json', 'w') as f:
    json.dump(data_dict, f, indent=4)  # Pretty-print the JSON with an indentation of 4 spaces
