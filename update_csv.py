import pandas as pd
import os

# Define the path to the CSV file
csv_file = 'counter.csv'

# Check if the CSV file exists
if os.path.exists(csv_file):
    # Read the existing CSV file
    df = pd.read_csv(csv_file)
    # Increment the value
    new_value = df['Value'].iloc[0] + 1
    # Update the DataFrame
    df.loc[0] = [new_value]
else:
    # Create a new DataFrame with initial value 0
    df = pd.DataFrame({'Value': [0]})

# Save the DataFrame to the CSV file
df.to_csv(csv_file, index=False)
