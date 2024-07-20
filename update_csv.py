import pandas as pd
import os

csv_file = 'counter.csv'

# Check if the file exists
if not os.path.isfile(csv_file):
    # Create the file with initial value if it does not exist
    df = pd.DataFrame({'count': [0]})
    df.to_csv(csv_file, index=False)
else:
    # Read the existing file
    try:
        df = pd.read_csv(csv_file)
    except pd.errors.EmptyDataError:
        # Handle the case where the file is empty
        df = pd.DataFrame({'count': [0]})

# Increment the count
df['count'] += 1

# Save the updated DataFrame
df.to_csv(csv_file, index=False)
