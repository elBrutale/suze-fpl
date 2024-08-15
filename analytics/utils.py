import pandas as pd
import json

# Function to read JSONL files into a list of dictionaries
def read_jsonl(file_path):
    with open(file_path, 'r') as file:
        return [json.loads(line) for line in file]

def jsonl_to_df(file_path):
    # Load the JSONL file
    data = read_jsonl(file_path)
    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(data)
    return df

def read_dataframe(file_path):
    return pd.read_csv(file_path)