import pandas as pd
from deepdiff import DeepDiff

def load_data(file_path):
    """
    Loads data from a CSV file into a pandas DataFrame.
    """
    try:
        data = pd.read_csv(file_path)
        return data
    except Exception as e:
        print(f"Error loading data from {file_path}: {e}")
        return None

def compare_schemas(old_data, new_data):
    """
    Compares the schemas of the old and new datasets and detects schema drift.
    """
    old_schema = old_data.dtypes.to_dict()
    new_schema = new_data.dtypes.to_dict()
    
    diff = DeepDiff(old_schema, new_schema, verbose_level=2)
    return diff

def handle_new_columns(old_data, new_schema):
    """
    Handles new columns in the new schema by adding them to the old data.
    """
    for col in new_schema.keys():
        if col not in old_data.columns:
            # For new columns, add them with default values
            if new_schema[col] == 'float64':
                old_data[col] = None  # Placeholder for numeric columns
            elif new_schema[col] == 'object':
                old_data[col] = "unknown"  # Placeholder for string columns
    return old_data

def handle_removed_columns(new_data, old_schema):
    """
    Removes columns from the new data that no longer exist in the old schema.
    """
    for col in old_schema.keys():
        if col not in new_data.columns:
            new_data = new_data.drop(col, axis=1)
    return new_data

def handle_data_type_changes(new_data, old_schema, new_schema):
    """
    Handles changed data types by converting new data to the old schema's types.
    """
    for col in old_schema.keys():
        if col in new_schema and old_schema[col] != new_schema[col]:
            try:
                new_data[col] = new_data[col].astype(old_schema[col])
            except Exception as e:
                print(f"Error converting column {col}: {e}")
                new_data[col] = None  # Fill with None if conversion fails
    return new_data

def detect_and_fix_schema_drift(old_data, new_data):
    """
    Detects schema drift and fixes the issues such as added/removed columns or changed data types.
    """
    old_schema = old_data.dtypes.to_dict()
    new_schema = new_data.dtypes.to_dict()

    # Compare schemas and get the differences
    schema_diff = compare_schemas(old_data, new_data)
    print("Schema Differences Detected: ", schema_diff)
    
    # Handle new columns
    old_data = handle_new_columns(old_data, new_schema)
    
    # Handle removed columns
    new_data = handle_removed_columns(new_data, old_schema)
    
    # Handle changed data types
    new_data = handle_data_type_changes(new_data, old_schema, new_schema)
    
    return old_data, new_data