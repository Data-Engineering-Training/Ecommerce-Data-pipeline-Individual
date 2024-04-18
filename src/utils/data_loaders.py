import os
import json
import shutil
from pathlib import Path
from datetime import datetime
from pyspark.sql.functions import monotonically_increasing_id

# Configure base path to dataset files
BASE_DIR = Path().parent.absolute()
DATA_SETS_PATH = BASE_DIR / 'datasets'

def load_file_paths():
    """
    Function to load file paths for different datasets.
    """
    # Keep files path
    customer_files = []
    delivery_files = []
    orders_files = []
    # load paths to dataset
    for path in os.listdir(DATA_SETS_PATH):
        if path.endswith('csv') | path.endswith('json'):
            if 'customers' in path.lower():
                customer_files.append(path)
            elif 'orders' in path.lower():
                orders_files.append(path)
            elif 'deliveries' in path.lower():
                delivery_files.append(path)

    print(f"Customers files: {len(customer_files)}")
    print(f"Delivery files: {len(delivery_files)}")
    print(f"Orders files: {len(orders_files)}")
    
    return customer_files, delivery_files, orders_files

def json_file_loader(path, schema, dynamic_schema, spark_session):
    """
    Function to load JSON files into a DataFrame.
    """
    try:
        file_path = f'{DATA_SETS_PATH}/{path}'
        if dynamic_schema:
            # Load JSON file into DataFrame with the defined schema
            df = spark_session.read.option('multiline', 'true') \
                        .option('header', 'true') \
                        .schema(schema).json(file_path)
        else:
            df = spark_session.read.option('multiline', 'true') \
                        .option('header', 'true') \
                        .option('inferSchema', 'true').json(file_path)
        return df
    except Exception as e:
        return None

def csv_file_loader(path, schema, dynamic_schema, spark_session):
    """
    Function to load CSV files into a DataFrame.
    """
    try:
        file_path = f'{DATA_SETS_PATH}/{path}'
        if dynamic_schema:
            # Load CSV file into DataFrame with the defined schema
            df = spark_session.read \
                        .option('header', 'true') \
                        .schema(schema).csv(file_path)
        else:
            df = spark_session.read \
                        .option('header', 'true') \
                        .option('inferSchema', 'true').csv(file_path)

        return df
    except Exception as e:
        return None

def load_data_from_file(filename, schema, dynamic_schema, spark_session):
    """
    Function to load data from file into a DataFrame.
    """
    file_type = filename.split('.')[1]
    start_from = 0

    log_data = load_log_file()  # load the log file check if the file log exist
    exist, dict = check_value_in_json(log_data, filename)
    
    if exist and dict:
        start_from = int(dict['last_data_processed'])
    
    if file_type == 'json':
       df = json_file_loader(filename, schema, dynamic_schema, spark_session)
       return filter_from_index(df, start_from), df.count() # filter for only new updates
    elif file_type == 'csv':
      df = csv_file_loader(filename, schema, dynamic_schema, spark_session)
      return filter_from_index(df, start_from), df.count() # filter for only new updates
    else:
      return None, 0

def load_from_db(spark, jdbc_url, customer_table, user, password):
    """
    Function to load and persist customer information from MySQL table into DataFrame.
    """
    try:
        customer_df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", customer_table) \
        .option("user", user) \
        .option("password", password) \
        .load()
        
        return customer_df, None  
    except Exception as e:
        None, e

def __move_files(file_path):
    """
    Function to move a file from the original destination to a different.
    """
    source_path = DATA_SETS_PATH / file_path
    destination_path = DATA_SETS_PATH / 'ingested_data' / file_path
    # Move the file
    shutil.move(source_path, destination_path)

def filter_from_index(df, start_index=0):
    """
    This function filters a Spark DataFrame to include rows from a specific index onwards.

    Args:
        df: The Spark DataFrame to process.
        start_index: The index from which to start filtering (default: 0).

    Returns:
        A new Spark DataFrame containing the filtered rows.
    """
    if start_index < 0:
        print("Error: Start index cannot be negative.")
        return df

    # Add a column with row numbers (monotonically increasing)
    df_with_row_num = df.withColumn("_row_num", monotonically_increasing_id())
    return df_with_row_num.filter(df_with_row_num["_row_num"] >= start_index).drop("_row_num")

def find_dict_position(log_data, target_dict):
    """
    This function finds the position of a dictionary in a list.

    Args:
        log_data: The list containing dictionaries.
        target_dict: The dictionary to search for.

    Returns:
        The index of the dictionary in the list if found, otherwise -1.
    """
    for i, item in enumerate(log_data):
        if item == target_dict:  # Check for dictionary equality
            return i
    return -1

def check_value_in_json(log_data, value):
    """
    This function checks if a specific value exists in a JSON file (simple structures).

    Args:
        log_data: The JSON data.
        value: The value to search for.

    Returns:
        True if the value exists in the JSON data, False otherwise.
    """
    # Check if value exists (assuming a flat or nested dictionary structure)
    if isinstance(log_data, list):
        for dict in log_data:
            value_in_dict = value in dict.values()
            if value_in_dict:
                return True, dict
        return False, {}
    return False, {}

def load_log_file() -> list:
    """
    Function to load log file.
    """
    # Access or create JSON data (replace with your data structure if needed)
    log_file_path = BASE_DIR / 'file_change_logs.json'
    try:
        with open(log_file_path, 'r') as f:
            log_data = json.load(f)
    except FileNotFoundError:
            log_data = []
    except Exception as e:
            log_data = []
            
    return log_data

def write_file_modification(filepath, data_processed):
    """
    This function writes the modification date of a file to a JSON file.

    Args:
        filepath: The path to the file to track.
        data_processed: The data processed.

    """
    file_to_track = DATA_SETS_PATH / filepath
    
    # Check if file exists
    if not os.path.isfile(file_to_track):
        print(f"Error: File '{file_to_track}' does not exist.")
        return
    
    log_file_path = BASE_DIR / 'file_change_logs.json'
    # Get file modification time
    modification_time = os.path.getmtime(file_to_track)
    # Convert timestamp to human-readable format (adjust if desired)
    formatted_time = datetime.fromtimestamp(modification_time).strftime("%Y-%m-%d %H:%M:%S")


    log_data = load_log_file()
    
    file_name = os.path.basename(file_to_track)
    # Update data with modification time for the specific file
    data_dict = {
                'last_modified': formatted_time,
                'last_data_processed': data_processed,
                'file_name': file_name
                }    
        
    exists, target_dict = check_value_in_json(log_data, file_name)
    if exists:
        position = find_dict_position(log_data, target_dict)
        if position != -1:
           log_data[position] = data_dict
    else:
         log_data.append(data_dict)    

    # Write updated data to JSON file
    with open(log_file_path, 'w') as f:
        json.dump(log_data, f, indent=2)  # Add indent for readability (optional)

    print(f"Wrote modification time for '{file_to_track}' to '{log_file_path}'.")
