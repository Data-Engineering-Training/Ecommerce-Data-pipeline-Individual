
import os
import shutil
from pathlib import Path
from data_transformation import add_market_column


# Configure base path to dataset files
BASE_DIR = Path().parent.absolute()
DATA_SETS_PATH = BASE_DIR / 'datasets'



def load_file_paths():
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
    try:
        file_path = f'{DATA_SETS_PATH}/{path}'
        if dynamic_schema:
                  # Load JSON file into DataFrame with the defined schema
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
        file_type = filename.split('.')[1]
        if file_type == 'json':
           df = json_file_loader(filename, schema, dynamic_schema, spark_session)
           return add_market_column(df, 'market', filename)
        elif file_type == 'csv':
          df = csv_file_loader(filename, schema, dynamic_schema, spark_session)
          return add_market_column(df, 'market', filename)
        else:
          return None
      
      
      
## Utils

def move_files(file_path):
    """Moves a file from the original destination to a different"""
    source_path = DATA_SETS_PATH / file_path
    destination_path = DATA_SETS_PATH / 'ingested_data' / file_path
    # Move the file
    shutil.move(source_path, destination_path)