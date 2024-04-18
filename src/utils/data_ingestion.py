# Importing necessary modules
import config
from utils.data_loaders import write_file_modification

def ingest_to_warehouse(df, table_name, file_path, last_index):
    """
    Function to ingest data into a warehouse.

    Parameters:
    df (DataFrame): The DataFrame to be ingested.
    table_name (str): The name of the table in the warehouse where the data will be ingested.
    file_path (str): The path of the file being ingested.
    last_index (int): The last index of the data in the DataFrame.

    Returns:
    dict: A dictionary containing the status of the operation and any error that occurred.
    """
    
    try:
        # Writing the DataFrame to the JDBC source
        df.write.jdbc(url=config.jdbc_url, table=table_name, mode="append", properties=config.mysql_properties)
        # Save the logs of the loaded file
        write_file_modification(file_path, last_index)
    except Exception as e:
        # If there is an exception, return a dictionary indicating failure and the error
        return {"status": False, "error": e}
    else:
        # If the operations are successful, return a dictionary indicating success
        return {"status": True, "error": None}
        
