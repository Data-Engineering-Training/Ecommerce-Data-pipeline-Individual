# Import necessary libraries and modules
from pyspark.sql import SparkSession
from utils.data_loaders import load_file_paths, load_data_from_file
from utils.data_schema import customers_schema, deliveries_schema, orders_schema
from utils.data_cleaning import clean_extra_commas
from utils.data_transformation import data_transformation_pipeline
from utils.data_cleaning import clean_column_names
from utils.data_ingestion import ingest_to_warehouse

def connect_tospark():
    """
    Function to create a SparkSession.
    Returns:
        spark: SparkSession object or None if an exception occurs.
    """
    try:
      spark = SparkSession.builder   \
                            .master("local") \
                            .appName("DataPipeline") \
                            .getOrCreate()
    except:
        return None
    else:
        return spark
                            

def stop_spark_session(spark_session):
    """
    Function to stop a SparkSession.
    Args:
        spark_session: The SparkSession to be stopped.
    """
    spark_session.stop()


def load_and_injest_data(file, schema, table, spark, use_schema = False, enrich = False, clean_id=False, join_on = 'customers_id'):
    """
    Function to load, clean, transform, and ingest data.
    Args:
        file (str): The file path of the data file.
        schema (StructType): The schema of the data.
        table (str): The table name in the data warehouse.
        spark (SparkSession): The SparkSession object.
        use_schema (bool): Whether to use the provided schema when loading data.
        enrich (bool): Whether to enrich the data during transformation.
        clean_id (bool): Whether to clean the ID column.
        join_on (str): The column name to join on.
    Raises:
        Exception: If an error occurs during data transformation or ingestion.
    """
    try:
       # load and filter data
       df, last_index = load_data_from_file(file, schema, use_schema, spark)
       # clean common problems
       df = clean_column_names(df, clean_id) 
       # Transform the data
       df, error = data_transformation_pipeline(spark, df, file, enrich, join_on)

       if error:
          raise Exception(error)
       if df:
            res = ingest_to_warehouse(df, table, file, last_index)
            if res['error'] != None:
                raise Exception(res['error'])       
    except Exception as e:
          raise Exception(e)


def run_pipeline():
    """
    Function to run the entire data pipeline.
    """
    # start the spark session
    spark = connect_tospark()
    
    if spark:
            # Load the file names of the data files
            customer_files, delivery_files, orders_files =  load_file_paths()
    
            # Clean structure issues in Market 1 Delivery
            try:
                print("Cleaning Deliveries Data")
                for path in delivery_files:
                    file_path = path.split('.')[0]
                    if file_path == 'Market 1 Deliveries':
                         cleaned_path = file_path + '_cleaned.' + str(path.split('.')[1])
                         clean_extra_commas(path, cleaned_path)
                
                         # update delivery_files with the cleaned data paths
                         delivery_files = [cleaned_path if item == path else item for item in delivery_files]


            except Exception as e:
                print("Error while cleaning delivery data", e)
          
       
            print("Data Ingestion Started")
            # Injest customer files
            try:
                
                for file in customer_files:
                    load_and_injest_data(file, customers_schema, 'customers', spark, True, False)
                
                print('===============================================================')
                print('Customer Data Ingestion Completed')
                print('===============================================================')
            
            except Exception as e:
                print("Error while ingesting customer data", e)

                
            # Injest orders files
            try:
                for file in orders_files:
                   load_and_injest_data(file, orders_schema, 'orders', spark, True, True)
                
                print('===============================================================')
                print('Orders Data Ingestion Completed')
                print('===============================================================')
                
            except Exception as e:
                print("Error while ingesting orders data", e)
                
            # Injest orders files
            try:
                for file in delivery_files:
                     load_and_injest_data(file, deliveries_schema, 'deliveries', spark, True, False, True, 'order_id')
                
                print('===============================================================')
                print('Deliveries Data Ingestion Completed')
                print('===============================================================')

            except Exception as e:
                print("Error while ingesting delivery data", e)
                
            # Stop Spark Session
            stop_spark_session(spark)
            
            print('===============================================================')
            print('Data Ingestion Completed')
            print('===============================================================')