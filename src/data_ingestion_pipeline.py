from pyspark.sql import SparkSession
from data_loaders import load_file_paths, load_data_from_file, move_files
from data_schema import customers_schema, deliveries_schema, orders_schema
from data_cleaning import clean_extra_commas
from data_ingestion import ingest_to_warehouse

def connect_tospark():
    try:
      return SparkSession.builder.config("spark.jars", "mysql-connector-j-8.3.0.jar") \
                            .master("local") \
                            .appName("DataPipeline") \
                            .getOrCreate()
    except:
        return None
                            

def stop_spark_session(spark_session):
    spark_session.stop()


def load_and_injest_data(file, customers_schema, table, spark):
        try:
           df = load_data_from_file(file, customers_schema, False, spark)
           if df:
                res = ingest_to_warehouse(df, table)
                if res['error'] != None:
                    raise Exception(e)
                
                # Move file after ingestion
                move_files(file)
                        
        except Exception as e:
              raise Exception(e)



def run_pipeline():
        # start the spark session
    spark = connect_tospark()
    
    if spark:
            customer_files, delivery_files, orders_files =  load_file_paths()
    
            # Clean strucher issues in Market 1 Delivery
            for path in delivery_files:
                file_path = path.split('.')[0]
                cleaned_path = file_path + '_cleaned.' + str(path.split('.')[1])
                clean_extra_commas(path, cleaned_path)
                
                # update delivery_files with the cleaned data paths
                delivery_files = [cleaned_path if item == path else item for item in delivery_files]
                move_files(path)
                break
       
            # Injest customer files
            for file in customer_files:
                load_and_injest_data(file, customers_schema, 'customers', spark)
                
            print('===============================================================')
            print('Customer Data Ingestion Completed')
            print('===============================================================')
                
            # Injest orders files
            for file in orders_files:
                load_and_injest_data(file, orders_schema, 'orders', spark)
                
            print('===============================================================')
            print('Orders Data Ingestion Completed')
            print('===============================================================')
                
            # Injest orders files
            for file in delivery_files:
                load_and_injest_data(file, deliveries_schema, 'deliveries', spark)
                
            print('===============================================================')
            print('Deliveries Data Ingestion Completed')
            print('===============================================================')

                 
            # Stop Spark Session
            stop_spark_session(spark)
            
            print('===============================================================')
            print('Data Ingestion Completed')
            print('===============================================================')




