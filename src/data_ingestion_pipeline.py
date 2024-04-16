from pyspark.sql import SparkSession
from data_loaders import load_file_paths, load_data_from_file, move_files, load_log_file
from data_schema import customers_schema, deliveries_schema, orders_schema
from data_cleaning import clean_extra_commas
from data_transformation import data_transformation_pipeline
from data_cleaning import clean_column_names
from data_ingestion import ingest_to_warehouse


def connect_tospark():
    try:
      return SparkSession.builder   \
                            .master("local") \
                            .appName("DataPipeline") \
                            .getOrCreate()
    except:
        return None
                            

def stop_spark_session(spark_session):
    spark_session.stop()


def load_and_injest_data(file, schema,  table, spark, use_schema = False, enrich = False):
        try:
            
           # load and filter data
           df, last_index = load_data_from_file(file, schema, use_schema, spark)
           
           # clean common problems
           df = clean_column_names(df) 
        
           # Transform the data
           df, error = data_transformation_pipeline(spark, df, file, enrich)

           if error:
              raise Exception(error)
           
           if df:
                res = ingest_to_warehouse(df, table, file, last_index)
                if res['error'] != None:
                    raise Exception(res['error'])     
                  
        except Exception as e:
              raise Exception(e)



def run_pipeline():
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
                     load_and_injest_data(file, deliveries_schema, 'deliveries', spark, True, False)
                
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




