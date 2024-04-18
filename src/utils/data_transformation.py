from utils.data_loaders import load_from_db
from pyspark.sql.functions import lit
import config
import re



def add_market_column(dataframe, new_column_name, expression):
    market_number = int(extract_market_number(expression))
    return dataframe.withColumn(new_column_name, lit(market_number))


# Enrich data during loading

def enrich_tables_with_customer_data(df, spark, join_on='customer_id'):
    try:
        if join_on == 'order_id':
            # load orders data, enriched with user data
            orders_df, error = load_from_db(spark, config.jdbc_url, 'orders', config.DB_USER, config.DB_PASSWORD)
            
            # Join DataFrames on the common customer_id column
            enriched_df = df.join(orders_df, on="order_id", how="left")

            # Select desired columns from customers for enrichment
            enriched_df = enriched_df.select(
                df["*"], orders_df["created_at"], orders_df["last_used_platform"], orders_df["loyalty_points"], orders_df["is_blocked"]
            )
            return enriched_df, None
        
        
        
        # load customer_df 
        customers_df, error = load_from_db(spark, config.jdbc_url, 'customers', config.DB_USER, config.DB_PASSWORD)
    
        if error:
          return df, error
    
        
        # Join DataFrames on the common customer_id column
        enriched_df = df.join(customers_df, on="customer_id", how="left")

        # Select desired columns from customers for enrichment
        enriched_df = enriched_df.select(
        df["*"], customers_df["created_at"], customers_df["last_used_platform"], customers_df["loyalty_points"], customers_df["is_blocked"]
            )
        return enriched_df, None
        
    
    except Exception as e:
        return df, e
        



# Create a pipeline to transform the data
def data_transformation_pipeline(spark, df, path, enrich, join_on):
    # Add the market column
    df = add_market_column(df, 'market', path)
    
    # Enrich data with customer data
    if enrich:
        df, error = enrich_tables_with_customer_data(df, spark, join_on)
        if error:
            return df, error
        
    return df, None
    
    

# Utils
def extract_market_number(string):
    # Define a regular expression pattern to find the number
    pattern = r"\b(\d+)\b"  # Matches any sequence of digits
    # Search for all occurrences of the pattern in the string
    matches = re.findall(pattern, string)
    # If matches are found, return the first occurrence
    if matches:
        return matches[0]
    else:
        return None  # If no match is found, return None
    