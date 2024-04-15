from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
import re



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
    

def add_market_column(dataframe, new_column_name, expression):
    market_number = int(extract_market_number(expression))
    return dataframe.withColumn(new_column_name, lit(market_number))