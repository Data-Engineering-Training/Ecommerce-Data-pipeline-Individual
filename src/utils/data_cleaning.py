# Importing necessary modules
from pyspark.sql import functions as F
from pathlib import Path
import csv

# Configure base path to dataset files
BASE_DIR = Path().parent.absolute()
DATA_SETS_PATH = BASE_DIR / 'datasets'

def merge_strings_and_remove(lst):
    """
    Function to merge strings at positions 1 and 2 in a list and remove the element at position 2.

    Parameters:
    lst (list): The list to be processed.

    Returns:
    list: The processed list.
    """
    # Check if the list has at least two elements
    if len(lst) >= 3:
        # Merge strings at positions 1 and 2
        merged_string = lst[1] + '-' + lst[2]
        # Update position 1 with the merged string
        lst[1] = merged_string
        # Remove the element at position 2
        del lst[2]
    return lst

def string_to_csv(input_string, output_file, start):
    """
    Function to convert a string to a CSV file.

    Parameters:
    input_string (str): The string to be converted.
    output_file (str): The path of the output CSV file.
    start (bool): A flag indicating whether it's the start of the file.

    Returns:
    None
    """
    # Split the input string by commas
    items = input_string.split(',')[0:-2]
    
    # File mode
    mode = 'w'
    
    # Merge order id
    if not start:
         items = merge_strings_and_remove(items) # type: ignore
         mode = 'a'
         
    
    # Open the output file in write mode
    with open(output_file, mode, newline=None) as csvfile:
        # Create a CSV writer object
        csv_writer = csv.writer(csvfile)
        
        # Write each item as a row in the CSV file
        csv_writer.writerow(items)

def clean_extra_commas(file_path, clean_file_path):
    """
    Function to clean extra commas in a file.

    Parameters:
    file_path (str): The path of the file to be cleaned.
    clean_file_path (str): The path of the cleaned file.

    Returns:
    None
    """
    start = True
    input_file_path = f'{DATA_SETS_PATH}/{file_path}' 
    with open(input_file_path, 'r') as file:
        headers = file.readline()
        for line in file.readlines():
            
            # Correct comma issues in Notes column
            positions = [i for i in range(len(line)) if line.startswith(',', i)]
            start_position = 5
            end_position = 7
            problem_txt = line[positions[start_position]+1: positions[end_position]]
            corrected_text = line[: positions[start_position]+1] + problem_txt.replace(',', ' ') + line[positions[end_position]:]

            # Correct comma issues in Special_Instructions column
            positions2 = [i for i in range(len(corrected_text)) if line.startswith(',', i)]
            start_position2 = 22
            end_position2 = 24
            problem_txt2 = corrected_text[positions2[start_position2]+1: positions2[end_position2]]
            corrected_text2 = corrected_text[: positions2[start_position2]+1] + problem_txt2.replace(',', ' ') + corrected_text[positions2[end_position2]:]
            
            
            #Write the corrected data to a new file
            output_file_path = f'{DATA_SETS_PATH}/{clean_file_path}' 
            if start: 
                string_to_csv(headers, output_file_path, start=start)
                start = False
                
            string_to_csv(corrected_text2, output_file_path, start=start)

def clean_column_names(df, clean):
    """
    This function cleans column names in a Spark DataFrame by replacing spaces with underscores.

    Args:
        df: The Spark DataFrame to process.

    Returns:
        A new Spark DataFrame with cleaned column names.
    """
    cleaned_cols = [F.col(col).alias(col.lower().replace(' ', '_')) for col in df.columns]
    df = df.select(*cleaned_cols)
    return clean_order_ids(df, clean=clean)

def clean_order_ids(df, clean = False):
    """
    This function cleans a column in a Spark DataFrame and selects the text between the hyphens.

    Args:
        df: The Spark DataFrame containing the data.
        column_name: The name of the column to clean.

    Returns:
        A new Spark DataFrame with the cleaned column.
    """
    try:
       if clean:        
        # Regular expression to capture numbers (adjust if needed)
        number_pattern = r"\d+"

        # Extract numbers using regexp_extract
        extracted_numbers = F.regexp_extract(df['order_id'], number_pattern, 0)

        # Handle potential errors (e.g., missing numbers)
        # You can add logic here (e.g., using coalesce for null handling)

        # Create a new column with the extracted numbers
        cleaned_df = df.withColumn('order_id', extracted_numbers)
        return cleaned_df
       else:
           return df
    except Exception as e:
        return df