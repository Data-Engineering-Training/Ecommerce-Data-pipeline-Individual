from pyspark.sql import functions as F
from pathlib import Path
import csv


# Configure base path to dataset files
BASE_DIR = Path().parent.absolute()
DATA_SETS_PATH = BASE_DIR / 'datasets'

def merge_strings_and_remove(lst):
    # Check if the list has at least two elements
    if len(lst) >= 3:
        # Merge strings at positions 1 and 2
        merged_string = lst[1] + lst[2]
        # Update position 1 with the merged string
        lst[1] = merged_string
        # Remove the element at position 2
        del lst[2]
    return lst


def string_to_csv(input_string, output_file, start):
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
            
            # print('"""""""""""""""""""""""""""""""""""""""""""""""')
            # print(corrected_text)
            # print(problem_txt2)
            # print(corrected_text2)
            # print('"""""""""""""""""""""""""""""""""""""""""""""""')
           
def clean_column_names(df):
  """
  This function cleans column names in a Spark DataFrame by replacing spaces with underscores.

  Args:
      df: The Spark DataFrame to process.

  Returns:
      A new Spark DataFrame with cleaned column names.
  """
  cleaned_cols = [F.col(col).alias(col.lower().replace(' ', '_')) for col in df.columns]
  return df.select(*cleaned_cols)