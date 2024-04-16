import os
import hashlib
import threading
import time
from pathlib import Path
from collections import defaultdict
from data_ingestion_pipeline import run_pipeline



# Configure base path to dataset files
BASE_DIR = Path().parent.absolute()
DATA_SETS_PATH = BASE_DIR / 'datasets'


def has_files(directory):
    """
    Checks if there are any files in the directory (non-recursively).

    Args:
        directory (str): The directory path to check.

    Returns:
        bool: True if there are files, False otherwise.
    """
    for item in os.listdir(directory):
        item_path = os.path.join(directory, item)
        if os.path.isfile(item_path):
            return True
    return False



def check_modified_files(directory):
  """
  This function checks a directory for modified files.

  Args:
      directory: The path to the directory to check.

  Returns:
      A list of paths to modified files.
  """
  # Initialize a dictionary to store file hashes
  file_hashes = defaultdict(str)

  # Check if directory exists
  if not os.path.isdir(directory):
    print(f"Error: Directory '{directory}' does not exist.")
    return []

  # Get initial listing of files and their hashes
  for filename in os.listdir(directory):
    filepath = os.path.join(directory, filename)
    with open(filepath, 'rb') as f:
      file_hashes[filepath] = hashlib.md5(f.read()).hexdigest()

  # Loop for continuous checking (can be modified for single run)
  while True:
    modified_files = []
    for filename in os.listdir(directory):
      filepath = os.path.join(directory, filename)

      # Check if file exists in previous check
      if filepath not in file_hashes:
        # New file, consider it modified for simplicity
        modified_files.append(filepath)
        continue

      # Open file and calculate new hash
      with open(filepath, 'rb') as f:
        new_hash = hashlib.md5(f.read()).hexdigest()

      # Check if hash has changed (indicating modification)
      if file_hashes[filepath] != new_hash:
        modified_files.append(filepath)
        file_hashes[filepath] = new_hash  # Update hash for future checks

    # Report modified files (if any)
    if modified_files:
      run_pipeline()
      for file in modified_files:
        print(file)
    # Optionally, add logic to handle modified files here

    # Sleep for a while before checking again
    # Modify this based on your desired checking frequency
    time.sleep(30)


# Example usage:
if __name__ == "__main__":
    
    directory_to_watch = DATA_SETS_PATH

    # Check for initial files and run the script if necessary
    if has_files(directory_to_watch):
        # Run the data pipeline
        run_pipeline()
  

    # Start the monitoring in a separate thread for better responsiveness
    watch_thread = threading.Thread(target=check_modified_files, args=[directory_to_watch])
    watch_thread.start()

    print(f"Monitoring directory '{directory_to_watch}' for changes...")
