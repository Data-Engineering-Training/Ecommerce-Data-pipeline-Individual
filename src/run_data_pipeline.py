import os
import time
import threading
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


def watch_directory(directory, other_script):
        """
        This function checks a directory for changes (new or moved files).

        Args:
            directory: The path to the directory to check.

        Returns:
            A list of paths to newly added or moved files.
        """
        # Initialize a dictionary to store file presence (avoiding timestamps)
        prev_files = set()

        # Check if directory exists
        if not os.path.isdir(directory):
            print(f"Error: Directory '{directory}' does not exist.")
            return []

        # Get initial listing of files
        prev_files = set(os.listdir(directory))

        # Loop for continuous checking (can be modified for single run)
        while True:
            new_files = []
            current_files = set(os.listdir(directory))

            # Identify changes (added or removed files)
            added_files = current_files - prev_files
            removed_files = prev_files - current_files

            # Since moved files might be removed then added, consider both changes
            new_files.extend(added_files)

            # Report changes (be aware removed files might reappear due to moves)
            if new_files or removed_files:
                print(f"Directory changes:")
                
            if new_files:
                print(f"  New files:")
                for file in new_files:
                    print(f"    {file}")
                    
                # Run the pipeline when new files are added to the directory
                run_pipeline()
                
            if removed_files:
                print(f"  Removed files:")
                for file in removed_files:
                    print(f"    {file}")
            
            # Optionally, add logic to handle new files here

            # Update previous files for next iteration
            prev_files = current_files

            # Sleep for a while before checking again
            # Modify this based on your desired checking frequency
            time.sleep(10)


# Example usage:
if __name__ == "__main__":
    directory = DATA_SETS_PATH
    other_script = BASE_DIR / 'src' / 'data_ingestion_pipeline.py'

    # Check for initial files and run the script if necessary
    if has_files(directory):
        print(f"Directory '{directory}' is empty. Running script '{other_script}'...")
        #os.system(f"python {other_script}")  # Execute the other script using python
        # Run the data pipeline
        run_pipeline()
  

    # Start the monitoring in a separate thread for better responsiveness
    watch_thread = threading.Thread(target=watch_directory, args=(directory, other_script))
    watch_thread.start()

    print(f"Monitoring directory '{directory}' for changes...")



