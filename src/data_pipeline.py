import os
import time
import threading
from pathlib import Path
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

    for _ in os.listdir(directory):
        return True
    return False

def get_last_modified_times(directory):
    """
    Gets the last modified times of all files in a directory (recursively).

    Args:
        directory (str): The directory path to monitor.

    Returns:
        dict: A dictionary mapping file paths to their last modified times.
    """

    file_times = {}
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                file_times[file_path] = os.path.getmtime(file_path)
            except OSError:
                pass  # Ignore errors (e.g., permission issues)

    return file_times

def watch_directory(directory, other_script):
    """
    Watches a directory for changes and runs another script when changes are detected.

    Args:
        directory (str): The directory path to monitor.
        other_script (str): The path to the script to run upon changes.
    """

    previous_file_times = get_last_modified_times(directory)

    while True:
        current_file_times = get_last_modified_times(directory)

        # Check for initial files or changes (created, modified, or deleted files)
        if not has_files(directory) and len(previous_file_times) == 0:
            print(f"Directory '{directory}' is empty. Running script '{other_script}'...")
            os.system(f"python {other_script}")  # Execute the other script using python
        else:
            changes_detected = False
            for file_path, current_time in current_file_times.items():
                if file_path not in previous_file_times or current_time != previous_file_times[file_path]:
                    changes_detected = True
                    break

            # Run the other script if changes were detected
            if changes_detected:
                print(f"Changes detected in '{directory}!' Running script '{other_script}'...")
                os.system(f"python {other_script}")  # Execute the other script using python

        previous_file_times = current_file_times.copy()
        time.sleep(1)  # Adjust polling interval as needed


# Example usage:
if __name__ == "__main__":
    directory = DATA_SETS_PATH
    other_script = BASE_DIR / 'src' / 'test.py' #'data_ingestion_pipeline.py'

    # Check for initial files and run the script if necessary
    if not has_files(directory):
        print(f"Directory '{directory}' is empty. Running script '{other_script}'...")
        os.system(f"python {other_script}")  # Execute the other script using python

    # Start the monitoring in a separate thread for better responsiveness
    watch_thread = threading.Thread(target=watch_directory, args=(directory, other_script))
    watch_thread.start()

    print(f"Monitoring directory '{directory}' for changes...")


