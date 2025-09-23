import sqlite3
import os
import sys
import datetime
import argparse

def create_local_db(db_path):
    """
    Creates the local_files table in the new database if it doesn't exist.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS local_files (
            local_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT NOT NULL UNIQUE,
            stash_file_id INTEGER NOT NULL
        );
    """)
    conn.commit()
    conn.close()

def get_stash_files(stash_db_path):
    """
    Reads all file paths and IDs from the stash.db and returns them as a dictionary.
    This is an in-memory lookup table to avoid multiple database queries.
    """
    stash_files = {}
    try:
        conn = sqlite3.connect(stash_db_path)
        cursor = conn.cursor()

        # Join files and folders to get the full path
        cursor.execute("""
            SELECT f.id, fl.path || '/' || f.basename
            FROM files f
            JOIN folders fl ON f.parent_folder_id = fl.id;
        """)
        
        rows = cursor.fetchall()
        for stash_id, full_path in rows:
            # Normalize the path to match the filesystem's format
            stash_files[os.path.normpath(full_path)] = stash_id
        
        conn.close()
    except sqlite3.Error as e:
        print(f"Error reading Stash DB: {e}")
        return None
    return stash_files

def get_file_count(filesystem_path):
    """
    Counts the number of media files in the specified directory tree.
    """
    count = 0
    MEDIA_EXTENSIONS = {
        '.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv',
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff',
        '.mp3', '.wav', '.flac', '.aac'
    }
    for dirpath, dirnames, filenames in os.walk(filesystem_path):
        for filename in filenames:
            if filename.lower().endswith(tuple(MEDIA_EXTENSIONS)):
                count += 1
    return count

def sync_filesystem_with_stash(stash_db_path, local_db_path, filesystem_path):
    """
    Scans the filesystem, verifies against stash.db, and populates the local database.
    Logs missing files to a file in /tmp.
    """
    # Define a set of valid media file extensions for faster lookup
    MEDIA_EXTENSIONS = {
        '.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv',  # Video
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', # Image
        '.mp3', '.wav', '.flac', '.aac'                 # Audio
    }

    create_local_db(local_db_path)
    
    print("Reading file paths from stash.db...")
    stash_files = get_stash_files(stash_db_path)
    if stash_files is None:
        return

    # Count the total number of files to process for the progress indicator
    total_files = get_file_count(filesystem_path)
    if total_files == 0:
        print("No media files found to process.")
        return

    # Connect to the local database
    local_conn = sqlite3.connect(local_db_path)
    local_cursor = local_conn.cursor()

    log_file_path = "/tmp/stash_missing_files.log"
    print(f"Scanning filesystem from {filesystem_path}...")
    print(f"Missing files will be logged to {log_file_path}")
    
    found_count = 0
    missing_count = 0
    processed_count = 0

    # Open the log file in write mode, overwriting it each time
    with open(log_file_path, 'w') as log_file:
        log_file.write(f"--- Missing Files Report - {datetime.datetime.now()} ---\n\n")

        # Walk the filesystem to find files
        for dirpath, dirnames, filenames in os.walk(filesystem_path):
            for filename in filenames:
                # Use the refined file extension check
                if not filename.lower().endswith(tuple(MEDIA_EXTENSIONS)):
                    continue
                
                full_path = os.path.normpath(os.path.join(dirpath, filename))
                
                # Check if this file exists in our in-memory Stash lookup table
                if full_path in stash_files:
                    stash_id = stash_files[full_path]
                    
                    # Insert the record into the local_files table
                    try:
                        local_cursor.execute(
                            "INSERT INTO local_files (file_path, stash_file_id) VALUES (?, ?)",
                            (full_path, stash_id)
                        )
                        found_count += 1
                    except sqlite3.IntegrityError:
                        # This file is already in our local database
                        pass
                else:
                    # Write the missing file path to the log file
                    log_file.write(f"{full_path}\n")
                    missing_count += 1
                
                processed_count += 1
                percentage = (processed_count / total_files) * 100
                sys.stdout.write(f'\rProgress: {percentage:.2f}% ({processed_count}/{total_files} files)')
                sys.stdout.flush()
    
    local_conn.commit()
    local_conn.close()
    
    print("\n\nSync complete!")
    print(f"Found and linked {found_count} files.")
    print(f"Files missing from stash.db: {missing_count}")
    print(f"Full log of missing files can be found at {log_file_path}")

if __name__ == "__main__":
    # Get DB paths from environment variables
    stash_db_path = os.getenv("STASH_DB_PATH")
    local_db_path = os.getenv("SYNC_DB_PATH", "file_sys_stash.db") # Default value if not set

    if not stash_db_path:
        print("Error: STASH_DB_PATH environment variable not set.")
        sys.exit(1)

    # Setup command-line argument parsing for the filesystem root
    parser = argparse.ArgumentParser(
        description="Sync a filesystem with a Stash database."
    )
    parser.add_argument(
        "filesystem_root",
        help="The root directory of the media library to scan."
    )
    args = parser.parse_args()

    if not os.path.isdir(args.filesystem_root):
        print(f"Error: Filesystem root '{args.filesystem_root}' is not a valid directory.")
        sys.exit(1)

    sync_filesystem_with_stash(stash_db_path, local_db_path, args.filesystem_root)