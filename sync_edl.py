import sqlite3
import os
import sys
import datetime
import argparse
import re

def create_local_db(conn):
    """
    Creates the local_files and edl_records tables in the provided database connection.
    """
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS local_files (
            local_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT NOT NULL UNIQUE,
            stash_file_id INTEGER NOT NULL
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS edl_records (
            record_id INTEGER PRIMARY KEY AUTOINCREMENT,
            local_file_id INTEGER NOT NULL,
            start_time_ms REAL NOT NULL,
            length_ms REAL NOT NULL,
            FOREIGN KEY(local_file_id) REFERENCES local_files(local_id),
            UNIQUE(local_file_id, start_time_ms, length_ms)
        );
    """)
    conn.commit()

def get_stash_files(stash_db_path):
    """
    Reads all file paths and IDs from the stash.db and returns them as a dictionary.
    """
    stash_files = {}
    try:
        conn = sqlite3.connect(stash_db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT f.id, fl.path || '/' || f.basename
            FROM files f
            JOIN folders fl ON f.parent_folder_id = fl.id;
        """)
        rows = cursor.fetchall()
        for stash_id, full_path in rows:
            stash_files[os.path.normpath(full_path)] = stash_id
        conn.close()
    except sqlite3.Error as e:
        print(f"Error reading Stash DB: {e}")
        return None
    return stash_files

def get_file_count(filesystem_path, extensions):
    """
    Counts the number of files with specified extensions in the directory tree.
    """
    count = 0
    for dirpath, dirnames, filenames in os.walk(filesystem_path):
        for filename in filenames:
            if filename.lower().endswith(tuple(extensions)):
                count += 1
    return count

def sync_filesystem_with_stash(stash_db_path, local_db_path, filesystem_path):
    """
    Scans the filesystem, verifies against stash.db, populates an in-memory
    database, and then saves it to a persistent file.
    """
    MEDIA_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.mp3', '.wav', '.flac', '.aac'}

    print("Reading file paths from stash.db...")
    stash_files = get_stash_files(stash_db_path)
    if stash_files is None:
        return

    total_files = get_file_count(filesystem_path, MEDIA_EXTENSIONS)
    if total_files == 0:
        print("No media files found to process.")
        return

    # Connect to an in-memory database for fast operations
    local_conn = sqlite3.connect(':memory:')
    create_local_db(local_conn) # Pass the connection object to the creation function

    local_cursor = local_conn.cursor()

    missing_log_path = "/tmp/stash_missing_files.log"
    print(f"Scanning filesystem from {filesystem_path}...")
    print(f"Missing files will be logged to {missing_log_path}")
    
    found_count = 0
    missing_count = 0
    processed_count = 0

    with open(missing_log_path, 'w') as log_file:
        log_file.write(f"--- Missing Files Report - {datetime.datetime.now()} ---\n\n")

        for dirpath, dirnames, filenames in os.walk(filesystem_path):
            for filename in filenames:
                if not filename.lower().endswith(tuple(MEDIA_EXTENSIONS)):
                    continue
                
                full_path = os.path.normpath(os.path.join(dirpath, filename))
                
                if full_path in stash_files:
                    stash_id = stash_files[full_path]
                    try:
                        local_cursor.execute(
                            "INSERT INTO local_files (file_path, stash_file_id) VALUES (?, ?)",
                            (full_path, stash_id)
                        )
                        found_count += 1
                    except sqlite3.IntegrityError:
                        pass
                else:
                    log_file.write(f"{full_path}\n")
                    missing_count += 1
                
                processed_count += 1
                percentage = (processed_count / total_files) * 100
                sys.stdout.write(f'\rProgress: {percentage:.2f}% ({processed_count}/{total_files} files)')
                sys.stdout.flush()
    
    local_conn.commit()

    # Backup the in-memory database to a persistent file
    print(f"\n\nSaving in-memory database to {local_db_path}...")
    final_conn = sqlite3.connect(local_db_path)
    local_conn.backup(final_conn)
    final_conn.close()
    local_conn.close()
    
    print("\nSync complete!")
    print(f"Found and linked {found_count} files.")
    print(f"Files missing from stash.db: {missing_count}")
    print(f"Full log of missing files can be found at {missing_log_path}")

def ingest_edl_files(local_db_path, edl_root_path):
    """
    Ingests EDL files, parsing records and populating the edl_records table.
    """
    EDL_EXTENSIONS = {'.edl'}
    
    if not os.path.exists(local_db_path):
        print(f"Error: Local database '{local_db_path}' not found. Please run the 'sync' command first.")
        sys.exit(1)

    local_conn = sqlite3.connect(local_db_path)
    
    local_file_lookup = {}
    local_cursor = local_conn.cursor()
    local_cursor.execute("SELECT local_id, file_path FROM local_files;")
    for local_id, file_path in local_cursor.fetchall():
        local_file_lookup[os.path.normpath(file_path)] = local_id

    total_edl_files = get_file_count(edl_root_path, EDL_EXTENSIONS)
    if total_edl_files == 0:
        print("No EDL files found to ingest.")
        return

    ingestion_log_path = "/tmp/edl_ingestion_errors.log"
    print(f"Starting EDL ingestion from {edl_root_path}...")
    print(f"Errors and unparsed lines will be logged to {ingestion_log_path}")
    
    records_added_count = 0
    edl_files_processed = 0

    with open(ingestion_log_path, 'w') as log_file:
        log_file.write(f"--- EDL Ingestion Error Report - {datetime.datetime.now()} ---\n\n")

        for dirpath, dirnames, filenames in os.walk(edl_root_path):
            for filename in filenames:
                if not filename.lower().endswith(tuple(EDL_EXTENSIONS)):
                    continue

                full_edl_path = os.path.join(dirpath, filename)
                try:
                    with open(full_edl_path, 'r', encoding='utf-8') as edl_file:
                        # Check for the required header
                        header_line = edl_file.readline().strip()
                        if header_line != "# mpv EDL v0":
                            log_file.write(f"[{full_edl_path}] Invalid header: {header_line}\n")
                            edl_files_processed += 1
                            continue
                        
                        for line_number, line in enumerate(edl_file, 2):
                            # New regex to handle comma separation
                            match = re.match(r'^(.*),(\d+(?:\.\d+)?),(\d+(?:\.\d+)?)$', line.strip())
                            if match:
                                file_path, start_time_s, length_s = match.groups()
                                normalized_path = os.path.normpath(file_path)
                                
                                if normalized_path in local_file_lookup:
                                    local_id = local_file_lookup[normalized_path]
                                    start_time_ms = float(start_time_s) * 1000
                                    length_ms = float(length_s) * 1000
                                    
                                    try:
                                        local_cursor.execute(
                                            "INSERT INTO edl_records (local_file_id, start_time_ms, length_ms) VALUES (?, ?, ?)",
                                            (local_id, start_time_ms, length_ms)
                                        )
                                        records_added_count += 1
                                    except sqlite3.IntegrityError:
                                        pass
                                else:
                                    log_file.write(f"[{full_edl_path}:{line_number}] File path not found in local DB: {normalized_path}\n")
                            else:
                                if line.strip():
                                    log_file.write(f"[{full_edl_path}:{line_number}] Unparsed line: {line.strip()}\n")

                except Exception as e:
                    log_file.write(f"Error processing EDL file {full_edl_path}: {e}\n")
                
                edl_files_processed += 1
                percentage = (edl_files_processed / total_edl_files) * 100
                sys.stdout.write(f'\rProgress: {percentage:.2f}% ({edl_files_processed}/{total_edl_files} files)')
                sys.stdout.flush()

    local_conn.commit()
    local_conn.close()
    
    print("\n\nEDL Ingestion complete!")
    print(f"Added {records_added_count} unique EDL records.")
    print(f"Full log of errors can be found at {ingestion_log_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A utility for syncing a filesystem with a Stash database and ingesting EDL files."
    )
    subparsers = parser.add_subparsers(dest='command', help='Available commands', required=True)

    sync_parser = subparsers.add_parser('sync', help='Scan a filesystem and sync with Stash.')
    sync_parser.add_argument('filesystem_root', help="The root directory of the media library to scan.")
    
    ingest_parser = subparsers.add_parser('ingest', help='Ingest EDL files from a directory.')
    ingest_parser.add_argument('edl_root', help="The root directory containing EDL files.")
    
    args = parser.parse_args()

    stash_db_path = os.getenv("STASH_DB_PATH")
    local_db_path = os.getenv("SYNC_DB_PATH", "file_sys_stash.db")

    if not stash_db_path:
        print("Error: STASH_DB_PATH environment variable not set.")
        sys.exit(1)

    if args.command == 'sync':
        if not os.path.isdir(args.filesystem_root):
            print(f"Error: Filesystem root '{args.filesystem_root}' is not a valid directory.")
            sys.exit(1)
        sync_filesystem_with_stash(stash_db_path, local_db_path, args.filesystem_root)
    elif args.command == 'ingest':
        if not os.path.isdir(args.edl_root):
            print(f"Error: EDL root '{args.edl_root}' is not a valid directory.")
            sys.exit(1)
        ingest_edl_files(local_db_path, args.edl_root)