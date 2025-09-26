import sqlite3
import os
import sys
import datetime
import argparse
import re

# ----------------------------------------------------------------------------------------------------------------------
# DATABASE STRUCTURE
# ----------------------------------------------------------------------------------------------------------------------

def create_local_db(conn):
    """
    Creates the local_files, edl_files, edl_records, and edl_metadata tables
    in the provided database connection.
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
        CREATE TABLE IF NOT EXISTS edl_files (
            edl_id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL UNIQUE,
            ingested_at DATETIME NOT NULL
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS edl_records (
            record_id INTEGER PRIMARY KEY AUTOINCREMENT,
            edl_id INTEGER NOT NULL,
            local_file_id INTEGER NOT NULL,
            start_time_ms REAL NOT NULL,
            length_ms REAL NOT NULL,
            FOREIGN KEY(edl_id) REFERENCES edl_files(edl_id),
            FOREIGN KEY(local_file_id) REFERENCES local_files(local_id),
            UNIQUE(edl_id, local_file_id, start_time_ms, length_ms)
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS edl_metadata (
            edl_id INTEGER PRIMARY KEY,
            style TEXT,
            age_group TEXT,
            rating INTEGER,
            power REAL,
            text1 TEXT,
            text2 TEXT,
            text3 TEXT,
            text4 TEXT,
            text5 TEXT,
            text6 TEXT,
            text7 TEXT,
            text8 TEXT,
            text9 TEXT,
            text10 TEXT,
            FOREIGN KEY(edl_id) REFERENCES edl_files(edl_id)
        );
    """)
    conn.commit()

# ----------------------------------------------------------------------------------------------------------------------
# UTILITY FUNCTIONS
# ----------------------------------------------------------------------------------------------------------------------

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
            # os.path.normpath is crucial for matching
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

# ----------------------------------------------------------------------------------------------------------------------
# COMMANDS
# ----------------------------------------------------------------------------------------------------------------------

def sync_filesystem_with_stash(stash_db_path, local_db_path, filesystem_path, rebuild=False):
    """
    Scans the filesystem, verifies against stash.db, populates a database,
    and handles incremental updates or full rebuilds.
    """
    MEDIA_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', 'webm', '.flv', '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.mp3', '.wav', '.flac', '.aac'}

    # 1. HANDLE REBUILD LOGIC
    if rebuild and os.path.exists(local_db_path):
        print(f"⚠️ Rebuild requested: Deleting existing local DB at '{local_db_path}'...")
        os.remove(local_db_path)
    
    # 2. SETUP DATABASE CONNECTION
    local_conn = sqlite3.connect(local_db_path)
    create_local_db(local_conn)
    local_cursor = local_conn.cursor()

    print("Reading file paths from stash.db...")
    stash_files = get_stash_files(stash_db_path)
    if stash_files is None:
        local_conn.close()
        return

    # 3. FILE SCANNING AND POPULATION LOGIC
    total_files = get_file_count(filesystem_path, MEDIA_EXTENSIONS)
    if total_files == 0:
        print("No media files found to process.")
        local_conn.close()
        return

    missing_log_path = "/tmp/stash_missing_files.log"
    print(f"Scanning filesystem from {filesystem_path}...")
    print(f"Missing files will be logged to {missing_log_path}")
    
    found_count = 0
    missing_count = 0
    processed_count = 0
    
    # New list to track and print new files
    new_files_added = []

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
                        # INSERT OR IGNORE is the core of the incremental update logic
                        local_cursor.execute(
                            """INSERT OR IGNORE INTO local_files (file_path, stash_file_id) 
                            VALUES (?, ?)""",
                            (full_path, stash_id)
                        )
                        
                        # <<< MODIFICATION HERE >>>
                        # sqlite3.cursor.rowcount is > 0 if a row was actually inserted
                        if local_cursor.rowcount > 0:
                            found_count += 1
                            new_files_added.append(full_path) 
                        
                    except sqlite3.Error as e:
                        print(f"\n[DB ERROR] Failed to process {full_path}: {e}")
                else:
                    log_file.write(f"{full_path}\n")
                    missing_count += 1
                
                processed_count += 1
                percentage = (processed_count / total_files) * 100
                sys.stdout.write(f'\rProgress: {percentage:.2f}% ({processed_count}/{total_files} files)')
                sys.stdout.flush()
    
    local_conn.commit()
    local_conn.close()
    
    # 4. REPORTING NEW FILES
    print("\n\n" + "="*50)
    print("Sync complete! 💾")
    
    if new_files_added:
        print(f"\n✅ Found and linked {len(new_files_added)} NEW files:")
        for file_path in new_files_added:
            print(f"  + {file_path}")
    else:
        print("\n✅ No new files were added to the database.")

    print(f"\nFiles missing from stash.db: {missing_count}")
    print(f"Full log of missing files can be found at {missing_log_path}")
    print("="*50)


def ingest_edl_files(local_db_path, edl_root_path):
    """
    Ingests EDL files, parsing records and populating the edl_records table.
    """
    EDL_EXTENSIONS = {'.edl'}
    
    if not os.path.exists(local_db_path):
        print(f"Error: Local database '{local_db_path}' not found. Please run the 'sync' command first.")
        sys.exit(1)

    local_conn = sqlite3.connect(local_db_path)
    
    # Pre-fetch lookup table for local files
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
                        file_contents = edl_file.read().strip()
                        lines = file_contents.splitlines()

                        if not lines:
                            log_file.write(f"[{full_edl_path}] File is empty.\n")
                            continue

                        header_line = lines[0].strip()
                        if header_line != "# mpv EDL v0":
                            log_file.write(f"[{full_edl_path}] Invalid header: '{header_line}'\n")
                            continue

                        # --- EDL File and Metadata Handling (Incremental) ---
                        filename_norm = filename
                        style_name = re.sub(r'_chopped\d-\d$', '', os.path.splitext(filename)[0])
                        
                        # Use INSERT OR IGNORE for the EDL file to prevent duplicates
                        local_cursor.execute(
                            """INSERT OR IGNORE INTO edl_files (filename, ingested_at) 
                            VALUES (?, ?)""",
                            (filename_norm, datetime.datetime.now())
                        )
                        
                        # Get the ID (whether inserted or already existing)
                        local_cursor.execute(
                            "SELECT edl_id FROM edl_files WHERE filename = ?", (filename_norm,)
                        )
                        edl_id = local_cursor.fetchone()[0]

                        # Use INSERT OR IGNORE for metadata
                        local_cursor.execute(
                            """INSERT OR IGNORE INTO edl_metadata (edl_id, style) 
                            VALUES (?, ?)""",
                            (edl_id, style_name)
                        )
                        # --- End EDL File and Metadata Handling ---

                        for line_number, line in enumerate(lines[1:], 2):
                            line = line.strip()
                            if not line or line.startswith('#'):
                                continue
                            
                            match = re.match(r'^(.*),(\d+(?:\.\d+)?),(\d+(?:\.\d+)?)$', line)
                            if match:
                                file_path, start_time_s, length_s = match.groups()
                                normalized_path = os.path.normpath(file_path)
                                
                                if normalized_path in local_file_lookup:
                                    local_id = local_file_lookup[normalized_path]
                                    start_time_ms = float(start_time_s) * 1000
                                    length_ms = float(length_s) * 1000
                                    
                                    try:
                                        # INSERT OR IGNORE is the core of the incremental update logic here too
                                        local_cursor.execute(
                                            """INSERT OR IGNORE INTO edl_records (edl_id, local_file_id, start_time_ms, length_ms) 
                                            VALUES (?, ?, ?, ?)""",
                                            (edl_id, local_id, start_time_ms, length_ms)
                                        )
                                        if local_cursor.rowcount > 0:
                                            records_added_count += 1
                                    except sqlite3.Error as e:
                                        log_file.write(f"\n[DB ERROR] Failed to insert record in {filename_norm} line {line_number}: {e}")
                                else:
                                    log_file.write(f"[{full_edl_path}:{line_number}] File path not found in local DB: {normalized_path}\n")
                            else:
                                log_file.write(f"[{full_edl_path}:{line_number}] Unparsed line: {line}\n")

                except Exception as e:
                    log_file.write(f"Error processing EDL file {full_edl_path}: {e}\n")
                
                edl_files_processed += 1
                percentage = (edl_files_processed / total_edl_files) * 100
                sys.stdout.write(f'\rProgress: {percentage:.2f}% ({edl_files_processed}/{total_edl_files} files)')
                sys.stdout.flush()

    local_conn.commit()
    local_conn.close()
    
    print("\n\nEDL Ingestion complete! 🚀")
    print(f"Added {records_added_count} unique EDL records.")
    print(f"Full log of errors can be found at {ingestion_log_path}")


# ----------------------------------------------------------------------------------------------------------------------
# MAIN EXECUTION
# ----------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A utility for syncing a filesystem with a Stash database and ingesting EDL files."
    )
    subparsers = parser.add_subparsers(dest='command', help='Available commands', required=True)

    # --- SYNC COMMAND ---
    sync_parser = subparsers.add_parser('sync', help='Scan a filesystem and sync with Stash.')
    sync_parser.add_argument('filesystem_root', help="The root directory of the media library to scan.")
    sync_parser.add_argument(
        '--rebuild', 
        action='store_true', 
        help="Completely delete and rebuild the local database file before syncing. Use only when necessary."
    )
    
    # --- INGEST COMMAND ---
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
        sync_filesystem_with_stash(stash_db_path, local_db_path, args.filesystem_root, args.rebuild)
    elif args.command == 'ingest':
        if not os.path.isdir(args.edl_root):
            print(f"Error: EDL root '{args.edl_root}' is not a valid directory.")
            sys.exit(1)
        ingest_edl_files(local_db_path, args.edl_root)