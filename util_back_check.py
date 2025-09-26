import sqlite3
import os
import sys
import argparse

def check_filesystem_integrity(local_db_path):
    """
    Reads all file paths from local_files table in sync_db and checks if
    they exist on the filesystem. Reports and returns missing file details.
    """
    if not os.path.exists(local_db_path):
        print(f"Error: Local database '{local_db_path}' not found. Cannot perform check.")
        return [], []

    conn = sqlite3.connect(local_db_path)
    cursor = conn.cursor()
    
    # Select local_id and file_path from the local_files table
    cursor.execute("SELECT local_id, file_path FROM local_files;")
    all_files = cursor.fetchall()
    conn.close()

    if not all_files:
        print("Success: The local_files table is empty. Nothing to check.")
        return [], []

    missing_files = []
    missing_ids = []
    
    total_count = len(all_files)
    print(f"Starting integrity check on {total_count} records in the database...")

    for i, (local_id, file_path) in enumerate(all_files):
        # Update progress counter (without newline)
        sys.stdout.write(f'\rProgress: {i + 1}/{total_count} files checked.')
        sys.stdout.flush()

        # Check if the file exists on the filesystem
        if not os.path.exists(file_path):
            missing_files.append(file_path)
            missing_ids.append(local_id)

    # Print a newline after the progress counter is done
    sys.stdout.write('\n')
    
    return missing_files, missing_ids


def delete_stale_records(local_db_path, missing_ids):
    """
    Deletes records from local_files and associated records in edl_records
    for files that no longer exist on the filesystem.
    """
    if not missing_ids:
        print("No records to delete.")
        return

    conn = sqlite3.connect(local_db_path)
    cursor = conn.cursor()
    
    # Convert list of IDs to a tuple for SQL IN clause
    id_tuple = tuple(missing_ids)
    
    # 1. Delete associated EDL records first (to satisfy foreign key constraints)
    edl_delete_query = f"DELETE FROM edl_records WHERE local_file_id IN ({','.join(['?'] * len(id_tuple))})"
    cursor.execute(edl_delete_query, id_tuple)
    edl_records_deleted = cursor.rowcount

    # 2. Delete the primary local_files records
    local_delete_query = f"DELETE FROM local_files WHERE local_id IN ({','.join(['?'] * len(id_tuple))})"
    cursor.execute(local_delete_query, id_tuple)
    local_records_deleted = cursor.rowcount
    
    conn.commit()
    conn.close()

    print("\nCleanup Pass Complete! 🎉")
    print(f"-> Deleted {local_records_deleted} records from 'local_files'.")
    print(f"-> Deleted {edl_records_deleted} records from 'edl_records' (associated EDL cuts).")


def main():
    parser = argparse.ArgumentParser(
        description="Utility to check the integrity of the sync database against the filesystem."
    )
    # No arguments needed for this utility, it relies entirely on the environment variable
    args = parser.parse_args()

    local_db_path = os.getenv("SYNC_DB_PATH", "file_sys_stash.db")

    if not os.getenv("SYNC_DB_PATH"):
        print("Error: SYNC_DB_PATH environment variable not set.")
        sys.exit(1)

    # --- PHASE 1: CHECK AND REPORT ---
    print("\n--- PHASE 1: FILESYSTEM INTEGRITY CHECK ---")
    missing_files, missing_ids = check_filesystem_integrity(local_db_path)
    print("\n" + "="*70)

    if not missing_files:
        print("SUCCESS: All files listed in the database were found on the filesystem.")
        print("Integrity check passed. Exiting.")
        sys.exit(0)

    # --- PHASE 2: REPORT FINDINGS ---
    print(f"🚨 ALERT: Found {len(missing_files)} file(s) in the database that are missing from the filesystem:")
    for file_path in missing_files:
        print(f"  - {file_path}")
    print("="*70)

    # --- PHASE 3: PROMPT FOR CLEANUP ---
    while True:
        prompt = "\nDo you want to DELETE these stale records from the database? (yes/no): "
        user_input = input(prompt).strip().lower()

        if user_input in ('yes', 'y'):
            print("\n--- PHASE 3: DATABASE CLEANUP ---")
            delete_stale_records(local_db_path, missing_ids)
            break
        elif user_input in ('no', 'n'):
            print("Cleanup cancelled. Stale records remain in the database. Exiting.")
            break
        else:
            print("Invalid input. Please enter 'yes' or 'no'.")

if __name__ == "__main__":
    main()