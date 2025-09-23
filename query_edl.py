import sqlite3
import os
import sys
import argparse
import datetime
import re

def get_stash_scenes_by_tag(conn, tag_names):
    """Queries stash.db for scene IDs associated with a list of tag names."""
    cursor = conn.cursor()
    where_clauses = [f"T1.name LIKE ?" for _ in tag_names]
    where_clause = " OR ".join(where_clauses)
    
    params = [f"%{name}%" for name in tag_names]

    query = f"""
    SELECT T2.scene_id
    FROM tags T1
    JOIN scenes_tags T2 ON T1.id = T2.tag_id
    WHERE {where_clause};
    """
    cursor.execute(query, params)
    return [row[0] for row in cursor.fetchall()]

def get_stash_scenes_by_performer(conn, performer_names):
    """Queries stash.db for scene IDs associated with a list of performer names."""
    cursor = conn.cursor()
    where_clauses = [f"T1.name LIKE ?" for _ in performer_names]
    where_clause = " OR ".join(where_clauses)
    
    params = [f"%{name}%" for name in performer_names]

    query = f"""
    SELECT T2.scene_id
    FROM performers T1
    JOIN performers_scenes T2 ON T1.id = T2.performer_id
    WHERE {where_clause};
    """
    cursor.execute(query, params)
    return [row[0] for row in cursor.fetchall()]

def get_stash_scenes_by_studio(conn, studio_names):
    """Queries stash.db for scene IDs associated with a list of studio names."""
    cursor = conn.cursor()
    where_clauses = [f"T1.name LIKE ?" for _ in studio_names]
    where_clause = " OR ".join(where_clauses)
    
    params = [f"%{name}%" for name in studio_names]

    query = f"""
    SELECT T2.id
    FROM studios T1
    JOIN scenes T2 ON T1.id = T2.studio_id
    WHERE {where_clause};
    """
    cursor.execute(query, params)
    return [row[0] for row in cursor.fetchall()]

def get_stash_file_id_by_scene_id(conn, scene_id):
    """Queries stash.db for file IDs associated with a given scene ID."""
    cursor = conn.cursor()
    query = """
    SELECT file_id
    FROM scenes_files
    WHERE scene_id = ?;
    """
    cursor.execute(query, (scene_id,))
    return [row[0] for row in cursor.fetchall()]

def generate_edl_by_stash(stash_db_path, local_db_path, query_type, query_values, limit, output_file):
    """
    Queries stash.db for scenes based on metadata and generates an EDL.
    """
    try:
        stash_conn = sqlite3.connect(stash_db_path)
        local_conn = sqlite3.connect(local_db_path)
    except sqlite3.Error as e:
        print(f"Error connecting to databases: {e}")
        return

    # 1. Get Scene IDs from Stash DB based on the query type
    scene_ids = set()
    if query_type == 'tag':
        scene_ids.update(get_stash_scenes_by_tag(stash_conn, query_values))
    elif query_type == 'performer':
        scene_ids.update(get_stash_scenes_by_performer(stash_conn, query_values))
    elif query_type == 'studio':
        scene_ids.update(get_stash_scenes_by_studio(stash_conn, query_values))

    if not scene_ids:
        print(f"No scenes found for {query_type} '{query_values}'.")
        stash_conn.close()
        local_conn.close()
        return

    # 2. Get Stash File IDs from the Scene IDs
    stash_file_ids = set()
    for scene_id in scene_ids:
        file_ids = get_stash_file_id_by_scene_id(stash_conn, scene_id)
        stash_file_ids.update(file_ids)

    if not stash_file_ids:
        print(f"No video files found for the selected scenes.")
        stash_conn.close()
        local_conn.close()
        return

    # 3. Get EDL records from our local DB
    local_cursor = local_conn.cursor()
    
    stash_file_ids_tuple = tuple(stash_file_ids)
    
    query = f"""
    SELECT T2.file_path, T1.start_time_ms / 1000.0, T1.length_ms / 1000.0
    FROM edl_records T1
    JOIN local_files T2 ON T1.local_file_id = T2.local_id
    WHERE T2.stash_file_id IN ({','.join(['?'] * len(stash_file_ids_tuple))})
    ORDER BY RANDOM()
    LIMIT {limit};
    """
    local_cursor.execute(query, stash_file_ids_tuple)
    
    edl_records = local_cursor.fetchall()
    
    stash_conn.close()
    local_conn.close()

    if not edl_records:
        print(f"No EDL records found linked to the selected Stash metadata.")
        return

    # 4. Generate the EDL output
    with open(output_file, 'w') as f:
        f.write("# mpv EDL v0\n")
        for file_path, start_time, length in edl_records:
            f.write(f"{file_path},{start_time},{length}\n")
    
    print(f"EDL generated and saved to {output_file}")


def generate_edl_by_edl_filename(local_db_path, edl_filenames, limit, output_file):
    """
    Queries sync.db for EDL records based on a list of filenames and generates an EDL.
    """
    try:
        local_conn = sqlite3.connect(local_db_path)
        local_cursor = local_conn.cursor()
    except sqlite3.Error as e:
        print(f"Error connecting to local database: {e}")
        return

    filename_patterns = []
    for filename in edl_filenames:
        if not filename.lower().endswith('.edl'):
            filename_patterns.append(f'%{filename}%.edl%')
        else:
            filename_patterns.append(f'%{filename}%')
            
    where_clauses = ['T2.filename LIKE ?'] * len(filename_patterns)
    where_clause = ' OR '.join(where_clauses)

    query = f"""
    SELECT T3.file_path, T1.start_time_ms / 1000.0, T1.length_ms / 1000.0
    FROM edl_records T1
    JOIN edl_files T2 ON T1.edl_id = T2.edl_id
    JOIN local_files T3 ON T1.local_file_id = T3.local_id
    WHERE {where_clause}
    ORDER BY RANDOM()
    LIMIT {limit};
    """
    local_cursor.execute(query, filename_patterns)
    edl_records = local_cursor.fetchall()
    local_conn.close()

    if not edl_records:
        print(f"No EDL records found for filenames '{edl_filenames}'.")
        return

    with open(output_file, 'w') as f:
        f.write("# mpv EDL v0\n")
        for file_path, start_time, length in edl_records:
            f.write(f"{file_path},{start_time},{length}\n")

    print(f"EDL generated and saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A utility to query Stash and Sync databases to generate EDL files based on various criteria.",
        epilog="""
        Examples:
        
        # Get up to 400 random clips related to the 'gym' or 'training' tags.
        python query_edl.py by_stash --tag gym training
        
        # Get up to 1000 random clips related to the performer 'Ben'.
        python query_edl.py by_stash --performer "Ben Dover" --limit 1000
        
        # Get up to 400 random clips from EDL files with 'athletics' or 'running' in their name.
        python query_edl.py by_edl --filename athletics running
        
        # Generate an EDL from files named 'sports.edl' and 'sports_chopped1-6.edl'
        # and save it to a specific file.
        python query_edl.py by_edl --filename sports --output my_sports_list.edl
        
        # All output files are saved to the directory specified by the QEO environment variable.
        """
    )
    subparsers = parser.add_subparsers(dest='mode', help='Available commands', required=True)

    # Subparser for the 'by_stash' mode
    stash_parser = subparsers.add_parser('by_stash', help='Query based on Stash metadata (tags, performers, studios).')
    group = stash_parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--tag', nargs='+', help="Filter by Stash tag names (partial matching).")
    group.add_argument('--performer', nargs='+', help="Filter by Stash performer names (partial matching).")
    group.add_argument('--studio', nargs='+', help="Filter by Stash studio names (partial matching).")
    stash_parser.add_argument('--limit', type=int, default=400, help="Maximum number of records to return (default: 400).")
    stash_parser.add_argument('--output', help="Output filename for the EDL. Overwrites existing file.")

    # Subparser for the 'by_edl' mode
    edl_parser = subparsers.add_parser('by_edl', help='Query based on EDL filename.')
    edl_parser.add_argument('--filename', nargs='+', required=True, help="Filter by one or more EDL filenames (partial matching).")
    edl_parser.add_argument('--limit', type=int, default=400, help="Maximum number of records to return (default: 400).")
    edl_parser.add_argument('--output', help="Output filename for the EDL. Overwrites existing file.")

    args = parser.parse_args()

    stash_db_path = os.getenv("STASH_DB_PATH")
    local_db_path = os.getenv("SYNC_DB_PATH")
    output_dir = os.getenv("QEO")
    
    if not stash_db_path or not local_db_path:
        print("Error: Both STASH_DB_PATH and SYNC_DB_PATH environment variables must be set.")
        sys.exit(1)

    # Generate output path based on arguments
    output_file_path = args.output
    if not output_file_path:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        if args.mode == 'by_stash':
            if args.tag:
                query_words = "_".join(args.tag)
                output_filename = f"by_stash_tag_{query_words}_{timestamp}.edl"
            elif args.performer:
                query_words = "_".join(args.performer)
                output_filename = f"by_stash_performer_{query_words}_{timestamp}.edl"
            elif args.studio:
                query_words = "_".join(args.studio)
                output_filename = f"by_stash_studio_{query_words}_{timestamp}.edl"
        elif args.mode == 'by_edl':
            query_words = "_".join(args.filename)
            output_filename = f"by_edl_filename_{query_words}_{timestamp}.edl"
        
        output_filename = re.sub(r'[^a-zA-Z0-9_.]', '', output_filename)
        
        if output_dir:
            output_file_path = os.path.join(output_dir, output_filename)
        else:
            print("Warning: QEO environment variable is not set. Saving output to the current directory.")
            output_file_path = output_filename
    
    else: # If --output is supplied, prepend the QEO directory if it exists
        if output_dir:
            output_file_path = os.path.join(output_dir, output_file_path)

    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)


    if args.mode == 'by_stash':
        if args.tag:
            generate_edl_by_stash(stash_db_path, local_db_path, 'tag', args.tag, args.limit, output_file_path)
        elif args.performer:
            generate_edl_by_stash(stash_db_path, local_db_path, 'performer', args.performer, args.limit, output_file_path)
        elif args.studio:
            generate_edl_by_stash(stash_db_path, local_db_path, 'studio', args.studio, args.limit, output_file_path)
    
    elif args.mode == 'by_edl':
        generate_edl_by_edl_filename(local_db_path, args.filename, args.limit, output_file_path)