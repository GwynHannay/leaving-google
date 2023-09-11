import os
from helpers import files, metadata, setup, sqlitedb, utils


photos_dir = setup.get_from_config("google_location")
edits_dir = setup.get_from_config("edited_location")
mp_dir = setup.get_from_config("mp_location")


def check_db():
    db_file = "".join([setup.get_from_config("db_name"), ".db"])
    if not os.path.exists(db_file):
        print("Creating SQLite database and tables")

        conn = sqlitedb.start_query()
        sqlitedb.create_db(conn)
        sqlitedb.end_query(conn)


def index_takeout_files():
    print("Indexing list of takeout files")
    zip_files = [file for file in files.get_files(photos_dir, [".zip"])]

    db_records = list()
    for zippy in zip_files:
        db_records.append((zippy.name, zippy.path, zippy.stat().st_size))

    conn = sqlitedb.start_query()
    sqlitedb.insert_many_records("add_takeout_files", db_records, conn)
    sqlitedb.end_query(conn)


def index_raw_files():
    index_folders()
    db_records = list()
    conn = sqlitedb.start_query()
    folders = sqlitedb.get_all_results("get_unlisted_folders", conn)
    sqlitedb.end_query(conn)

    insert_sql_file = "add_filelist"
    for id, folder in folders:
        actual_files = [
            file for file in files.get_files(folder, None) if file.is_file()
        ]
        for file in actual_files:
            filename, ext = os.path.splitext(file.name)
            db_records.append((id, filename, ext, file.stat().st_size))
            db_records = utils.prepare_batch(insert_sql_file, db_records, False)
    if len(db_records) > 0:
        utils.prepare_batch(insert_sql_file, db_records, True)


def index_xmp_files():
    conn = sqlitedb.start_query()
    folders = sqlitedb.get_all_results("get_all_folders", conn)
    sqlitedb.end_query(conn)

    for id, folder in folders:
        db_records = list()
        insert_sql_file = "add_filelist"

        xmp_files = [
            file for file in files.get_files(folder, [".xmp"]) if file.is_file()
        ]
        for xmp in xmp_files:
            filename, ext = os.path.splitext(xmp.name)
            db_records.append((id, filename, ext, 0))
            db_records = utils.prepare_batch(insert_sql_file, db_records, False)
        if len(db_records) > 0:
            utils.prepare_batch(insert_sql_file, db_records, True)


def index_folders():
    db_records = list()
    folders = [dir for dir in files.get_directories(photos_dir) if len(dir[1]) > 0]
    for entry in folders:
        filepaths = [(os.path.join(entry[0], folder),) for folder in entry[1]]
        db_records.extend(filepaths)

    conn = sqlitedb.start_query()
    sqlitedb.insert_many_records("add_folders", db_records, conn)
    sqlitedb.end_query(conn)


def index_filesystem():
    pass


def process_takeout_files():
    conn = sqlitedb.start_query()
    for id, zip_file in sqlitedb.get_all_results("get_takeout_files", conn):
        print(f"On file: {zip_file}")
        files.extract_files(zip_file)
        sqlitedb.execute_query_with_single_val("update_unzipped_status", (id,), conn)
    sqlitedb.end_query(conn)


def process_raw_files():
    print("Moving edited files to new folder")
    process_edited_files()
    print("Moving MP files to new folder")
    process_mp_files()
    print("Clean up moved files in database")
    utils.clean_up_filelist()

    print("Begin matching JSON files to original media")
    conn = sqlitedb.start_query()
    sqlitedb.execute_query("add_json_files", conn)
    sqlitedb.execute_query("add_json_no_ext", conn)
    sqlitedb.end_query(conn)

    utils.clean_up_filelist()
    print("Restructure filenames to find missing JSON matches")
    process_parantheses()
    utils.clean_up_filelist()

    print("Read remaining JSON files to find matches")
    conn = sqlitedb.start_query()
    sqlitedb.execute_query("add_json_cropped", conn)
    sqlitedb.end_query(conn)
    utils.clean_up_filelist()

    print("Creating first XMP files")
    metadata.generate_xmp_files()

    print("Adding XMP files to database")
    index_xmp_files()


def process_metadata_files():
    print("Updating XMP files with JSON metadata")
    metadata.update_xmp_files()

    print("Deleting original XMP files")
    conn = sqlitedb.start_query()
    folders = sqlitedb.get_all_results("get_all_folders", conn)
    sqlitedb.end_query(conn)

    for id, folder in folders:
        old_xmps = [
            file
            for file in files.get_files(folder, [".xmp_original"])
            if file.is_file()
        ]
        files.delete_files(old_xmps)


def process_edited_files():
    conn = sqlitedb.start_query()
    for records in sqlitedb.get_batched_results("get_edits", conn):
        db_records = None
        db_records = list()
        filepaths = None
        filepaths = list()
        for id, old_filepath in records:
            new_filepath = old_filepath.replace(photos_dir, edits_dir)
            filepaths.append((old_filepath, new_filepath))
            db_records.append(id)
        files.move_files(filepaths)
        sqlitedb.execute_query_with_list("add_filelist_edits", db_records, conn)
    sqlitedb.end_query(conn)


def process_mp_files():
    db_records = list()
    conn = sqlitedb.start_query()
    i = 0
    for records in sqlitedb.get_batched_results("get_mp_files", conn):
        filepaths = None
        filepaths = list()
        i = i + 1
        print(f"Moving batch {i}")
        for id, old_filepath in records:
            new_filepath = old_filepath.replace(photos_dir, mp_dir)
            filepaths.append((old_filepath, new_filepath))
            db_records.append(id)
        print(f"Moving files: {filepaths}")
        files.move_files(filepaths)
    sqlitedb.execute_query_with_list("add_filelist_mp", db_records, conn)
    sqlitedb.end_query(conn)


def process_unmatched_json():
    conn = sqlitedb.start_query()
    for records in sqlitedb.get_batched_results("json_no_media", conn):
        db_records = None
        db_records = list()
        for id, filepath in records:
            result = metadata.read_filename(filepath)
            db_records.append((result, id))
        sqlitedb.execute_many_with_many_vals("add_more_json_files", db_records, conn)
    sqlitedb.end_query(conn)


def process_parantheses():
    conn = sqlitedb.start_query()
    for records in sqlitedb.get_batched_results("media_paranthesis", conn):
        db_records = None
        db_records = list()
        for id, folder, filename in records:
            new_filename = utils.restructure_filename(filename)
            db_records.append((id, new_filename, folder))
        sqlitedb.execute_many_with_many_vals("add_matching_json", db_records, conn)
    sqlitedb.end_query(conn)


def process_dates():
    # conn = sqlitedb.start_query()
    # for records in sqlitedb.get_batched_results("get_xmp_files", conn):
    #     db_records = None
    #     db_records = list()
    #     for id, filepath in records:
    #         for sec in metadata.get_rdf_sections(filepath):
    #             db_records.append((id, str(sec)))
    #     sqlitedb.insert_many_records("add_xmp_data", db_records, conn)
    # sqlitedb.end_query(conn)
    metadata.get_tags()
