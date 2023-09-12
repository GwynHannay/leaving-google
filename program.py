import os
from helpers import files, metadata, setup, sqlitedb, utils


photos_dir = setup.get_from_settings("google_location")
valid_extensions = setup.get_extensions_list()


def check_db():
    db_file = "".join([setup.get_from_settings("db_name"), ".db"])
    if not os.path.exists(db_file):
        print("Creating SQLite database and tables")
        sqlitedb.create_db()


def process_takeout_files():
    print("Indexing list of takeout files")
    index_takeout_files()
    for records, conn in sqlitedb.batch_updates("get_takeout_files"):
        for id, zip_file in records:
            print(f"Extracting contents from file: {zip_file}")
            files.extract_files(zip_file)
            sqlitedb.execute_with_val_during_batch("update_unzipped_status", id, conn)


def process_extracted_files():
    print("Indexing folders extracted from takeouts")
    index_folders(photos_dir)

    print("Indexing files extracted from takeouts")
    add_existing_files()

    move_excess_files(setup.get_from_settings("edited_location"), "get_edits")
    move_excess_files(setup.get_from_settings("mp_location"), "get_mp_files")

    utils.clean_up_filelist()


def process_json_files():
    print("Matching JSON files to Google Photos")
    match_json_files()

    print("Restructure filenames to find missing JSON matches")
    match_parantheses()

    print("Read remaining JSON files to find filename matches")
    match_json_with_title()

    print("Try to match remaining JSON files with cropped names")
    sqlitedb.execute_query("add_json_cropped")
    utils.clean_up_filelist()


def process_xmp_files():
    print("Creating XMP files for Immich")
    metadata.generate_xmp_files()
    add_new_files([".xmp"])

    print("Updating XMP files with JSON metadata")
    metadata.update_xmp_files()

    print("Deleting original XMP files")
    folders = sqlitedb.get_all_results("get_all_folders")

    for id, folder in folders:
        old_xmps = [
            file
            for file in files.get_files(folder, [".xmp_original"])
            if file.is_file()
        ]
        files.delete_files(old_xmps)


def process_google_photos():
    sqlitedb.execute_list("add_google_photos", valid_extensions)


def add_existing_files(extensions: list | None = None):
    index_files("get_unlisted_folders", extensions)


def add_new_files(extensions: list | None = None):
    index_files("get_all_folders", extensions)


def index_takeout_files():
    zip_files = [file for file in files.get_files(photos_dir, [".zip"])]

    db_records = list()
    for zippy in zip_files:
        db_records.append((zippy.name, zippy.path, zippy.stat().st_size))

    sqlitedb.insert_many("add_takeout_files", db_records)


def index_folders(parent_dir: str):
    db_records = list()
    folders = [dir for dir in files.get_directories(parent_dir) if len(dir[1]) > 0]
    for entry in folders:
        filepaths = [(os.path.join(entry[0], folder),) for folder in entry[1]]
        db_records.extend(filepaths)

    sqlitedb.insert_many("add_folders", db_records)


def index_files(sql_script: str, extensions: list | None = None):
    insert_sql_file = "add_filelist"
    for records, conn in sqlitedb.batch_updates(sql_script):
        db_records = None
        db_records = list()
        for valid_record in validate_files(records, extensions):
            db_records.append(valid_record)
            if utils.check_batch_ready(db_records):
                sqlitedb.insert_during_batch(insert_sql_file, db_records, conn)
                db_records = None
                db_records = list()
        if len(db_records) > 0:
            sqlitedb.insert_during_batch(insert_sql_file, db_records, conn)


def validate_files(records: list, extensions: list | None = None):
    for id, folder in records:
        actual_files = [file for file in files.get_files(folder, extensions) if file.is_file()]
        for file in actual_files:
            db_record = utils.get_file_properties(id, file)
            yield db_record


def index_filesystem():
    for dupe in setup.get_from_settings("duplicate_locations"):
        index_folders(dupe)

    db_records = list()
    insert_sql_file = "add_duplicate_photos"
    for records, conn in sqlitedb.batch_updates("get_unlisted_folders"):
        id = records[0]
        folder = records[1]
        actual_files = [file for file in files.get_files(folder, valid_extensions) if file.is_file()]

        for file in actual_files:
            db_records.append((id, file.name, file.stat().st_size))
            batch_ready = utils.check_batch_ready(db_records)
            if batch_ready:
                sqlitedb.insert_during_batch(insert_sql_file, db_records, conn)
                db_records = None
                db_records = list()
        if len(db_records) > 0:
            sqlitedb.insert_during_batch(insert_sql_file, db_records, conn)


def move_excess_files(new_dir: str, sql_script: str):
    i = 0
    for records, conn in sqlitedb.batch_updates(sql_script):
        db_records = None
        filepaths = None
        db_records = list()
        filepaths = list()
        i = i + 1

        for id, old_filepath in records:
            new_filepath = old_filepath.replace(photos_dir, new_dir)
            filepaths.append((old_filepath, new_filepath))
            db_records.append(id)
            
        print(f"Moving excess files, batch {i}")
        files.move_files(filepaths)
        sqlitedb.execute_list_during_batch("add_excess_filelist", db_records, conn)


def match_json_files():
    sqlitedb.execute_query("add_json_files")
    sqlitedb.execute_query("add_json_no_ext")

    utils.clean_up_filelist()


def match_parantheses():
    for records, conn in sqlitedb.batch_updates("media_paranthesis"):
        db_records = None
        db_records = list()
        for id, folder, filename in records:
            new_filename = utils.restructure_filename(filename)
            db_records.append((id, new_filename, folder))
        sqlitedb.execute_many_during_batch("add_matching_json", db_records, conn)
    utils.clean_up_filelist()


def match_json_with_title():
    for records, conn in sqlitedb.batch_updates("json_no_media"):
        db_records = None
        db_records = list()

        for id, filepath in records:
            result = metadata.read_filename(filepath)
            db_records.append((result, id))

        sqlitedb.execute_many_during_batch("add_more_json_files", db_records, conn)
    utils.clean_up_filelist()


def process_dates():
    # for records, conn in sqlitedb.batch_updates("get_xmp_files"):
    #     db_records = None
    #     db_records = list()
    #     for id, filepath in records:
    #         for sec in metadata.get_rdf_sections(filepath):
    #             db_records.append((id, str(sec)))
    #     sqlitedb.insert_during_batch("add_xmp_data", db_records, conn)
    metadata.get_tags()
