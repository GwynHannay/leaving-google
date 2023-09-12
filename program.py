import os
from helpers import files, metadata, setup, sqlitedb, utils


photos_dir = setup.get_from_settings("google_location")
edits_dir = setup.get_from_settings("edited_location")
mp_dir = setup.get_from_settings("mp_location")


def check_db():
    db_file = "".join([setup.get_from_settings("db_name"), ".db"])
    if not os.path.exists(db_file):
        print("Creating SQLite database and tables")
        sqlitedb.create_db()


def index_takeout_files():
    print("Indexing list of takeout files")
    zip_files = [file for file in files.get_files(photos_dir, [".zip"])]

    db_records = list()
    for zippy in zip_files:
        db_records.append((zippy.name, zippy.path, zippy.stat().st_size))

    sqlitedb.insert_many("add_takeout_files", db_records)


def index_raw_files():
    index_folders()
    db_records = list()
    folders = sqlitedb.get_all_results("get_unlisted_folders")

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
    folders = sqlitedb.get_all_results("get_all_folders")

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

    sqlitedb.insert_many("add_folders", db_records)


def index_filesystem():
    pass


def process_takeout_files():
    takeout_files = sqlitedb.get_all_results("get_takeout_files")
    conn = sqlitedb.start_query()
    for id, zip_file in takeout_files:
        print(f"On file: {zip_file}")
        files.extract_files(zip_file)
        sqlitedb.execute_with_val_during_batch("update_unzipped_status", id, conn)
    sqlitedb.end_query(conn)


def process_raw_files():
    print("Moving edited files to new folder")
    process_edited_files()
    print("Moving MP files to new folder")
    process_mp_files()
    print("Clean up moved files in database")
    utils.clean_up_filelist()

    print("Begin matching JSON files to original media")
    sqlitedb.execute_query("add_json_files")
    sqlitedb.execute_query("add_json_no_ext")

    utils.clean_up_filelist()
    print("Restructure filenames to find missing JSON matches")
    process_parantheses()
    utils.clean_up_filelist()

    print("Read remaining JSON files to find matches")
    sqlitedb.execute_query("add_json_cropped")
    utils.clean_up_filelist()

    print("Creating first XMP files")
    metadata.generate_xmp_files()

    print("Adding XMP files to database")
    index_xmp_files()


def process_metadata_files():
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


def process_edited_files():
    for records, conn in sqlitedb.batch_updates("get_edits"):
        db_records = None
        db_records = list()
        filepaths = None
        filepaths = list()

        for id, old_filepath in records:
            new_filepath = old_filepath.replace(photos_dir, edits_dir)
            filepaths.append((old_filepath, new_filepath))
            db_records.append(id)

        files.move_files(filepaths)
        sqlitedb.execute_list_during_batch("add_filelist_edits", db_records, conn)


def process_mp_files():
    i = 0
    for records, conn in sqlitedb.batch_updates("get_mp_files"):
        db_records = None
        filepaths = None
        db_records = list()
        filepaths = list()
        i = i + 1

        for id, old_filepath in records:
            new_filepath = old_filepath.replace(photos_dir, mp_dir)
            filepaths.append((old_filepath, new_filepath))
            db_records.append(id)
            
        print(f"Moving batch {i}")
        files.move_files(filepaths)
        sqlitedb.execute_list_during_batch("add_filelist_mp", db_records, conn)


def process_unmatched_json():
    for records, conn in sqlitedb.batch_updates("json_no_media"):
        db_records = None
        db_records = list()

        for id, filepath in records:
            result = metadata.read_filename(filepath)
            db_records.append((result, id))

        sqlitedb.execute_many_during_batch("add_more_json_files", db_records, conn)


def process_parantheses():
    for records, conn in sqlitedb.batch_updates("media_paranthesis"):
        db_records = None
        db_records = list()
        for id, folder, filename in records:
            new_filename = utils.restructure_filename(filename)
            db_records.append((id, new_filename, folder))
        sqlitedb.execute_many_during_batch("add_matching_json", db_records, conn)


def process_dates():
    # for records, conn in sqlitedb.batch_updates("get_xmp_files"):
    #     db_records = None
    #     db_records = list()
    #     for id, filepath in records:
    #         for sec in metadata.get_rdf_sections(filepath):
    #             db_records.append((id, str(sec)))
    #     sqlitedb.insert_during_batch("add_xmp_data", db_records, conn)
    metadata.get_tags()
