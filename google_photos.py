from helpers import setup, db, filesystem, utils


EXTENSIONS = setup.get_extensions_list()
DUPE_LOCATION = setup.get_from_settings("dupe_location")
PHOTOS_DIR = setup.get_from_settings("google_location")
MP_LOCATION = setup.get_from_settings("mp_location")


def process_takeout_files():
    index_takeout_files()
    extract_google_photos()
    index_extracted_folders()
    index_extracted_files()
    move_excess_files(DUPE_LOCATION, "get_google_dupes")
    move_excess_files(MP_LOCATION, "get_mp_files")
    utils.clean_up_filelist()


def process_google_photos():
    build_google_table()
    db.run_query("update_edit_id")
    db.run_query("delete_google_from_filelist")


def index_takeout_files():
    zip_files = [file for file in filesystem.get_files(PHOTOS_DIR, [".zip"])]

    db_records = []
    for zippy in zip_files:
        db_records.append((zippy.name, zippy.path, zippy.stat().st_size))

    db.insert_many("add_takeout_files", db_records)


def index_extracted_folders():
    db_records = []
    insert_sql_file = "add_folders"
    folders = [dir for dir in filesystem.get_directories(PHOTOS_DIR)]
    for folder in folders:
        db_records.append((folder,))
        db_records = utils.prepare_batch(insert_sql_file, db_records, False)
    if len(db_records) > 0:
        utils.prepare_batch(insert_sql_file, db_records, True)


def index_extracted_files():
    query_script = "get_unlisted_folders"
    insert_sql_file = "add_filelist"
    for records, conn in db.begin_batch_updates(query_script):
        db_records = []
        for id, folder in records:
            actual_files = [
                file
                for file in filesystem.get_files(folder, EXTENSIONS)
                if file.is_file()
            ]
            for file in actual_files:
                db_records.append(utils.get_file_properties(id, file))
                if utils.check_batch_ready(db_records):
                    db.insert_many(insert_sql_file, db_records, conn)
                    db_records = []
        if len(db_records) > 0:
            db.insert_many(insert_sql_file, db_records, conn)

    db.run_query("update_indexed_status")


def move_excess_files(new_dir: str, sql_script: str):
    i = 0
    for records, conn in db.begin_batch_updates(sql_script):
        db_records = []
        filepaths = []
        i = i + 1

        for id, old_filepath in records:
            new_filepath = old_filepath.replace(PHOTOS_DIR, new_dir)
            filepaths.append((old_filepath, new_filepath))
            db_records.append(id)

        print(f"Moving excess files, batch {i}")
        filesystem.move_files(filepaths)
        db.update_with_list("add_excess_filelist", db_records, conn)


def extract_google_photos():
    for records, conn in db.begin_batch_updates("get_takeout_files"):
        for id, zip_file in records:
            print(f"Extracting contents from file: {zip_file}")
            filesystem.extract_files(zip_file)
            db.update_with_single_val("update_unzipped_status", id, conn)


def build_google_table():
    for records, conn in db.begin_batch_updates_with_list(
        "get_media", EXTENSIONS
    ):
        db_records = build_new_records(records)
        db.insert_many("add_google_photos", db_records, conn)


def build_new_records(records: list):
    try:
        new_records = list()
        for record in records:
            distinct_name = clean_filename(record[2])

            new_records.append(record + (distinct_name,))
        return new_records
    except Exception as e:
        raise Exception(f"Trouble creating new record for photos: {records} {e}")


def clean_filename(filename: str):
    sans_parans = utils.remove_parantheses(filename)

    if sans_parans:
        filename = sans_parans
    
    return filename.replace("-edited", "")
