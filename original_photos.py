import exiftool
from helpers import setup, filesystem, utils, db

EXTENSIONS = setup.get_extensions_list()
ORIGINAL_LOCATIONS = setup.get_from_settings("original_locations")


def process_original_photos():
    for original in setup.get_from_settings("original_locations"):
        check_directories(original, EXTENSIONS)
    index_original_photos()
    db.run_query("update_indexed_status")

    for records, conn in db.begin_batch_updates_with_list(
        "get_media", EXTENSIONS
    ):
        db_records = build_new_records(records)
        db.insert_many("add_original_photos", db_records, conn)

    db.run_query("delete_original_from_filelist")
    db.run_query("delete_mac_files")
    db.run_query("update_duplicate_originals")
    db.run_query("update_duplicate_google")


def build_matching_metadata():
    get_original_photos_tags("matchtags")
    db.run_query("add_original_tags")
    db.run_query("delete_xmp_data")


def build_time_metadata():
    get_original_photos_tags("timetags")


def check_directories(dir_path: str, extensions: list):
    valid_folders = []
    insert_sql_file = "add_folders"
    folders = [dir for dir in filesystem.get_directories(dir_path)]
    for folder in folders:
        for file in filesystem.get_files(folder, extensions):
            if file.is_file():
                valid_folders.append((folder,))
                break
        valid_folders = utils.prepare_batch(insert_sql_file, valid_folders, False)
    if len(valid_folders) > 0:
        utils.prepare_batch(insert_sql_file, valid_folders, True)


def index_original_photos():
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


def get_original_photos_tags(tag_list: str):
    sql_script = "get_original_photos"
    with exiftool.ExifToolHelper(config_file="config/exiftool.config") as et:
        for records, conn in db.begin_batch_updates_with_list(sql_script, EXTENSIONS):
            weeded_tags = list()
            for id, filepath in records:
                try:
                    tags = et.get_tags(filepath, tags=tag_list)
                    weeded_tags.extend(weed_tags(tags[0], id))
                except exiftool.exceptions.ExifToolExecuteError:
                    print(f"Could not generate tags for file: {filepath}")
                    continue
                except Exception as e:
                    raise Exception(
                        f"Trouble getting tags for this file: {filepath} {e}"
                    )
            db.insert_many("add_xmp_data", weeded_tags, conn)


def build_new_records(records: list):
    try:
        new_records = list()
        for record in records:
            distinct_name = clean_filename(record[2])

            new_records.append(record + (distinct_name,))
        return new_records
    except Exception as e:
        raise Exception(f"Trouble creating new record for photos: {records} {e}")


def weed_tags(tags: list, id: int):
    db_records = None
    db_records = list()
    for file_tag in tags:
        if file_tag == "SourceFile":
            continue
        db_records.append((id, file_tag, str(tags[file_tag])))
    return db_records


def clean_filename(filename: str):
    sans_parans = utils.remove_parantheses(filename)

    if sans_parans:
        filename = sans_parans
    
    return filename.replace("-edited", "")
