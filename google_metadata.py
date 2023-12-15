import re
import exiftool
from helpers import setup, db, utils, filesystem

EXTENSIONS = setup.get_extensions_list()


def process_json_files():
    db.run_query("add_json_files")
    clean_up_filelist()
    match_parantheses()
    match_json_with_title()
    db.run_query("add_json_with_cropped_names")
    utils.clean_up_filelist()


def process_xmp_files():
    generate_xmp_files()
    index_xmp_files()
    update_xmp_files()
    delete_original_xmp_files()


def build_matching_metadata():
    get_google_tags("matchtags")
    db.run_query("add_google_match_tags")
    db.run_query("delete_xmp_data")


def build_time_metadata():
    get_google_tags("timetags")


def match_parantheses():
    for records, conn in db.begin_batch_updates("get_media_with_paranthesis"):
        db_records = []
        for id, folder, filename in records:
            new_filename = restructure_filename(filename)
            db_records.append((id, new_filename, folder))
        db.update_with_many_vals("add_json_with_paranthesis", db_records, conn)
    clean_up_filelist()


def match_json_with_title():
    for records, conn in db.begin_batch_updates("get_unmatched_json"):
        db_records = []

        for id, filepath in records:
            result = read_filename(filepath)
            db_records.append((result, id))

        db.update_with_many_vals("add_json_using_filenames", db_records, conn)
    clean_up_filelist()


def generate_xmp_files():
    with exiftool.ExifToolHelper(config_file="config/exiftool.config") as et:
        i = 0
        for records in db.begin_batch_query_with_list("get_media_files", EXTENSIONS):
            i = i + 1
            print(f"Generating batch {i}")
            for id, filepath in records:
                try:
                    et.execute(f"{filepath}", "-o", "%d%f.%e.xmp")
                except Exception as e:
                    raise Exception(f"Could not process file: {filepath} {e}")


def update_xmp_files():
    with exiftool.ExifToolHelper(config_file="config/exiftool.config") as et:
        i = 0
        for records in db.begin_batch_query("get_metadata_files"):
            i = i + 1
            print(f"Updating batch {i}")
            for json_filepath, xmp_filepath in records:
                try:
                    et.execute("-tagsfromfile", json_filepath, "-GOOG", xmp_filepath)
                except Exception as e:
                    raise Exception(
                        f"Could not process files: {json_filepath} {xmp_filepath} {e}"
                    )


def index_xmp_files():
    query_sql = "get_all_folders"
    insert_sql_file = "add_filelist"
    for records, conn in db.begin_batch_updates(query_sql):
        db_records = []
        for id, folder in records:
            actual_files = [
                file
                for file in filesystem.get_files(folder, [".xmp"])
                if file.is_file()
            ]
            for file in actual_files:
                db_records.append(utils.get_file_properties(id, file))
                if utils.check_batch_ready(db_records):
                    db.insert_many(insert_sql_file, db_records, conn)
                    db_records = []
        if len(db_records) > 0:
            db.insert_many(insert_sql_file, db_records, conn)


def get_google_tags(tag_list: str):
    sql_script = "get_google_photos"
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


def delete_original_xmp_files():
    for records in db.begin_batch_query("get_all_folders"):
        for id, folder in records:
            old_xmps = [
                file
                for file in filesystem.get_files(folder, [".xmp_original"])
                if file.is_file()
            ]
            filesystem.delete_files(old_xmps)


def read_filename(filepath: str):
    with exiftool.ExifToolHelper(config_file="config/exiftool.config") as et:
        title = et.get_tags(filepath, tags="title")
        return title[0].get("JSON:Title")


def restructure_filename(filename: str):
    matches = extract_parantheses(filename)
    if matches:
        split_name = matches.groups()
        new_filename = "".join([split_name[0], split_name[2], split_name[1]])
        return new_filename


def weed_tags(tags: list, id: int):
    db_records = None
    db_records = list()
    for file_tag in tags:
        if file_tag == "SourceFile":
            continue
        db_records.append((id, file_tag, str(tags[file_tag])))
    return db_records


def extract_parantheses(filename: str):
    matches = re.search(r"(.*)(\([^)]*\))(\.\S*)", filename)
    return matches


def clean_up_filelist():
    db.run_query("delete_filelist_entries")
