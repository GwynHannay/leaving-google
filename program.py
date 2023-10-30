import os
from itertools import zip_longest
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

    sqlitedb.execute_query("update_indexed_status")

    move_excess_files(setup.get_from_settings("dupe_location"), "get_google_dupes")
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
    sqlitedb.execute_query("add_json_with_cropped_names")
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
    for records, conn in sqlitedb.batch_updates_from_list("get_media", valid_extensions):
        db_records = build_google_record(records)
        sqlitedb.insert_during_batch("add_google_photos", db_records, conn)

    sqlitedb.execute_query("delete_google_from_filelist")
    metadata.get_tags("get_google_photos")
    metadata.build_tag_table("add_google_tags")


def process_original_photos():
    for original in setup.get_from_settings("original_locations"):
        check_directories(original, valid_extensions)
        
    add_existing_files(valid_extensions)

    sqlitedb.execute_query("update_indexed_status")

    # sqlitedb.execute_query("add_original_dupes")
    # sqlitedb.execute_query("delete_original_dupes")

    for records, conn in sqlitedb.batch_updates_from_list("get_media", valid_extensions):
        db_records = build_google_record(records)
        sqlitedb.insert_during_batch("add_original_photos", db_records, conn)

    sqlitedb.execute_query("delete_original_from_filelist")
    sqlitedb.execute_query("delete_mac_files")
    sqlitedb.execute_query("update_duplicate_originals")
    sqlitedb.execute_query("update_duplicate_google")

    metadata.get_tags("get_original_photos")
    metadata.build_tag_table("add_original_tags")


def match_photos():
    # match_via_tags()
    match_visually()



def match_via_tags():
    sqlitedb.execute_query("add_matches_using_tags")
    sqlitedb.execute_query("add_matches_using_dimensions")
    sqlitedb.execute_query("add_matches_using_cameras")
    sqlitedb.execute_query("add_matches_without_cameras")
    sqlitedb.execute_query("add_matches_using_names")


def match_visually():
    for records, conn in sqlitedb.batch_updates("get_matching_photos"):
        original_photos = list()
        google_image = None
        previous_id = 0
        db_records = list()
        for google_id, google_filepath, original_id, original_filepath, rotate in records:
            if google_image and previous_id is not google_id:
                results = compare_image_hashes(google_image, original_photos)
                for scores in results:
                    db_records.append((scores[1], previous_id, scores[0]))
                original_photos = list()

            previous_id = google_id
            google_image = google_filepath
            original_photos.append((rotate, original_filepath, original_id))

        if google_image:
            results = compare_image_hashes(google_image, original_photos)
            for scores in results:
                db_records.append((scores[1], previous_id, scores[0]))
            sqlitedb.execute_many_during_batch("update_match_scores", db_records, conn)


def compare_image_hashes(google_filepath: str, original_photos: list):
    scores = list()
    diff = None
    google_hash = files.get_image_hash(google_filepath)

    for rotate, filepath, photo_id in original_photos:
        if rotate == 'Y':
            left_rotation_hash = files.get_rotated_image_hash(filepath, 90)
            right_rotation_hash = files.get_rotated_image_hash(filepath, 270)

            left_diff = google_hash - left_rotation_hash
            right_diff = google_hash - right_rotation_hash

            if left_diff < right_diff:
                diff = left_diff
            else:
                diff = right_diff
        else:
            original_hash = files.get_image_hash(filepath)
            diff = google_hash - original_hash

            if diff > 15:
                rotated_hash = files.get_rotated_image_hash(filepath, 180)
                rotated_diff = google_hash - rotated_hash

                if rotated_diff < diff:
                    diff = rotated_diff
        scores.append((photo_id, diff))
    
    return scores


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
    insert_sql_file = "add_folders"
    folders = [dir for dir in files.get_directories(parent_dir)]
    for folder in folders:
        db_records.append((folder,))
        db_records = utils.prepare_batch(insert_sql_file, db_records, False)
    if len(db_records) > 0:
        utils.prepare_batch(insert_sql_file, db_records, True)


def index_files(sql_script: str, extensions: list | None = None):
    insert_sql_file = "add_filelist"
    for records, conn in sqlitedb.batch_updates(sql_script):
        db_records = None
        db_records = list()
        for id, folder in records:
            actual_files = [file for file in files.get_files(folder, extensions) if file.is_file()]
            for file in actual_files:
                db_records.append(utils.get_file_properties(id, file))
                if utils.check_batch_ready(db_records):
                    sqlitedb.insert_during_batch(insert_sql_file, db_records, conn)
                    db_records = None
                    db_records = list()
        if len(db_records) > 0:
            sqlitedb.insert_during_batch(insert_sql_file, db_records, conn)


def index_filesystem():

    db_records = list()
    insert_sql_file = "add_original_photos"
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
    # sqlitedb.execute_query("add_json_no_ext")

    utils.clean_up_filelist()


def match_parantheses():
    for records, conn in sqlitedb.batch_updates("get_media_with_paranthesis"):
        db_records = None
        db_records = list()
        for id, folder, filename in records:
            new_filename = utils.restructure_filename(filename)
            db_records.append((id, new_filename, folder))
        sqlitedb.execute_many_during_batch("add_json_with_paranthesis", db_records, conn)
    utils.clean_up_filelist()


def match_json_with_title():
    for records, conn in sqlitedb.batch_updates("get_unmatched_json"):
        db_records = None
        db_records = list()

        for id, filepath in records:
            result = metadata.read_filename(filepath)
            db_records.append((result, id))

        sqlitedb.execute_many_during_batch("add_json_using_filenames", db_records, conn)
    utils.clean_up_filelist()


def build_google_record(records: list):
    try:
        new_records = list()
        for record in records:
            sans_parans = utils.remove_parantheses(record[2])
            if sans_parans:
                distinct_name = sans_parans
            else:
                distinct_name = record[2]
            
            new_records.append(record + (distinct_name,))
        return new_records
    except Exception as e:
        raise Exception(f"Trouble creating record for Google Photos: {records} {e}")


def check_directories(dir_path: str, extensions: list):
    valid_folders = list()
    insert_sql_file = "add_folders"
    folders = [dir for dir in files.get_directories(dir_path)]
    for folder in folders:
        for file in files.get_files(folder, extensions):
            if file.is_file():
                valid_folders.append((folder,))
                break
        valid_folders = utils.prepare_batch(insert_sql_file, valid_folders, False)
    if len(valid_folders) > 0:
        utils.prepare_batch(insert_sql_file, valid_folders, True)


# def process_dates():
#     # for records, conn in sqlitedb.batch_updates("get_xmp_files"):
#     #     db_records = None
#     #     db_records = list()
#     #     for id, filepath in records:
#     #         for sec in metadata.get_rdf_sections(filepath):
#     #             db_records.append((id, str(sec)))
#     #     sqlitedb.insert_during_batch("add_xmp_data", db_records, conn)
#     metadata.get_tags()
