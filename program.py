import os
from helpers import files, filesystem, metadata, setup, db, utils, media


EXTENSIONS = setup.get_extensions_list()


def check_db():
    db_file = setup.get_db()
    if not os.path.exists(db_file):
        print("Creating SQLite database and tables")
        db.create_db()


def process_takeout_files():
    print("Indexing list of takeout files")
    files.index_takeout_files()

    for records, conn in db.begin_batch_updates("get_takeout_files"):
        for id, zip_file in records:
            print(f"Extracting contents from file: {zip_file}")
            filesystem.extract_files(zip_file)
            db.update_with_single_val("update_unzipped_status", id, conn)


def process_extracted_files():
    print("Indexing folders extracted from takeouts")
    files.index_folders()

    print("Indexing files extracted from takeouts")
    add_existing_files()

    db.run_query("update_indexed_status")

    files.move_excess_files(setup.get_from_settings("edited_location"), "get_edits")
    files.move_excess_files(
        setup.get_from_settings("dupe_location"), "get_google_dupes"
    )
    files.move_excess_files(setup.get_from_settings("mp_location"), "get_mp_files")

    utils.clean_up_filelist()
    files.rename_edited_files("get_originals_of_edits")


def process_json_files():
    print("Matching JSON files to Google Photos")
    db.run_query("add_json_files")
    utils.clean_up_filelist()

    print("Restructure filenames to find missing JSON matches")
    files.match_parantheses()

    print("Read remaining JSON files to find filename matches")
    files.match_json_with_title()

    print("Try to match remaining JSON files with cropped names")
    db.run_query("add_json_with_cropped_names")
    utils.clean_up_filelist()


def process_xmp_files():
    print("Creating XMP files for Immich")
    metadata.generate_xmp_files()
    add_new_files([".xmp"])

    print("Updating XMP files with JSON metadata")
    metadata.update_xmp_files()

    print("Deleting original XMP files")
    for records in db.begin_batch_query("get_all_folders"):
        for id, folder in records:
            old_xmps = [
                file
                for file in filesystem.get_files(folder, [".xmp_original"])
                if file.is_file()
            ]
            filesystem.delete_files(old_xmps)


def process_google_photos():
    for records, conn in db.begin_batch_updates_with_list(
        "get_media", EXTENSIONS
    ):
        db_records = build_new_records(records)
        db.insert_many("add_google_photos", db_records, conn)

    db.run_query("delete_google_from_filelist")
    metadata.get_tags("get_google_photos")
    metadata.build_tag_table("add_google_tags")


def process_original_photos():
    for original in setup.get_from_settings("original_locations"):
        files.check_directories(original, EXTENSIONS)

    add_existing_files(EXTENSIONS)

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

    metadata.get_tags("get_original_photos")
    metadata.build_tag_table("add_original_tags")


def process_matching_photos():
    match_via_tags()
    media.match_visually()


def match_via_tags():
    db.run_query("add_matches_using_tags")
    db.run_query("add_matches_using_dimensions")
    db.run_query("add_matches_using_cameras")
    db.run_query("add_matches_without_cameras")
    db.run_query("add_matches_using_names")
    db.run_query("add_matches_from_duplicates")


def build_new_records(records: list):
    try:
        new_records = list()
        for record in records:
            distinct_name = files.clean_filename(record[2])

            new_records.append(record + (distinct_name,))
        return new_records
    except Exception as e:
        raise Exception(f"Trouble creating new record for photos: {records} {e}")


def add_existing_files(extensions: list | None = None):
    files.index_files("get_unlisted_folders", extensions)


def add_new_files(extensions: list | None = None):
    files.index_files("get_all_folders", extensions)
