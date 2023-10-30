from helpers import setup, filesystem, db, utils, metadata

PHOTOS_DIR = setup.get_from_settings("google_location")


def index_takeout_files():
    zip_files = [file for file in filesystem.get_files(PHOTOS_DIR, [".zip"])]

    db_records = list()
    for zippy in zip_files:
        db_records.append((zippy.name, zippy.path, zippy.stat().st_size))

    db.insert_many("add_takeout_files", db_records)


def index_folders():
    db_records = list()
    insert_sql_file = "add_folders"
    folders = [dir for dir in filesystem.get_directories(PHOTOS_DIR)]
    for folder in folders:
        db_records.append((folder,))
        db_records = utils.prepare_batch(insert_sql_file, db_records, False)
    if len(db_records) > 0:
        utils.prepare_batch(insert_sql_file, db_records, True)


def index_files(sql_script: str, extensions: list | None = None):
    insert_sql_file = "add_filelist"
    for records, conn in db.begin_batch_updates(sql_script):
        db_records = None
        db_records = list()
        for id, folder in records:
            actual_files = [
                file
                for file in filesystem.get_files(folder, extensions)
                if file.is_file()
            ]
            for file in actual_files:
                db_records.append(utils.get_file_properties(id, file))
                if utils.check_batch_ready(db_records):
                    db.insert_many(insert_sql_file, db_records, conn)
                    db_records = None
                    db_records = list()
        if len(db_records) > 0:
            db.insert_many(insert_sql_file, db_records, conn)


def move_excess_files(new_dir: str, sql_script: str):
    i = 0
    for records, conn in db.begin_batch_updates(sql_script):
        db_records = None
        filepaths = None
        db_records = list()
        filepaths = list()
        i = i + 1

        for id, old_filepath in records:
            new_filepath = old_filepath.replace(PHOTOS_DIR, new_dir)
            filepaths.append((old_filepath, new_filepath))
            db_records.append(id)

        print(f"Moving excess files, batch {i}")
        filesystem.move_files(filepaths)
        db.update_with_list("add_excess_filelist", db_records, conn)


def match_parantheses():
    for records, conn in db.begin_batch_updates("get_media_with_paranthesis"):
        db_records = None
        db_records = list()
        for id, folder, filename in records:
            new_filename = utils.restructure_filename(filename)
            db_records.append((id, new_filename, folder))
        db.update_with_many_vals("add_json_with_paranthesis", db_records, conn)
    utils.clean_up_filelist()


def match_json_with_title():
    for records, conn in db.begin_batch_updates("get_unmatched_json"):
        db_records = None
        db_records = list()

        for id, filepath in records:
            result = metadata.read_filename(filepath)
            db_records.append((result, id))

        db.update_with_many_vals("add_json_using_filenames", db_records, conn)
    utils.clean_up_filelist()


def check_directories(dir_path: str, extensions: list):
    valid_folders = list()
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
