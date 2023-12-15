import re
import os
from helpers import filesystem, setup, db


CONFIG_BATCH_SIZE = setup.get_from_settings("batch_size")


def init_db():
    db_file = setup.get_db()
    if not os.path.exists(db_file):
        print("Creating SQLite database and tables")
        db.create_db()


def prepare_batch(
    script_name: str, records: list, state: bool, batch_size: int = 0
) -> list:
    if batch_size == 0:
        batch_size = CONFIG_BATCH_SIZE

    if len(records) >= batch_size or state:
        db.insert_many(script_name, records)

        empty_list = list()
        return empty_list
    else:
        return records


def check_batch_ready(records: list, batch_size: int = 0) -> bool:
    if batch_size == 0:
        batch_size = CONFIG_BATCH_SIZE
    if len(records) >= batch_size:
        return True
    else:
        return False


def get_file_properties(id, file) -> tuple:
    size = filesystem.get_file_size(file)
    filename, ext = filesystem.split_file_extension(file.name)
    filehash = filesystem.get_file_hash(file)

    return (id, filename, ext, size, filehash)


def remove_parantheses(filename: str):
    removed = re.sub(r"\([^)]*\)$", "", filename)
    return removed.rstrip(" ")


def clean_up_filelist():
    db.run_query("delete_filelist_entries")
