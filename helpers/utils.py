import re
from helpers import filesystem, setup, db


CONFIG_BATCH_SIZE = setup.get_from_settings("batch_size")


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


def extract_parantheses(filename: str):
    matches = re.search(r"(.*)(\([^)]*\))(\.\S*)", filename)
    return matches


def restructure_filename(filename: str):
    matches = extract_parantheses(filename)
    if matches:
        split_name = matches.groups()
        new_filename = "".join([split_name[0], split_name[2], split_name[1]])
        return new_filename


def remove_parantheses(filename: str):
    removed = re.sub(r"\([^)]*\)$", "", filename)
    return removed.rstrip(" ")


def clean_up_filelist():
    db.run_query("delete_filelist_entries")


def prepare_list_query(raw_query: str, records: list):
    list_placeholders = ",".join(["?"] * len(records))
    query = raw_query % list_placeholders
    return query
