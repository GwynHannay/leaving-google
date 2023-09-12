import os
import json
import zipfile
from helpers import setup



def get_files(dir_name: str, extensions: list | None):
    if extensions:
        for file in os.scandir(dir_name):
            if any(file.name.lower().endswith(ext) for ext in extensions):
                yield file
    else:
        for file in os.scandir(dir_name):
            yield file


def get_directories(dir_path: str):
    for root, dirs, files in os.walk(dir_path):
        yield (root, dirs)


def get_file_size(file) -> str:
    return file.stat().st_size


def split_file_extension(file) -> tuple:
    return os.path.splitext(file)


def extract_files(filepath: str):
    working_dir = setup.get_from_settings("working_location")
    with zipfile.ZipFile(filepath, "r") as z:
        z.extractall(working_dir)


def read_json_file(filepath: str):
    contents = None
    with open(filepath, "r") as f:
        contents = json.loads(f.read())

    return contents


def delete_files(filepaths: list):
    for filepath in filepaths:
        try:
            os.remove(filepath)
        except Exception as e:
            raise Exception(f"Could not delete file: {filepath} {e}")


def move_files(filepaths: list):
    for original_file, new_file in filepaths:
        try:
            os.renames(original_file, new_file)
        except Exception as e:
            raise Exception(
                f"Could not move file: old {original_file} to new {new_file} {e}"
            )


def get_creation_scripts() -> list:
    sql_queries = list()
    tables_folder = setup.get_creation_folder()
    for filename in os.listdir(tables_folder):
        sql_file = read_sql_file(os.path.join(tables_folder, filename))
        sql_queries.append(sql_file)

    return sql_queries


def get_change_script(filename: str) -> str:
    filepath = "".join([setup.get_changes_folder(), filename])
    sql_file = read_sql_file(filepath)
    return sql_file


def get_query_script(filename: str) -> str:
    filepath = "".join([setup.get_queries_folder(), filename])
    sql_file = read_sql_file(filepath)
    return sql_file


def read_sql_file(filepath: str) -> str:
    query = str
    if not filepath.endswith(".sql"):
        filepath = "".join([filepath, ".sql"])

    with open(filepath) as s:
        query = s.read()
    return query
