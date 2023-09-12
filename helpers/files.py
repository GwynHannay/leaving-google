import os
import json
import zipfile
from helpers import setup


photos_dir = setup.get_from_settings("google_location")
working_dir = setup.get_from_settings("working_location")


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


def extract_files(filepath: str):
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
