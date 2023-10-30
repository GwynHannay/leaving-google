import hashlib
import imagehash
import os
import zipfile
from helpers import setup
from PIL import Image, ImageFile


ImageFile.LOAD_TRUNCATED_IMAGES = True


def get_files(dir_name: str, extensions: list | None):
    if extensions:
        for file in os.scandir(dir_name):
            if any(file.name.lower().endswith(ext) for ext in extensions):
                yield file
    else:
        for file in os.scandir(dir_name):
            yield file


def get_directories(dir_path: str):
    all_dirs = list()
    folders = [dir for dir in get_directory_tree(dir_path) if len(dir[1]) > 0]
    for entry in folders:
        filepaths = [os.path.join(entry[0], folder) for folder in entry[1]]
        all_dirs.extend(filepaths)

    for dir in all_dirs:
        yield dir


def get_directory_tree(dir_path: str):
    for root, dirs, files in os.walk(dir_path):
        yield (root, dirs)


def get_image_hash(filepath: str):
    try:
        with Image.open(filepath) as im:
            return imagehash.dhash_vertical(im)
    except Exception as e:
        raise Exception(f"Error processing image: {filepath}, {e}")


def get_rotated_image_hash(filepath: str, angle: int):
    with Image.open(filepath) as im:
        rotated_image = im.rotate(angle)
        return imagehash.dhash_vertical(rotated_image)


def get_file_size(file) -> str:
    return file.stat().st_size


def get_file_hash(file) -> str:
    hasher = hashlib.md5()

    with open(file.path, "rb") as f:
        buffer = f.read()
        hasher.update(buffer)

    return hasher.hexdigest()


def split_file_extension(file) -> tuple:
    return os.path.splitext(file)


def extract_files(filepath: str):
    working_dir = setup.get_from_settings("working_location")
    with zipfile.ZipFile(filepath, "r") as z:
        z.extractall(working_dir)


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
    filepath = os.path.join(setup.get_changes_folder(), filename)
    sql_file = read_sql_file(filepath)
    return sql_file


def get_query_script(filename: str) -> str:
    filepath = os.path.join(setup.get_queries_folder(), filename)
    sql_file = read_sql_file(filepath)
    return sql_file


def read_sql_file(filepath: str) -> str:
    query = str
    if not filepath.endswith(".sql"):
        filepath = "".join([filepath, ".sql"])

    with open(filepath) as s:
        query = s.read()
    return query
