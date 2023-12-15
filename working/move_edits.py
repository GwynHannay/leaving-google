import os
from posixpath import splitext

def move(old_file: str, new_file: str):
    try:
        os.renames(old_file, new_file)
    except Exception as e:
        raise Exception(f"Could not move {old_file} to {new_file}: {e}")


def rename_edits(filepath: str):
    path, ext = splitext(filepath)
    new_name = "".join([path, "-edited", ext])
    if os.path.exists(filepath):
        try:
            os.renames(filepath, new_name)
        except Exception as e:
            raise Exception(f"Could not rename file from {filepath} to {new_name}: {e}")


def get_directory_tree(dir_path: str):
    for root, dirs, files in os.walk(dir_path):
        yield (root, dirs, files)


def get_edits(filepath: str):
    for root, dirs, files in get_directory_tree(filepath):
        print(root, dirs, files)
        # if len(dirs) < 1:
        #     for file in files:
        #         full_path = os.path.join(root, file)
        #         new_path = full_path.replace(filepath, "/Volumes/array/storage/temp/gwyn/Photos")
        #         print(f"Renaming {new_path}")
        #         rename_edits(new_path)
        #         print(f"Moving {full_path}")
        #         move(full_path, new_path)


if __name__ == "__main__":
    get_edits("/Volumes/array/storage/temp/gwyn/Edits")