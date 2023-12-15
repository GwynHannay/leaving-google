import itertools
import os
import sqlite3
import numpy as np


def process_edits():
    edits = get_edits()

    filepaths = (build_paths(file[1]) for file in edits)
    full_list = (list(itertools.chain(*i)) for i in zip(edits, filepaths))
    print(len([*full_list]))


def build_paths(filepath: str):
    extensions = [".json", ".xmp"]
    root, filename = os.path.split(filepath)
    base, ext = os.path.splitext(filename)
    edited_name = "".join([base, "-edited", ext])

    old = []
    new = []
    for e in extensions:
        old_name = "".join([base, e])
        new_name = "".join([edited_name, e])
        rename_files(os.path.join(root, old_name), os.path.join(root, new_name))
        old.append(old_name)
        new.append(new_name)

    return ()


def rename_files(old_name, new_name):
    try:
        os.renames(old_name, new_name)
    except Exception as e:
        raise Exception(f"Could not rename file {old_name} to {new_name}: {e}")


def get_excess_files(db_path: str):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        query = """
        SELECT DISTINCT fi.id, filepath || '/' || fi.filename || fi.extension AS fullpath
        FROM excess_filelist fi
        JOIN folders fo ON fi.folder_id = fo.id
        """

        result = cursor.execute(query)
        while True:
            matches = result.fetchmany(300)
            if matches:
                yield matches
            else:
                break

    except Exception as e:
        raise Exception(f"Failed to run query: {e}")



def get_edits():
    edited_files = []
    for results in get_excess_files("photos.db"):
        for id, file in results:
            filepath = "".join(["/Volumes", file])
            if os.path.exists(filepath):
                edited_files.append((id, file))
    
    return edited_files


if __name__ == "__main__":
    process_edits()


    # original_filenames = (split_filename(file[1]) for file in edits)
    # full_list = (list(itertools.chain(*i)) for i in zip(edits, original_filenames))

    # files = np.array([file for file in full_list])
    
    # splitNames = np.vectorize(split_filename)
    # files2 = splitNames(files[:,3])
    # print(files[:,3])


    # root_dirs = (file[0] for file in original_filenames)
    # name_ext = (split_extension(file[1]) for file in original_filenames)
    # edited_filenames = (join_items([file[0], "-edited", file[1]]) for file in name_ext)
    # edited_xmp = (join_items([file, ".xmp"]) for file in edited_filenames)
    # xmp_filepaths = (join_items([*items]) for items in itertools.zip_longest(root_dirs, ["/"], edited_xmp, fillvalue="/"))




    # new_list = zip(edited_filenames, edited_xmp)


    # full_list = (list(itertools.chain(*i)) for i in zip(edits, original_filenames, new_list))

    # for f in edited_xmp:
    #     print(f)

    # original_filenames = [os.path.splitext(os.path.split(filename)[1]) for filename in files]
    # edited_filenames = ["".join([file[0], "-edited", file[1]]) for file in original_filenames]
    # edited_json = ["".join([file, ".json"]) for file in edited_filenames]
    # edited_xmp = ["".join([file, ".xmp"]) for file in edited_filenames]
    # original_xmp = ["".join([file[0], file[1], ".xmp"]) for file in original_filenames]
    # big_list = itertools.zip_longest(ids, files, original_filenames, edited_filenames, edited_json, edited_xmp, original_xmp)



# def join_items(items: list):
#     return "".join(items)

# def split_filename(filepath: str):
#     return os.path.split(filepath)

# def split_extension(filepath: str):
#     return os.path.splitext(filepath)