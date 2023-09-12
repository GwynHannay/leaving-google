import json
import re
from helpers import files, setup, sqlitedb


config_batch_size = setup.get_from_config("batch_size")


def prepare_batch(
    script_name: str, records: list, state: bool, batch_size: int = 0
) -> list:
    if batch_size == 0:
        batch_size = config_batch_size

    if len(records) >= batch_size or state:
        sqlitedb.insert_many(script_name, records)

        empty_list = list()
        return empty_list
    else:
        return records


def check_batch_ready(records: list, batch_size: int = 0) -> bool:
    if batch_size == 0:
        batch_size = config_batch_size
    if len(records) >= batch_size:
        return True
    else:
        return False


def get_file_properties(id, file) -> tuple:
    size = files.get_file_size(file)
    filename, ext = files.split_file_extension(file.name)

    return (id, filename, ext, size)


def extract_parantheses(filename: str):
    matches = re.search(r"(.*)(\([^)]*\))(\.\S*)", filename)
    return matches


def restructure_filename(filename: str):
    matches = extract_parantheses(filename)
    if matches:
        split_name = matches.groups()
        new_filename = ''.join([split_name[0], split_name[2], split_name[1]])
        return new_filename


def clean_up_filelist():
    sqlitedb.execute_query("delete_filelist_entries")


def validate_metadata(file_id: int, contents: dict):
    try:
        title = contents.get("title")
        description = contents.get("description")
        image_views = contents.get("imageViews")
        creation_time = json.dumps(contents.get("creationTime"))
        photo_time = json.dumps(contents.get("photoTakenTime"))
        geo_data = json.dumps(contents.get("geoData"))
        geo_data_exif = json.dumps(contents.get("geoDataExif"))
        url = contents.get("url")
        origin = json.dumps(contents.get("googlePhotosOrigin"))
        archived = contents.get("archived")
        favorited = contents.get("favorited")

        if not archived:
            archived = 0
        if not favorited:
            favorited = 0

        return (
            file_id,
            title,
            description,
            image_views,
            creation_time,
            photo_time,
            geo_data,
            geo_data_exif,
            url,
            archived,
            favorited,
            origin,
        )
    except Exception as e:
        raise Exception(f"Could not process file: {contents} {e}")
