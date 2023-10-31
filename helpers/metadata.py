import exiftool
import pytz
from helpers import setup, db


TIMEZONE = setup.get_from_settings("timezone")
DEFAULT_TZ = pytz.timezone(TIMEZONE)
TZ_MAP = setup.get_from_config("tz_map")

EXTENSIONS = setup.get_extensions_list()


def get_tz(location: str):
    matched_tz = TZ_MAP.get(location)
    if matched_tz:
        return pytz.timezone(matched_tz)
    else:
        return DEFAULT_TZ


def generate_xmp_files():
    with exiftool.ExifToolHelper(config_file="config/exiftool.config") as et:
        i = 0
        for records in db.begin_batch_query_with_list("get_media_files", EXTENSIONS):
            i = i + 1
            print(f"Generating batch {i}")
            for id, filepath in records:
                try:
                    et.execute(f"{filepath}", "-o", "%d%f.%e.xmp")
                except Exception as e:
                    raise Exception(f"Could not process file: {filepath} {e}")


def update_xmp_files():
    with exiftool.ExifToolHelper(config_file="config/exiftool.config") as et:
        i = 0
        for records in db.begin_batch_query("get_metadata_files"):
            i = i + 1
            print(f"Updating batch {i}")
            for json_filepath, xmp_filepath in records:
                try:
                    et.execute("-tagsfromfile", json_filepath, "-GOOG", xmp_filepath)
                except Exception as e:
                    raise Exception(
                        f"Could not process files: {json_filepath} {xmp_filepath} {e}"
                    )


def read_filename(filepath: str):
    with exiftool.ExifToolHelper(config_file="config/exiftool.config") as et:
        title = et.get_tags(filepath, tags="title")
        return title[0].get("JSON:Title")


def get_tags(source_script: str):
    with exiftool.ExifToolHelper(config_file="config/exiftool.config") as et:
        for records, conn in db.begin_batch_updates_with_list(source_script, EXTENSIONS):
            weeded_tags = list()
            for id, filepath in records:
                try:
                    tags = et.get_tags(filepath, tags="matchtags")
                    weeded_tags.extend(weed_tags(tags[0], id))
                except exiftool.exceptions.ExifToolExecuteError:
                    print(f"Could not generate tags for file: {filepath}")
                    continue
                except Exception as e:
                    raise Exception(
                        f"Trouble getting tags for this file: {filepath} {e}"
                    )
            db.insert_many("add_xmp_data", weeded_tags, conn)


def weed_tags(tags: list, id: int):
    db_records = None
    db_records = list()
    for file_tag in tags:
        if file_tag == "SourceFile":
            continue
        db_records.append((id, file_tag, str(tags[file_tag])))
    return db_records


def build_tag_table(source_script: str):
    db.run_query(source_script)
    db.run_query("delete_xmp_data")
