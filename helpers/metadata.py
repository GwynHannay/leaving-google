import exiftool
import pytz
import xmltodict
from datetime import datetime
from helpers import setup, sqlitedb


timezone = setup.get_from_settings("timezone")
default_tz = pytz.timezone(timezone)
tz_map = setup.get_from_config("tz_map")
tag_priority = setup.get_from_config("tag_priority")

extensions = setup.get_extensions_list()


def get_tz(location: str):
    matched_tz = tz_map.get(location)
    if matched_tz:
        return pytz.timezone(matched_tz)
    else:
        return default_tz


def generate_xmp_files():
    with exiftool.ExifToolHelper(config_file="config/exiftool.config") as et:
        i = 0
        for records in sqlitedb.batch_query_from_list("get_media_files", extensions):
            i = i + 1
            print(f"Generating batch {i}")
            for id, filepath in records:
                try:
                    et.execute(f"{filepath[0]}", "-o", "%d%f.%e.xmp")
                except Exception as e:
                    raise Exception(f"Could not process file: {filepath[0]} {e}")


def update_xmp_files():
    with exiftool.ExifToolHelper(config_file="config/exiftool.config") as et:
        i = 0
        for records in sqlitedb.batch_query("get_metadata_files"):
            i = i + 1
            print(f"Updating batch {i}")
            for json_filepath, xmp_filepath in records:
                try:
                    et.execute(
                        "-tagsfromfile", json_filepath, "-GOOG", xmp_filepath
                    )
                except Exception as e:
                    raise Exception(
                        f"Could not process files: {json_filepath} {xmp_filepath} {e}"
                    )


def read_filename(filepath: str):
    with exiftool.ExifToolHelper(config_file="config/exiftool.config") as et:
        title = et.get_tags(filepath, tags="title")
        return title[0].get('JSON:Title')


def get_rdf_sections(filepath: str):
    with open(filepath, 'r') as f:
        xmp = xmltodict.parse(f.read())
    
    sections = list()
    if xmp:
        for section in xmp['x:xmpmeta']['rdf:RDF']['rdf:Description']:
            sections.append(section)
    return sections


# def get_tags():
#     with exiftool.ExifToolHelper(config_file="config/exiftool.config") as et:
#         tags_list = setup.get_from_config("tags")
#         for records, conn in sqlitedb.batch_updates("get_xmp_files"):
#             filepaths = [record[1] for record in records]
#             ids = [record[0] for record in records]
#             try:
#                 tags = et.get_tags(filepaths, tags=tags_list, params=[
#                     "-x", 
#                     "File:all",
#                     "-x",
#                     "Composite:all"
#                 ])
#                 weeded_tags = weed_tags(tags, ids)
#                 sqlitedb.insert_during_batch("add_xmp_data", weeded_tags, conn)
#             except Exception as e:
#                 raise Exception(f"Trouble getting tags: {e}")


def get_tags(source_script: str):
    with exiftool.ExifToolHelper(config_file="config/exiftool.config") as et:
        for records, conn in sqlitedb.batch_updates_from_list(source_script, extensions):
            # filepaths = [record[1] for record in records]
            # ids = [record[0] for record in records]
            weeded_tags = list()
            for id, filepath in records:
                try:
                    tags = et.get_tags(filepath, tags='matchtags')
                    weeded_tags.extend(weed_tags(tags[0], id))
                except exiftool.exceptions.ExifToolExecuteError:
                    print(f"Could not generate tags for file: {filepath}")
                    continue
                except Exception as e:
                    raise Exception(f"Trouble getting tags for this file: {filepath} {e}")
            sqlitedb.insert_during_batch("add_xmp_data", weeded_tags, conn)


def weed_tags(tags: list, id: int):
    db_records = None
    db_records = list()
    for file_tag in tags:
        if file_tag == "SourceFile":
            continue
        db_records.append((id, file_tag, str(tags[file_tag])))
    return db_records


def build_tag_table(source_script: str):
    sqlitedb.execute_query(source_script)
    sqlitedb.execute_query("delete_xmp_data")


# def db_batch_query(script: str):
#     conn = sqlitedb.start_query()
#     for records in sqlitedb.get_batched_results(script, conn):
#         yield records
#     sqlitedb.end_query(conn)


# def db_batch_updates_from_list(script: str, vals: list):
#     conn = sqlitedb.start_query()
#     for records in sqlitedb.get_batched_results_from_list(script, vals, conn):
#         yield (records, conn)
#     sqlitedb.end_query(conn)


# def db_batch_updates(script: str):
#     conn = sqlitedb.start_query()
#     for records in sqlitedb.get_batched_results(script, conn):
#         yield (records, conn)
#     sqlitedb.end_query(conn)


# def get_all_tags(filepaths: list):
#     db_records = list()
#     with exiftool.ExifToolHelper(config_file="config/exiftool.config") as et:
#         tags = et.get_tags(filepaths, tags=None)
#         file_dates = process_dates(tags)

#         print([dates for dates in file_dates])


# def process_dates(tags: list):
#     for meta in tags:
#         photo_taken_ts = meta.get("JSON:PhotoTakenTimeTimestamp")
#         file_created_ts = meta.get("JSON:CreationTimeTimestamp")

#         photo_taken_utc = datetime.utcfromtimestamp(photo_taken_ts).replace(
#             tzinfo=pytz.utc
#         )
#         file_created_utc = datetime.utcfromtimestamp(file_created_ts).replace(
#             tzinfo=pytz.utc
#         )

# country = meta.get('XMP:Country')

# if not country:
#     photo_taken_dt = photo_taken_utc.astimezone(tz)
#     file_created_dt = file_created_utc.astimezone(tz)
# else:
#     match country:
#         case "NEW ZEALAND":
#             temp_tz = pytz.timezone("NZ")
#         case _:
#             temp_tz = tz

#     photo_taken_dt = photo_taken_utc.astimezone(temp_tz)
#     file_created_dt = file_created_utc.astimezone(temp_tz)

# temp_tz = pytz.timezone("NZ")
# photo_taken_dt = photo_taken_utc.astimezone(temp_tz)
# file_created_dt = file_created_utc.astimezone(temp_tz)

# photo_taken = photo_taken_dt.strftime("%Y:%m:%d %H:%M:%S%z")
# file_created = file_created_dt.strftime("%Y:%m:%d %H:%M:%S%z")
# yield (photo_taken, file_created)
