import pytz

import program
from helpers import setup


### BEGIN SETUP
timezone = setup.get_from_settings("timezone")
db_file = "".join([setup.get_from_settings("db_name"), ".db"])
batch_size = setup.get_from_settings("batch_size")
photos_dir = setup.get_from_settings("google_location")
working_dir = setup.get_from_settings("working_location")
original_locations = setup.get_from_settings("original_locations")

tz = pytz.timezone(timezone)
### END SETUP


if __name__ == "__main__":
    program.check_db()
    # program.process_takeout_files()
    # program.process_extracted_files()
    # program.process_json_files()
    # program.process_xmp_files()
    # program.process_google_photos()
    # program.process_original_photos()
    program.match_photos()
