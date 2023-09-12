import pytz

import program
from helpers import setup


### BEGIN SETUP
timezone = setup.get_from_settings("timezone")
db_file = "".join([setup.get_from_settings("db_name"), ".db"])
batch_size = setup.get_from_settings("batch_size")
photos_dir = setup.get_from_settings("google_location")
working_dir = setup.get_from_settings("working_location")
duplicate_locations = setup.get_from_settings("duplicate_locations")

tz = pytz.timezone(timezone)
### END SETUP


if __name__ == "__main__":
    program.check_db()
    # program.index_takeout_files()
    # program.process_takeout_files()
    # program.index_raw_files()
    # program.process_raw_files()
    # program.process_metadata_files()
    # program.process_dates()
    program.index_filesystem()
