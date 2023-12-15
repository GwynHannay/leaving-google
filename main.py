import google_photos
import google_metadata
import match_photos
import original_photos
import replace_photos
from helpers import utils



if __name__ == "__main__":
    utils.init_db()
    google_photos.process_takeout_files()
    google_metadata.process_json_files()
    google_metadata.process_xmp_files()
    google_metadata.build_matching_metadata()
    google_metadata.build_time_metadata()
    original_photos.process_original_photos()
    original_photos.build_matching_metadata()
    original_photos.build_time_metadata()
    match_photos.find_matching_photos()
    replace_photos.get_best_copies()
