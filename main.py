import program


if __name__ == "__main__":
    program.check_db()
    program.process_takeout_files()
    program.process_extracted_files()
    program.process_json_files()
    program.process_xmp_files()
    program.process_google_photos()
    program.process_original_photos()
    # program.process_matching_photos()
