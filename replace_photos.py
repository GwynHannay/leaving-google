import os
from helpers import db, filesystem


def get_best_copies():
    copy_better_photos()


def copy_better_photos():
    new_folder = None
    old_folder = None
    folder_id = None
    for records, conn in db.begin_batch_updates("get_better_photos"):
        db_records = []
        filepaths = []
        for id, google_folder, google_filename, original_path in records:
            if not old_folder or (old_folder != google_folder):
                print(google_folder, old_folder)
                new_folder = google_folder.replace("Takeout", "NewCopies")
                if not os.path.exists(new_folder):
                    os.makedirs(new_folder)
                
                folder_id = db.insert_row("add_folders", (new_folder,), conn)

            new_path = os.path.join(new_folder, google_filename)
            filepaths.append((original_path, new_path))
            db_records.append((folder_id, id))
            old_folder = google_folder

        filesystem.copy_files(filepaths)
        db.update_with_many_vals("update_new_folder", db_records, conn)
