CREATE TABLE original_photos (
    id INTEGER PRIMARY KEY,
    folder_id INTEGER NOT NULL,
    filename TEXT NOT NULL,
    extension TEXT,
    size INTEGER,
    hash TEXT,
    distinct_name TEXT NOT NULL,
    duplicate_of_original_id INTEGER,
    duplicate_of_google_id INTEGER,
    searched TEXT,
    new_folder_id INTEGER,
    FOREIGN KEY(folder_id) REFERENCES folders(id),
    FOREIGN KEY(duplicate_of_original_id) REFERENCES original_photos(id),
    FOREIGN KEY(duplicate_of_google_id) REFERENCES google_photos(id),
    FOREIGN KEY(new_folder_id) REFERENCES folders(id),
    CONSTRAINT files_unique_matches UNIQUE(folder_id, filename, extension)
);