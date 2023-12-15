CREATE TABLE google_photos (
    id INTEGER PRIMARY KEY,
    folder_id INTEGER NOT NULL,
    filename TEXT NOT NULL,
    extension TEXT,
    size INTEGER,
    hash TEXT,
    distinct_name TEXT NOT NULL,
    edit_of_id INTEGER,
    FOREIGN KEY(folder_id) REFERENCES folders(id),
    FOREIGN KEY(edit_of_id) REFERENCES google_photos(id),
    CONSTRAINT files_unique_matches UNIQUE(folder_id, filename, extension)
);