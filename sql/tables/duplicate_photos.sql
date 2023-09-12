CREATE TABLE duplicate_photos (
    id INTEGER PRIMARY KEY,
    folder_id INTEGER NOT NULL,
    filename TEXT NOT NULL,
    extension TEXT,
    size INTEGER,
    searched TEXT,
    FOREIGN KEY(folder_id) REFERENCES folders(id),
    CONSTRAINT files_unique_matches UNIQUE(folder_id, filename, extension)
);