CREATE TABLE filelist (
    id INTEGER PRIMARY KEY,
    folder_id INTEGER NOT NULL,
    filename TEXT NOT NULL,
    extension TEXT,
    size INTEGER,
    FOREIGN KEY(folder_id) REFERENCES folders(id),
    CONSTRAINT files_unique_matches UNIQUE(folder_id, filename, extension)
);