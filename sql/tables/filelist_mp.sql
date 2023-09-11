CREATE TABLE filelist_mp (
    id INTEGER PRIMARY KEY,
    folder_id INTEGER NOT NULL,
    filename TEXT NOT NULL,
    extension TEXT,
    size INTEGER,
    FOREIGN KEY(folder_id) REFERENCES folders(id)
);