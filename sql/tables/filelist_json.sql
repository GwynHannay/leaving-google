CREATE TABLE filelist_json (
    id INTEGER PRIMARY KEY,
    folder_id INTEGER NOT NULL,
    filename TEXT NOT NULL,
    extension TEXT,
    size INTEGER,
    file_id INTEGER NOT NULL,
    FOREIGN KEY(folder_id) REFERENCES folders(id),
    FOREIGN KEY(file_id) REFERENCES filelist(id)
);