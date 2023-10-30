CREATE TABLE json_filelist (
    id INTEGER PRIMARY KEY,
    folder_id INTEGER NOT NULL,
    filename TEXT NOT NULL,
    extension TEXT,
    file_id INTEGER NOT NULL,
    FOREIGN KEY(folder_id) REFERENCES folders(id)
);