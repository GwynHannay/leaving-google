CREATE TABLE excess_filelist (
    id INTEGER PRIMARY KEY,
    folder_id INTEGER NOT NULL,
    filename TEXT NOT NULL,
    extension TEXT,
    size INTEGER,
    hash TEXT,
    FOREIGN KEY(folder_id) REFERENCES folders(id)
);