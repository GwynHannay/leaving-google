CREATE TABLE takeout_files (
    id INTEGER PRIMARY KEY,
    filename TEXT NOT NULL,
    filepath TEXT NOT NULL UNIQUE,
    size INTEGER,
    unzipped TEXT
);