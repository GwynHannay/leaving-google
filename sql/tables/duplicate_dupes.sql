CREATE TABLE duplicate_dupes (
    id INTEGER PRIMARY KEY,
    duplicate_filepath TEXT NOT NULL UNIQUE,
    filehash TEXT NOT NULL,
    size INTEGER
);