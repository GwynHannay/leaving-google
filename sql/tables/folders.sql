CREATE TABLE folders (
    id INTEGER PRIMARY KEY,
    filepath TEXT NOT NULL UNIQUE,
    indexed TEXT
);