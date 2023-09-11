CREATE TABLE json_metadata (
    id INTEGER PRIMARY KEY,
    file_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    imageviews INTEGER,
    creationtime TEXT,
    phototakentime TEXT,
    geodata TEXT,
    geodataexif TEXT,
    url TEXT NOT NULL,
    archived INTEGER,
    favorited INTEGER,
    googlephotosorigin TEXT,
    FOREIGN KEY(file_id) REFERENCES filelist(id)
);