CREATE TABLE original_tags (
    file_id INTEGER NOT NULL UNIQUE,
    distinct_name TEXT NOT NULL,
    make TEXT,
    model TEXT,
    height INTEGER,
    width INTEGER,
    focal_length TEXT,
    light TEXT,
    scene TEXT,
    macro INTEGER,
    FOREIGN KEY(file_id) REFERENCES original_photos(id)
);