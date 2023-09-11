CREATE TABLE xmp_data (
    id INTEGER PRIMARY KEY,
    file_id INTEGER NOT NULL,
    tag_name TEXT NOT NULL,
    tag_contents TEXT NOT NULL,
    FOREIGN KEY(file_id) REFERENCES filelist(id)
);