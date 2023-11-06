CREATE TABLE matching_tags (
    google_id INTEGER NOT NULL,
    original_id INTEGER NOT NULL,
    rotate TEXT,
    similarity_one INTEGER,
    similarity_two INTEGER,
    FOREIGN KEY(google_id) REFERENCES google_photos(id),
    FOREIGN KEY(original_id) REFERENCES original_photos(id),
    CONSTRAINT tags_unique_matches UNIQUE(google_id, original_id)
);