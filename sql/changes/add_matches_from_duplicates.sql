INSERT INTO matching_tags (google_id, original_id, rotate)
SELECT DISTINCT mt.google_id, op.id AS original_id, mt.rotate
FROM original_photos op
JOIN matching_tags mt ON op.duplicate_of_original_id = mt.original_id
WHERE op.id NOT IN (SELECT original_id FROM matching_tags)