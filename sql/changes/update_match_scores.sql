UPDATE matching_tags
SET similarity = ?
WHERE google_id = ?
    AND original_id = ?;