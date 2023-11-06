UPDATE matching_tags
SET similarity_one = ?,
    similarity_two = ?
WHERE google_id = ?
    AND original_id = ?;