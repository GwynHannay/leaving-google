UPDATE matching_tags
SET google_image_hash = ?,
    original_image_hash = ?,
    similarity = ?
WHERE google_id = ?
    AND original_id = ?;