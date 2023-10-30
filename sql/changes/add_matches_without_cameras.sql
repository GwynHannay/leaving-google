INSERT INTO matching_tags (google_id, original_id, rotate)
SELECT gt.file_id, ot.file_id, 'N' AS rotate
FROM google_tags gt
JOIN original_tags ot ON ot.distinct_name = gt.distinct_name
WHERE gt.make IS NULL AND ot.make IS NULL
    AND gt.height = ot.height
    AND gt.width = ot.width
    AND gt.file_id NOT IN (SELECT google_id FROM matching_tags)
    AND ot.file_id NOT IN (SELECT original_id FROM matching_tags)
