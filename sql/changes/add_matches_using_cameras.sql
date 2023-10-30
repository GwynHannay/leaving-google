INSERT INTO matching_tags (google_id, original_id)
SELECT gt.file_id, ot.file_id
FROM google_tags gt
JOIN original_tags ot ON ot.distinct_name = gt.distinct_name
WHERE gt.make = ot.make
    AND gt.model = ot.model
    AND gt.file_id NOT IN (SELECT google_id FROM matching_tags)
    AND ot.file_id NOT IN (SELECT original_id FROM matching_tags)
