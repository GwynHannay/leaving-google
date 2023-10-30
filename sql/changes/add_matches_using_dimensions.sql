INSERT INTO matching_tags(google_id, original_id, rotate)
SELECT gt.file_id, ot.file_id,
    CASE 
        WHEN gt.height = ot.height AND gt.width = ot.width THEN 'N'
        WHEN gt.height = ot.width AND gt.width = ot.height THEN 'Y'
        ELSE NULL 
    END AS rotate
FROM google_tags gt
JOIN original_tags ot ON ot.distinct_name = gt.distinct_name
WHERE gt.make = ot.make
    AND gt.model = ot.model
    AND ((gt.height = ot.height AND gt.width = ot.width)
        OR (gt.height = ot.width AND gt.width = ot.height))
    AND gt.file_id NOT IN (SELECT google_id FROM matching_tags)
    AND ot.file_id NOT IN (SELECT original_id FROM matching_tags)
