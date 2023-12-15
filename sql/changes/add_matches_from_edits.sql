INSERT INTO matching_tags (google_id, original_id, rotate)
SELECT DISTINCT gp.id AS google_id, mt.original_id,
    CASE 
        WHEN gt.height = ot.height AND gt.width = ot.width THEN 'N'
        WHEN gt.height = ot.width AND gt.width = ot.height THEN 'Y'
        ELSE NULL 
    END AS rotate
FROM google_photos gp
JOIN matching_tags mt ON gp.edit_of_id = mt.google_id
JOIN google_tags gt ON gt.file_id = gp.id
JOIN original_tags ot ON ot.file_id = mt.original_id
WHERE gp.id NOT IN (SELECT google_id FROM matching_tags)