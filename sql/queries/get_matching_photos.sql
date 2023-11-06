SELECT 
	gp.id AS google_photo_id, 
	gfo.filepath || '/' || gp.filename || gp.extension AS google_filepath, 
	op.id AS original_photo_id, 
	ofo.filepath || '/' || op.filename || op.extension AS original_filepath,
	rotate
FROM matching_tags mt
JOIN google_photos gp ON gp.id = mt.google_id
JOIN original_photos op ON op.id = mt.original_id
JOIN folders gfo ON gfo.id = gp.folder_id
JOIN folders ofo ON ofo.id = op.folder_id
WHERE mt.google_id IN (12714, 49158, 49190)
-- 	WHERE similarity_one IS NULL
ORDER BY 2, 4;