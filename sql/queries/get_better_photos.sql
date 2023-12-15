WITH all_matches AS (
	SELECT 
        gd.google_id, 
        gd.original_id, 
        gd.similarity, 
        dp.size,
		CASE WHEN gd.google_id IN (SELECT edit_of_id FROM google_photos) THEN 'Y'
			ELSE 'N'
		END AS has_edit,
		CASE WHEN gp.edit_of_id IS NOT NULL THEN 'Y'
			ELSE 'N'
		END AS is_edit,
		CASE WHEN gd.original_id IN (SELECT duplicate_of_original_id FROM original_photos) THEN 'Y'
			ELSE 'N'
		END AS has_orig_dupe,
		CASE WHEN dp.duplicate_of_google_id IS NOT NULL THEN 'Y'
			ELSE 'N'
		END AS has_goog_dupe
    FROM google_photos gp
    JOIN matching_tags gd ON gd.google_id = gp.id
    JOIN original_photos dp ON gd.original_id = dp.id
    WHERE dp.size > gp.size
		AND similarity < 15
), ranking AS (
	SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY google_id 
            ORDER BY similarity, size DESC, is_edit, has_edit DESC, has_orig_dupe DESC, has_goog_dupe
        ) AS rank,
        ROW_NUMBER() OVER (
            PARTITION BY original_id 
            ORDER BY similarity, size DESC, is_edit, has_edit DESC, has_orig_dupe DESC, has_goog_dupe
        ) AS orank
	FROM all_matches
), best_copies AS (
	SELECT google_id, original_id
	FROM ranking
	WHERE orank = 1
		AND rank = 1
)
SELECT 
	op.id, 
	fog.filepath AS google_folder,
	gp.filename || gp.extension AS google_filename, 
	foo.filepath || '/' || op.filename || op.extension AS original_filepath
FROM original_photos op
JOIN best_copies bc ON bc.original_id = op.id
JOIN google_photos gp ON bc.google_id = gp.id
JOIN folders foo ON foo.id = op.folder_id
JOIN folders fog ON fog.id = gp.folder_id
ORDER BY fog.id;