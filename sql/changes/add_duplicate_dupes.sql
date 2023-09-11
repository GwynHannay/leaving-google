WITH dupes AS (
    SELECT filehash, MAX(id) AS id
    FROM duplicate_photos
    GROUP BY filehash
    HAVING COUNT(DISTINCT id) > 1
)
INSERT INTO duplicate_dupes
SELECT dp.id, dp.duplicate_filepath, dp.filehash, dp.size 
FROM duplicate_photos dp
JOIN dupes ON dupes.filehash = dp.filehash
	AND dp.id <> dupes.id;