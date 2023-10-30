WITH dupes AS (
    SELECT hash, MAX(id) AS id
    FROM filelist
    GROUP BY hash
    HAVING COUNT(DISTINCT id) > 1
)
UPDATE original_photos
SET duplicate_of_original_id = dupes.id
WHERE hash = dupes.hash
    AND id <> dupes.id;