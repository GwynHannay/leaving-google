WITH dupes AS (
    SELECT hash, MAX(id) AS id
    FROM original_photos
    GROUP BY hash
    HAVING COUNT(DISTINCT id) > 1
)
UPDATE original_photos
SET duplicate_of_original_id = dupes.id
FROM dupes
WHERE original_photos.hash = dupes.hash
    AND original_photos.id <> dupes.id;