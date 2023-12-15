WITH edits AS (
    SELECT id, filename, folder_id
    FROM filelist fi
    WHERE fi.filename LIKE '%-edited'
)
UPDATE google_photos
SET edit_of_id = fi.id
FROM filelist fi
JOIN edits e ON REPLACE(e.filename, '-edited', '') = fi.filename
    AND e.folder_id = fi.folder_id
WHERE google_photos.id = e.id;