WITH edits AS (
    SELECT fi.*
    FROM filelist fi
    WHERE fi.filename LIKE '%-edited'
)
SELECT fi.id, filepath || '/' || fi.filename || fi.extension AS fullpath
FROM filelist fi
JOIN edits e ON REPLACE(e.filename, '-edited', '') = fi.filename
JOIN folders fo ON fi.folder_id = fo.id
LEFT JOIN excess_filelist ed ON ed.id = fi.id
WHERE e.folder_id = fi.folder_id
    AND ed.id IS NULL;