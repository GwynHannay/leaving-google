SELECT DISTINCT fi.id, filepath || '/' || fi.filename || fi.extension AS fullpath
FROM filelist fi
JOIN folders fo ON fi.folder_id = fo.id
LEFT JOIN excess_filelist ed ON ed.id = fi.id
WHERE fi.filename LIKE '%-edited'
    AND ed.id IS NULL;