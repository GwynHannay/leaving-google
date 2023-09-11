SELECT DISTINCT fi.id, filepath || '/' || fi.filename || fi.extension AS fullpath
FROM filelist fi
JOIN folders fo ON fi.folder_id = fo.id
LEFT JOIN filelist_mp mp ON mp.id = fi.id
WHERE fi.extension = '.MP'
    AND mp.id IS NULL;