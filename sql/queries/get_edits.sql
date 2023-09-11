SELECT DISTINCT fi.id, filepath || '/' || fi.filename || fi.extension AS fullpath
FROM filelist fi
JOIN folders fo ON fi.folder_id = fo.id
WHERE filename LIKE '%-edited';