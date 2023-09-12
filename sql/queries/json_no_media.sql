SELECT 
    fi.id,
    filepath || '/' || fi.filename || fi.extension AS fullpath
FROM filelist fi
JOIN folders fo ON fi.folder_id = fo.id
LEFT JOIN json_filelist js ON js.file_id = fi.id
WHERE fi.extension = '.json'
    AND js.id IS NULL
    AND fi.filename NOT IN ('metadata', 'print-subscriptions', 'shared_album_comments', 'user-generated-memory-titles')
    AND fi.filename NOT LIKE '%(%)';
