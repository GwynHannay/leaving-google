SELECT 
    filepath || '/' || fi.filename || fi.extension AS fullpath,
    fi.extension
FROM filelist fi
JOIN folders fo ON fi.folder_id = fo.id
LEFT JOIN filelist js ON js.folder_id = fo.id
    AND js.filename = fi.filename || fi.extension
    AND js.extension = '.json'
WHERE fi.extension NOT IN ('.json', '.xmp')
    AND js.id IS NULL
ORDER BY 2;

SELECT 
    filepath || '/' || js.filename || js.extension AS fullpath,
    js.extension
FROM filelist js
JOIN folders fo ON js.folder_id = fo.id
LEFT JOIN filelist fi ON fi.folder_id = fo.id
    AND fi.filename || fi.extension = js.filename
    AND fi.extension <> '.json'
WHERE js.extension = '.json'
    AND fi.id IS NULL
    AND js.filename NOT IN ('metadata', 'print-subscriptions', 'shared_album_comments', 'user-generated-memory-titles')
ORDER BY 2;

SELECT 
   fi.filename,
	fi.extension
FROM filelist fi
JOIN folders fo ON fi.folder_id = fo.id
LEFT JOIN filelist js ON js.folder_id = fo.id
	AND js.filename || js.extension = fi.filename
	AND js.extension <> '.json'
WHERE fi.extension = '.json'
	AND js.id IS NULL
ORDER BY 2;