SELECT 
    fi.id,
    fi.folder_id,
    fi.filename || fi.extension AS filepath
FROM filelist fi
JOIN folders fo ON fi.folder_id = fo.id
LEFT JOIN json_filelist js ON js.file_id = fi.id
WHERE fi.extension NOT IN ('.json', '.xmp')
    AND fi.filename LIKE '%(%)'
    AND js.id IS NULL;
