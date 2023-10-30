SELECT fi.id, filepath || '/' || fi.filename || extension as fullpath
FROM google_photos fi
JOIN folders fo ON fi.folder_id = fo.id 
WHERE LOWER(extension) IN (%s);