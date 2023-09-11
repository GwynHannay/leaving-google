SELECT filepath || '/' || fi.filename || extension as fullpath
FROM filelist fi
JOIN folders fo ON fi.folder_id = fo.id 
WHERE LOWER(extension) IN (%s);