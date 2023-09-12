SELECT DISTINCT
    filepath || '/' || fi.filename || fi.extension as json_path,
    filepath || '/' || xmp.filename || xmp.extension as xmp_path
FROM json_filelist fi
JOIN filelist me ON me.id = fi.file_id
JOIN folders fo ON fi.folder_id = fo.id 
JOIN filelist xmp ON xmp.folder_id = fo.id
    AND xmp.filename = me.filename || me.extension
    AND xmp.extension = '.xmp';