SELECT folders.id, filepath FROM folders
LEFT JOIN filelist ON filelist.folder_id = folders.id 
WHERE filelist.id IS NULL;