SELECT id FROM filelist 
WHERE filename = ?
    AND folder_id = ?
    AND extension = '.json';