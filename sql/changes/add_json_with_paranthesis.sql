INSERT INTO json_filelist
SELECT id, folder_id, filename, extension, ? AS file_id 
FROM filelist
WHERE filename = ?
    AND folder_id = ?
    AND extension = '.json';