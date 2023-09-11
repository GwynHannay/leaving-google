INSERT INTO filelist_json
SELECT *, ? AS file_id 
FROM filelist
WHERE filename = ?
    AND folder_id = ?
    AND extension = '.json';