INSERT INTO filelist_mp (id, folder_id, filename, extension, size)
SELECT * FROM filelist
WHERE id IN (%s);