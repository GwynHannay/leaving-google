INSERT INTO excess_filelist (id, folder_id, filename, extension, size, hash)
SELECT * FROM filelist
WHERE id IN (%s);