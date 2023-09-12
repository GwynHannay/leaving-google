INSERT INTO excess_filelist (id, folder_id, filename, extension, size)
SELECT * FROM filelist
WHERE id IN (%s);