INSERT INTO filelist_edits (id, folder_id, filename, extension, size)
SELECT * FROM filelist
WHERE id IN (%s);