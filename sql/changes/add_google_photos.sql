INSERT INTO google_photos (folder_id, filename, extension, size, distinct_name)
SELECT * FROM filelist
WHERE LOWER(extension) IN (%s);