SELECT id, filepath
FROM takeout_files
WHERE unzipped IS NULL;