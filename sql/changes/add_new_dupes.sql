INSERT INTO duplicate_dupes
SELECT dp.id, dp.duplicate_filepath, dp.filehash, dp.size
FROM duplicate_photos dp 
JOIN google_photos gp ON dp.filehash = gp.filehash;