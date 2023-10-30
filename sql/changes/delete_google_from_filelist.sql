DELETE FROM filelist
WHERE id IN (SELECT id FROM google_photos);