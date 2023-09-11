DELETE FROM duplicate_photos
WHERE id IN (SELECT id FROM duplicate_dupes);