DELETE FROM original_photos
WHERE filename LIKE '._%'
    AND size = 4096;