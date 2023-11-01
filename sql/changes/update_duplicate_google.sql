UPDATE original_photos
SET duplicate_of_google_id = google_photos.id
FROM google_photos
WHERE original_photos.hash = google_photos.hash;