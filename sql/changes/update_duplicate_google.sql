UPDATE original_photos
SET duplicate_of_google_id = google_photos.id
WHERE hash = google_photos.hash;