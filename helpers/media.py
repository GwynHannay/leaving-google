from helpers import filesystem, db


def match_visually():
    for records, conn in db.begin_batch_updates("get_matching_photos"):
        original_photos = list()
        google_image = None
        previous_id = 0
        db_records = list()
        for (
            google_id,
            google_filepath,
            original_id,
            original_filepath,
            rotate,
        ) in records:
            if google_image and previous_id is not google_id:
                results = compare_image_hashes(google_image, original_photos)
                for scores in results:
                    db_records.append(
                        (scores[1], scores[2], scores[3], previous_id, scores[0])
                    )
                original_photos = list()

            previous_id = google_id
            google_image = google_filepath
            original_photos.append((rotate, original_filepath, original_id))

        if google_image:
            results = compare_image_hashes(google_image, original_photos)
            for scores in results:
                db_records.append(
                    (scores[1], scores[2], scores[3], previous_id, scores[0])
                )
            db.update_with_many_vals("update_match_scores", db_records, conn)


def compare_image_hashes(google_filepath: str, original_photos: list):
    scores = list()
    diff = None
    original_hash = None
    google_hash = filesystem.get_image_hash(google_filepath)

    for rotate, filepath, photo_id in original_photos:
        if rotate == "Y":
            left_rotation_hash = filesystem.get_rotated_image_hash(filepath, 90)
            right_rotation_hash = filesystem.get_rotated_image_hash(filepath, 270)

            left_diff = google_hash - left_rotation_hash
            right_diff = google_hash - right_rotation_hash

            if left_diff < right_diff:
                diff = left_diff
                original_hash = left_rotation_hash
            else:
                diff = right_diff
                original_hash = right_rotation_hash
        else:
            original_hash = filesystem.get_image_hash(filepath)
            diff = google_hash - original_hash

            if diff > 15:
                rotated_hash = filesystem.get_rotated_image_hash(filepath, 180)
                rotated_diff = google_hash - rotated_hash

                if rotated_diff < diff:
                    diff = rotated_diff
                    original_hash = rotated_hash
        scores.append((photo_id, google_hash, original_hash, diff))

    return scores
