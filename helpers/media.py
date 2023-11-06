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
                results = compare_media(google_image, original_photos)
                print(google_image, original_photos, results)
                for scores in results:
                    db_records.append(
                        (scores[1], scores[2], previous_id, scores[0])
                    )
                original_photos = list()

            previous_id = google_id
            google_image = google_filepath
            original_photos.append((rotate, original_filepath, original_id))

        if google_image:
            results = compare_media(google_image, original_photos)
            print(google_image, original_photos, results)
            for scores in results:
                db_records.append(
                    (scores[1], scores[2], previous_id, scores[0])
                )
            # db.update_with_many_vals("update_match_scores", db_records, conn)


def compare_media(google_filepath: str, original_photos: list):
    scores = list()
    google_file = filesystem.get_media_contents(google_filepath)
    for rotate, filepath, photo_id in original_photos:
        original_file = filesystem.get_media_contents(filepath)

        similarity_sum_one = 0
        similarity_sum_two = 0
        frame_count = 0
        while True:
            google_retval, google_frame = google_file.read()
            original_retval, original_frame = original_file.read()

            if not google_retval or not original_retval:
                break

            similarity_one, similarity_two = compare_frames(google_frame, original_frame, rotate)
            similarity_sum_one += similarity_one
            similarity_sum_two += similarity_two
            frame_count += 1
            print(similarity_one, similarity_two, similarity_sum_one, similarity_sum_two, frame_count)
        
        if frame_count > 0:
            scores.append((photo_id, similarity_sum_one / frame_count, similarity_sum_two / frame_count))
        else:
            scores.append((photo_id, 0, 0))
        
        original_file.release()

    google_file.release()
    return scores


def compare_frames(google_frame, original_frame, rotate: str):
    original_frames = list()
    google_resized = filesystem.resize_frame(google_frame)
    google_grayscale = filesystem.convert_to_grayscale(google_resized)

    if rotate == 'Y':
        original_frames.append(filesystem.rotate_frame_left(original_frame))
        original_frames.append(filesystem.rotate_frame_right(original_frame))
    else:
        original_frames.append(original_frame)
        original_frames.append(filesystem.rotate_frame_180(original_frame))
    
    similarity_scores = list()
    for frame in original_frames:
        original_resized = filesystem.resize_frame(frame)
        original_grayscale = filesystem.convert_to_grayscale(original_resized)
        similarity_scores.append(filesystem.get_similarity(google_grayscale, original_grayscale))

    return similarity_scores


# def compare_image_hashes(google_filepath: str, original_photos: list):
#     scores = list()
#     diff = None
#     original_hash = None
#     google_hash = filesystem.get_image_hash(google_filepath)

#     for rotate, filepath, photo_id in original_photos:
#         if rotate == "Y":
#             left_rotation_hash = filesystem.get_rotated_image_hash(filepath, 90)
#             right_rotation_hash = filesystem.get_rotated_image_hash(filepath, 270)

#             left_diff = google_hash - left_rotation_hash
#             right_diff = google_hash - right_rotation_hash

#             if left_diff < right_diff:
#                 diff = left_diff
#                 original_hash = left_rotation_hash
#             else:
#                 diff = right_diff
#                 original_hash = right_rotation_hash
#         else:
#             original_hash = filesystem.get_image_hash(filepath)
#             diff = google_hash - original_hash
#             print("First compare:", diff)

#             if diff > 15:
#                 rotated_hash = filesystem.get_rotated_image_hash(filepath, 180)
#                 rotated_diff = google_hash - rotated_hash
#                 print("Second compare:", rotated_diff)

#                 if rotated_diff < diff:
#                     diff = rotated_diff
#                     original_hash = rotated_hash
#         scores.append((photo_id, str(google_hash), str(original_hash), diff))

#     return scores
