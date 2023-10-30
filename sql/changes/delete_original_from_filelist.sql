DELETE FROM filelist
WHERE EXISTS (
    SELECT id FROM original_photos dp
    WHERE dp.folder_id = filelist.folder_id
        AND dp.filename = filelist.filename
        AND dp.extension = filelist.extension
        AND dp.size = filelist.size
        AND dp.hash = filelist.hash
);