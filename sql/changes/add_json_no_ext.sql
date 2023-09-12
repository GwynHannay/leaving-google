WITH json AS (
    SELECT DISTINCT
        fi.*,
        me.id AS file_id
    FROM filelist fi
    JOIN folders fo ON fi.folder_id = fo.id
    JOIN filelist me ON me.folder_id = fi.folder_id
        AND me.filename = fi.filename
        AND me.extension <> fi.extension
    LEFT JOIN json_filelist js ON js.file_id = fi.id
    WHERE fi.extension = '.json'
        AND js.id IS NULL
        AND fi.filename NOT IN ('metadata', 'print-subscriptions', 'shared_album_comments', 'user-generated-memory-titles')
        AND fi.filename NOT LIKE '%(%)'
)
INSERT INTO json_filelist
SELECT * FROM json;