WITH json AS (
    SELECT DISTINCT 
        js.id, 
        js.folder_id, 
        js.filename, 
        js.extension, 
        fi.id AS file_id
    FROM filelist fi
    JOIN filelist js ON fi.filename || fi.extension = js.filename
        AND js.folder_id = fi.folder_id
        AND js.id <> fi.id
        AND js.extension = '.json'
), no_ext AS (
    SELECT DISTINCT
        fi.id, 
        fi.folder_id, 
        fi.filename, 
        fi.extension, 
        me.id AS file_id
    FROM filelist fi
    JOIN folders fo ON fi.folder_id = fo.id
    JOIN filelist me ON me.folder_id = fi.folder_id
        AND me.filename = fi.filename
        AND me.extension <> fi.extension
    LEFT JOIN json js ON js.file_id = fi.id
    WHERE fi.extension = '.json'
        AND js.id IS NULL
        AND fi.filename NOT IN ('metadata', 'print-subscriptions', 'shared_album_comments', 'user-generated-memory-titles')
        AND fi.filename NOT LIKE '%(%)'
)
INSERT INTO json_filelist
SELECT * 
FROM json
UNION
SELECT *
FROM no_ext;