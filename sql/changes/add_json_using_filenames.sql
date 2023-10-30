WITH json AS (
    SELECT DISTINCT 
        js.id, 
        js.folder_id, 
        js.filename, 
        js.extension, 
        fi.id AS file_id
    FROM filelist js
    JOIN folders fo ON fo.id = js.folder_id
    JOIN filelist fi ON fi.folder_id = fo.id
        AND fi.filename || fi.extension = ?
    WHERE js.id = ?
)
INSERT INTO json_filelist
SELECT * FROM json;