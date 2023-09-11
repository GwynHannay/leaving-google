WITH json AS (
	SELECT DISTINCT js.*, fi.id AS file_id
    FROM filelist js
    JOIN folders fo ON fo.id = js.folder_id
    JOIN filelist fi ON fi.folder_id = fo.id
        AND fi.filename || fi.extension = ?
    WHERE js.id = ?
)
INSERT INTO filelist_json
SELECT * FROM json;