WITH json AS (
	SELECT DISTINCT js.*, fi.id AS file_id
	FROM filelist fi
	JOIN filelist js ON fi.filename || fi.extension = js.filename
		AND js.folder_id = fi.folder_id
		AND js.id <> fi.id
		AND js.extension = '.json'
)
INSERT INTO json_filelist
SELECT * FROM json;