WITH dupes AS (
    SELECT hash
    FROM filelist
    GROUP BY hash
    HAVING COUNT(DISTINCT id) > 1
), preference AS (
    SELECT id, ROW_NUMBER() OVER(PARTITION BY fi.hash 
        ORDER BY size) AS num
    FROM filelist fi
    JOIN dupes ON dupes.hash = fi.hash
)
SELECT fi.id, filepath || '/' || fi.filename || fi.extension AS fullpath
FROM filelist fi
JOIN folders fo ON fi.folder_id = fo.id
JOIN preference p ON p.id = fi.id
WHERE num = 1;