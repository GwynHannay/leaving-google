DELETE FROM filelist
WHERE id IN (
    SELECT id FROM filelist_json
    UNION
    SELECT id FROM filelist_edits
    UNION
    SELECT id FROM filelist_mp
);