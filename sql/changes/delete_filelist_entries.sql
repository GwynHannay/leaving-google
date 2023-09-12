DELETE FROM filelist
WHERE id IN (
    SELECT id FROM json_filelist
    UNION
    SELECT id FROM excess_filelist
);