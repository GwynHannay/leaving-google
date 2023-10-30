WITH height AS (
    SELECT file_id, NULLIF(TRIM(tag_contents), '') AS image_height, 1 AS priority
    FROM xmp_data
    WHERE tag_name IN (
        'XMP:ImageHeight', 
        'EXIF:ImageHeight', 
        'RIFF:ImageHeight', 
        'QuickTime:ImageHeight', 
        'PNG:ImageHeight', 
        'MPEG:ImageHeight', 
        'Matroska:ImageHeight', 
        'H264:ImageHeight', 
        'GIF:ImageHeight', 
        'ASF:ImageHeight', 
        'Flash:ImageHeight'
    )

    UNION
    SELECT file_id, NULLIF(TRIM(tag_contents), '') AS image_height, 2 AS priority
    FROM xmp_data
    WHERE tag_name = 'EXIF:ExifImageHeight'

    UNION
    SELECT file_id, NULLIF(TRIM(tag_contents), '') AS image_height, 3 AS priority
    FROM xmp_data
    WHERE tag_name = 'File:ImageHeight'
), sorted_height AS (
    SELECT file_id, CAST(image_height AS INTEGER) AS image_height, ROW_NUMBER() OVER(PARTITION BY file_id ORDER BY priority) AS p
    FROM height
), width AS (
    SELECT file_id, NULLIF(TRIM(tag_contents), '') AS image_width, 1 AS priority
    FROM xmp_data
    WHERE tag_name IN (
        'XMP:ImageWidth', 
        'EXIF:ImageWidth', 
        'RIFF:ImageWidth', 
        'QuickTime:ImageWidth', 
        'PNG:ImageWidth', 
        'MPEG:ImageWidth', 
        'Matroska:ImageWidth', 
        'H264:ImageWidth', 
        'GIF:ImageWidth', 
        'ASF:ImageWidth', 
        'Flash:ImageWidth'
    )

    UNION
    SELECT file_id, NULLIF(TRIM(tag_contents), '') AS image_width, 2 AS priority
    FROM xmp_data
    WHERE tag_name = 'EXIF:ExifImageWidth'

    UNION
    SELECT file_id, NULLIF(TRIM(tag_contents), '') AS image_width, 3 AS priority
    FROM xmp_data
    WHERE tag_name = 'File:ImageWidth'
), sorted_width AS (
    SELECT file_id, CAST(image_width AS INTEGER) AS image_width, ROW_NUMBER() OVER(PARTITION BY file_id ORDER BY priority) AS p
    FROM width
), make AS (
    SELECT file_id, NULLIF(TRIM(tag_contents), '') AS image_make, 1 AS priority
    FROM xmp_data
    WHERE tag_name = 'XMP:Make'

    UNION
    SELECT file_id, NULLIF(TRIM(tag_contents), '') AS image_make, 2 AS priority
    FROM xmp_data
    WHERE tag_name = 'EXIF:Make'
), sorted_make AS (
    SELECT file_id, image_make, ROW_NUMBER() OVER(PARTITION BY file_id ORDER BY priority) AS p
    FROM make
), model AS (
    SELECT file_id, NULLIF(TRIM(tag_contents), '') AS image_model, 1 AS priority
    FROM xmp_data
    WHERE tag_name = 'XMP:Model'

    UNION
    SELECT file_id, NULLIF(TRIM(tag_contents), '') AS image_model, 2 AS priority
    FROM xmp_data
    WHERE tag_name = 'EXIF:Model'
), sorted_model AS (
    SELECT file_id, image_model, ROW_NUMBER() OVER(PARTITION BY file_id ORDER BY priority) AS p
    FROM model
), focal AS (
    SELECT file_id, NULLIF(TRIM(tag_contents), '') AS focal_length
    FROM xmp_data
    WHERE tag_name = 'Composite:FocalLength35efl'
), light AS (
    SELECT file_id, NULLIF(TRIM(tag_contents), '') AS image_light
    FROM xmp_data
    WHERE tag_name = 'Composite:LightValue'
), macro AS (
    SELECT file_id, CAST(NULLIF(TRIM(tag_contents), '') AS INTEGER) AS image_macro
    FROM xmp_data
    WHERE tag_name IN ('MakerNotes:Macro', 'MakerNotes:MacroMode')
), scene AS (
    SELECT file_id, NULLIF(TRIM(tag_contents), '') AS scene_mode
    FROM xmp_data
    WHERE tag_name = 'MakerNotes:SceneMode'
)
INSERT INTO google_tags (file_id, distinct_name, make, model, height, width, focal_length, light, scene, macro)
SELECT
    sh.file_id,
    gp.distinct_name || extension AS distinct_name,
    image_make AS make,
    image_model AS model,
    image_height AS height,
    image_width AS width,
    focal_length,
    image_light AS light,
    scene_mode AS scene,
    image_macro AS macro
FROM google_photos gp
JOIN sorted_height sh ON sh.file_id = gp.id
JOIN sorted_width sw ON sw.file_id = sh.file_id
LEFT JOIN sorted_make sma ON sma.file_id = sh.file_id
    AND sma.p = 1
LEFT JOIN sorted_model smo ON smo.file_id = sh.file_id
    AND smo.p = 1
LEFT JOIN focal fo ON fo.file_id = sh.file_id
LEFT JOIN light li ON li.file_id = sh.file_id
LEFT JOIN macro mo ON mo.file_id = sh.file_id
LEFT JOIN scene sc ON sc.file_id = sh.file_id
WHERE sh.p = 1 AND sw.p = 1
