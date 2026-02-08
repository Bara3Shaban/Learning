 WITH issues AS (

    SELECT
        'EMPTY_TABLE' AS issue_type,
        COUNT(*) AS issue_count
    FROM `learning-486315.staging.weather`
    HAVING COUNT(*) = 0

    UNION ALL

    SELECT
        'NULL_OBSERVATION_TS' AS issue_type,
        COUNT(*) AS issue_count
    FROM `learning-486315.staging.weather`
    WHERE observation_ts IS NULL
    HAVING COUNT(*) > 0

    UNION ALL

    SELECT
        'INVALID_LATITUDE' AS issue_type,
        COUNT(*) AS issue_count
    FROM `learning-486315.staging.weather`
    WHERE latitude NOT BETWEEN -90 AND 90
    HAVING COUNT(*) > 0

    UNION ALL

    SELECT
        'INVALID_LONGITUDE' AS issue_type,
        COUNT(*) AS issue_count
    FROM `learning-486315.staging.weather`
    WHERE longitude NOT BETWEEN -180 AND 180
    HAVING COUNT(*) > 0

    UNION ALL

    SELECT
        'DUPLICATE_OBSERVATIONS' AS issue_type,
        COUNT(*) AS issue_count
    FROM (
        SELECT
            city,
            observation_ts,
            COUNT(*) AS cnt
        FROM `learning-486315.staging.weather`
        GROUP BY city, observation_ts
        HAVING COUNT(*) > 1
    )
)

SELECT 
CASE
WHEN COUNT(*) = 0 then 1
ELSE
0
end
FROM issues;