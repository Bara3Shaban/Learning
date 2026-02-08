{{% macro duplicate_observations_check(table_ref) %}}

SELECT
        'DUPLICATE_OBSERVATIONS' AS issue_type,
        COUNT(*) AS issue_count
FROM (
    SELECT
        city,
        observation_ts,
        COUNT(*) AS cnt
    FROM {{}}
    GROUP BY city, observation_ts
    HAVING COUNT(*) > 1
)
{{% endmacro %}}