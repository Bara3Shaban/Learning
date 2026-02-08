{% macro dq_invalid_longitude(table_ref) %}

SELECT
    'invalid_longitude' AS issue_type
FROM {{ table_ref }}
WHERE longitude NOT BETWEEN -180 AND 180
LIMIT 1

{% endmacro %}