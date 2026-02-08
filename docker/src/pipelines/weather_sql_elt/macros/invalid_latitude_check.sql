{% macro dq_invalid_latitude_check(table_ref) %}

SELECT
        'INVALID_LATITUDE' AS issue_type,
        COUNT(*) AS issue_count
FROM {{ table_ref }}
WHERE latitude NOT BETWEEN -90 AND 90
HAVING COUNT(*) > 0

{% endmacro %}