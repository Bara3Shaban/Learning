{% macro dq_empty_table_check(table_ref) %}

SELECT
    'empty_table' AS issue_type
FROM {{ table_ref }}
HAVING COUNT(*) = 0

{% endmacro %}