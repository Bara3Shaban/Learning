{% macro dq_null_observation_ts_check(table_ref) %}
  SELECT
        'NULL_OBSERVATION_TS' AS issue_type,
        COUNT(*) AS issue_count
    FROM {{ table_ref }}
    WHERE observation_ts IS NULL
    HAVING COUNT(*) > 0
{% endmacro %}