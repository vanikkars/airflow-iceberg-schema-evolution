{% macro trino__get_merge_sql(target, source, unique_key, dest_columns, predicates) -%}
MERGE INTO {{ target }} AS t
USING {{ source }} AS s
ON t.{{ unique_key }} = s.{{ unique_key }}

-- 🗑️ Delete rows if latest record marks them deleted
WHEN MATCHED AND s.audit_operation = 'D' THEN DELETE

-- 🔄 Update when not deleted
WHEN MATCHED AND s.audit_operation <> 'D' THEN UPDATE SET
  {%- for col in dest_columns if col not in [unique_key] %}
    t.{{ col }} = s.{{ col }}{% if not loop.last %}, {% endif %}
  {%- endfor %}

-- ➕ Insert new rows only if not deleted
WHEN NOT MATCHED AND s.audit_operation <> 'D' THEN INSERT (
  {{ dest_columns | join(', ') }}
)
VALUES (
  {%- for col in dest_columns %}
    s.{{ col }}{% if not loop.last %}, {% endif %}
  {%- endfor %});
{%- endmacro %}
