{% macro trino__get_upsert_sql(target, source, unique_keys, dest_columns) -%}
MERGE INTO {{ target }} AS t
USING {{ source }} AS s
ON {% for uk in unique_keys %}t.{{ uk }} = s.{{ uk }}{% if not loop.last %} AND {% endif %}{% endfor %}

-- Update when matched
WHEN MATCHED THEN UPDATE SET
  {%- for col in dest_columns if col not in unique_keys %}
  t.{{ col }} = s.{{ col }}{{ "," if not loop.last }}
  {%- endfor %}

-- Insert new rows when not matched
WHEN NOT MATCHED THEN INSERT (
  {{ dest_columns | join(', ') }}
)
VALUES (
  {%- for col in dest_columns %}
  s.{{ col }}{{ "," if not loop.last }}
  {%- endfor %}
);
{%- endmacro %}

{% materialization trino_incremental_upsert, adapter='trino' %}
  {%- set target_relation = this -%}
  {%- set unique_key_config = config.get('unique_key') -%}
  {%- set sql = model.get('compiled_sql') -%}

  {% if not unique_key_config %}
    {% do exceptions.raise_compiler_error("unique_key must be set for trino_incremental_upsert") %}
  {% endif %}

  {# Handle both string and list unique_key formats #}
  {%- if unique_key_config is string -%}
    {%- set unique_keys = [unique_key_config] -%}
  {%- else -%}
    {%- set unique_keys = unique_key_config -%}
  {%- endif -%}

  {%- set existing_relation = adapter.get_relation(target_relation.database, target_relation.schema, target_relation.identifier) -%}

  {% if not existing_relation %}
    {{ log("Creating initial table...", info=True) }}

    {% call statement('main', fetch_result=False) %}
      CREATE TABLE {{ target_relation }} AS
      SELECT * FROM (
        {{ sql }}
      )
    {% endcall %}

  {% else %}
    {{ log("Performing incremental MERGE...", info=True) }}

    {# Get columns from existing relation #}
    {%- set existing_columns = adapter.get_columns_in_relation(existing_relation) -%}
    {%- set dest_columns = existing_columns | map(attribute='name') | list -%}

    {# Execute merge #}
    {% set merge_sql = trino__get_upsert_sql(target_relation, '(' ~ sql ~ ')', unique_keys, dest_columns) %}

    {% call statement('main', fetch_result=False) %}
      {{ merge_sql }}
    {% endcall %}

  {% endif %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}