{% materialization trino_incremental_always_merge, adapter='trino' %}
  {%- set target_relation = this -%}
  {%- set unique_key_list = config.get('unique_key') -%}
  {%- set sql = model.get('compiled_sql') -%}

  {% if not unique_key_list %}
    {% do exceptions.raise_compiler_error("unique_key must be set for trino_incremental_always_merge") %}
  {% endif %}

  {# Handle both string and list unique_key formats #}
  {%- if unique_key_list is string -%}
    {%- set unique_key = unique_key_list -%}
  {%- else -%}
    {%- set unique_key = unique_key_list[0] -%}
  {%- endif -%}

  {%- set existing_relation = adapter.get_relation(target_relation.database, target_relation.schema, target_relation.identifier) -%}

  {% if not existing_relation %}
    {{ log("Target table does not exist, creating initial table with full load...", info=True) }}

    {# Initial load: create table and filter out deletes #}
    {% call statement('main', fetch_result=False) %}
      CREATE TABLE {{ target_relation }} AS
      SELECT * FROM (
        {{ sql }}
      ) WHERE audit_operation <> 'D'
    {% endcall %}

    {% set build_sql = "SELECT 'Table created with initial data' as result" %}
    {% call statement('build', fetch_result=True) %}
      {{ build_sql }}
    {% endcall %}

  {% else %}
    {{ log("Target table exists, performing incremental MERGE with delete support...", info=True) }}

    {# Get columns from existing relation #}
    {%- set existing_columns = adapter.get_columns_in_relation(existing_relation) -%}
    {%- set col_list = existing_columns | map(attribute='name') | list -%}

    {# Build dynamic column lists #}
    {%- set update_cols = [] -%}
    {%- set insert_col_names = [] -%}
    {%- set insert_col_values = [] -%}

    {%- for col in col_list -%}
      {%- do update_cols.append(col ~ ' = s.' ~ col) -%}
      {%- do insert_col_names.append(col) -%}
      {%- do insert_col_values.append('s.' ~ col) -%}
    {%- endfor -%}

    {% set merge_sql %}
      MERGE INTO {{ target_relation }} t
      USING ({{ sql }}) s
      ON t.{{ unique_key }} = s.{{ unique_key }}

      -- Delete clause: Remove rows marked for deletion (only if source timestamp is newer)
      WHEN MATCHED AND s.audit_operation = 'D' AND s.audit_timestamp > t.audit_timestamp
        THEN DELETE

      -- Update clause: Update existing rows (non-delete operations, only if source timestamp is newer)
      WHEN MATCHED AND s.audit_operation <> 'D' AND s.audit_timestamp > t.audit_timestamp
        THEN UPDATE SET
          {{ update_cols | join(',\n          ') }}

      -- Insert clause: Add new rows (non-delete operations)
      WHEN NOT MATCHED AND s.audit_operation <> 'D'
        THEN INSERT (
          {{ insert_col_names | join(',\n          ') }}
        ) VALUES (
          {{ insert_col_values | join(',\n          ') }}
        )
    {% endset %}

    {% call statement('main', fetch_result=False) %}
      {{ merge_sql }}
    {% endcall %}

    {% set build_sql = "SELECT 'MERGE completed' as result" %}
    {% call statement('build', fetch_result=True) %}
      {{ build_sql }}
    {% endcall %}

  {% endif %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
