{% materialization trino_incremental_always_merge, adapter='trino' %}
  {%- set target_relation = this -%}
  {%- set unique_key = config.get('unique_key') -%}
  {%- set sql = model.get('compiled_sql') -%}

  {% if not unique_key %}
    {% do exceptions.raise_compiler_error("unique_key must be set for trino_incremental_always_merge") %}
  {% endif %}

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

    {# Get columns from existing table for MERGE #}
    {%- set dest_columns = adapter.get_columns_in_relation(existing_relation) -%}

    {% set merge_sql %}
      MERGE INTO {{ target_relation }} AS t
      USING ({{ sql }}) AS s
      ON t.{{ unique_key }} = s.{{ unique_key }}

      -- Delete clause: Remove rows marked for deletion
      WHEN MATCHED AND s.audit_operation = 'D' THEN DELETE

      -- Update clause: Update existing rows (non-delete operations)
      WHEN MATCHED AND s.audit_operation <> 'D' THEN UPDATE SET
        {% for col in dest_columns %}t.{{ col.name }} = s.{{ col.name }}{% if not loop.last %}, {% endif %}{% endfor %}

      -- Insert clause: Add new rows (non-delete operations)
      WHEN NOT MATCHED AND s.audit_operation <> 'D' THEN INSERT (
        {% for col in dest_columns %}{{ col.name }}{% if not loop.last %}, {% endif %}{% endfor %}
      ) VALUES (
        {% for col in dest_columns %}s.{{ col.name }}{% if not loop.last %}, {% endif %}{% endfor %}
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
