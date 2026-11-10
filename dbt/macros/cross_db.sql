{#
  Cross-database helpers so the same models run on DuckDB (local stand-in)
  and BigQuery. `payload` holds the event payload as JSON text.
  The BigQuery branches are UNVERIFIED against a real project; they are
  syntax-checked in CI by compiling with the bigquery adapter and parsing
  the result as BigQuery SQL.
#}

{% macro json_str(column, path) %}{{ return(adapter.dispatch('json_str', 'ecomm')(column, path)) }}{% endmacro %}
{% macro duckdb__json_str(column, path) %}json_extract_string({{ column }}, '$.{{ path }}'){% endmacro %}
{% macro bigquery__json_str(column, path) %}json_value({{ column }}, '$.{{ path }}'){% endmacro %}

{% macro money_type() %}{{ return(adapter.dispatch('money_type', 'ecomm')()) }}{% endmacro %}
{% macro duckdb__money_type() %}decimal(18, 2){% endmacro %}
{% macro bigquery__money_type() %}numeric{% endmacro %}

{% macro int_type() %}{{ return(adapter.dispatch('int_type', 'ecomm')()) }}{% endmacro %}
{% macro duckdb__int_type() %}bigint{% endmacro %}
{% macro bigquery__int_type() %}int64{% endmacro %}

{% macro json_money(column, path) %}cast({{ json_str(column, path) }} as {{ money_type() }}){% endmacro %}
{% macro json_int(column, path) %}cast({{ json_str(column, path) }} as {{ int_type() }}){% endmacro %}
{% macro json_bool(column, path) %}cast({{ json_str(column, path) }} as boolean){% endmacro %}

{# Timestamps are ISO-8601 UTC strings like 2026-11-10T10:15:30.000Z #}
{% macro json_ts(column, path) %}{{ return(adapter.dispatch('json_ts', 'ecomm')(column, path)) }}{% endmacro %}
{% macro duckdb__json_ts(column, path) %}strptime({{ json_str(column, path) }}, '%Y-%m-%dT%H:%M:%S.%fZ'){% endmacro %}
{% macro bigquery__json_ts(column, path) %}timestamp({{ json_str(column, path) }}){% endmacro %}

{#
  Explode a JSON array into one row per element. Renders a complete join
  clause; `alias` is a JSON value readable with json_str(alias, 'field').
#}
{% macro cross_join_json_array(column, path, alias) %}{{ return(adapter.dispatch('cross_join_json_array', 'ecomm')(column, path, alias)) }}{% endmacro %}
{% macro duckdb__cross_join_json_array(column, path, alias) %}cross join unnest(cast(json_extract({{ column }}, '$.{{ path }}') as json[])) as _{{ alias }}({{ alias }}){% endmacro %}
{% macro bigquery__cross_join_json_array(column, path, alias) %}cross join unnest(json_query_array({{ column }}, '$.{{ path }}')) as {{ alias }}{% endmacro %}
