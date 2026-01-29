-- Macro: S3 path per table for external materialization and Glue registration
-- Structure: s3://bucket/<layer>/<table>/<table>.parquet
-- Layer (silver/gold) is inferred from file path (models/silver/ or models/gold/)

{% macro get_external_location(model) -%}
  {%- set path = model.original_file_path | default('') -%}
  {%- set layer = 'gold' if 'gold' in path else 'silver' -%}
  {%- set table = model.name -%}
  {%- set location = 's3://' ~ var('s3_bucket_name') ~ '/' ~ layer ~ '/' ~ table ~ '/' ~ table ~ '.parquet' -%}
  {{- location -}}
{%- endmacro %}
