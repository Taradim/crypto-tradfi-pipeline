-- Macro: DuckDB S3 access (technical-directives: DuckDB local/edge)
-- Sets S3 credentials from env vars; call from on-run-start hook in dbt_project.yml

{% macro configure_s3() %}
    {% set aws_access_key_id = env_var('AWS_ACCESS_KEY_ID') %}
    {% set aws_secret_access_key = env_var('AWS_SECRET_ACCESS_KEY') %}
    {% set aws_region = env_var('AWS_DEFAULT_REGION', 'eu-west-1') %}
    
    -- Install and load required extensions
    INSTALL httpfs;
    LOAD httpfs;
    INSTALL aws;
    LOAD aws;
    
    -- Configure S3 credentials
    CREATE SECRET IF NOT EXISTS (
        TYPE S3,
        KEY_ID '{{ aws_access_key_id }}',
        SECRET '{{ aws_secret_access_key }}',
        REGION '{{ aws_region }}'
    );
{% endmacro %}
