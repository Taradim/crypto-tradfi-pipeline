-- Macro to calculate ratio
-- Safely handles division by zero and null values
-- Returns ratio or NULL
-- Multiply by 100 in SQL if you need percentage

{% macro calculate_ratio(numerator, denominator) %}
    CASE
        WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL THEN NULL
        WHEN {{ numerator }} IS NULL THEN NULL
        ELSE {{ numerator }} / {{ denominator }}
    END
{% endmacro %}
