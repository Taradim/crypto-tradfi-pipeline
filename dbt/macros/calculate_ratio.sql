-- Macro: safe ratio (numerator/denominator); returns NULL on zero or null
-- Use * 100 in SQL for percentage

{% macro calculate_ratio(numerator, denominator) %}
    CASE
        WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL THEN NULL
        WHEN {{ numerator }} IS NULL THEN NULL
        ELSE {{ numerator }} / {{ denominator }}
    END
{% endmacro %}
