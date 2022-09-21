{% macro get_end_timestamp() %}
least(to_timestamp('{{ var('end_date') }}'), current_timestamp())
{% endmacro %}

{% macro timebound(column_name) %}
    {% if var('end_date') == 'now' %}
    {{column_name}} between to_date('{{ var('start_date') }}', '{{var('date_format')}}') and getdate()
    {% else %}
    {{column_name}} between to_date('{{ var('start_date') }}', '{{var('date_format')}}') and  to_date('{{ var('end_date') }}', '{{var('date_format')}}')
    {% endif %}
{% endmacro %}