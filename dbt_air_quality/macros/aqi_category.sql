{% macro aqi_category(column_name) %}
case
  when {{ column_name }} is null then 'Unknown'
  when {{ column_name }} <= 50 then 'Good'
  when {{ column_name }} <= 100 then 'Moderate'
  when {{ column_name }} <= 150 then 'Unhealthy for Sensitive Groups'
  when {{ column_name }} <= 200 then 'Unhealthy'
  when {{ column_name }} <= 300 then 'Very Unhealthy'
  else 'Hazardous'
end
{% endmacro %}
