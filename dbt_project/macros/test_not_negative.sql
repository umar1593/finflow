{% test not_negative(model, column_name) %}
-- Кастомный generic-тест: значения столбца не должны быть отрицательными.
-- Тест «падает», если запрос вернёт хотя бы одну строку.
select {{ column_name }}
from {{ model }}
where {{ column_name }} < 0
{% endtest %}
