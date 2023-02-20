{{
    config(
        materialized='incremental',
        incremental_strategy="insert_overwrite",
        partition_by={
            'field': 'updated_at',
            'data_type': 'TIMESTAMP',
            'granularity': 'DAY'
        }
    )
}}

{% set execution_date = '' %}
{% set execution_date_express = '' %}
{% if is_incremental() %}
    {% if execute %}
            {% set incremental_date_results = run_query('SELECT FORMAT_DATE("%Y-%m-%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))') %}
            {% set incremental_date_values = incremental_date_results.columns[0].values() %}
            {% set execution_date = incremental_date_values[0] %}
            {% set execution_date_express = 'DATE(updated_at) = DATE("' + execution_date + '")' %}
    {% endif %}
{% endif %}

{% if var('execution_date','') != '' %}
    {% set execution_date = var('execution_date') %}
    {% set execution_date_express = 'DATE(updated_at) = DATE("' + execution_date + '")' %}
{% endif %}


{% if should_full_refresh() %}
    {% set execution_date_express = 'DATE(updated_at) >= "2020-01-01"' %}
{% endif %}


SELECT
    id,
    order_id,
    user_id,
    product_id,
    inventory_item_id,
    status,
    created_at,
    updated_at,
    shipped_at,
    delivered_at,
    returned_at,
    sale_price
FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY id, DATE(updated_at) ORDER BY updated_at DESC) AS row_num
    FROM {{ source("bronze", "delta_order_items") }}
    WHERE {{ execution_date_express }}
)
WHERE row_num = 1
