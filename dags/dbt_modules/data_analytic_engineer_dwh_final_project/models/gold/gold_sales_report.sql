{{
    config(
        materialized='incremental',
        incremental_strategy="insert_overwrite",
        partition_by={
            'field': 'sales_date',
            'data_type': 'DATE',
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

WITH sales_transaction_data AS (
    SELECT *
    FROM {{ ref("gold_order_items_fact") }} sales_trans
    WHERE {{ execution_date_express }} AND delivered_at IS NOT NULl
    AND (
        DATE(delivered_at) = DATE(updated_at)
        OR DATE(returned_at) = DATE(updated_at)
    )
)
SELECT DATE(updated_at) AS sales_date, category, brand, department,
    SUM(CASE WHEN returned_at IS NOT NULL THEN sale_price * -1 ELSE sale_price END) AS total_sales
FROM (
 SELECT * EXCEPT(row_num)
 FROM (
   SELECT sales_trans.updated_at, category, brand, department, sale_price, returned_at,
   ROW_NUMBER() OVER(PARTITION BY sales_trans.id ORDER BY product_dim.updated_at DESC) AS row_num
   FROM sales_transaction_data sales_trans
     LEFT JOIN {{ ref("gold_product_dimension") }} product_dim
       ON sales_trans.product_id = product_dim.id and sales_trans.created_at >= product_dim.updated_at
 )
 WHERE row_num = 1
)
GROUP BY sales_date, category, brand, department
