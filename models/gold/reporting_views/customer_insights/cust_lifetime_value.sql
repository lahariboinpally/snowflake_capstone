{{ config(materialized='view') }}
SELECT
    c.customerkey,
    c.full_name,
SUM(f.total_sales_amount) AS customer_lifetime_value,
COUNT(DISTINCT f.order_id) AS total_orders
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_customer') }} c
    ON f.customerkey = c.customerkey
GROUP BY
    c.customerkey,
    c.full_name
ORDER BY customer_lifetime_value DESC