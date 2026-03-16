{{ config(materialized='view') }}
SELECT
c.customer_segment,
COUNT(DISTINCT f.customerkey) AS total_customers,
SUM(f.total_sales_amount) AS total_sales,
AVG(f.total_sales_amount) AS avg_spend_per_order
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_customer') }} c
ON f.customerkey = c.customerkey
GROUP BY c.customer_segment
ORDER BY total_sales DESC