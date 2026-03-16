{{ config(materialized='view') }}
SELECT
p.category,
SUM(f.total_sales_amount) AS total_sales_amt
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_product') }} p
ON f.productkey = p.productkey
GROUP BY p.category
ORDER BY total_sales_amt DESC