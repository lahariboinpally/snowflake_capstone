{{ config(materialized='view') }}
SELECT
p.product_name,
AVG(f.stockturnoverratio) AS avg_stock_turnover
FROM {{ ref('fact_inventory') }} f
JOIN {{ ref('dim_product') }} p
ON f.productkey = p.productkey
GROUP BY p.product_name