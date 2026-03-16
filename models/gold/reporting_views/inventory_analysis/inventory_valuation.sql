{{ config(materialized='view') }}
SELECT
p.product_name,
SUM(f.inventoryvalue) AS total_inventory_value
FROM {{ ref('fact_inventory') }} f
JOIN {{ ref('dim_product') }} p
ON f.productkey = p.productkey
GROUP BY p.product_name
ORDER BY total_inventory_value DESC
 