{{ config(materialized='view') }} 
SELECT
e.full_name AS employee_name,
s.region,
SUM(f.total_sales_amount) AS total_sales,
COUNT(DISTINCT f.order_id) AS total_orders
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_employee') }} e
ON f.employeekey = e.employeekey
JOIN {{ ref('dim_store') }} s
ON f.storekey = s.storekey
GROUP BY
e.full_name,
s.region
ORDER BY total_sales DESC