SELECT
 
--Surrogate Key
{{ dbt_utils.generate_surrogate_key(['product_id']) }} AS productkey,
 
--Business Key
product_id,
 
--Product Details
product_name,
category,
subcategory,
brand,
color,
size,
stock_quantity,
reorder_level,
 
--Pricing
unit_price,
cost_price,
 
--Supplier Information
supplier_id
 
FROM {{ ref('silver_product_data') }}