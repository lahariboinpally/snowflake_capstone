SELECT
 
/* Surrogate Key */
ROW_NUMBER() OVER (ORDER BY product_id) AS productkey,
 
/* Business Key */
product_id,
 
/* Product Details */
product_name,
category,
subcategory,
brand,
 
/* Attributes */
color,
size,
 
/* Pricing */
unit_price,
cost_price,
 
/* Supplier Information */
supplier_id
 
FROM {{ ref('silver_product_data') }}