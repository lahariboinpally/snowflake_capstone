WITH srcdata AS (
SELECT *
FROM {{ ref('orders_data') }}
),
clean AS (
SELECT
    --IDs
    {{trim_clean('order_id')}} as order_id,
    {{trim_clean('product_id')}} as product_id,
    --numeric cleaning
    {{numeric_clean('quantity')}} as quantity,
    {{numeric_clean('unit_price')}} as unit_price,
    {{numeric_clean('cost_price')}} as cost_price,
    {{numeric_clean('item_discount')}} as discount_amount
,
    --calcualtions
    quantity*unit_price as item_total_amount,
    quantity*cost_price as item_cost
FROM srcdata
)
SELECT *
FROM clean