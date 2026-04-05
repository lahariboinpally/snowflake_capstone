WITH ord_items AS (
    SELECT *
    FROM {{ ref('silver_order_items') }}
),
--aggregation
ord_items_agg AS (
    SELECT
    order_id,
    COUNT(product_id) AS total_items,
    SUM(quantity) AS total_quantity,
    SUM(item_total_amount) AS calculated_total_amount,
    SUM(item_cost) AS total_cost,
    SUM(discount_amount) AS total_discount
    FROM ord_items
    GROUP BY order_id
),
--header cleaning
ord_header AS (
    SELECT DISTINCT
        {{trim_clean('order_id')}} as order_id,
        {{trim_clean('customer_id')}} as customer_id,
        {{trim_clean('employee_id')}} as employee_id,
        {{trim_clean('store_id')}} as store_id,
        {{trim_clean('campaign_id')}} as campaign_id,
        {{text_clean('order_status','initcap')}} as order_status,
        {{text_clean('order_source','initcap')}} as order_source,
        {{text_clean('payment_method','initcap')}} as payment_method,
        {{timestamp('order_date')}} as order_date,
        {{timestamp('created_at')}} as created_at,
        {{datw('delivery_date')}} as delivery_date,
        {{datw('estimated_delivery_date')}} as estimated_delivery_date,
        {{datw('shipping_date')}} as shipping_date,
        {{numeric_clean('tax_amount')}} as tax_amount,
        {{numeric_clean('shipping_cost')}} as shipping_cost,
        {{numeric_clean('order_discount')}} as order_discount,
        {{numeric_clean('total_amount')}} as total_amount
    FROM {{ ref('orders_data') }}
),
--join
ord_joined AS (
    SELECT
        h.*,
        a.total_items,
        a.total_quantity,
        a.calculated_total_amount,
        a.total_cost,
        a.total_discount
        FROM ord_header h
        LEFT JOIN ord_items_agg a
        ON h.order_id = a.order_id
),
--profit calculation
profit_cal AS (
    SELECT
        *,
        {{profit('total_amount','total_cost','total_discount','shipping_cost','tax_amount')}} as profit_amount,
        case 
            when total_amount>0 then 
                {{profit('total_amount','total_cost','total_discount','shipping_cost','tax_amount')}}/nullif(total_amount,0)
            else null 
        END AS profit_margin_percentage
    FROM ord_joined
),
ord_time AS (
SELECT
    *,
    {{order_time('order_date')}} as order_time_of_day
    FROM profit_cal
),
date_parts AS (
    SELECT
        *,
        DATE_PART(week, order_date) AS order_week,
        DATE_PART(month, order_date) AS order_month,
        DATE_PART(quarter, order_date) AS order_quarter,
        DATE_PART(year, order_date) AS order_year
    FROM ord_time
),
shipping_metrics AS (
    SELECT
        *,
        DATEDIFF(day, order_date, shipping_date) AS processing_days,
        DATEDIFF(day, shipping_date, delivery_date) AS shipping_days,
        {{delivery_status('delivery_date','estimated_delivery_date')}} as delivery_status
    FROM date_parts
)
SELECT *
FROM shipping_metrics