WITH srcdata AS (
    SELECT *
    FROM {{ ref('snp_product_data') }}
    WHERE dbt_valid_to IS NULL
),
clean_product AS (
    SELECT
        -- PRIMARY KEY
        {{ text_clean('brand','initcap') }} AS brand,
        {{ text_clean('category','initcap') }} AS category,
        {{ text_clean('subcategory','initcap') }} AS subcategory,
        {{ text_clean('product_line','initcap') }} AS product_line,
        {{ trim_clean('supplier_id') }} AS supplier_id,
        {{ trim_clean('product_id') }} AS product_id,
        {{ text_clean('product_name','initcap') }} AS product_name,
        {{ text_clean('color','initcap') }} AS color,
        {{ text_clean('size','upper') }} AS size,
        {{ trim_clean('dimensions') }} AS dimensions,
        {{ numeric_clean('weight') }} AS weight,
        {{ trim_clean('short_description') }} AS short_description,
        {{ trim_clean('technical_specs') }} AS technical_specs,
        CONCAT(
            {{ text_clean('product_name','initcap') }},
            ' - ',
            {{ trim_clean('short_description') }},
            ' - ',
            {{ trim_clean('technical_specs') }}
        ) AS product_full_description,
        {{ number('cost_price',0) }} AS cost_price,
        {{ number('unit_price',0) }} AS unit_price,
        {{ number('stock_quantity',0) }} AS stock_quantity,
        {{ number('reorder_level',0) }} AS reorder_level,
        {{ datw('launch_date') }} AS launch_date,
        {{ timestamp('last_modified_date') }} AS last_modified_date,
        {{ trim_clean('warranty_period') }} AS warranty_period,
        COALESCE(TRY_TO_BOOLEAN(is_featured), FALSE) AS is_featured,
        dbt_valid_from,
        dbt_valid_to,
        dbt_updated_at
    FROM srcdata
)
SELECT
    *,
    CASE
        WHEN unit_price > 0
        THEN ROUND({{ divide('(unit_price - cost_price)', 'unit_price') }} * 100, 2)
        ELSE NULL
    END AS profit_margin_percentage,
    stock_quantity < reorder_level AS low_stock_flag
FROM clean_product