WITH srcdata AS (
    SELECT *
    FROM {{ ref('snp_store_data') }}
    WHERE dbt_valid_to IS NULL
),
clean AS (
    SELECT
        {{ text_clean('region','upper') }} AS region,
        {{ email_clean('email') }} AS email,
        {{ email_validate('email') }} AS valid_email,
        {{ phone_clean('phone_number') }} AS phone_cleaned,
        {{ phone_validate("REGEXP_REPLACE(phone_number,'[^0-9]','')") }} AS valid_phone,
        {{ number('employee_count',0) }} AS employee_count,
        {{ number('size_sq_ft',0) }} AS size_sq_ft,
        {{ trim_clean('store_id') }} AS store_id,
        {{ trim_clean('manager_id') }} AS manager_id,
        {{ text_clean('store_name','initcap') }} AS store_name,
        {{ text_clean('store_type','initcap') }} AS store_type,
        {{ number('current_sales',0) }} AS current_sales,
        {{ number('sales_target',0) }} AS sales_target,
        {{ number('monthly_rent',0) }} AS monthly_rent,
        COALESCE(TRY_TO_BOOLEAN(is_active), FALSE) AS is_active,
        case
            WHEN {{ number('size_sq_ft') }} < 5000 THEN 'Small'
            WHEN {{ number('size_sq_ft') }} BETWEEN 5000 AND 10000 THEN 'Medium'
            WHEN {{ number('size_sq_ft') }} > 10000 THEN 'Large'
            ELSE 'Unknown'
        end as store_size_category,
        {{ datw('opening_date') }} AS opening_date,
        {{ timestamp('last_modified_date') }} AS last_modified_date,
        {{ age_cal('opening_date') }} AS store_age_years,
        CASE
            WHEN sales_target > 0
            THEN ROUND({{ divide('current_sales','sales_target') }} * 100, 2)
            ELSE NULL
        END AS sales_target_achievement_percentage,
        CASE
            WHEN size_sq_ft > 0
            THEN {{ divide('current_sales','size_sq_ft') }}
            ELSE NULL
        END AS revenue_per_sq_ft,
        CASE
            WHEN employee_count > 0
            THEN {{ divide('current_sales','employee_count') }}
            ELSE NULL
        END AS employee_efficiency,
        CASE
            WHEN sales_target > 0
                 AND ({{ divide('current_sales','sales_target') }} * 100) < 90
            THEN 'Underperforming'
            ELSE 'Normal'
        END AS performance_flag,
        {{ text_clean('weekday_hours','initcap') }} AS weekday_hours,
        {{ text_clean('weekend_hours','initcap') }} AS weekend_hours,
        {{ text_clean('holiday_hours','initcap') }} AS holiday_hours,
        {{ text_clean('services','initcap') }} AS services,
        {{ text_clean('street','initcap') }} AS street,
        {{ text_clean('city','initcap') }} AS city,
        {{ text_clean('state','upper') }} AS state,
        {{ text_clean('country','upper') }} AS country,
        {{ trim_clean('zip_code') }} AS zip_code,
        dbt_valid_from,
        dbt_valid_to,
        dbt_updated_at
    FROM srcdata
)
SELECT * FROM clean