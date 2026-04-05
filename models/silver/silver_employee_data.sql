WITH srcdata AS (
    SELECT *
    FROM {{ ref('snp_employee_data') }}
    WHERE dbt_valid_to IS NULL
),
 
clean_emp AS (
    SELECT
        --id
        {{trim_clean('employee_id')}} as employee_id,
        --names 
        {{text_clean('first_name','initcap')}} as first_name,
        {{text_clean('last_name','initcap')}} as last_name,
        {{text_clean('first_name','initcap')}} || ' ' || {{text_clean('last_name','initcap')}} as full_name,
        --job
        {{text_clean('department','initcap')}} as department,
        {{text_clean('role','initcap')}} as role,
        {{trim_clean('manager_id')}} as manager_id,
        {{text_clean('employment_status','initcap')}} as employment_status,
        --dates
        TRY_TO_DATE(hire_date) AS hire_date,
        COALESCE(
            TRY_TO_DATE(date_of_birth,'DD-MM-YYYY'),
            TRY_TO_DATE(date_of_birth,'YYYY-MM-DD'),
            TRY_TO_DATE(date_of_birth,'MM/DD/YYYY')
        ) AS date_of_birth,
        TRY_TO_DATE(last_modified_date) AS last_modified_date,
        --tenure
        case 
            when hire_date is not null 
            then DATEDIFF('year', hire_date, CURRENT_DATE)
            else null 
        end as tenure_years,
        --numbers 
        {{number('salary,0')}} as salary, performance_rating,
        {{numeric_clean('sales_target')}} as sales_target,
        {{numeric_clean('current_sales')}} as current_sales,
        --location 
        {{text_clean('education','initcap')}} as education,
        {{text_clean('work_location','initcap')}} as work_location,
        {{text_clean('city','initcap')}} as city,
        {{text_clean('state','upper')}} as state,
        {{text_clean('street','initcap')}} as street,
        {{trim_clean('zip_code')}} as zip_code, certifications,
        --meta
        dbt_scd_id,
        dbt_valid_from,
        dbt_valid_to,
        dbt_updated_at,
        --role standardization
        {{standardized_role('role')}} as standardized_role,
        --email
        {{email_clean('email')}} as email,
        {{email_validate('email')}} as valid_email,
        --phone 
        {{phone_clean('phone')}} as phone_number,
        {{phone_validate('phone')}} as valid_phone
    FROM srcdata
),
employee_sales AS (
    SELECT
        employee_id,
        COUNT(order_id) AS orders_processed,
        SUM(total_amount) AS total_sales_amount
    FROM {{ ref('silver_orders_data') }}
    GROUP BY employee_id
),
employee_final AS (
    SELECT
        e.*,
        s.orders_processed,
        s.total_sales_amount,
        --KPI 
        CASE
            WHEN e.sales_target > 0
            THEN (s.total_sales_amount / e.sales_target) * 100
            ELSE NULL
        END AS target_achievement_percentage
    FROM clean_emp e
    LEFT JOIN employee_sales s
        ON e.employee_id = s.employee_id
)
 
SELECT *
FROM employee_final