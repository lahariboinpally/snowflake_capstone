WITH srcdata AS (
    SELECT *
    FROM {{ ref('snp_supplier_data') }}
    WHERE dbt_valid_to IS NULL
),
clean_base AS (
    SELECT
        {{ trim_clean('supplier_id') }} AS supplier_id,
        {{ text_clean('supplier_name','initcap') }} AS supplier_name,
        {{ text_clean('supplier_type','upper') }} AS supplier_type,
        {{ trim_clean('tax_id') }} AS tax_id,
        {{ text_clean('website','lower') }} AS website,
        {{ number('year_established') }} AS year_established,
        {{ text_clean('credit_rating','upper') }} AS credit_rating,
        COALESCE(TRY_TO_BOOLEAN(is_active), FALSE) AS is_active,
        {{ datw('last_modified_date') }} AS last_modified_date,
        {{ datw('last_order_date') }} AS last_order_date,
        {{ number('lead_time_days') }} AS lead_time_days,
        {{ number('minimum_order_quantity',0) }} AS minimum_order_quantity,
        {{ text_clean('payment_terms','upper') }} AS payment_terms,
        {{ text_clean('preferred_carrier','initcap') }} AS preferred_carrier,
        {{ email_clean('contact_email') }} AS contact_email,
        {{ email_validate('contact_email') }} AS valid_contact_email,
        {{ phone_clean('contact_phone') }} AS phone_cleaned,
        {{ trim_clean('contract_id') }} AS contract_id,
        {{ datw('contract_start_date') }} AS contract_start_date,
        {{ datw('contract_end_date') }} AS contract_end_date,
        COALESCE(TRY_TO_BOOLEAN(exclusivity), FALSE) AS exclusivity,
        COALESCE(TRY_TO_BOOLEAN(renewal_option), FALSE) AS renewal_option,
        {{ number('average_delay_days') }} AS average_delay_days,
        {{ number('defect_rate') }} AS defect_rate,
        {{ number('on_time_delivery_rate') }} AS on_time_delivery_rate,
        {{ text_clean('quality_rating','initcap') }} AS quality_rating,
        {{ number('response_time_hours') }} AS response_time_hours,
        {{ number('returns_percentage') }} AS returns_percentage,
        {{ text_clean('categories_supplied','lower') }} AS categories_supplied,
        {{ text_clean('address','initcap') }} AS address,
        {{ text_clean('contact_person','initcap') }} AS contact_person,
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    FROM srcdata
),
final_cleaned AS (
    SELECT
        *,
        {{ phone_validate('phone_cleaned') }} AS valid_phone
    FROM clean_base
)
SELECT *
FROM final_cleaned