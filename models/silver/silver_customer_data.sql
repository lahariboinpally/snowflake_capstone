WITH srcdata AS (
SELECT *
FROM {{ ref('snp_customer_data') }}
WHERE dbt_valid_to IS NULL
),
clean AS (
SELECT
--primary key
{{trim_clean('customer_id')}} as customer_id,
--name
{{text_clean('first_name','initcap')}} as first_name,
{{text_clean('last_name','initcap')}} as last_name,
{{text_clean('first_name','initcap')}} || ' ' || {{text_clean('last_name','initcap')}} as full_name,
--email 
{{email_clean('email')}} as email,
{{email_validate('email')}} as valid_email,
--phone 
{{phone_clean('phone')}} as phone_number,
{{phone_validate('phone')}} as valid_phone,
--birthdate
{{datw('birth_date')}} as birth_date,
{{datw('registration_date')}} as registration_date,
{{datw('last_purchase_date')}} as last_purchase_date,
{{datw('last_modified_date')}} as last_modified_date,
--numeric 
{{number('total_purchases',0)}} as total_purchases,
{{number('total_spend',0)}} as total_spend,
--text standardization
{{text_clean('income_bracket','upper')}} as income_bracket,
{{text_clean('loyalty_tier','upper')}} as loyalty_tier,
--boolean
try_to_boolean(marketing_opt_in) as marketing_opt_in,
--other text 
{{text_clean('occupation','initcap')}} as occupation,
{{text_clean('preferred_payment_method','initcap')}} as preferred_payment_method,
{{text_clean('preferred_communication','upper')}} as preferred_communication,
--address
{{text_clean('street','initcap')}} as street,
{{text_clean('city','initcap')}} as city,
{{text_clean('state','upper')}} as state,
{{trim_clean('zip_code')}} as zip_code,
{{text_clean('country','upper')}} as country,
--snapshot meta
dbt_scd_id, 
dbt_valid_from,
dbt_valid_to,
dbt_updated_at
FROM srcdata
),
final_clean AS (
    SELECT
        *,
        --age
        {{age_cal('birth_date')}} as customer_age,
        --segment
        {{customer_segment('customer_age')}} as customer_segment
        FROM clean 
    )
SELECT *
FROM final_clean