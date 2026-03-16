SELECT
 
--Surrogate Key (pk)
{{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customerkey,
 
--Business Key
customer_id,
 
--Full Name
first_name || ' ' || last_name AS full_name,
 
--Email
valid_email AS email,

--Phone
valid_phone AS phone,
 
--Address Details
city,
state,
country,
 
--Demographic Information
birth_date,
customer_age,
 
--Segment
customer_segment,
 
--Registration Date
registration_date,
 
--Type 2 SCD tracking
dbt_valid_from AS start_date,
dbt_valid_to AS end_date,
 
CASE
WHEN dbt_valid_to IS NULL THEN TRUE   --if null latest record
ELSE FALSE  --if not historical record
END AS is_current
 
FROM {{ ref('silver_customer_data') }}