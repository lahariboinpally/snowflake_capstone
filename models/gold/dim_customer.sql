SELECT
 
/* Surrogate Key */
ROW_NUMBER() OVER (ORDER BY customer_id, dbt_valid_from) AS customerkey,
 
/* Business Key */
customer_id,
 
/* Full Name */
first_name || ' ' || last_name AS full_name,
 
/* Contact */
valid_email AS email,
valid_phone AS phone,
 
/* Address */
city,
state,
country,
 
/* Demographics */
 
birth_date,
customer_age,
 
/* Segment */
customer_segment,
 
/* Registration */
registration_date,
 
/* SCD Type 2 tracking */
dbt_valid_from AS start_date,
dbt_valid_to AS end_date,
 
CASE
WHEN dbt_valid_to IS NULL THEN TRUE
ELSE FALSE
END AS is_current
 
FROM {{ ref('silver_customer_data') }}