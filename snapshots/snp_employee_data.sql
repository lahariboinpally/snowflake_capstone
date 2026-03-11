{% snapshot snp_employee_data %}
 
{{
config(
target_database='snowflake_capstone',
target_schema='bronze',
unique_key='employee_id',
strategy='timestamp',
updated_at='last_modified_date'
)
}}
 
SELECT *
FROM {{ ref('employee_data') }}
 
QUALIFY ROW_NUMBER() OVER(
PARTITION BY employee_id
ORDER BY last_modified_date DESC
)=1
 
{% endsnapshot %}