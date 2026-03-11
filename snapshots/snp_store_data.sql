{% snapshot snp_store_data %}
 
{{
config(
target_database='snowflake_capstone',
target_schema='bronze',
unique_key='store_id',
strategy='timestamp',
updated_at='last_modified_date'
)
}}
 
SELECT *
FROM {{ ref('store_data') }}
 
QUALIFY ROW_NUMBER() OVER(
PARTITION BY store_id
ORDER BY last_modified_date DESC
)=1
 
{% endsnapshot %}