{% snapshot snp_supplier_data %}
 
{{
config(
target_database='snowflake_capstone',
target_schema='bronze',
unique_key='supplier_id',
strategy='timestamp',
updated_at='last_modified_date'
)
}}
 
SELECT *
FROM {{ ref('supplier_data') }}
 
QUALIFY ROW_NUMBER() OVER(
PARTITION BY supplier_id
ORDER BY last_modified_date DESC
)=1
 
{% endsnapshot %}