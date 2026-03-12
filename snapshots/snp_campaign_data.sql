{% snapshot snp_campaign_data %}
 
{{
config(
target_database='snowflake_capstone',
target_schema='bronze',
unique_key='campaign_id',
strategy='timestamp',
updated_at='last_modified_date'
)
}}
--strategy and unique_key is used for updating the inc records 
SELECT *
FROM {{ ref('campaign_data') }}
 
QUALIFY ROW_NUMBER() OVER(
PARTITION BY campaign_id
ORDER BY last_modified_date DESC
)=1
 
{% endsnapshot %}