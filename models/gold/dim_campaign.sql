SELECT
 
--Surrogate Key
--creates hashed key where internally dbt does MD5(campaign_id)
{{ dbt_utils.generate_surrogate_key(['campaign_id']) }} AS campaignkey,
 
--Business Key
campaign_id AS campaignid,
 
--Dimension Attributes
audience_segment AS target_audience_segment,
campaign_type AS campaign_type,
campaign_name AS campaign_name,
 
budget,
 
campaign_duration_days AS duration,
 
expected_roi AS roi,
 
start_date,
 
end_date
 
FROM {{ ref('silver_campaign_data') }}