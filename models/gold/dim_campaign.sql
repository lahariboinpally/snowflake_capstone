SELECT
 
/* Surrogate Key */
ROW_NUMBER() OVER (ORDER BY campaign_id) AS campaignkey,
 
/* Business Key */
campaign_id AS campaignid,
 
/* Dimension Attributes */
audience_segment AS target_audience_segment,
 
budget,
 
campaign_duration_days AS duration,
 
expected_roi AS roi,
 
start_date,
 
end_date
 
FROM {{ ref('silver_campaign_data') }}