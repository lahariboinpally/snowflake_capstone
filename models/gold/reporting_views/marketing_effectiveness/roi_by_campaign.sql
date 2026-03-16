{{ config(materialized='view') }}
SELECT
c.campaign_type,
AVG(f.roi_percentage) AS avg_roi_percentage
FROM {{ ref('fact_marketingperformance') }} f
JOIN {{ ref('dim_campaign') }} c
ON f.campaignkey = c.campaignkey
GROUP BY c.campaign_type
ORDER BY avg_roi_percentage DESC
 