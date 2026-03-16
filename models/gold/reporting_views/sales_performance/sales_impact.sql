{{ config(materialized='view') }}
 
SELECT
 
c.campaign_name,
 
SUM(f.total_sales_influenced) AS total_sales_generated
 
FROM {{ ref('fact_marketingperformance') }} f
 
JOIN {{ ref('dim_campaign') }} c
ON f.campaignkey = c.campaignkey
 
GROUP BY c.campaign_name
 
ORDER BY total_sales_generated DESC