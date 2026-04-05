WITH srcdata AS (
    SELECT *
    FROM {{ ref('snp_campaign_data') }}
    WHERE dbt_valid_to IS NULL
),
clean AS (
    SELECT
    --primary key
    {{trim_clean('campaign_id')}} as campaign_id,
    --name 
    {{text_clean('campaign_name','initcap')}} as campaign_name,
    {{text_clean('campaign_type','upper')}} as campaign_type,
    {{text_clean('channel','upper')}} as channel,
    {{text_clean('description','initcap')}} as description,
    --date
    {{datw('start_date')}} as start_date,
    {{datw('end_date')}} as end_date,
    {{datw('last_modified_date')}} as last_modified_date,
    --campaign duration
    {{cal_duration(
        datw('start_date'),
        datw('end_date')
    )}} as campaign_duration_days,
    --target audience
    {{text_clean('target_audience','initcap')}} as target_audience,
    case 
        when lower(target_audience) like '%young%' then 'Young Audience'
        when lower(target_audience) like '%professional%' then 'Working Professionals'
        when lower(target_audience) like '%senior%' then 'Senior Audience'
        else 'General'
    end as audience_segment,
    --currency 
    {{numeric_clean('budget')}} as budget,
    {{numeric_clean('total_cost')}} as total_cost,
    {{numeric_clean('total_revenue')}} as total_revenue,
    --expected roi 
    {{cal_roi(
        numeric_clean('total_revenue'),
        numeric_clean('total_cost')
    )}} as expected_roi,
    --reported roi
    {{number('roi_calculation',0)}} as roi_calculation,
    --roi validation 
    case 
        when{{number('roi_calculation',0)}} is null 
            then 'Missing ROI'
        when ABS(
            {{number('roi_calculation',0)}}-
            {{cal_roi(
                numeric_clean('total_revenue'),
                numeric_clean('total_cost')
            )}}
        )<0.01
            then 'Valid'
        else 'Mismatch'
    end as roi_validation_status,
    --snapshot meta
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at
from srcdata
)
SELECT *
FROM clean