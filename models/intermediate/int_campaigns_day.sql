 -- int_campaigns_day.sql

WITH campaigns AS (
 SELECT *
 FROM {{ref('int_campaigns')}} 
),

aggregated AS (
    SELECT
        date_date,
        paid_source,
        SUM(ads_cost) AS ads_cost,
        sum(impression)    as impressions,
        sum(click)         as clicks
    from campaigns
    group by
        date_date,
        paid_source
)
select
    *
from aggregated
order by
    date_date desc,
    paid_source