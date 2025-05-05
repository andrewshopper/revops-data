/*
DROP TABLE IF EXISTS `sdp-for-analysts-platform.rev_ops_prod.raw_rep_scorecard_metrics`;

CREATE OR REPLACE EXTERNAL TABLE `sdp-for-analysts-platform.rev_ops_prod.raw_rep_scorecard_metrics`
(
  vault_team_group STRING,
  system STRING,
  metric STRING,
  points FLOAT64,
  unit STRING,
  metric_group STRING,
  metric_subgroup STRING,
  definition STRING,
  targets FLOAT64,
  denominator FLOAT64
)
OPTIONS (
  sheet_range = 'raw_rep_scorecard_metrics!A:Z',
  skip_leading_rows = 1,
  format = 'GOOGLE_SHEETS',
  uris = ['https://docs.google.com/spreadsheets/d/1AY4qpPgA40sOPlD1vtLPAiE8k4Qjbb0s_kDQiOQAV6o/edit?gid=956866071#gid=956866071']
);
*/

CREATE OR REPLACE TABLE `sdp-for-analysts-platform.rev_ops_prod.modelled_rep_scorecard`
AS 

WITH worker_snap as (
  SELECT w.worker_full_name as rep_name, LAST_DAY(DATE(w.effective_on),MONTH) as month_date, w.discipline_end as discipline, w.subdiscipline_end as subdiscipline, w.vault_team_end as vault_team, w.tenure_category, w.worker_full_name,
  case 
  -- D2C Acq
  when w.vault_team_end = 'Sales MM LA' then 'D2C MM Acquisition'
  when w.vault_team_end = 'Sales Mid Market' then 'D2C MM Acquisition'
  when w.vault_team_end = 'Sales Large' then 'D2C LA Acquisition'
  when w.vault_team_end = 'Sales SMB' then 'D2C SMB Acquisition'
  -- D2C Acc
  when w.vault_team_end = 'Sales Cross-Sell - MM' then 'D2C MM Acceleration'
  when w.vault_team_end = 'Sales Cross-Sell - MM LA' then 'D2C MM Acceleration'
  when w.vault_team_end = 'Sales Cross-Sell - SMB' then 'D2C SMB Acceleration'
  when w.vault_team_end = 'Sales Cross-Sell - Large' then 'D2C LA Acceleration'
  -- ENT & GLOBAL
  when w.vault_team_end = 'Sales Enterprise' and (user_role LIKE '%DI%' or user_role LIKE '%EN-ALL-MFG%' or user_role LIKE '%EN-ALL-FAB%') then 'ENT Emerging'
  when w.vault_team_end = 'Sales Enterprise' and NOT (user_role LIKE '%DI%' or user_role LIKE '%EN-ALL-MFG%' or user_role LIKE '%EN-ALL-FAB%') then 'ENT Established'
  when w.vault_team_end LIKE 'Sales Global%' then 'Global Accounts'
  -- Retail Acq
  when w.vault_team_end = 'Sales Retail MM LA' then 'Retail MM Acquisition'
  when w.vault_team_end = 'Sales Retail Mid Market' then 'Retail MM Acquisition'
  when w.vault_team_end = 'Sales Retail Large' then 'Retail LA Acquisition'
  when w.vault_team_end = 'Sales Retail SMB' then 'Retail SMB Acquisition'
  -- Retail Acc
  when w.vault_team_end = 'Sales Retail Cross-Sell - MM' then 'Retail MM Acceleration'
  when w.vault_team_end = 'Sales Retail Cross-Sell - MM LA' then 'Retail MM Acceleration'
  when w.vault_team_end = 'Sales Retail Cross-Sell - SMB' then 'Retail SMB Acceleration'
  when w.vault_team_end = 'Sales Retail Cross-Sell - Large' then 'Retail LA Acceleration'
  -- Lending
  when w.vault_team_end like '%Sales Lending Cross-Sell%' then 'Lending Acceleration'
  when w.vault_team_end like '%Sales Ads Cross-Sell%' then 'Ads'

  end as vault_team_group,
  ROW_NUMBER() OVER (PARTITION BY w.worker_full_name, LAST_DAY(DATE(w.effective_on),MONTH) ORDER BY w.effective_on DESC) as ranking
  FROM (SELECT * FROM `shopify-dw.people.worker_daily_snapshot` /*WHERE vault_team_end LIKE 'Sales Global%'*/) w
    LEFT JOIN `shopify-dw.people.worker_current` m ON w.managers_worker_id_end = m.worker_id
    JOIN `sdp-for-analysts-platform.rev_ops_prod.salesforce_users` u ON w.worker_id = u.employee_id AND LEFT(u.user_role,4) IN ('AMER','EMEA','APAC','LATA') AND u.source_system = 'Banff'
  WHERE DATE(w.effective_on) >= '2024-07-01'
  AND m.is_active = true
  QUALIFY ranking = 1
)

, worker as (
  SELECT w.worker_full_name as rep_name, m.worker_full_name as managers_name, 
    w.country as country, DATE_SUB(CURRENT_DATE,INTERVAL CAST(w.tenure as int) DAY) as shopify_start_date,
  CASE 
    WHEN u.user_role LIKE '%AMER%' THEN 'AMER' 
    WHEN u.user_role LIKE '%EMEA%' THEN 'EMEA' 
    WHEN u.user_role LIKE '%APAC%' THEN 'APAC' 
    WHEN u.user_role LIKE '%LATA%' THEN 'LATAM' 
    END as region,
  CASE 
    WHEN u.user_role LIKE '%AMER%' THEN 'AMER' 
    ELSE 'International'
    END as region_compare,
  u.user_role as user_rep_role
  FROM `shopify-dw.people.worker_current` w
    LEFT JOIN `shopify-dw.people.worker_current` m ON w.managers_worker_id = m.worker_id
    JOIN `sdp-for-analysts-platform.rev_ops_prod.salesforce_users` u ON w.worker_id = u.employee_id AND LEFT(u.user_role,4) IN ('AMER','EMEA','APAC','LATA') AND u.source_system = 'Banff'
    WHERE w.is_active = true
)

--1a. Emails Sent, 1b. Email Open Rate, 1c. Response Rate - Email
, emails_cte as (
  SELECT LAST_DAY(DATE(e.created_at), MONTH) as month_date, u.name as rep_name, 
  -- Email open and response metrics
  SAFE_DIVIDE(COUNT(CASE WHEN view_tracking = true then e.id end), COUNT(*)) as email_open_rate,
  SAFE_DIVIDE(COUNT(CASE WHEN counts.replies > 0 then e.id end), COUNT(*)) as email_response_rate,
  COUNT(*) as emails_sent
  -- Rate x volume metrics
  -- SUM(counts.replies) as total_emails_replied,
  -- SAFE_DIVIDE(SUM(counts.replies), COUNT(*)) as email_rate_to_volume_ratio
  FROM `shopify-dw.raw_salesloft.emails` e
  JOIN `shopify-dw.raw_salesloft.users` u ON e.user.id = u.id
GROUP BY ALL
)

--1a. Calls Sent
, calls_cte as (
  SELECT LAST_DAY(DATE(c.created_at), MONTH) as month_date, u.name as rep_name,
  -- Calls response rate metrics
  SAFE_DIVIDE(COUNT(CASE WHEN disposition = 'Connected' then c.id end), COUNT(*)) as calls_response_rate,
  COUNT(*) as calls_sent,
  -- Rate x volume metrics
  COUNT(CASE WHEN sentiment = 'Interested' then c.id end) as total_calls_interested,
  SAFE_DIVIDE(COUNT(CASE WHEN sentiment = 'Interested' then c.id end), COUNT(*)) as call_interest_rate
  FROM `shopify-dw.raw_salesloft.calls` c 
  JOIN `shopify-dw.raw_salesloft.users` u ON c.user.id = u.id
GROUP BY ALL
)

--1d. Appointments booked responses
, appts_booked_responses as (
  SELECT LAST_DAY(DATE(c.created_at), MONTH) as month_date, u.name as rep_name, 
  SAFE_DIVIDE(SUM(counts.meetings_booked), SUM(counts.people_acted_on_count)) as value 
  FROM `shopify-dw.raw_salesloft.cadences` c
  JOIN `shopify-dw.raw_salesloft.users` u ON c.owner.id = u.id
GROUP BY ALL
)

-- 1e. Show rate (needs a better primary key join because m.person.id isnt working)
, show_rate as (
  SELECT LAST_DAY(DATE(m.created_at), MONTH) as month_date, u.name as rep_name,
  SAFE_DIVIDE(COUNT(CASE WHEN no_show = false then 1 end), COUNT(*)) as value 
  FROM `shopify-dw.raw_salesloft.meetings` m
  JOIN `shopify-dw.raw_salesloft.users` u ON calendar_id = u.email
GROUP BY ALL
)

-- 1f. Average meeting time (hr)
, avg_meeting_time as (
  SELECT LAST_DAY(DATE(m.start_time), MONTH) as month_date, u.name as rep_name, 
  AVG(DATE_DIFF(end_time,start_time,MINUTE)) as value 
  FROM `shopify-dw.raw_salesloft.meetings` m
  JOIN `shopify-dw.raw_salesloft.cadences` c ON m.cadence.id = c.id
  JOIN `shopify-dw.raw_salesloft.users` u ON c.owner.id = u.id
GROUP BY ALL
)

-- 1g. Created Opps
, opps_created as (
  SELECT opportunity_owner, LAST_DAY(full_date,MONTH) as month_date, SUM(value) as value
  FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` rad
  WHERE dataset = 'Actuals' AND metric IN ('Created') AND value_type = 'Opportunity Deal Count'
  GROUP BY ALL
)

--1h. % Book Contacted
, book_contacted as (
  SELECT rep_name, LAST_DAY(snapshot_date,MONTH) as month_date, snapshot_date, SAFE_DIVIDE(COUNT(DISTINCT numerator),COUNT(DISTINCT denominator)) as value,
  ROW_NUMBER() OVER (PARTITION BY rep_name, LAST_DAY(snapshot_date,MONTH) ORDER BY snapshot_date DESC) as ranking
  FROM (
    SELECT rep.name as rep_name, acc.snapshot_date, CASE WHEN act.account_id IS NOT NULL THEN act.account_id END as numerator, acc.account_id as denominator,
    ROW_NUMBER() OVER (PARTITION BY rep.name, LAST_DAY(acc.snapshot_date,MONTH) ORDER BY acc.snapshot_date DESC) as ranking,
    FROM `sdp-for-analysts-platform.rev_ops_prod.salesforce_account_snapshots` acc
      JOIN `sdp-for-analysts-platform.rev_ops_scratch.temp_bob_reps` rep ON UPPER(acc.territory_name) = UPPER(rep.territoryname) AND rep.fq_end = LAST_DAY(acc.snapshot_date,QUARTER)
      LEFT JOIN `sdp-for-analysts-platform.rev_ops_prod.salesforce_activity` act ON acc.account_id = act.account_id AND LAST_DAY(bob_activity_date,MONTH) = LAST_DAY(acc.snapshot_date,MONTH) AND act.activity_group = 'Sales'
      WHERE d2c_tier IN ('Tier 1', 'Tier 2', 'Tier 3')
  )
  GROUP BY ALL
  QUALIFY ranking = 1
)

--1i. # Accounts Contacted
, accounts_contacted as (
  SELECT rep_name, LAST_DAY(snapshot_date,MONTH) as month_date, snapshot_date, COUNT(DISTINCT denominator) as value,
  ROW_NUMBER() OVER (PARTITION BY rep_name, LAST_DAY(snapshot_date,MONTH) ORDER BY snapshot_date DESC) as ranking
  FROM (
    SELECT rep.name as rep_name, acc.snapshot_date, CASE WHEN act.account_id IS NOT NULL THEN act.account_id END as numerator, acc.account_id as denominator,
    ROW_NUMBER() OVER (PARTITION BY rep.name, LAST_DAY(acc.snapshot_date,MONTH) ORDER BY acc.snapshot_date DESC) as ranking,
    FROM `sdp-for-analysts-platform.rev_ops_prod.salesforce_account_snapshots` acc
      JOIN `sdp-for-analysts-platform.rev_ops_scratch.temp_bob_reps` rep ON UPPER(acc.territory_name) = UPPER(rep.territoryname) AND rep.fq_end = LAST_DAY(acc.snapshot_date,QUARTER)
      JOIN `sdp-for-analysts-platform.rev_ops_prod.salesforce_activity` act ON acc.account_id = act.account_id AND LAST_DAY(bob_activity_date,MONTH) = LAST_DAY(acc.snapshot_date,MONTH) AND act.activity_group = 'Sales'
  )
  GROUP BY ALL
  QUALIFY ranking = 1
)

-- 1j. Pipe Coverage
, quotas as (
SELECT w.worker_full_name, LAST_DAY(DATE(
        CAST(SUBSTRING(periods, 3, 4) AS INT64), -- year
        CAST(SUBSTRING(periods, 8, 2) AS INT64), -- month
        1
    ),QUARTER) as month_date,
    sum(case when attributeID = 'INCREMENTAL PRODUCT PROFIT : PERIODIC : QUOTA' then quota end) as ipp_quota,
    sum(case when attributeID = 'TOTAL REVENUE : PERIODIC : QUOTA' then quota end) as ltr_quota,
    sum(case when attributeID = 'SOLUTION ACTIVATIONS : PERIODIC : QUOTA' then quota end) as sau_quota
FROM `shopify-dw.raw_varicent.attainmentdata` v
LEFT JOIN `shopify-dw.people.worker_current` w ON v.PayeeID_ = w.worker_id
where attributeID in ('INCREMENTAL PRODUCT PROFIT : PERIODIC : QUOTA', 'TOTAL REVENUE : PERIODIC : QUOTA', 'SOLUTION ACTIVATIONS : PERIODIC : QUOTA')
and worker_full_name is not null
group by all
order by worker_full_name, month_date
)

, actuals as (
select
   opportunity_owner,
   LAST_DAY(full_date,QUARTER) as month_date,
   sum(case when value_type = 'Incremental Product Profit' then value end) as ipp_actuals, 
   sum(case when value_type = 'Total Revenue' then value end) as ltr_actuals,
   sum(case when value_type = 'Solution Activations' then value end) as sau_actuals
from `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
where dataset = 'Actuals' and metric = 'Closed Won' and value_type in ('Incremental Product Profit', 'Total Revenue', 'Solution Activations')
and opportunity_owner != ''
group by all
order by 1,2
)

, open_pipe_sal as (
select
   opportunity_owner,
   LAST_DAY(full_date,QUARTER) as month_date,
   sum(case when value_type = 'Incremental Product Profit' then value end) as open_pipe_ipp_actuals, 
   sum(case when value_type = 'Total Revenue' then value end) as open_pipe_ltr_actuals,
   sum(case when value_type = 'Solution Activations' then value end) as open_pipe_sau_actuals
from `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
where dataset = 'Actuals' and metric = 'Open Pipe SAL' and value_type in ('Incremental Product Profit', 'Total Revenue', 'Solution Activations')
and opportunity_owner != ''
group by all
order by 1,2
)

, pipe_coverage as (
   select q.worker_full_name as rep_name, LAST_DAY(CURRENT_DATE,MONTH) as month_date,
   case when safe_divide(open_pipe_ipp_actuals, (ipp_quota - IFNULL(ipp_actuals,0))) < 0 or safe_divide(open_pipe_ipp_actuals, (ipp_quota - IFNULL(ipp_actuals,0))) > 10 then 10 else safe_divide(open_pipe_ipp_actuals, (ipp_quota - IFNULL(ipp_actuals,0))) end as ipp_pipe_coverage,
   case when safe_divide(open_pipe_ltr_actuals, (ltr_quota - IFNULL(ltr_actuals,0))) < 0 or safe_divide(open_pipe_ltr_actuals, (ltr_quota - IFNULL(ltr_actuals,0))) > 10 then 10 else safe_divide(open_pipe_ltr_actuals, (ltr_quota - IFNULL(ltr_actuals,0))) end as ltr_pipe_coverage,
   case when safe_divide(open_pipe_sau_actuals, (sau_quota - IFNULL(sau_actuals,0))) < 0 or safe_divide(open_pipe_sau_actuals, (sau_quota - IFNULL(sau_actuals,0))) > 10 then 10 else safe_divide(open_pipe_sau_actuals, (sau_quota - IFNULL(sau_actuals,0))) end as sau_pipe_coverage,
   open_pipe_ipp_actuals, ipp_quota,
   case when (ipp_quota - IFNULL(ipp_actuals,0)) < 0 then 0 else (ipp_quota - IFNULL(ipp_actuals,0)) end as ipp_diff,
   open_pipe_ltr_actuals, ltr_quota,
   case when (ltr_quota - IFNULL(ltr_actuals,0)) < 0 then 0 else (ltr_quota - IFNULL(ltr_actuals,0)) end as ltr_diff,
   open_pipe_sau_actuals, sau_quota
  --  case when ipp_quota - IFNULL(sau_actuals,0) < 0 then 0 else (sau_quota - IFNULL(sau_actuals,0)) end as sau_diff
   from quotas q
   left join actuals a on q.worker_full_name = a.opportunity_owner and q.month_date = a.month_date
   left join open_pipe_sal op on q.worker_full_name = op.opportunity_owner and q.month_date = op.month_date
   where q.month_date = last_day(current_date,quarter)
)

-- 2a. Total Meetings Recorded
, total_meetings as (
  select event_owner_name as rep_name, LAST_DAY(DATE(ActivityDate),MONTH) as month_date, 
count(event_id) as total_meetings, 
count(case when Call_Recording_URL__c is not null then event_id end) as total_recorded_meetings
from `sdp-for-analysts-platform.rev_ops_prod.modelled_andrew_yu_rtx`
where event_owner_name is not null and event_owner_name != 'Unknown'
group by all
)

-- 2b. Talk-to-Meeting Conversion Rate
, talk_to_meeting_conv_rate as (
  SELECT LAST_DAY(DATE(c.created_at), MONTH) as month_date, u.name as rep_name,
  SUM(duration/60000) / sum(o.value) as value
  FROM `shopify-dw.raw_salesloft.conversations` c
  JOIN `shopify-dw.raw_salesloft.users` u ON c.user_guid = CAST (u.guid as string)
  JOIN opps_created o ON u.name = o.opportunity_owner and LAST_DAY(DATE(c.created_at), MONTH) = o.month_date
GROUP BY ALL
)

-- 2c. Multi vs Single Yr Deals - Open Pipe
, multi_year_term_length_open_pipe as (
  SELECT opportunity_owner, LAST_DAY(full_date,MONTH) as month_date, 
  SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN cast(term_length as float64) > 1 THEN opportunity_id END),COUNT(DISTINCT opportunity_id)) as value
  FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` rad
  WHERE dataset = 'Actuals' AND metric IN ('Open Pipe SAL') AND value_type = 'Opportunity Deal Count'
  AND term_length is not null
  GROUP BY ALL
)

-- 2d. # of Demos Scheduled
, demos_scheduled as (
  SELECT opportunity_owner, LAST_DAY(full_date,MONTH) as month_date, SUM(value) as value
  FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` rad
  WHERE dataset = 'Activity' AND metric IN ('Activity Subtype Count') AND value_type IN ('Demo')
  GROUP BY ALL
)

-- 2e. Follow-Up Rate
, activity_counts AS (
    SELECT distinct
        sa.opportunity_id,
        sa.opportunity_owner_name,
        sa.owner_name,
        sa.activity_type,
        MIN(DATE_TRUNC(bob_activity_date, MONTH)) as first_activity_month,
        COUNT(*) as activity_count
    FROM `sdp-for-analysts-platform.rev_ops_prod.salesforce_activity` sa
    WHERE 
        sa.opportunity_id IS NOT NULL
        AND opportunity_owner_name IS NOT NULL
        AND sa.activity_type IN ('Call', 'Meeting')
        AND opportunity_owner_name = sa.owner_name
    GROUP BY ALL
)

, follow_up_rate as (
  SELECT distinct
    opportunity_owner_name as opportunity_owner,
    LAST_DAY(first_activity_month) as month_date,
    COUNT(CASE WHEN activity_count > 1 THEN 1 END) / COUNT(*) as value
FROM activity_counts
GROUP BY ALL
ORDER BY opportunity_owner_name, month_date
)

-- 2f. Stage Progression - Envision
, stage_progression_envision as (
select owner_name as opportunity_owner,LAST_DAY(DATE(envision_at), MONTH) as month_date,
CASE 
WHEN AVG(date_diff(solution_at, envision_at, day)) > 0 then SAFE_DIVIDE(1, AVG(date_diff(solution_at, envision_at, day))) 
else AVG(date_diff(solution_at, envision_at, day)) 
end as days_in_envision
from `shopify-dw.mart_revenue_data.sales_funnel_accumulating_facts` fun
left join `sdp-for-analysts-platform.rev_ops_prod.salesforce_opportunities` o
on fun.opportunity_id = o.opportunity_id
where owner_name is not null
group by all
)

-- 2f. Stage Progression - Solution
, stage_progression_solution as (
select LAST_DAY(DATE(solution_at), MONTH) as month_date, owner_name as opportunity_owner,
CASE 
WHEN AVG(date_diff(demonstrate_at, solution_at, day)) > 0 then SAFE_DIVIDE(1, AVG(date_diff(demonstrate_at, solution_at, day)))
else AVG(date_diff(demonstrate_at, solution_at, day))
end as days_in_solution
from `shopify-dw.mart_revenue_data.sales_funnel_accumulating_facts` fun
left join `sdp-for-analysts-platform.rev_ops_prod.salesforce_opportunities` o
on fun.opportunity_id = o.opportunity_id
where owner_name is not null
group by all
)

-- 2f. Stage Progression - Demonstrate
, stage_progression_demonstrate as (
select LAST_DAY(DATE(demonstrate_at), MONTH) as month_date, owner_name as opportunity_owner,
CASE 
WHEN AVG(date_diff(deal_craft_at, demonstrate_at, day)) > 0 then SAFE_DIVIDE(1, AVG(date_diff(deal_craft_at, demonstrate_at, day)))
else AVG(date_diff(deal_craft_at, demonstrate_at, day))
end as days_in_demonstrate
from `shopify-dw.mart_revenue_data.sales_funnel_accumulating_facts` fun
left join `sdp-for-analysts-platform.rev_ops_prod.salesforce_opportunities` o
on fun.opportunity_id = o.opportunity_id
where owner_name is not null
group by all
)

-- 2f. Stage Progression - Deal Craft
, stage_progression_deal_craft as (
select LAST_DAY(DATE(deal_craft_at), MONTH) as month_date, owner_name as opportunity_owner,
CASE 
WHEN AVG(date_diff(closed_at, deal_craft_at, day)) > 0 then SAFE_DIVIDE(1, AVG(date_diff(closed_at, deal_craft_at, day)))
else AVG(date_diff(closed_at, deal_craft_at, day))
end as days_in_deal_craft
from `shopify-dw.mart_revenue_data.sales_funnel_accumulating_facts` fun
left join `sdp-for-analysts-platform.rev_ops_prod.salesforce_opportunities` o
on fun.opportunity_id = o.opportunity_id
where owner_name is not null
group by all
)

-- 2g. % of Pipe w/ Next steps
, next_steps as (
  SELECT opportunity_owner, LAST_DAY(full_date,MONTH) as month_date, 
  SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN next_step is not null THEN opportunity_id END),COUNT(DISTINCT opportunity_id)) as value
  FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` rad
  WHERE dataset = 'Actuals' AND metric IN ('Open Pipe') AND value_type = 'Opportunity Deal Count' AND full_date >= CURRENT_DATE
  GROUP BY ALL
)

-- 2h. Average Age - Open Pipe
, avg_age as (
   SELECT opportunity_owner, LAST_DAY(qualified_date,MONTH) as month_date,
  CASE 
    WHEN AVG(DATE_DIFF(current_date(), qualified_date, day)) > 0 then SAFE_DIVIDE(1, AVG(DATE_DIFF(current_date(), qualified_date, day)))
    else AVG(DATE_DIFF(current_date(), qualified_date, day))
  end as value
  -- AVG(DATE_DIFF(current_date(), qualified_date, day)) as value
  FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` rad
  WHERE dataset = 'Actuals' AND metric IN ('Open Pipe SAL') AND value_type = 'Opportunity Deal Count'
  GROUP BY ALL
)

-- 3a. Win Rate
, winrate as (
  SELECT opportunity_owner, LAST_DAY(full_date,MONTH) as month_date, SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN metric = 'Closed Won' THEN opportunity_id END),COUNT(DISTINCT opportunity_id)) as winrate
  FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` rad
  WHERE dataset = 'Actuals' AND metric IN ('Closed Won','Closed Lost') AND value_type = 'Opportunity Deal Count'
  GROUP BY ALL
)

-- 3c. Cycle Length
, cycle_length as (
select owner_name as opportunity_owner, 
LAST_DAY(DATE(opportunity_closed_date), MONTH) as month_date,
CASE 
WHEN AVG(date_diff(date(opportunity_closed_date), date(envision_at), day)) > 0 then SAFE_DIVIDE(1, AVG(date_diff(date(opportunity_closed_date), date(envision_at), day)))
else AVG(date_diff(date(opportunity_closed_date), date(envision_at), day))
end as days_from_envision_to_closed
from `shopify-dw.mart_revenue_data.sales_funnel_accumulating_facts` fun
left join `sdp-for-analysts-platform.rev_ops_prod.salesforce_opportunities` o
on fun.opportunity_id = o.opportunity_id
where owner_name is not null
and stage_name like '%Closed%'
group by all
)

-- 3d. Unified Commerce Exposure, 3e. Partner
, uc_partner_exposure as (
  SELECT 
    rad.opportunity_owner,
    LAST_DAY(rad.full_date, MONTH) as month_date,
    -- UC Exposure calculation
    SAFE_DIVIDE(
      COUNT(DISTINCT CASE WHEN rad.is_unified_commerce = true THEN rad.opportunity_id END),
      COUNT(DISTINCT rad.opportunity_id)
    ) as uc_exposure_value,
    -- Partner Attach calculation
    SAFE_DIVIDE(
      COUNT(DISTINCT CASE WHEN sopr.opportunity_id IS NOT NULL THEN rad.opportunity_id END),
      COUNT(DISTINCT rad.opportunity_id)
    ) as partner_attach_value
  FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` rad
  -- Left join for partner relationships
  LEFT JOIN (
    SELECT DISTINCT opportunity_id
    FROM `shopify-dw.sales.sales_opportunity_partner_relationships_v1`
    WHERE is_coselling_partner = TRUE
  ) sopr ON rad.opportunity_id = sopr.opportunity_id
  WHERE 
    rad.dataset = 'Actuals'
    AND rad.metric IN ('Closed Won')
    AND rad.value_type = 'Opportunity Deal Count'
  GROUP BY 
    rad.opportunity_owner,
    month_date
)

-- , uc_exposure as (
-- select opportunity_owner, LAST_DAY(full_date,MONTH) as month_date,
-- SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN is_unified_commerce = true THEN opportunity_id END), COUNT(DISTINCT opportunity_id)) as value
-- from `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
-- WHERE dataset = 'Actuals' AND metric IN ('Closed Won') AND value_type = 'Opportunity Deal Count'
-- GROUP BY ALL
-- )

-- -- 3e. Partner attach
-- , partner_attach1 as (
--   SELECT opportunity_owner, opportunity_id, LAST_DAY(full_date,MONTH) as month_date
--   -- sf_close_date, sf_created_date, lead_qualified_date, lead_converted_date, qualified_date
--   FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` rad
--   WHERE true
--   AND dataset = 'Actuals' 
--   AND metric IN ('Closed Won') 
--   AND value_type = 'Opportunity Deal Count'
-- )

-- , partner_attach2 as (
-- SELECT
-- partner_attach1.*,
-- CASE WHEN b.opportunity_id IS NULL THEN FALSE ELSE TRUE END AS is_partner_attached_opp
-- FROM partner_attach1
-- LEFT JOIN (#all opportunities where a partner was flagged as a co-seller
-- SELECT distinct opportunity_id
-- FROM `shopify-dw.sales.sales_opportunity_partner_relationships_v1` 
-- WHERE is_coselling_partner = TRUE) b
-- ON partner_attach1.opportunity_id = b.opportunity_id
-- )

-- , partner_attach as (
-- select 
-- opportunity_owner, month_date,
-- count(case when is_partner_attached_opp = true then 1 end) / count(*) as value
-- from partner_attach2
-- group by all
-- )

-- 3g. Multi vs Single Yr Deals - Closed Won
, multi_year_term_length_closed_won as (
  SELECT opportunity_owner, LAST_DAY(full_date,MONTH) as month_date, 
  SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN cast(term_length as float64) > 1 THEN opportunity_id END),COUNT(DISTINCT opportunity_id)) as value
  FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` rad
  WHERE dataset = 'Actuals' AND metric IN ('Closed Won') AND value_type = 'Opportunity Deal Count'
  AND term_length is not null
  GROUP BY ALL
)

-- 3h. Contract Length
, term_length as (
  SELECT opportunity_owner, LAST_DAY(full_date,MONTH) as month_date, round(avg(cast(term_length as float64)),2) as value
  FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` rad
  WHERE dataset = 'Actuals' AND metric IN ('Closed Won') AND value_type = 'Opportunity Deal Count'
  GROUP BY ALL
)

-- 3i, 3j, 3k, 3l Product Mix Capital, Payments
, product_mix AS (
  SELECT 
    opportunity_owner,
    LAST_DAY(full_date, MONTH) as month_date,
    SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN product_name = 'Capital' THEN opportunity_id END),
                COUNT(DISTINCT opportunity_id)) as capital_product_mix,
    SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN product_name = 'Payments' THEN opportunity_id END),
                COUNT(DISTINCT opportunity_id)) as payments_product_mix,
    SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN product_name = 'Retail Payments' THEN opportunity_id END),
                COUNT(DISTINCT opportunity_id)) as retail_payments_product_mix,
    SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN product_name = 'Installments' THEN opportunity_id END),
                COUNT(DISTINCT opportunity_id)) as installments_product_mix
  FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` rad
  WHERE dataset = 'Actuals' 
    AND metric IN ('Closed Won') 
    AND value_type = 'Product Deal Count'
  GROUP BY 1, 2
)

-- , capital_product_mix as (
--   SELECT opportunity_owner, LAST_DAY(full_date,MONTH) as month_date, 
--   SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN product_name = 'Capital' THEN opportunity_id END),COUNT(DISTINCT opportunity_id)) as capital_product_mix
--   FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` rad
--   WHERE dataset = 'Actuals' AND metric IN ('Closed Won') AND value_type = 'Product Deal Count'
--   GROUP BY ALL
-- )

-- -- 3j. Product Mix Payments
-- , payments_product_mix as (
--   SELECT opportunity_owner, LAST_DAY(full_date,MONTH) as month_date, 
--   SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN product_name = 'Payments' THEN opportunity_id END),COUNT(DISTINCT opportunity_id)) as payments_product_mix
--   FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` rad
--   WHERE dataset = 'Actuals' AND metric IN ('Closed Won') AND value_type = 'Product Deal Count'
--   GROUP BY ALL
-- )

-- -- 3k. Product Mix Retail Payments
-- , retail_payments_product_mix as (
--   SELECT opportunity_owner, LAST_DAY(full_date,MONTH) as month_date, 
--   SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN product_name = 'Retail Payments' THEN opportunity_id END),COUNT(DISTINCT opportunity_id)) as retail_payments_product_mix
--   FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` rad
--   WHERE dataset = 'Actuals' AND metric IN ('Closed Won') AND value_type = 'Product Deal Count'
--   GROUP BY ALL
-- )

-- -- 3l. Product Mix Installments
-- , installments_product_mix as (
--   SELECT opportunity_owner, LAST_DAY(full_date,MONTH) as month_date, 
--   SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN product_name = 'Installments' THEN opportunity_id END),COUNT(DISTINCT opportunity_id)) as installments_product_mix
--   FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` rad
--   WHERE dataset = 'Actuals' AND metric IN ('Closed Won') AND value_type = 'Product Deal Count'
--   GROUP BY ALL
-- )

-- 3m. Attainment
, attainment_quotas as (
SELECT w.worker_full_name, LAST_DAY(DATE(
        CAST(SUBSTRING(periods, 3, 4) AS INT64), -- year
        CAST(SUBSTRING(periods, 8, 2) AS INT64), -- month
        1
    ),MONTH) as month_date,
    sum(case when attributeID = 'INCREMENTAL PRODUCT PROFIT : PERIODIC : QUOTA' then quota end) as ipp_quota,
    sum(case when attributeID = 'TOTAL REVENUE : PERIODIC : QUOTA' then quota end) as ltr_quota,
    sum(case when attributeID = 'SOLUTION ACTIVATIONS : PERIODIC : QUOTA' then quota end) as sau_quota
FROM `shopify-dw.raw_varicent.attainmentdata` v
LEFT JOIN `shopify-dw.people.worker_current` w ON v.PayeeID_ = w.worker_id
where attributeID in ('INCREMENTAL PRODUCT PROFIT : PERIODIC : QUOTA', 'TOTAL REVENUE : PERIODIC : QUOTA', 'SOLUTION ACTIVATIONS : PERIODIC : QUOTA')
and worker_full_name is not null
group by all
order by worker_full_name, month_date
)

, closed_won_actuals as (
select
   opportunity_owner,
   LAST_DAY(full_date,MONTH) as month_date,
   sum(case when value_type = 'Incremental Product Profit' then value end) as closed_won_ipp_actuals, 
   sum(case when value_type = 'Total Revenue' then value end) as closed_won_ltr_actuals,
   sum(case when value_type = 'Solution Activations' then value end) as closed_won_sau_actuals
from `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
where dataset = 'Actuals' and metric = 'Closed Won' and value_type in ('Incremental Product Profit', 'Total Revenue', 'Solution Activations')
and opportunity_owner != ''
group by all
order by 1,2
)

, attainment as (
   select q.worker_full_name as rep_name, q.month_date,
    safe_divide(closed_won_ipp_actuals, ipp_quota) ipp_attainment,
    safe_divide(closed_won_ltr_actuals, ltr_quota) ltr_attainment,
    safe_divide(closed_won_sau_actuals, sau_quota) sau_attainment
   from attainment_quotas q
   -- left join actuals a on q.worker_full_name = a.opportunity_owner and q.month_date = a.month_date
   left join closed_won_actuals op on q.worker_full_name = op.opportunity_owner and q.month_date = op.month_date
   -- where q.month_date = last_day(current_date,month)
)

-- 3n. Pacing
-- , pacing_quotas as (
-- SELECT w.worker_full_name, LAST_DAY(DATE(
--         CAST(SUBSTRING(periods, 3, 4) AS INT64), -- year
--         CAST(SUBSTRING(periods, 8, 2) AS INT64), -- month
--         1
--     ),QUARTER) as month_date,
--     sum(case when attributeID = 'INCREMENTAL PRODUCT PROFIT : PERIODIC : QUOTA' then quota end) as ipp_quota,
--     sum(case when attributeID = 'TOTAL REVENUE : PERIODIC : QUOTA' then quota end) as ltr_quota,
--     sum(case when attributeID = 'SOLUTION ACTIVATIONS : PERIODIC : QUOTA' then quota end) as sau_quota
-- FROM `shopify-dw.raw_varicent.attainmentdata` v
-- LEFT JOIN `shopify-dw.people.worker_current` w ON v.PayeeID_ = w.worker_id
-- where attributeID in ('INCREMENTAL PRODUCT PROFIT : PERIODIC : QUOTA', 'TOTAL REVENUE : PERIODIC : QUOTA', 'SOLUTION ACTIVATIONS : PERIODIC : QUOTA')
-- and worker_full_name is not null
-- group by all
-- order by worker_full_name, month_date
-- )

-- , pacing_actuals as (
-- select
--    opportunity_owner,
--    LAST_DAY(full_date,QUARTER) as actuals_month_date,
--    sum(case when value_type = 'Incremental Product Profit' then value end) as ipp_actuals, 
--    sum(case when value_type = 'Total Revenue' then value end) as ltr_actuals,
--    sum(case when value_type = 'Solution Activations' then value end) as sau_actuals
-- from `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
-- where dataset = 'Actuals' and metric = 'Closed Won' and value_type in ('Incremental Product Profit', 'Total Revenue', 'Solution Activations')
-- and opportunity_owner != ''
-- group by all
-- order by 1,2
-- )
, pacing as (
  SELECT q.*, a.* EXCEPT (month_date), SAFE_DIVIDE((total_days_in_quarter - days_remaining_in_quarter), total_days_in_quarter) * 100 as percent_of_quarter_complete, LAST_DAY(CURRENT_DATE,MONTH) as pacing_month_date,
SAFE_DIVIDE(ipp_actuals, (ipp_quota * (total_days_in_quarter - days_remaining_in_quarter) / total_days_in_quarter)) as ipp_pacing,
SAFE_DIVIDE(ltr_actuals, (ltr_quota * (total_days_in_quarter - days_remaining_in_quarter) / total_days_in_quarter)) as ltr_pacing,
SAFE_DIVIDE(sau_actuals, (sau_quota * (total_days_in_quarter - days_remaining_in_quarter) / total_days_in_quarter)) as sau_pacing
FROM 
(
SELECT *,
    DATE_DIFF(LAST_DAY(CURRENT_DATE, QUARTER), CURRENT_DATE, DAY) as days_remaining_in_quarter,
    DATE_DIFF(LAST_DAY(CURRENT_DATE, QUARTER), 
              DATE_TRUNC(CURRENT_DATE, QUARTER), DAY) + 1 as total_days_in_quarter
FROM quotas
) q
left join actuals a on q.worker_full_name = a.opportunity_owner and q.month_date = a.month_date
   where q.month_date = last_day(current_date,quarter)
)

-- 99a. 25Q1 Pipe Coverage & Open Pipe LTR/IPP
, actuals_25q1 as (
select
   opportunity_owner,
   LAST_DAY(full_date,QUARTER) as month_date,
   sum(case when value_type = 'Incremental Product Profit' then value end) as ipp_actuals, 
   sum(case when value_type = 'Total Revenue' then value end) as ltr_actuals
from `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
where dataset = 'Snapshots' and metric = 'Closed Won' and value_type in ('Incremental Product Profit', 'Total Revenue')
-- AND snapshot_date = '2025-01-06'
AND snapshot_date = '2025-02-03'
AND year = 2025 AND quarter = 'Q1' 
and opportunity_owner != ''
group by all
order by 1,2
)

-- 99g. 25Q1 Open Pipe LTR/IPP
, open_pipe_sal_25q1 as (
select
   opportunity_owner,
   LAST_DAY(full_date,QUARTER) as month_date,
   sum(case when value_type = 'Incremental Product Profit' then value end) as open_pipe_ipp_actuals, 
   sum(case when value_type = 'Total Revenue' then value end) as open_pipe_ltr_actuals
from `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
where dataset = 'Snapshots' and metric = 'Open Pipe SAL' and value_type in ('Incremental Product Profit', 'Total Revenue')
-- AND snapshot_date = '2025-01-06'
AND snapshot_date = '2025-02-03'
AND year = 2025 AND quarter = 'Q1' 
and opportunity_owner != ''
group by all
order by 1,2
)

, pipe_coverage_25q1 as (
   select q.worker_full_name as rep_name, q.month_date,
   case when safe_divide(open_pipe_ipp_actuals, (ipp_quota - IFNULL(ipp_actuals,0))) < 0 or safe_divide(open_pipe_ipp_actuals, (ipp_quota - IFNULL(ipp_actuals,0))) > 10 then 10 else safe_divide(open_pipe_ipp_actuals, (ipp_quota - IFNULL(ipp_actuals,0))) end as ipp_pipe_coverage,
   case when safe_divide(open_pipe_ltr_actuals, (ltr_quota - IFNULL(ipp_actuals,0))) < 0 or safe_divide(open_pipe_ltr_actuals, (ltr_quota - IFNULL(ipp_actuals,0))) > 10 then 10 else safe_divide(open_pipe_ltr_actuals, (ltr_quota - IFNULL(ipp_actuals,0))) end as ltr_pipe_coverage
   from quotas q
   left join actuals_25q1 a on q.worker_full_name = a.opportunity_owner and q.month_date = a.month_date
   left join open_pipe_sal_25q1 op on q.worker_full_name = op.opportunity_owner and q.month_date = op.month_date
   where q.month_date = '2025-03-31'
)

-- 99b. Created LTR/IPP
, opps_created_ltr_ipp as (
  SELECT opportunity_owner, LAST_DAY(full_date,MONTH) as month_date, 
    SUM(CASE WHEN value_type = 'Total Revenue' THEN value END) as value_ltr, 
    SUM(CASE WHEN value_type = 'Incremental Product Profit' THEN value END) as value_ipp
  FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` rad
  WHERE dataset = 'Actuals' AND metric IN ('Created') AND value_type IN ('Total Revenue','Incremental Product Profit')
  GROUP BY ALL
)

-- -- 99c. Dialer Calls
-- , calls_sent_total as (
--   SELECT LAST_DAY(DATE(c.created_at), MONTH) as month_date, u.name as rep_name, COUNT(*) as value
--   FROM `shopify-dw.raw_salesloft.calls` c 
--   JOIN `shopify-dw.raw_salesloft.users` u ON c.user.id = u.id
-- GROUP BY ALL
-- )

-- 99d. Meetings Accepted by Manager
, meetings_accepted_by_manager as (
  SELECT month_date, rep_name, COUNT(*) as value
  FROM (
    SELECT LAST_DAY(DATE(m.start_time), MONTH) as month_date, u.name as rep_name, attnd.email as attendee_email
    FROM `shopify-dw.raw_salesloft.meetings` m
      -- JOIN `shopify-dw.raw_salesloft.cadences` c ON m.cadence.id = c.id
      JOIN `shopify-dw.raw_salesloft.users` u ON m.calendar_id = u.email,
      UNNEST(attendees) as attnd
      LEFT JOIN `sdp-for-analysts-platform.rev_ops_prod.salesforce_users` u2 ON u.name = u2.user_name
    JOIN `shopify-dw.raw_salesloft.users` u3 ON u3.email = attnd.email
    WHERE u3.name = u2.manager_name
        AND attnd.status = 'accepted'
  )
  group by all
)

-- 99f. Recorded Calls Percentage
-- , rtx_meetings as (
-- select event_owner_name as rep_name, LAST_DAY(DATE(ActivityDate),MONTH) as month_date, 
-- count(event_id) as total_meetings, 
-- count(case when Call_Recording_URL__c is not null then event_id end) as total_recorded_meetings
-- from `sdp-for-analysts-platform.rev_ops_prod.modelled_andrew_yu_rtx`
-- where event_owner_name is not null and event_owner_name != 'Unknown'
-- group by all
-- )

-- 99h. 25Q1 Attainment IPP/LTR & 99i. 25Q1 IPP/LTR Targets
, attainment_quotas_25q1 as (
SELECT w.worker_full_name, LAST_DAY(DATE(
        CAST(SUBSTRING(periods, 3, 4) AS INT64), -- year
        CAST(SUBSTRING(periods, 8, 2) AS INT64), -- month
        1
    ),QUARTER) as month_date,
    sum(case when attributeID = 'INCREMENTAL PRODUCT PROFIT : PERIODIC : QUOTA' then quota end) as ipp_quota,
    sum(case when attributeID = 'TOTAL REVENUE : PERIODIC : QUOTA' then quota end) as ltr_quota,
    sum(case when attributeID = 'SOLUTION ACTIVATIONS : PERIODIC : QUOTA' then quota end) as sau_quota
FROM `shopify-dw.raw_varicent.attainmentdata` v
LEFT JOIN `shopify-dw.people.worker_current` w ON v.PayeeID_ = w.worker_id
where attributeID in ('INCREMENTAL PRODUCT PROFIT : PERIODIC : QUOTA', 'TOTAL REVENUE : PERIODIC : QUOTA', 'SOLUTION ACTIVATIONS : PERIODIC : QUOTA')
and worker_full_name is not null
group by all
order by worker_full_name, month_date
)

, closed_won_actuals_25q1 as (
select
   opportunity_owner,
   LAST_DAY(full_date,QUARTER) as month_date,
   sum(case when value_type = 'Incremental Product Profit' then value end) as closed_won_ipp_actuals, 
   sum(case when value_type = 'Total Revenue' then value end) as closed_won_ltr_actuals,
   sum(case when value_type = 'Solution Activations' then value end) as closed_won_sau_actuals
from `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
where dataset = 'Actuals' and metric = 'Closed Won' and value_type in ('Incremental Product Profit', 'Total Revenue', 'Solution Activations')
and opportunity_owner != ''
group by all
order by 1,2
)

, attainment_25q1 as (
   select q.worker_full_name as rep_name, LAST_DAY(CURRENT_DATE,MONTH) as month_date,
    safe_divide(closed_won_ipp_actuals, ipp_quota) ipp_attainment,
    safe_divide(closed_won_ltr_actuals, ltr_quota) ltr_attainment,
    safe_divide(closed_won_sau_actuals, sau_quota) sau_attainment
   from attainment_quotas_25q1 q
   left join closed_won_actuals_25q1 op on q.worker_full_name = op.opportunity_owner and q.month_date = op.month_date
   where q.month_date = last_day(current_date,quarter)
)

-- , open_pipe_sal_q2 as (
-- select
--    opportunity_owner,
--    LAST_DAY(full_date,QUARTER) as month_date,
--    sum(case when value_type = 'Incremental Product Profit' then value end) as open_pipe_ipp_actuals, 
--    sum(case when value_type = 'Total Revenue' then value end) as open_pipe_ltr_actuals,
--    sum(case when value_type = 'Solution Activations' then value end) as open_pipe_sau_actuals
-- from `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
-- where dataset = 'Actuals' and metric = 'Open Pipe SAL' and value_type in ('Incremental Product Profit', 'Total Revenue', 'Solution Activations')
-- and opportunity_owner != ''
-- group by all
-- order by 1,2
-- )

, rep as (
  SELECT *
  FROM (
    SELECT sub.*, 
    COALESCE(
    -- Emails
    emails_sent.emails_sent, email_open_rate.email_open_rate, email_response_rate.email_response_rate, 
    -- total_emails_replied.total_emails_replied,
    -- email_rate_to_volume_ratio.email_rate_to_volume_ratio, 
    -- Calls
    calls_sent.calls_sent, calls_response_rate.calls_response_rate,
    total_calls_interested.total_calls_interested, call_interest_rate.call_interest_rate,
    -- Other Metrics
    appts_booked_responses.value,
    opps_created.value, book_contacted.value, accounts_contacted.value, show_rate.value,
    pipe_coverage_ipp.ipp_pipe_coverage, pipe_coverage_ltr.ltr_pipe_coverage, pipe_coverage_sa.sau_pipe_coverage, 
    avg_meeting_time.value,
    total_meetings_recorded.total_recorded_meetings,
    talk_to_meeting_conv_rate.value, demos_scheduled.value, follow_up_rate.value,
    stage_progression_envision.days_in_envision, stage_progression_solution.days_in_solution,
    stage_progression_demonstrate.days_in_demonstrate, stage_progression_deal_craft.days_in_deal_craft, multi_year_term_length_open_pipe.value, avg_age.value,
    cycle_length.days_from_envision_to_closed, 
    -- UC & Partner Exposure
    uc_exposure.uc_exposure_value ,partner_attach.partner_attach_value, 
    term_length.value, winrate.winrate, multi_year_term_length_closed_won.value, 
    next_steps.value, 
    -- Product Mix
    capital_product_mix.capital_product_mix, payments_product_mix.payments_product_mix, 
    retail_payments_product_mix.retail_payments_product_mix, installments_product_mix.installments_product_mix,
    attainment_ltr.ltr_attainment, attainment_ipp.ipp_attainment, pacing_ipp.ipp_pacing, pacing_ltr.ltr_pacing, 
    attainment_sa.sau_attainment, pacing_sau.sau_pacing,
    pipe_coverage_ipp_25q1.ipp_pipe_coverage, pipe_coverage_ltr_25q1.ltr_pipe_coverage,
    opps_created_ltr.value_ltr, opps_created_ipp.value_ipp,
    ipp_quota_minus_actuals.ipp_diff, ltr_quota_minus_actuals.ltr_diff,
    -- calls_sent_total.value, 
    meetings_accepted_by_manager.value,
    -- open_pipe_ltr_25q1.open_pipe_ltr_actuals, open_pipe_ipp_25q1.open_pipe_ipp_actuals, 
    attainment_ltr_25q1.ltr_attainment, attainment_ipp_25q1.ipp_attainment, 
    attainment_sau_25q1.sau_attainment,
    meetings.total_meetings, 
    -- recorded_meetings.total_recorded_meetings,
    -- recorded_calls_percent_25q1.value,
    ipp_quotas.ipp_quota, ltr_quotas.ltr_quota, 
    -- sau_quotas.sau_quota,
    -- conversations_recorded.value,
    open_pipe_sal_ltr.open_pipe_ltr_actuals, open_pipe_sal_ipp.open_pipe_ipp_actuals
    -- open_pipe_sal_sau.open_pipe_sau_actuals
    -- closed_won_ipp_actuals_25q1.closed_won_ipp_actuals, closed_won_ltr_actuals_25q1.closed_won_ltr_actuals
    ) as value
    FROM (
      SELECT ws.discipline, ws.subdiscipline, ws.vault_team, ws.vault_team_group,
        w.country, w.region, w.region_compare, m.points, m.metric_group, m.metric_subgroup, m.metric, m.system, m.unit,
        ws.month_date, w.shopify_start_date,  
        w.managers_name, 
        w.rep_name, w.user_rep_role
      FROM worker_snap ws
        JOIN worker w ON w.rep_name = ws.rep_name
        -- CROSS JOIN `sdp-for-analysts-platform.rev_ops_prod.raw_rep_scorecard_metrics` m
        JOIN `sdp-for-analysts-platform.rev_ops_prod.raw_rep_scorecard_metrics` m ON ((ws.vault_team_group = m.vault_team_group AND m.metric_group IN ('1. Pipe Generation (out of 100)','2. Deal Management (out of 100)','3. Performance (out of 100)')) OR (m.metric_group NOT IN ('1. Pipe Generation (out of 100)','2. Deal Management (out of 100)','3. Performance (out of 100)')))
      WHERE ws.subdiscipline NOT IN ('Account Management','Incubation Sales','Revenue Operations','Program Management')
        AND ws.subdiscipline NOT LIKE 'Management%'
        AND ws.discipline NOT IN ('Legal','Talent Density','Engineering','No discipline provided','Support')
        --AND rad.metric = 'Created'
      GROUP BY ALL
    ) sub

    -- Group 1 joins
    -- Emails 1a, 1b, 1c, 1k
      LEFT JOIN emails_cte emails_sent ON emails_sent.rep_name = sub.rep_name AND emails_sent.month_date = sub.month_date AND sub.metric = '1a. Emails Sent'
      LEFT JOIN emails_cte email_open_rate ON email_open_rate.rep_name = sub.rep_name AND email_open_rate.month_date = sub.month_date AND sub.metric = '1b. Email Open Rate'
      LEFT JOIN emails_cte email_response_rate ON email_response_rate.rep_name = sub.rep_name AND email_response_rate.month_date = sub.month_date AND sub.metric = '1c. Response Rate - Email'
      -- LEFT JOIN emails_cte total_emails_replied ON total_emails_replied.rep_name = sub.rep_name AND total_emails_replied.month_date = sub.month_date AND sub.metric = '1k. Total Emails Replied'
      -- LEFT JOIN emails_cte email_rate_to_volume_ratio ON email_rate_to_volume_ratio.rep_name = sub.rep_name AND email_rate_to_volume_ratio.month_date = sub.month_date AND sub.metric = '1k. Rate x Volume - Email Rate to Volume Ratio'
    -- Calls 1a, 1c, 1k
      LEFT JOIN calls_cte calls_sent ON calls_sent.rep_name = sub.rep_name AND calls_sent.month_date = sub.month_date AND sub.metric = '1a. Calls Sent'
      LEFT JOIN calls_cte calls_response_rate ON calls_response_rate.rep_name = sub.rep_name AND calls_response_rate.month_date = sub.month_date AND sub.metric = '1c. Response Rate - Call'
      LEFT JOIN calls_cte total_calls_interested ON total_calls_interested.rep_name = sub.rep_name AND total_calls_interested.month_date = sub.month_date AND sub.metric = '1k. Total Call interested'
      LEFT JOIN calls_cte call_interest_rate ON call_interest_rate.rep_name = sub.rep_name AND call_interest_rate.month_date = sub.month_date AND sub.metric = '1k. Call Interest Rate'

      LEFT JOIN appts_booked_responses ON appts_booked_responses.rep_name = sub.rep_name AND appts_booked_responses.month_date = sub.month_date AND sub.metric = '1d. Appointments booked responses'
      LEFT JOIN show_rate ON show_rate.rep_name = sub.rep_name AND show_rate.month_date = sub.month_date AND sub.metric = '1e. Show rate'
      LEFT JOIN avg_meeting_time ON avg_meeting_time.rep_name = sub.rep_name AND avg_meeting_time.month_date = sub.month_date AND sub.metric = '1f. Average meeting time (hr)'
      LEFT JOIN opps_created ON opps_created.opportunity_owner = sub.rep_name AND opps_created.month_date = sub.month_date AND sub.metric = '1g. Created Opps'
      LEFT JOIN book_contacted ON book_contacted.rep_name = sub.rep_name AND book_contacted.month_date = sub.month_date AND sub.metric = '1h. % Book Contacted'
      LEFT JOIN accounts_contacted ON accounts_contacted.rep_name = sub.rep_name AND accounts_contacted.month_date = sub.month_date AND sub.metric = '1i. # Accounts Contacted'
      LEFT JOIN pipe_coverage pipe_coverage_ltr ON pipe_coverage_ltr.rep_name = sub.rep_name AND pipe_coverage_ltr.month_date = sub.month_date AND sub.metric = '1j. Pipe Coverage LTR'
      LEFT JOIN pipe_coverage pipe_coverage_ipp ON pipe_coverage_ipp.rep_name = sub.rep_name AND pipe_coverage_ipp.month_date = sub.month_date AND sub.metric = '1j. Pipe Coverage IPP'
      LEFT JOIN pipe_coverage pipe_coverage_sa ON pipe_coverage_sa.rep_name = sub.rep_name AND pipe_coverage_sa.month_date = sub.month_date AND sub.metric = '1j. Pipe Coverage SA'

-- Group 2 joins
    LEFT JOIN total_meetings total_meetings_recorded ON total_meetings_recorded.rep_name = sub.rep_name AND total_meetings_recorded.month_date = sub.month_date AND sub.metric = '2a. Total Meetings Recorded'
    LEFT JOIN talk_to_meeting_conv_rate ON talk_to_meeting_conv_rate.rep_name = sub.rep_name AND talk_to_meeting_conv_rate.month_date = sub.month_date AND sub.metric = '2b. Talk-to-Meeting Conversion Rate'
    LEFT JOIN multi_year_term_length_open_pipe ON multi_year_term_length_open_pipe.opportunity_owner = sub.rep_name AND multi_year_term_length_open_pipe.month_date = sub.month_date AND sub.metric = '2c. Multi vs Single Yr Deals - Open Pipe'
    LEFT JOIN demos_scheduled ON demos_scheduled.opportunity_owner = sub.rep_name AND demos_scheduled.month_date = sub.month_date AND sub.metric = '2d. # of Demos Scheduled'
    LEFT JOIN follow_up_rate ON follow_up_rate.opportunity_owner = sub.rep_name AND follow_up_rate.month_date = sub.month_date AND sub.metric = '2e. Follow-Up Rate'
    LEFT JOIN stage_progression_envision ON stage_progression_envision.opportunity_owner = sub.rep_name AND stage_progression_envision.month_date = sub.month_date AND sub.metric = '2f. Stage Progression - Envision'
    LEFT JOIN stage_progression_solution ON stage_progression_solution.opportunity_owner = sub.rep_name AND stage_progression_solution.month_date = sub.month_date AND sub.metric = '2f. Stage Progression - Solution'
    LEFT JOIN stage_progression_demonstrate ON stage_progression_demonstrate.opportunity_owner = sub.rep_name AND stage_progression_demonstrate.month_date = sub.month_date AND sub.metric = '2f. Stage Progression - Demonstrate'
    LEFT JOIN stage_progression_deal_craft ON stage_progression_deal_craft.opportunity_owner = sub.rep_name AND stage_progression_deal_craft.month_date = sub.month_date AND sub.metric = '2f. Stage Progression - Deal Craft'
    LEFT JOIN next_steps ON next_steps.opportunity_owner = sub.rep_name AND next_steps.month_date = sub.month_date AND sub.metric = '2g. % of Pipe w/ Next steps'
    LEFT JOIN avg_age ON avg_age.opportunity_owner = sub.rep_name AND avg_age.month_date = sub.month_date AND sub.metric = '2h. Average Age - Open Pipe'

-- Group by 3 joins
    LEFT JOIN winrate ON winrate.opportunity_owner = sub.rep_name AND winrate.month_date = sub.month_date AND sub.metric = '3a. Win Rate'
    LEFT JOIN cycle_length ON cycle_length.opportunity_owner = sub.rep_name AND cycle_length.month_date = sub.month_date AND sub.metric = '3c. Cycle Length'
    LEFT JOIN uc_partner_exposure uc_exposure ON uc_exposure.opportunity_owner = sub.rep_name AND uc_exposure.month_date = sub.month_date AND sub.metric = '3d. Unified Commerce Exposure'
    LEFT JOIN uc_partner_exposure partner_attach ON partner_attach.opportunity_owner = sub.rep_name AND partner_attach.month_date = sub.month_date AND sub.metric = '3e. Partner attach'
    LEFT JOIN term_length ON term_length.opportunity_owner = sub.rep_name AND term_length.month_date = sub.month_date AND sub.metric = '3h. Contract Length'
    LEFT JOIN multi_year_term_length_closed_won ON multi_year_term_length_closed_won.opportunity_owner = sub.rep_name AND multi_year_term_length_closed_won.month_date = sub.month_date AND sub.metric = '3g. Multi vs Single Yr Deals - Closed Won'
    LEFT JOIN product_mix capital_product_mix ON capital_product_mix.opportunity_owner = sub.rep_name AND capital_product_mix.month_date = sub.month_date AND sub.metric = '3i. Product Mix Capital'
    LEFT JOIN product_mix payments_product_mix ON payments_product_mix.opportunity_owner = sub.rep_name AND payments_product_mix.month_date = sub.month_date AND sub.metric = '3j. Product Mix Payments'
    LEFT JOIN product_mix retail_payments_product_mix ON retail_payments_product_mix.opportunity_owner = sub.rep_name AND retail_payments_product_mix.month_date = sub.month_date AND sub.metric = '3k. Product Mix Retail Payments'
    LEFT JOIN product_mix installments_product_mix ON installments_product_mix.opportunity_owner = sub.rep_name AND installments_product_mix.month_date = sub.month_date AND sub.metric = '3l. Product Mix Installments'
    LEFT JOIN attainment attainment_ltr ON attainment_ltr.rep_name = sub.rep_name AND attainment_ltr.month_date = sub.month_date AND sub.metric = '3m. Attainment LTR'
    LEFT JOIN attainment attainment_ipp ON attainment_ipp.rep_name = sub.rep_name AND attainment_ipp.month_date = sub.month_date AND sub.metric = '3m. Attainment IPP'
    LEFT JOIN attainment attainment_sa ON attainment_sa.rep_name = sub.rep_name AND attainment_sa.month_date = sub.month_date AND sub.metric = '3m. Attainment SA'
    LEFT JOIN pacing pacing_ipp ON pacing_ipp.worker_full_name = sub.rep_name AND pacing_ipp.pacing_month_date = sub.month_date AND sub.metric = '3n. Pacing IPP'
    LEFT JOIN pacing pacing_ltr ON pacing_ltr.worker_full_name = sub.rep_name AND pacing_ltr.pacing_month_date = sub.month_date AND sub.metric = '3n. Pacing LTR'
    LEFT JOIN pacing pacing_sau ON pacing_sau.worker_full_name = sub.rep_name AND pacing_sau.pacing_month_date = sub.month_date AND sub.metric = '3n. Pacing SA'

-- Ghost metrics for Bobby 4/3/2025
    LEFT JOIN pipe_coverage_25q1 pipe_coverage_ltr_25q1 ON pipe_coverage_ltr_25q1.rep_name = sub.rep_name AND pipe_coverage_ltr_25q1.month_date = sub.month_date AND sub.metric = '1l. LQ Pipe Coverage LTR'
    LEFT JOIN pipe_coverage_25q1 pipe_coverage_ipp_25q1 ON pipe_coverage_ipp_25q1.rep_name = sub.rep_name AND pipe_coverage_ipp_25q1.month_date = sub.month_date AND sub.metric = '1l. LQ Pipe Coverage IPP'
    LEFT JOIN opps_created_ltr_ipp opps_created_ltr ON opps_created_ltr.opportunity_owner = sub.rep_name AND opps_created_ltr.month_date = sub.month_date AND sub.metric = '99b. Created LTR'
    LEFT JOIN opps_created_ltr_ipp opps_created_ipp ON opps_created_ipp.opportunity_owner = sub.rep_name AND opps_created_ipp.month_date = sub.month_date AND sub.metric = '99b. Created IPP'
    -- LEFT JOIN calls_sent_total ON calls_sent_total.rep_name = sub.rep_name AND calls_sent_total.month_date = sub.month_date AND sub.metric = '99c. Dialer Calls'
    LEFT JOIN meetings_accepted_by_manager ON meetings_accepted_by_manager.rep_name = sub.rep_name AND meetings_accepted_by_manager.month_date = sub.month_date AND sub.metric = '2i. Meetings Accepted by Manager' --'99d. Meetings Accepted by Manager'
    LEFT JOIN pipe_coverage as ipp_quota_minus_actuals ON ipp_quota_minus_actuals.rep_name = sub.rep_name AND ipp_quota_minus_actuals.month_date = sub.month_date AND sub.metric = '99e. Targets - Actuals IPP'
    LEFT JOIN pipe_coverage as ltr_quota_minus_actuals ON ltr_quota_minus_actuals.rep_name = sub.rep_name AND ltr_quota_minus_actuals.month_date = sub.month_date AND sub.metric = '99e. Targets - Actuals LTR'
    LEFT JOIN total_meetings meetings ON meetings.rep_name = sub.rep_name AND meetings.month_date = sub.month_date AND sub.metric = '99f. Total Meetings'
    -- LEFT JOIN rtx_meetings recorded_meetings ON recorded_meetings.rep_name = sub.rep_name AND recorded_meetings.month_date = sub.month_date AND sub.metric = '2a. Total Meetings Recorded' --'99f. Total Meetings Recorded'
    -- LEFT JOIN recorded_calls_25q1 ON recorded_calls_25q1.rep_name = sub.rep_name AND recorded_calls_25q1.month_date = sub.month_date AND sub.metric = '99f. Total Calls Recorded'
    -- LEFT JOIN recorded_calls_percent_25q1 ON recorded_calls_percent_25q1.rep_name = sub.rep_name AND recorded_calls_percent_25q1.month_date = sub.month_date AND sub.metric = '99f. Recorded Calls Percentage'
    -- LEFT JOIN open_pipe_sal_25q1 open_pipe_ltr_25q1 ON open_pipe_ltr_25q1.opportunity_owner = sub.rep_name AND open_pipe_ltr_25q1.month_date = sub.month_date AND sub.metric = '99g. 25Q1 Open Pipe LTR'
    -- LEFT JOIN open_pipe_sal_25q1 open_pipe_ipp_25q1 ON open_pipe_ipp_25q1.opportunity_owner = sub.rep_name AND open_pipe_ipp_25q1.month_date = sub.month_date AND sub.metric = '99g. 25Q1 Open Pipe IPP'
    LEFT JOIN pipe_coverage open_pipe_sal_ltr ON open_pipe_sal_ltr.rep_name = sub.rep_name AND open_pipe_sal_ltr.month_date = sub.month_date AND sub.metric = '99k. Open Pipe LTR'
    LEFT JOIN pipe_coverage open_pipe_sal_ipp ON open_pipe_sal_ipp.rep_name = sub.rep_name AND open_pipe_sal_ipp.month_date = sub.month_date AND sub.metric = '99k. Open Pipe IPP'
    -- LEFT JOIN pipe_coverage open_pipe_sal_sau ON open_pipe_sal_sau.rep_name = sub.rep_name AND open_pipe_sal_sau.month_date = sub.month_date AND sub.metric = '99k. Open Pipe SAU'
    LEFT JOIN attainment_25q1 attainment_ltr_25q1 ON attainment_ltr_25q1.rep_name = sub.rep_name AND attainment_ltr_25q1.month_date = sub.month_date AND sub.metric = '99h. 25Q1 Attainment LTR'
    LEFT JOIN attainment_25q1 attainment_ipp_25q1 ON attainment_ipp_25q1.rep_name = sub.rep_name AND attainment_ipp_25q1.month_date = sub.month_date AND sub.metric = '99h. 25Q1 Attainment IPP'
    LEFT JOIN attainment_25q1 attainment_sau_25q1 ON attainment_sau_25q1.rep_name = sub.rep_name AND attainment_sau_25q1.month_date = sub.month_date AND sub.metric = '99h. 25Q1 Attainment SAU'
    LEFT JOIN pipe_coverage ipp_quotas ON ipp_quotas.rep_name = sub.rep_name AND ipp_quotas.month_date = sub.month_date AND sub.metric = '99i. IPP Targets'
    LEFT JOIN pipe_coverage ltr_quotas ON ltr_quotas.rep_name = sub.rep_name AND ltr_quotas.month_date = sub.month_date AND sub.metric = '99i. LTR Targets'
    -- LEFT JOIN pipe_coverage sau_quotas ON sau_quotas.rep_name = sub.rep_name AND sau_quotas.month_date = sub.month_date AND sub.metric = '99i. SAU Targets'
    -- LEFT JOIN conversations_recorded ON conversations_recorded.rep_name = sub.rep_name AND conversations_recorded.month_date = sub.month_date AND sub.metric = '99j. Conversations Recorded'
    -- LEFT JOIN closed_won_actuals_25q1 closed_won_ipp_actuals_25q1 ON closed_won_ipp_actuals_25q1.opportunity_owner = sub.rep_name AND closed_won_ipp_actuals_25q1.month_date = sub.month_date AND sub.metric = '99j. 25Q1 IPP Actuals'
    -- LEFT JOIN closed_won_actuals_25q1 closed_won_ltr_actuals_25q1 ON closed_won_ltr_actuals_25q1.opportunity_owner = sub.rep_name AND closed_won_ltr_actuals_25q1.month_date = sub.month_date AND sub.metric = '99j. 25Q1 LTR Actuals'
  ) sub2
  WHERE value IS NOT NULL
)

, percentile as (
  -- SELECT vault_team, metric, month_date, 
  SELECT 
  -- vault_team,
  vault_team_group, region_compare, metric, month_date, -- adding in region 2/25/2025
    MIN(value) as min_value, MAX(value) as max_value
    --,MIN(value)+((MAX(value)-MIN(value))/4) as percentile25th
    --,MIN(value)+((MAX(value)-MIN(value))/4*3) as percentile75th
    ,MAX(value)/4 as percentile25th
    ,MAX(value)/4*2 as percentile50th
    ,MAX(value)/4*3 as percentile75th
  FROM rep GROUP BY ALL
)

,final as (
  select *, 
    ROW_NUMBER() OVER (PARTITION BY rep_name, month_date, metric_group, vault_team_group ORDER BY metric) as metric_group_ranking,
    ROW_NUMBER() OVER (PARTITION BY rep_name, month_date, metric_subgroup, vault_team_group ORDER BY metric) as metric_subgroup_ranking 
    FROM 
    (
  SELECT rep.*, percentile.* EXCEPT(metric,month_date,vault_team_group,region_compare), --vault_team removed 3/19/2025
  ROUND(CASE 
    WHEN rep.value >= percentile.percentile75th THEN points
    WHEN rep.value < percentile.percentile75th AND rep.value >= percentile.percentile50th THEN points/4*3
    WHEN rep.value < percentile.percentile50th AND rep.value >= percentile.percentile25th THEN points/4*2
    WHEN rep.value < percentile.percentile25th THEN points/4
    END,0) as rep_points
    -- ,ROW_NUMBER() OVER (PARTITION BY rep.rep_name, rep.metric_group, rep.month_date, rep.vault_team_group ORDER BY rep.metric) as metric_group_ranking,
    -- ROW_NUMBER() OVER (PARTITION BY rep.rep_name, rep.month_date, rep.metric_subgroup, rep.vault_team_group ORDER BY rep.metric) as metric_subgroup_ranking
FROM rep
  LEFT JOIN percentile ON rep.vault_team_group = percentile.vault_team_group AND rep.region_compare = percentile.region_compare AND rep.metric = percentile.metric AND rep.month_date = percentile.month_date
  --LEFT JOIN points ON rep.metric = points.metric
WHERE rep.metric IS NOT NULL AND rep.vault_team NOT LIKE '%DNU%'
)
WHERE percentile75th > 0
  --AND rep.subdiscipline = 'Sales Acceleration'
)

, metric_group_aggr as (
  SELECT rep_name, vault_team_group, month_date, metric_group, SUM(rep_points) as metric_group_aggr_rep_points
  FROM final
  GROUP BY ALL
)

, metric_subgroup_aggr as (
  SELECT rep_name, vault_team_group, month_date, metric_subgroup, SUM(rep_points) as metric_subgroup_aggr_rep_points
  FROM final
  GROUP BY ALL
)

, metric_subgroup_total as (
  SELECT metric_subgroup, vault_team_group, SUM(points) as total_points
  FROM `sdp-for-analysts-platform.rev_ops_prod.raw_rep_scorecard_metrics`
  GROUP BY ALL
)

-- , final_v2 as (
  SELECT final.*, 
  metric_group_aggr.metric_group_aggr_rep_points, metric_subgroup_aggr.metric_subgroup_aggr_rep_points, metric_subgroup_total.total_points
  FROM final
    LEFT JOIN metric_group_aggr ON final.rep_name = metric_group_aggr.rep_name AND final.vault_team_group = metric_group_aggr.vault_team_group AND final.metric_group = metric_group_aggr.metric_group AND final.month_date = metric_group_aggr.month_date AND final.metric_group_ranking = 1
    LEFT JOIN metric_subgroup_aggr ON final.rep_name = metric_subgroup_aggr.rep_name AND final.vault_team_group = metric_subgroup_aggr.vault_team_group AND final.metric_subgroup = metric_subgroup_aggr.metric_subgroup AND final.month_date = metric_subgroup_aggr.month_date AND final.metric_subgroup_ranking = 1
    LEFT JOIN metric_subgroup_total ON final.metric_subgroup = metric_subgroup_total.metric_subgroup AND final.vault_team_group = metric_subgroup_total.vault_team_group AND final.metric_subgroup_ranking = 1
   
