CREATE OR REPLACE TABLE shopify-dw.scratch.modelled_opty_tasks
AS

WITH incubation_leads AS
(
 SELECT
  distinct
    l.OwnerId AS lead_owner_id
  -- FROM `sdp-prd-commercial.raw_salesforce_banff.from_longboat_campaign` c
  FROM `shopify-dw.base.base__salesforce_banff_campaign` c
  -- INNER JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_campaign_member` cm
  INNER JOIN `shopify-dw.base.base__salesforce_banff_campaign_member` cm
  on c.campaign_id = cm.campaign_id
  INNER JOIN `shopify-dw.raw_salesforce_banff.lead` l
  ON l.id = cm.lead_id
  where LOWER(c.name) like 'incubation-smb%'
  and l.Owner_Role__c in ('AMER-SALES-REP-INC1', 'AMER-SALES-REP-INC2')
)

, events as (
   select 
  *
-- from `sdp-prd-commercial.raw_salesforce_banff.from_longboat_events`
from `shopify-dw.raw_salesforce_banff.event`
-- from `shopify-dw.base.base__salesforce_banff_events`
where IsDeleted = false
and Activity_Type__c = 'Meeting'
)

, tasks_and_events as (
  select 
  id as task_id,
  AccountId as account_id, 
  ActivityDate as activity_date,
  ActivityDateTime as completed_date_time,
  CreatedById as created_by_id, 
  CreatedDate as created_date,
  IsDeleted as is_deleted,
  OwnerId as owner_id,
  Activity_Type__c as activity_type,
  Activity_Sub_Type__c as activity_sub_type,
  WhatId as what_id,
  SalesLoft1__Call_Disposition__c as salesloft_call_disposition,
  SalesLoft1__Call_Duration_in_Minutes__c as salesloft_call_duration_in_minutes,
  SalesLoft1__Call_Sentiment__c as salesloft_call_sentiment,
  SalesLoft1__SalesLoft_Cadence_Name__c as salesloft_cadence_name,
  SalesLoft1__SalesLoft_Clicked_Count__c as salesloft_clicked_count,
  SalesLoft1__SalesLoft_Connected_Count__c as salesloft_connected_count,
  SalesLoft1__SalesLoft_Email_Template_Title__c as salesloft_email_template_title,
  SalesLoft1__SalesLoft_Replies_Count__c as salesloft_replies_count,
  SalesLoft1__SalesLoft_Type__c as salesloft_type,
  SalesLoft1__SalesLoft_Unique_Click_Count__c as salesloft_unique_click_count,
  SalesLoft1__SalesLoft_Unique_Reply_Count__c as salesloft_unique_reply_count,
  SalesLoft1__SalesLoft_Unique_View_Count__c as salesloft_unique_view_count,
  SalesLoft1__SalesLoft_View_Count__c as salesloft_view_count
from events

UNION ALL

select 
  id as task_id,
  AccountId as account_id, 
  ActivityDate as activity_date,
  CompletedDateTime as completed_date_time,
  CreatedById as created_by_id, 
  CreatedDate as created_date,
  IsDeleted as is_deleted,
  OwnerId as owner_id,
  Activity_Type__c as activity_type,
  Activity_Sub_Type__c as activity_sub_type,
  WhatId as what_id,
  SalesLoft1__Call_Disposition__c as salesloft_call_disposition,
  SalesLoft1__Call_Duration_in_Minutes__c as salesloft_call_duration_in_minutes,
  SalesLoft1__Call_Sentiment__c as salesloft_call_sentiment,
  SalesLoft1__SalesLoft_Cadence_Name__c as salesloft_cadence_name,
  SalesLoft1__SalesLoft_Clicked_Count__c as salesloft_clicked_count,
  SalesLoft1__SalesLoft_Connected_Count__c as salesloft_connected_count,
  SalesLoft1__SalesLoft_Email_Template_Title__c as salesloft_email_template_title,
  SalesLoft1__SalesLoft_Replies_Count__c as salesloft_replies_count,
  SalesLoft1__SalesLoft_Type__c as salesloft_type,
  SalesLoft1__SalesLoft_Unique_Click_Count__c as salesloft_unique_click_count,
  SalesLoft1__SalesLoft_Unique_Reply_Count__c as salesloft_unique_reply_count,
  SalesLoft1__SalesLoft_Unique_View_Count__c as salesloft_unique_view_count,
  SalesLoft1__SalesLoft_View_Count__c as salesloft_view_count
-- from sdp-prd-commercial.raw_salesforce_banff.from_longboat_tasks 
from `shopify-dw.raw_salesforce_banff.task`
where IsDeleted = false
)

, users_and_tasks AS (
  SELECT distinct u.user_id AS owner_id, u.name AS task_assigned_to, ur.name as task_assigned_to_role,
  srr.market_segment, srr.team_segment, srr.sales_region,
  t.task_id, t.account_id as account_id, ur.user_role_id,
  DATE(completed_date_time) AS task_completed_date,
  activity_type, salesLoft_type
  FROM `shopify-dw.base.base__salesforce_banff_users` u
  INNER JOIN tasks_and_events t
  ON t.owner_id = u.user_id
  INNER JOIN `shopify-dw.base.base__salesforce_banff_user_roles` ur
  on u.user_role_id = ur.user_role_id
  -- LEFT JOIN `sdp-prd-commercial.raw_google_sheets.from_longboat_revenue_sales_rep_role` srr 
  LEFT JOIN `shopify-dw.raw_google_sheets.revenue_sales_rep_role` srr
  ON ur.name = srr.sales_rep_role
  WHERE t.is_deleted = false
)

, accounts_opps as (
  SELECT a.account_id AS account_id,
    date(o.CreatedDate) as opportunity_created_date,
    DATE(o.CloseDate) as opportunity_close_date,
    -- nac.market_segment, nac.primary_sales_rep_name as owner_name,
    --date_diff(date(o.created_date), completed_date, DAY) as datediff,
    DATE(LAG(CloseDate) OVER (PARTITION BY a.account_id ORDER BY o.CloseDate ASC)) AS prev_opportunity_close_date,
    --case when completed_date <= date(o.created_date) then 'Yes' else 'No' end as task_influenced_opty,
    o.id as opportunity_id, o.name as opportunity_name, o.type as opportunity_type, o.StageName as stage_name,
    o.Total_Revenue__c as total_revenue, o.Incremental_Product_Gross_Profit__c as lifetime_incremental_product_profit_usd, o.Total_ACV_Amount__c as total_acv_amount, o.Region__c as region, o.Sales_Rep_Role__c as sales_rep_role, o.Opportunity_Source__c
    -- regexp_replace(upper(coalesce(a.named_account_code, nac.named_account_code)),' ',"") as named_account_code,
  FROM `shopify-dw.base.base__salesforce_banff_accounts` a
    -- left join `shopify-data-bigquery-global.revenue_operations.nac_rep_assignment` nac
    -- on regexp_replace(upper(a.named_account_code),' ',"") = regexp_replace(upper(nac.named_account_code),' ',"")
    left join `shopify-dw.raw_salesforce_banff.opportunity` o
    on a.account_id = o.AccountId
  WHERE a.is_deleted = false AND o.IsDeleted = false
  ORDER BY a.account_id, o.CreatedDate)

, accounts_tasks AS (
  SELECT a.account_id,
  ut.task_assigned_to,
  -- a.owner_name,
  task_id,
  ut.task_assigned_to_role,
  task_completed_date,
  a.opportunity_created_date,
  a.opportunity_close_date,
  date_diff(date(a.opportunity_created_date), task_completed_date, DAY) as datediff,
  case when task_completed_date <= a.opportunity_created_date then 'Yes' else 'No' end as task_influenced_opty,
  a.opportunity_id, a.opportunity_name, a.opportunity_type, a.stage_name,
  a.total_revenue, a.lifetime_incremental_product_profit_usd, a.total_acv_amount, a.region, a.sales_rep_role,
  -- a.named_account_code,
    case
      WHEN LOWER(coalesce(salesLoft_type, activity_type)) IN ('email', 'reply') THEN 'Email'
      WHEN LOWER(coalesce(salesLoft_type, activity_type)) IN ('linkedin - connection request', 'linkedin - inmail', 'linkedin - research') THEN 'LinkedIn'
      WHEN LOWER(coalesce(salesLoft_type, activity_type)) IN ('call', 'message') THEN 'Call'
      WHEN LOWER(coalesce(salesLoft_type, activity_type)) IN ('meeting', 'in-person meeting') THEN 'Meeting'
      ELSE 'Other'
    END AS activity_type,
  ut.market_segment, ut.team_segment, ut.sales_region,
  FROM accounts_opps a
  left join users_and_tasks ut
  -- ON ut.owner_id = a.owner_id
  ON left(ut.account_id,15) = left(a.account_id,15) AND ut.task_completed_date BETWEEN COALESCE(DATE_ADD(prev_opportunity_close_date,INTERVAL 1 DAY),DATE('1900-01-01')) AND opportunity_close_date
  where task_completed_date is not null
ORDER BY a.account_id, a.opportunity_id, a.opportunity_created_date, task_completed_date
)

, accounts_final as (
SELECT
  account_id, 
  task_assigned_to,
  -- owner_name,
  task_assigned_to_role, 
  task_id, task_completed_date, opportunity_created_date, datediff, task_influenced_opty, a.opportunity_id, opportunity_name, opportunity_type, stage_name, total_revenue, total_acv_amount, lifetime_incremental_product_profit_usd, region, sales_rep_role, activity_type, 
-- named_account_code, 
market_segment, team_segment, sales_region
FROM accounts_tasks a --removed join to sales_funnel table
WHERE datediff between 0 and 90
and task_influenced_opty = 'Yes'
and activity_type IN ('Call', 'Meeting')
and a.opportunity_id is not null
ORDER BY task_completed_date
-- , owner_name
)

select
*
from accounts_final
-- from opty_creation
-- and activity_type in ('Call', 'Meeting')
-- and (named_account_code like '%-b'
-- or named_account_code like '%-c')
-- and owner_name = 'Nasif Choudhury'
-- where segment_name is not null
order by task_completed_date, 
-- owner_name, 
opportunity_id
-- select *
-- from seamster_backroom_revenue.opportunity_product_finance_metrics_accumulating_facts
-- where lifetime_total_revenue_usd > 0
-- limit 100;
