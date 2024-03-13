CREATE OR REPLACE TABLE shopify-dw.scratch.modelled_n3_mql_data
AS 

WITH opp as (
  SELECT o.id as opportunity_id, owner_role_at_creation__c as sdr_role, ur.name as opp_owner_role, DATE(o.createddate) as opp_created_date, stagename as opp_stage_name, opp_annual_offline_revenue__c, COALESCE(o.Opp_Annual_Total_Revenue__c
,opp_annual_total_revenue_verified__c) as opp_committed_total_gmv, COALESCE(ops_lead_source,'Unknown') as ops_lead_source
-- , case when (owner_role_at_creation__c LIKE 'AMER-%N3%' OR ur.name LIKE 'AMER-%N3%') then 'AMER' when (owner_role_at_creation__c LIKE 'EMEA-%N3%' OR ur.name LIKE 'EMEA-%N3%') then 'EMEA' else 'Other' end as opty_region
  FROM `shopify-dw.raw_salesforce_banff.opportunity` o 
  LEFT JOIN (SELECT DISTINCT opportunity_id, ops_lead_source FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` WHERE dataset = 'Actuals' AND year >= 2024) fun ON o.id = fun.opportunity_id
  -- LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_users` u 
  -- ON o.ownerid = u.id 
  LEFT JOIN `shopify-dw.base.base__salesforce_banff_users` u
  ON o.ownerid = u.user_id 
  -- LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_user_roles` ur 
  -- ON u.user_role_id = ur.id
  LEFT JOIN `shopify-dw.base.base__salesforce_banff_user_roles` ur
  ON u.user_role_id = ur.user_role_id
  WHERE o.isdeleted = false
  -- AND (owner_role_at_creation__c LIKE '%N3%RET%' OR ur.name LIKE '%N3%RET%')
  AND (owner_role_at_creation__c LIKE 'AMER-%N3%' OR ur.name LIKE 'AMER-%N3%')
  AND DATE(o.createddate) >= '2024-01-01'
)

, leads as (
  SELECT l.id as lead_id, l.Owner_Role__c as lead_owner, l.ownerid AS lead_owner_id, u.name as lead_owner_name, l.leadsource, l.status, l.Annual_Offline_Revenue__c, l.Annual_Offline_Revenue_Verified__c, l.Third_Party_Enriched_Revenue__c, l.createddate, l.Marketing_Qualified_Lead_Date__c, l.Sales_Accepted_Lead_Date__c, l.ConvertedAccountId, l.ConvertedOpportunityId, l.Is_Shopify_Customer__c, l.Retail_Locations__c, l.Disqualified_Reason__c, acc.territory_name, acc.account_id--, COALESCE(l.AnnualRevenue,l.Third_Party_Enriched_Revenue__c) as lead_estimated_total_gmv
-- , case when (((l.Owner_Role__c LIKE 'AMER-%N3%' AND extract (year from l.createddate) = 2024) OR c.name = 'N3_Lead_List_January_2024' OR c.name = 'Content_Syndication_Retail_January_N3')) then 'AMER' when (l.Owner_Role__c LIKE 'EMEA-%N3%') then 'EMEA' else 'Other' end as region
  FROM `shopify-dw.raw_salesforce_banff.lead` l 
    -- LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_campaign_member` cm
  LEFT JOIN `shopify-dw.base.base__salesforce_banff_campaign_member` cm
  ON l.id = cm.lead_id
  -- LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_campaign` c
  LEFT JOIN `shopify-dw.base.base__salesforce_banff_campaign` c
  on c.campaign_id = cm.campaign_id
  LEFT JOIN `shopify-dw.sales.salesforce_accounts` acc
  on l.ConvertedAccountId = acc.account_id AND l.isdeleted = false
  -- LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_users` u
  LEFT JOIN `shopify-dw.base.base__salesforce_banff_users` u
  on u.user_id = l.ownerid
  WHERE l.isdeleted = false 
    -- AND ((l.Owner_Role__c LIKE '%N3%RET%' AND extract (year from l.createddate) = 2024) OR c.name = 'N3_Lead_List_January_2024' OR c.name = 'Content_Syndication_Retail_January_N3')
    AND ((l.Owner_Role__c LIKE 'AMER-%N3%' AND extract (year from l.createddate) = 2024) OR c.name = 'N3_Lead_List_January_2024' OR c.name = 'Content_Syndication_Retail_January_N3')
)

, combine AS (
SELECT 
  distinct
    l.lead_id, l.lead_owner, l.lead_owner_id, l.lead_owner_name, l.leadsource, l.status, l.Annual_Offline_Revenue__c, l.createddate, l.Marketing_Qualified_Lead_Date__c, l.Sales_Accepted_Lead_Date__c, l.ConvertedAccountId, l.ConvertedOpportunityId, l.Is_Shopify_Customer__c, l.Retail_Locations__c, l.Disqualified_Reason__c, l.territory_name, l.account_id, l.Third_Party_Enriched_Revenue__c, opp.*
from leads l
FULL JOIN opp
ON l.ConvertedOpportunityId = opp.opportunity_id
where l.lead_id IS NOT NULL OR opp.sdr_role LIKE 'AMER-%N3%' OR opp.opp_owner_role LIKE 'AMER-%N3%'
)

, final as (
select distinct *, 
case 
  when Annual_Offline_Revenue__c = 'Up to $5,000' then 2500
  when Annual_Offline_Revenue__c = '$0 to $50,000' then 25000
  when Annual_Offline_Revenue__c = '$5,000 to $50,000' then 27500
  when Annual_Offline_Revenue__c = '$0 to $250,000' then 125000
  when Annual_Offline_Revenue__c = '$50,000 to $250,000' then 150000
  when Annual_Offline_Revenue__c = '$50,000 to $500,000' then 275000
  when Annual_Offline_Revenue__c = '$250,000 to $500,000' then 375000
  when Annual_Offline_Revenue__c = '$250,000 to $1,000,000' then 625000
  when Annual_Offline_Revenue__c = '$500,000 to $1,000,000' then 750000
  when Annual_Offline_Revenue__c in ('$1,000,000+') then 5000000
  when Annual_Offline_Revenue__c in ('$1,000,000 to $10,000,000') then 5500000
  when Annual_Offline_Revenue__c = '$10,000,000+' then 15000000
  --WHEN Third_Party_Enriched_Revenue__c > 0 THEN Third_Party_Enriched_Revenue__c
end as lead_estimated_total_gmv,
case when DATE(createddate) < '2024-01-01' then '2024-01-25 19:20:50 UTC' else createddate end as new_created_date, -- Bassam wants to show 2024 only since these leads were reassigned to N3
case when DATE(Marketing_Qualified_Lead_Date__c) < '2024-01-01' then '2024-01-25 19:20:50 UTC' else Marketing_Qualified_Lead_Date__c end as new_marketing_qualified_lead_date
from combine)

select 
  *,
  final.lead_estimated_total_gmv as lead_estimated_offline_gmv,
  final.lead_estimated_total_gmv as estimated_gmv,
  CASE WHEN lead_id IS NOT NULL THEN 1 ELSE 0 END as is_lead, 
  CASE WHEN new_marketing_qualified_lead_date IS NOT NULL THEN 1 ELSE 0 END is_mql, 
  CASE WHEN opportunity_id IS NOT NULL THEN 1 ELSE 0 END as is_sql, 
  CASE WHEN lead_id IS NULL THEN 'Outbound' 
    WHEN opportunity_id IS NULL THEN 'No SQL' 
    WHEN new_marketing_qualified_lead_date IS NULL THEN 'Lead not qualified' 
    ELSE 'MQL'
  END as sql_source, 
  CASE WHEN opp_stage_name NOT IN ('Pre-Qualified','Closed Lost') THEN 1 ELSE 0 END as is_sal  
from final
WHERE COALESCE(account_id, '') NOT IN ('0018V00002d2BY2QAM')

--second old code
-- WITH n3_leads AS (
-- SELECT 
--   distinct
--     l.id as lead_id, l.Owner_Role__c as lead_owner, l.ownerid AS lead_owner_id, u.name as lead_owner_name, l.leadsource, l.Annual_Offline_Revenue__c, l.Annual_Offline_Revenue_Verified__c, l.Third_Party_Enriched_Revenue__c, l.createddate, l.Marketing_Qualified_Lead_Date__c, l.Sales_Accepted_Lead_Date__c, l.ConvertedAccountId, l.ConvertedOpportunityId, l.Is_Shopify_Customer__c, l.Retail_Locations__c, l.Disqualified_Reason__c, acc.territory_name, acc.account_id, o.opp_annual_offline_revenue
-- from `shopify-dw.raw_salesforce_banff.lead` l
-- LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_campaign_member` cm
-- ON l.id = cm.lead_id
-- LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_campaign` c
-- on c.id = cm.campaign_id
-- LEFT JOIN `shopify-dw.sales.salesforce_accounts` acc
-- on l.ConvertedAccountId = acc.account_id AND l.isdeleted = false
-- LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_users` u
-- on u.id = l.ownerid
-- LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_opportunities` o
-- ON l.ConvertedOpportunityId = o.id
-- FULL JOIN o ON l.converted_opportunity_id = o.opportunity_id
-- where l.Owner_Role__c LIKE '%N3%' 
-- and extract (year from l.createddate) = 2024)

-- , n3_campaigns as (
--  SELECT 
--   distinct
--     l.id as lead_id, l.Owner_Role__c as lead_owner, l.ownerid AS lead_owner_id, u.name as lead_owner_name, l.leadsource, l.Annual_Offline_Revenue__c, l.Annual_Offline_Revenue_Verified__c, l.Third_Party_Enriched_Revenue__c, l.createddate, l.Marketing_Qualified_Lead_Date__c, l.Sales_Accepted_Lead_Date__c, l.ConvertedAccountId, l.ConvertedOpportunityId, l.Is_Shopify_Customer__c, l.Retail_Locations__c, l.Disqualified_Reason__c, acc.territory_name, acc.account_id, o.opp_annual_offline_revenue
-- from `shopify-dw.raw_salesforce_banff.lead` l
-- LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_campaign_member` cm
-- ON l.id = cm.lead_id
-- LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_campaign` c
-- on c.id = cm.campaign_id
-- LEFT JOIN `shopify-dw.sales.salesforce_accounts` acc
-- on l.ConvertedAccountId = acc.account_id AND l.isdeleted = false
-- LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_opportunities` o
-- ON l.ConvertedOpportunityId = o.id and o.is_deleted = false
-- LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_users` u
-- on u.id = l.ownerid
--   -- where LOWER(c.name) like '%n3%'
-- where c.name = 'N3_Lead_List_January_2024'
-- )

-- , union_tables as (
-- select * from n3_leads
-- UNION ALL 
-- select * from n3_campaigns
-- )

-- , final as (
-- select distinct *, 
-- case 
--   when Annual_Offline_Revenue__c = '$0 to $250,000' then 125000
--   when Annual_Offline_Revenue__c = '$5,000 to $50,000' then 30000
--   when Annual_Offline_Revenue__c = '$50,000 to $250,000' then 150000
--   when Annual_Offline_Revenue__c = '$250,000 to $500,000' then 375000
--   when Annual_Offline_Revenue__c = '$250,000 to $1,000,000' then 625000
--   when Annual_Offline_Revenue__c = '$500,000 to $1,000,000' then 750000
--   when Annual_Offline_Revenue__c in ('$1,000,000 to $10,000,000', '$1,000,000+') then 5000000
--   when Annual_Offline_Revenue__c = '$10,000,000+' then 10000000
-- end as estimated_gmv
-- from union_tables)

-- select 
--   *, 
--   case when createddate < '2024-01-01' then '2024-01-25 19:20:50 UTC' else createddate end as new_created_date, -- Bassam wants to show 2024 only since these leads were reassigned to N3
--   case when Marketing_Qualified_Lead_Date__c < '2024-01-01' then '2024-01-25 19:20:50 UTC' else Marketing_Qualified_Lead_Date__c end as new_marketing_qualified_lead_date
-- from final


--old model
-- WITH n3_leads AS 
-- (
--  SELECT 
--   distinct
--     l.id as lead_id, l.owner_role as lead_owner, l.owner_id AS lead_owner_id, u.name as lead_owner_name, l.lead_source, l.annual_offline_revenue, l.annual_online_revenue, l.annual_revenue, l.created_date, marketing_qualified_lead_date,
--     -- DATE(case when l.created_date < '2024-01-01' then '2024-01-01 19:20:50 UTC' else l.created_date end as created_date), -- Bassam wants to show 2024 only since these leads were reassigned to N3
--     -- DATE(case when marketing_qualified_lead_date < '2024-01-01' then '2024-01-01 19:20:50 UTC' else marketing_qualified_lead_date end as marketing_qualified_lead_date),
--     acc.territory_name, acc.account_id, sales_accepted_lead_date
--   FROM `sdp-prd-commercial.raw_salesforce_banff.from_longboat_leads` l
--   LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_campaign_member` cm
--   ON l.id = cm.lead_id
--   LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_campaign` c
--   on c.id = cm.campaign_id
--   LEFT JOIN `shopify-dw.sales.salesforce_accounts` acc
--   on l.converted_account_id = acc.account_id AND l.is_deleted = false
--   LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_users` u
--   on u.id = l.owner_id
--   where l.owner_role LIKE '%N3%' 
--   and extract (year from l.created_date) = 2024
-- )

-- --120 rows
-- , n3_campaigns as (
--  SELECT 
--   distinct
--     l.id as lead_id, l.owner_role as lead_owner, l.owner_id AS lead_owner_id, u.name as lead_owner_name, l.lead_source, l.annual_offline_revenue, l.annual_online_revenue, l.annual_revenue, l.created_date, marketing_qualified_lead_date,
--     -- DATE(case when l.created_date < '2024-01-01' then '2024-01-01 19:20:50 UTC' else l.created_date end as created_date), -- Bassam wants to show 2024 only since these leads were reassigned to N3
--     -- DATE(case when marketing_qualified_lead_date < '2024-01-01' then '2024-01-01 19:20:50 UTC' else marketing_qualified_lead_date end as marketing_qualified_lead_date),
--     acc.territory_name, acc.account_id, sales_accepted_lead_date
--   FROM `sdp-prd-commercial.raw_salesforce_banff.from_longboat_leads` l
--   LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_campaign_member` cm
--   ON l.id = cm.lead_id
--   LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_campaign` c
--   on c.id = cm.campaign_id
--   LEFT JOIN `shopify-dw.sales.salesforce_accounts` acc
--   on l.converted_account_id = acc.account_id AND l.is_deleted = false
--   LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_users` u
--   on u.id = l.owner_id
--   -- where LOWER(c.name) like '%n3%'
--   where c.name = 'N3_Lead_List_January_2024'
-- )

-- , union_tables as (
-- select * from n3_leads
-- UNION ALL 
-- select * from n3_campaigns
-- )
-- -- collect bands from merchants (can be very large), because its self-inputed it can be incorrect and needs to be verified by SQL which is after, we cant trust the GMV bands, once SDR talks and qualifies the merchant, we're able to find out the industry and confirm GMV
-- -- have to look at the lead source for a better assumption on MQL, dont need to look at AOR (annual offline revenue)
-- -- if MQL and has some bands, then AOF will have the bands, once fixed by Maurice (Olivia on his team) or whoever then we can report on it
-- -- ask to see how to find gmv list on the MQLs
-- -- figure out why there's one lead that isnt MQL'd, filter out MQL dates that is null
-- -- select sum(annual_revenue) from (
-- , final as (
-- select distinct *, 
-- case 
--   when annual_offline_revenue = '$0 to $250,000' then 125000
--   when annual_offline_revenue = '$5,000 to $50,000' then 30000
--   when annual_offline_revenue = '$50,000 to $250,000' then 150000
--   when annual_offline_revenue = '$250,000 to $500,000' then 375000
--   when annual_offline_revenue = '$250,000 to $1,000,000' then 625000
--   when annual_offline_revenue = '$500,000 to $1,000,000' then 750000
--   when annual_offline_revenue in ('$1,000,000 to $10,000,000', '$1,000,000+') then 5000000
--   when annual_offline_revenue = '$10,000,000+' then 10000000
-- end as estimated_gmv
-- from union_tables)

-- select 
--   *, 
--   case when created_date < '2024-01-01' then '2024-01-01 19:20:50 UTC' else created_date end as new_created_date, -- Bassam wants to show 2024 only since these leads were reassigned to N3
--   case when marketing_qualified_lead_date < '2024-01-01' then '2024-01-01 19:20:50 UTC' else marketing_qualified_lead_date end as new_marketing_qualified_lead_date
-- from final
