--New Query
CREATE OR REPLACE TABLE shopify-dw.scratch.modelled_campaign_accounts
AS

  SELECT
  DISTINCT campaign_account.account_id,
  campaign_account.campaign_id,
  account.name,
  account.named_account_code,
  opps.owner_manager,
  opps.sales_sub_region,
  opps.ops_market_segment,
  sales_region,
  ops_product_name,
  ops_lead_source,
  sf_close_date,
  current_e_commerce_platform_group,
  value,
  age,
  current_stage_name,
  annual_online_revenue_verified_usd,
  account_annual_revenue,
  opportunity_owner,
  created_by_function,
  created_by,
  sales_owner_role_current,
  account_name,
  sf_created_date,
  campaign,
  account_campaign,
  opp_id_url,
  opportunity_name,
  snapshot_date SAL,
  metric,
  value_type,
  dataset,
  product_name,
  tasks.description,
  tasks.SalesLoft1__SalesLoft_Unique_Click_Count__c as salesLoft_unique_click_count,
  tasks.SalesLoft1__SalesLoft_Cadence_Name__c as salesLoft_cadence_name,
  tasks.Activity_Type__c as campaign_activity_type,
  tasks.ActivityDate as campaign_activity_date,
  tasks.subject
FROM
  `shopify-dw.base.base__salesforce_banff_campaign_member` AS campaign_account
left outer join
  `shopify-dw.base.base__salesforce_banff_accounts` AS account
ON
  campaign_account.account_id = account.account_id
-- left outer JOIN
--   `shopify-data-bigquery-global.revenue_operations.nac_rep_assignment` AS nac_account
-- ON
--   account.named_account_code=nac_account.named_account_code
left outer JOIN
  `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` AS opps
ON
  opps.account_id = account.account_id 
  and opps.campaign in ('Q4-2023-BigCommerce-Takedown-Campaign', '2023-Magento-Takedown-Campaign') --this

left outer join
  -- `shopify-data-bigquery-global.raw_salesforce_banff.from_longboat_events` as events
    `shopify-dw.raw_salesforce_banff.task` as tasks
ON
  campaign_account.account_id = tasks.AccountId

WHERE
  regexp_contains(campaign_account.campaign_id,"7018V000000zkMYQAY") --this
  OR regexp_contains(campaign_account.campaign_id,"7018V000000zkVpQAI") --this
  -- OR regexp_contains(campaign,"") -- opportunity
  -- OR regexp_contains(campaign,"") -- opportunity

-- Old Query
-- SELECT
--   DISTINCT campaign_account.account_id,
--   campaign_account.campaign_id,
--   campaign_account.type,
--   campaign_account.contact_id,
--   campaign_account.lead_id,
--   account.name,
--   account.named_account_code,
--   nac_account.region,
--   nac_account.manager_lvl1_name,
--   nac_account.manager_lvl2_name,
--   nac_account.primary_sales_rep_name,
--   nac_account.subregion,
--   nac_account.market_segment,
--   sales_region,
--   ops_product_name,
--   ops_market_segment,
--   ops_lead_source,
--   sf_close_date,
--   snapshot_date,
--   owner_manager,
--   current_e_commerce_platform_group,
--   value,
--   age,
--   current_stage_name,
--   annual_online_revenue_verified_usd,
--   account_annual_revenue,
--   opportunity_owner,
--   created_by_function,
--   created_by,
--   sales_owner_role_current,
--   account_name,
--   sf_created_date,
--   campaign,
--   account_campaign,
--   opp_id_url,
--   opportunity_name,
--   snapshot_date SAL,
--   metric,
--   value_type,
--   dataset,
--   product_name,
--   events.description,
--   salesLoft_unique_click_count__c, -- shows # of engaged
--   "test"
--   subject
-- FROM
--   `shopify-data-bigquery-global.raw_salesforce_banff.from_longboat_campaign_member` AS campaign_account
-- INNER JOIN
--   `shopify-data-bigquery-global.raw_salesforce_banff.from_longboat_accounts` AS account
-- ON
--   campaign_account.account_id=account.id
-- INNER JOIN
--   `shopify-data-bigquery-global.revenue_operations.nac_rep_assignment` AS nac_account
-- ON
--   account.named_account_code=nac_account.named_account_code
-- INNER JOIN
--   `shopify-data-bigquery-global.revenue_operations.modelled_cross_pl_3` AS opps
-- ON
--   opps.account_id=account.id
-- INNER JOIN
--   `shopify-data-bigquery-global.raw_salesforce_banff.from_longboat_events` as events
-- ON
--   campaign_account.account_id=events.account_id

-- WHERE
--   regexp_contains(campaign_account.campaign_id,"7018V000000zkMYQAY")
--   AND REGEXP_CONTAINS(opps.product_name, "Plus|Commerce Components|Unknown")
--   AND opps.dataset="Actuals"
--   AND opps.value_type="Product Deal Count"
--   AND opps.metric="Created"
--   -- AND regexp_contains(events.subject,"Partner Summit") -- get the cadence from Maria to get # of reached
