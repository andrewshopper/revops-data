CREATE OR REPLACE TABLE shopify-dw.scratch.derekfeher_retail_modelled_retail_sales_funnel_closed_conversion AS

WITH 
#migrated
opportunity_attributes AS (
  SELECT od.opportunity_id,
         DATE_TRUNC(od.opportunity_closed_date, DAY) AS closed_date,
         DATE_TRUNC(od.opportunity_created_at, DAY) AS created_date,
         od.opportunity_type,
         od.salesforce_account_id,
         od.stage_name AS current_stage_name,
         lower(od.lead_source) AS lead_source,
         CASE WHEN lower(od.lead_source) IS NULL OR lower(od.lead_source) IN ('outbound', 'plus cross sell', 'internal referral', 
                                                  'repeat customer', 'zendesk', 'intal', 'marketing crm', 
                                                  'outblund', 'n/a', 'no pe') THEN 'Outbound'
              WHEN lower(od.lead_source) IN ('partner directory inquiry', 'partner referral', 'partner pos referral', 'partner') THEN 'Partner'
              WHEN lower(od.lead_source) IN ('3field', '3rd party lead gen', 'contact us', 'content', 'content syndication', 'direct mail', 'drift',
                                          'event', 'facebook lead ads', 'guru pos referral', 'guru referral - vault', 'marketing campaign', 'demo', 
                                          'other', 'paid', 'spaces', 'inbound call', 'advertisement', 'customer event', 'inbound sales inquiry', 'website') THEN 'Demand-Gen'
              ELSE 'Other Inbound' END AS lead_source_bucket,
         od.opportunity_country_code AS country_code,
         CASE WHEN sales_region = 'NA' THEN 'AMER' ELSE sales_region END AS region,
         CASE WHEN  cf_flag = 'Y' THEN 'Intuit - Must Win' 
              WHEN (LOWER(campaign_name) LIKE '%emerald%' OR shop_id IS NOT NULL) THEN 'Intuit - other' 
              ELSE 'Non-Intuit' END AS intuit_opportunity,
        CASE  
              WHEN TIMESTAMP(od.opportunity_created_at) < TIMESTAMP'2023-06-01' THEN 'N/A'
              WHEN NOT REGEXP_CONTAINS(owner_role_at_creation, 'SC|N3') THEN 'Shopify Rep'
              WHEN REGEXP_CONTAINS(owner_role_at_creation, 'SC') AND NOT REGEXP_CONTAINS(owner_role_at_creation, 'N3') THEN 'Shopify SC'
              WHEN REGEXP_CONTAINS(owner_role_at_creation, 'N3') AND NOT REGEXP_CONTAINS(owner_role_at_creation, 'SC') THEN 'N3 Rep'
              WHEN REGEXP_CONTAINS(owner_role_at_creation, 'N3') AND REGEXP_CONTAINS(owner_role_at_creation, 'SC') THEN 'N3 SC'
              ELSE 'OTHER' -- nothing should fall into this category, we should investigate if this ever appears
        END AS created_by,
        CASE  
              WHEN TIMESTAMP(od.opportunity_created_at) < TIMESTAMP'2023-06-01' THEN 'N/A'
              WHEN NOT REGEXP_CONTAINS(sales_rep_role, 'SC|N3') THEN 'Shopify Rep'
              WHEN REGEXP_CONTAINS(sales_rep_role, 'SC') AND NOT REGEXP_CONTAINS(sales_rep_role, 'N3') THEN 'Shopify SC'
              WHEN REGEXP_CONTAINS(sales_rep_role, 'N3') AND NOT REGEXP_CONTAINS(sales_rep_role, 'SC') THEN 'N3 Rep'
              WHEN REGEXP_CONTAINS(sales_rep_role, 'N3') AND REGEXP_CONTAINS(sales_rep_role, 'SC') THEN 'N3 SC'
              ELSE 'OTHER' -- nothing should fall into this category, we should investigate if this ever appears
        END AS owned_by,
        od.primary_shop_id,
        od.market_segment
    FROM  `shopify-dw.sales.sales_opportunity_current` AS od 
    LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_opportunities` AS o
      ON o.id = od.opportunity_id
    LEFT JOIN `sdp-prd-commercial.intermediate.project_emerald_migrated_and_net_new_merchants`
      ON od.primary_shop_id = SAFE_CAST(shop_id AS INT64)
    LEFT JOIN `sdp-prd-commercial.base.base__from_longboat_project_emerald_merchant_list` 
      USING (promo_code)
), 

#migrated
retail_sales_funnel_accumulating_snapshot AS (
  --- Converts the grain from product + opportunity to Opportunity
  SELECT
    opportunity_id,
    ANY_VALUE(product) AS product_name,
    -- Retail Payments amount_usd takes precedence.
    -- MAX_BY(sales_price * quantity, product) AS amount_usd,
    MAX_BY(sales_price, product) AS amount_usd,
    -- POS Pro locations takes precedence.
    MIN_BY(total_retail_locations, product) AS rdr_locations,
    -- Checks how many products are there per opportunity.
    -- Opportunity can be associated with POS Pro and Retail Payments
    COUNT(DISTINCT product) AS total_retail_products
  FROM
    `shopify-dw.marts.sales_funnel_accumulating_facts`
  WHERE
    product IN ('POS Pro', 'Retail Payments')
  GROUP BY
    opportunity_id
),
#migrated
sal_opportunities AS (
  SELECT opportunity_id,
         ANY_VALUE(COALESCE(envision_at, solution_at, demonstrate_at, deal_craft_at, closed_at)) AS qualified_sal_date,
         1 AS is_sal
  FROM   `shopify-dw.marts.sales_funnel_accumulating_facts`
  WHERE product IN ('POS Pro', 'Retail Payments')
  AND (COALESCE(envision_at, deal_craft_at, solution_at, demonstrate_at)  IS NOT NULL OR is_won = True)
  GROUP BY opportunity_id
),
#pending migration
retail_sales_opportunities AS (
  SELECT
    opportunity_id,
    created_date,
    closed_date,
    lead_source,
    lead_source_bucket,
    ---- Country based fields
    CASE WHEN shopify_hardware_eligible_on IS NOT NULL THEN country_code ELSE 'Rest of the World' END AS country_code,
    CASE WHEN shopify_hardware_eligible_on IS NOT NULL THEN country_name ELSE 'Rest of the World' END AS country_name,
    region as region_code,
    1 AS SQL,
    COALESCE(is_sal, 0) AS SAL,
    qualified_sal_date,
    DATE_DIFF(closed_date, CAST(qualified_sal_date AS DATE), DAY) AS sal_to_closed_days,
    DATE_DIFF(closed_date, CAST(created_date AS DATE), DAY) AS sql_to_closed_days,
    ---- Product conditioned fields
    ---- 1. If both products are associated, then we call it 'POS Pro + Retail Payments'
    CASE WHEN total_retail_products = 1 THEN product_name ELSE 'POS Pro + Retail Payments' END AS product_name,
    ---- 2. If only Retail Payments is associated with the product, then we 0 locations
    CASE WHEN total_retail_products = 1 AND product_name = 'Retail Payments' THEN 0 ELSE rdr_locations END AS number_of_locations,
    ---- 3. If only POS Pro is associated with the product, then we 0 amount_usd
    CASE WHEN total_retail_products = 1 AND product_name = 'POS Pro' THEN 0 ELSE amount_usd END AS retail_payments_amount,
    intuit_opportunity,
    CASE WHEN current_stage_name IN ('Closed Lost', 'Closed Won') THEN True ELSE False END AS is_closed,
    CASE WHEN current_stage_name = 'Closed Won' THEN True ELSE False END AS is_closed_won,
    primary_shop_id,
    opportunity_type,
    created_by,
    owned_by,
    market_segment
  FROM retail_sales_funnel_accumulating_snapshot
  JOIN opportunity_attributes USING (opportunity_id)
  LEFT JOIN `shopify-dw.utils.countries` USING (country_code)
  LEFT JOIN `shopify-data-bigquery-global.retail.country_shopify_hardware_eligibility_facts` USING (country_code) #pending migration
  LEFT JOIN sal_opportunities USING (opportunity_id)
), 
#migrated
IPP AS (
  SELECT
    opportunity_id,
    MAX(subscription_solutions_revenue) AS subscription_solutions_revenue,
    MAX(total_revenue) AS total_revenue,
    MAX(incremental_product_profit) AS incremental_product_profit,
    MAX(product_attach_count) AS product_attach_count,
    MAX(sales_accepted_lead) AS sales_accepted_lead
  FROM `shopify-dw.marts.sales_opportunity_revenue`
  GROUP BY opportunity_id
)

SELECT
  closed_date,
  created_date,
  opportunity_type,
  opportunity_id,
  lead_source_bucket,
  lead_source,
  product_name,
  CASE WHEN region_code IN ('No Associated geographic_region_code', 'Unknown geographic_region_code','UNKNOWN') THEN 'Unknown Region' WHEN region_code IN ('NA') then 'AMER' ELSE region_code END AS 
  region_code,
  country_name,
  intuit_opportunity,
  created_by,
  owned_by,
  market_segment,
  SUM(CASE WHEN SAL = 1 AND is_closed THEN DATE_DIFF(qualified_sal_date, created_date, DAY) ELSE 0 END) as SQL_to_SAL_days,
  SUM(CASE WHEN SAL = 1 AND is_closed THEN DATE_DIFF(closed_date, CAST(qualified_sal_date AS DATE), DAY) ELSE 0 END)  AS SAL_to_closed_days,
  SUM(CASE WHEN is_closed THEN DATE_DIFF(closed_date, CAST(created_date AS DATE), DAY) ELSE 0 END) AS SQL_to_closed_days,
  SUM(CASE WHEN is_closed_won THEN DATE_DIFF(closed_date, CAST(created_date AS DATE), DAY) ELSE 0 END) AS SQL_to_closed_won_days,
  
  COUNT(opportunity_id) as total_opps,
  COUNT(CASE WHEN is_closed THEN opportunity_id ELSE NULL END) AS SQL,
  COUNT(CASE WHEN SAL = 1 AND is_closed THEN opportunity_id ELSE NULL END) AS SAL,
  COUNT(CASE WHEN is_closed_won THEN opportunity_id ELSE NULL END) AS CW,
  
  SUM(CASE WHEN is_closed_won THEN retail_payments_amount ELSE 0 END) AS payments_amount,
  SUM(CASE WHEN is_closed_won THEN number_of_locations ELSE 0 END) AS number_of_locations,
  SUM(CASE WHEN is_closed_won THEN incremental_product_profit ELSE 0 END) AS incremental_product_profit,

  SUM(case when product_name = 'POS Pro + Retail Payments' and is_closed_won then 1 else 0 end) as pro_and_payments_opps,
  SUM(case when product_name in ('POS Pro + Retail Payments','POS Pro') and is_closed_won then 1 else 0 end) as pro_or_pro_payments_opps
FROM retail_sales_opportunities
  LEFT JOIN IPP USING (opportunity_id)
WHERE 
  DATE(closed_date) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 20 MONTH), MONTH)
  AND DATE(closed_date) <= DATE_TRUNC(CURRENT_DATE(), MONTH) + INTERVAL 1 MONTH - INTERVAL 1 DAY
GROUP BY
  1,2,3,4,5,6,7,8,9,10,11,12,13
