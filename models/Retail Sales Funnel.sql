CREATE OR REPLACE TABLE shopify-dw.scratch.derekfeher_retail_modelled_retail_sales_funnel AS

WITH 

##############################################################
#CTE Creates lead_source / lead_source_bucket table
##############################################################

#migrated
sales_header_dimension AS (
SELECT 
  lower(lead_source) AS lead_source,
  CASE 
      WHEN lower(lead_source) IS NULL OR lower(lead_source) IN ('outbound', 'plus cross sell', 'internal referral', 
                                              'repeat customer', 'zendesk', 'intal', 'marketing crm', 
                                              'outblund', 'n/a', 'no pe') 
              THEN 'Outbound'
      WHEN lower(lead_source) IN ('partner directory inquiry', 'partner referral', 'partner pos referral', 'partner')
              THEN 'Partner'
      WHEN lower(lead_source) IN ('3field', '3rd party lead gen', 'contact us', 'content', 'content syndication', 'demo', 'direct mail', 'drift',
                                      'event', 'event - booth scan', 'event - sales meeting', 'email management', 'facebook lead ads', 'guru pos referral', 'guru referral - vault', 'lead list', 'marketing campaign', 
                                      'other', 'paid', 'spaces', 'inbound call', 'advertisement', 'customer event', 'inbound sales inquiry', 'website', 'webinar') 
              THEN 'Demand-Gen'
              ELSE 'Other Inbound' 
    END as lead_source_bucket
FROM `shopify-dw.sales.sales_opportunity_current` AS od
GROUP BY 1,2
),

##############################################################
#Brings retail opportunities, and enrich them. 1 row per opportunity_id
##############################################################
#partially migrated
opportunity_attributes AS (
    SELECT od.opportunity_id,
          DATE_TRUNC(od.opportunity_closed_date, DAY) AS closed_date,
          DATE_TRUNC(od.opportunity_created_at, DAY) AS created_date,
          od.opportunity_type,
          od.salesforce_account_id,
          od.stage_name AS current_stage_name,
          lower(od.lead_source) AS lead_source,
          -- COALESCE(CASE WHEN u.user_role LIKE '%N3%RET%' THEN 'N3 Retail' 
          -- ELSE shd.lead_source_bucket END, 'Outbound') AS lead_source_bucket,
          COALESCE(shd.lead_source_bucket, 'Outbound') AS lead_source_bucket,
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
          od.market_segment,
          od.team_segment
    FROM  `shopify-dw.sales.sales_opportunity_current` AS od 
    -- LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_opportunities` AS o
    LEFT JOIN `shopify-dw.base.base__salesforce_banff_opportunities` o
      ON o.opportunity_id = od.opportunity_id
    LEFT JOIN `sdp-prd-commercial.intermediate.project_emerald_migrated_and_net_new_merchants`
      ON od.primary_shop_id = SAFE_CAST(shop_id AS INT64)
    LEFT JOIN `sdp-prd-commercial.base.base__from_longboat_project_emerald_merchant_list` 
      USING (promo_code)
    LEFT JOIN sales_header_dimension AS shd
      ON lower(od.lead_source) = shd.lead_source
    LEFT JOIN `shopify-dw.sales.salesforce_users` u
      ON u.user_id = od.salesforce_owner_id
),

##############################################################
#When opps have more than 1 opportunity_product_line_item, this CTE aims to reduce that to 1 row per opp_id, and add the retail product count
##############################################################

retail_sales_funnel_accumulating_snapshot AS (
  --- Converts the grain from product + opportunity to Opportunity
  SELECT
    opportunity_id, team_segment,
    ANY_VALUE(product) AS product_name,
    -- Retail Payments amount_usd takes precedence.
    MAX_BY(sales_price, product) AS amount_usd, -- removed quantity in sales_price * quantity because we want sales_price only
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
    opportunity_id, team_segment
),

##############################################################
#I don't know why are we pulling ANY_VALUE from that. Depending on the day, it can change the month/quarter the sal was done
##############################################################
#migrated
sal_opportunities AS (
  SELECT opportunity_id, team_segment,
         ANY_VALUE(COALESCE(envision_at, solution_at, demonstrate_at, deal_craft_at, closed_at)) AS qualified_sal_date,
         1 AS is_sal
  FROM `shopify-dw.marts.sales_funnel_accumulating_facts`
  WHERE product IN ('POS Pro', 'Retail Payments')
  AND (COALESCE(envision_at, deal_craft_at, solution_at, demonstrate_at)  IS NOT NULL OR is_won = True)
  GROUP BY opportunity_id, team_segment
),

retail_sales_opportunities AS (
  SELECT
    opportunity_id,
    created_date,
    closed_date,
    lead_source,
    lead_source_bucket,
    retail_sales_funnel_accumulating_snapshot.team_segment,
    ---- Country based fields
    -- CASE WHEN shopify_hardware_eligible_on IS NOT NULL THEN country_code ELSE 'Rest of the World' END AS country_code,
    -- CASE WHEN shopify_hardware_eligible_on IS NOT NULL THEN country_name ELSE 'Rest of the World' END AS country_name,
    CASE WHEN shopify_hardware_eligible_since IS NOT NULL THEN country_code ELSE 'Rest of the World' END AS country_code,
    CASE WHEN shopify_hardware_eligible_since IS NOT NULL THEN country_name ELSE 'Rest of the World' END AS country_name,
    region as region_code,
    1 AS SQL,
    COALESCE(is_sal, 0) AS SAL,
    qualified_sal_date,
    ---- Product conditioned fields
    ---- 1. If both products are associated, then we call it 'POS Pro + Retail Payments'
    CASE WHEN total_retail_products = 1 THEN product_name ELSE 'POS Pro + Retail Payments' END AS product_name,
    ---- 2. If only Retail Payments is associated with the product, then we 0 locations
    CASE WHEN total_retail_products = 1 AND product_name = 'Retail Payments' THEN 0 ELSE rdr_locations END AS number_of_locations,
    ---- 3. If only POS Pro is associated with the product, then we 0 amount_usd
    CASE WHEN total_retail_products = 1 AND product_name = 'POS Pro' THEN 0 ELSE amount_usd END AS retail_payments_amount,
    intuit_opportunity,
    CASE WHEN current_stage_name = 'Pre-Qualified' THEN current_stage_name
         WHEN is_sal!=1 THEN 'Not Accepted'
         WHEN current_stage_name = 'Closed Lost' THEN current_stage_name
         WHEN current_stage_name = 'Closed Won' THEN current_stage_name
         ELSE 'In progress' END as current_opportunity_stage,
    primary_shop_id,
    opportunity_type,
    created_by,
    owned_by,
    market_segment
  FROM retail_sales_funnel_accumulating_snapshot
  JOIN opportunity_attributes USING (opportunity_id)
  LEFT JOIN `shopify-dw.utils.countries` USING (country_code)
  -- LEFT JOIN `shopify-data-bigquery-global.retail.country_shopify_hardware_eligibility_facts` USING (country_code) #pending migration
  LEFT JOIN `sdp-prd-retail.marts.country_shopify_hardware_eligibility` USING (country_code) --post migration, waiting for reply from #retail-data-science
  LEFT JOIN sal_opportunities USING (opportunity_id)
)

, first_active as (
  SELECT 
    rmsds.organization_id AS merchant_id, 
    min(date) as first_active
  FROM `sdp-prd-retail.marts.pos_organization_daily_snapshot` AS rmsds
  where ending_pos_active >=1
  group by 1
)
--- Merchant to Opportunity Mapping.
--- Per the previous logic keeping the last closed opportunity
, retail_fWAS_opportunities as (
    SELECT  soc.organization_id AS merchant_id,
            MAX(fa.first_active) AS first_active_date,
            MAX_BY(opportunity_id, closed_date) AS opportunity_id
    FROM retail_sales_opportunities
    LEFT JOIN `shopify-dw.accounts_and_administration.shop_organization_current` soc
      ON soc.shop_id = primary_shop_id
    LEFT JOIN first_active as fa
      ON merchant_id = "current merchant id"
    WHERE current_opportunity_stage = 'Closed Won'
    GROUP BY merchant_id
)  
, sales_qualified_leads AS (
  SELECT   region_code,
           country_name,
           country_code,
           created_date AS datetime,
           current_opportunity_stage,
           lead_source,
           lead_source_bucket,
           opportunity_type,
           product_name,
           'Not Applicable' AS location_band,
           'Not Applicable' AS revenue_band,
           '02. Sales qualified lead' AS metric,
           intuit_opportunity,
           created_by,
           owned_by,
           market_segment,
           team_segment,
           SUM (number_of_locations) AS total_locations,
           SUM (retail_payments_amount) AS total_payments_amount,
           COUNT (opportunity_id) AS total_leads
    FROM retail_sales_opportunities
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
), 
sales_accepted_leads AS (
  SELECT   region_code,
           country_name,
           country_code,
           CAST(closed_date AS TIMESTAMP) AS datetime,
           current_opportunity_stage,
           lead_source,
           lead_source_bucket,
           opportunity_type,
           product_name,
           'Not Applicable' AS location_band,
           'Not Applicable' AS revenue_band,
           '03. Sales accepted lead' AS metric,
           intuit_opportunity,
           created_by,
           owned_by,
           market_segment,
           team_segment,
           SUM (number_of_locations) AS total_locations,
           SUM (retail_payments_amount) AS total_payments_amount,
           COUNT (opportunity_id) AS total_leads
    FROM retail_sales_opportunities
    WHERE SAL = 1
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
), 

closed_won_opportunities AS (
  SELECT   region_code,
           country_name,
           country_code,
           CAST(closed_date AS TIMESTAMP) AS datetime,
           current_opportunity_stage,
           lead_source,
           lead_source_bucket,
           opportunity_type,
           product_name,
           'Not Applicable' AS location_band,
           'Not Applicable' AS revenue_band,
           '04. Closed won' AS metric,
           intuit_opportunity,
           created_by,
           owned_by,
           market_segment,
           team_segment,
           SUM (number_of_locations) AS total_locations,
           SUM (retail_payments_amount) AS total_payments_amount,
           COUNT (opportunity_id) AS total_leads
    FROM retail_sales_opportunities
    WHERE current_opportunity_stage = 'Closed Won'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
), 

fwas as(
 SELECT    region_code,
           country_name,
           country_code,
           TIMESTAMP(first_active_date) AS datetime,
           current_opportunity_stage,
           lead_source,
           lead_source_bucket,
           opportunity_type,
           product_name,
           'Not Applicable' AS location_band,
           'Not Applicable' AS revenue_band,
           '05. Retail FWAS' AS metric,
           intuit_opportunity,
           created_by,
           owned_by,
           market_segment,
           team_segment,
           SUM (number_of_locations) AS total_locations,
           SUM (retail_payments_amount) AS total_payments_amount,
           COUNT (merchant_id) AS total_leads
    FROM retail_sales_opportunities
    JOIN retail_fWAS_opportunities USING (opportunity_id)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
),
#migrated
marketing_qualified_leads AS (
    SELECT 
        CASE 
          WHEN SPLIT(ble.region, ' - ')[SAFE_OFFSET(0)] = 'AMER' THEN 'AMER'
          WHEN SPLIT(ble.region, ' - ')[SAFE_OFFSET(0)] = 'APAC' THEN 'APAC'
          WHEN SPLIT(ble.region, ' - ')[SAFE_OFFSET(0)] = 'EMEA' THEN 'EMEA'
          WHEN SPLIT(ble.region, ' - ')[SAFE_OFFSET(0)] = 'LATAM' THEN 'LATAM'
          ELSE 'Unknown Region'
        END AS region_code,
        SPLIT(ble.region, ' - ')[SAFE_OFFSET(1)] AS country_name,
        cd.country_code,
        DATE_TRUNC(new_sales_ready_at, DAY) AS datetime,
        'Not Applicable for MQLs' AS current_opportunity_stage,
        ble.lead_source,
        -- COALESCE(CASE WHEN bl.Owner_Role__c LIKE '%N3%RET%' 
        -- THEN 'N3 Retail' ELSE shd.lead_source_bucket END, 'Outbound') AS lead_source_bucket,
        COALESCE(shd.lead_source_bucket, 'Outbound') AS lead_source_bucket,
        'Not Applicable for MQLs' AS opportunity_type,
        'Not Applicable for MQLs' AS product_name,
        CASE 
            WHEN Retail_Locations__c = '1' THEN '01. 1'
            WHEN Retail_Locations__c = '2-5' THEN '02. 2-5'
            WHEN Retail_Locations__c = '6-10' THEN '03. 6-10'
            WHEN Retail_Locations__c = 'More than 10' THEN '04. 10+'
            WHEN Retail_Locations__c = 'Opening location soon' THEN '05. Opening soon'
            WHEN Retail_Locations__c IN ('No locations yet', '') OR Retail_Locations__c IS NULL THEN '06. Unknown' 
            ELSE Retail_Locations__c 
        END AS location_band,
        -- COALESCE(bl.Annual_Offline_Revenue__c, tl.annual_revenue_band) AS revenue_band,
        case 
  when bl.Annual_Offline_Revenue__c = 'Up to $5,000' then '2500'
  when bl.Annual_Offline_Revenue__c = '$0 to $50,000' then '25000'
  when bl.Annual_Offline_Revenue__c = '$5,000 to $50,000' then '27500'
  when bl.Annual_Offline_Revenue__c = '$0 to $250,000' then '125000'
  when bl.Annual_Offline_Revenue__c = '$50,000 to $250,000' then '150000'
  when bl.Annual_Offline_Revenue__c = '$50,000 to $500,000' then '275000'
  when bl.Annual_Offline_Revenue__c = '$250,000 to $500,000' then '375000'
  when bl.Annual_Offline_Revenue__c = '$250,000 to $1,000,000' then '625000'
  when bl.Annual_Offline_Revenue__c = '$500,000 to $1,000,000' then '750000'
  when bl.Annual_Offline_Revenue__c in ('$1,000,000+') then '5000000'
  when bl.Annual_Offline_Revenue__c in ('$1,000,000 to $10,000,000') then '5500000'
  when bl.Annual_Offline_Revenue__c = '$10,000,000+' then '15000000'
  --WHEN Third_Party_Enriched_Revenue__c > 0 THEN Third_Party_Enriched_Revenue__c
end as revenue_band,
        '01. Marketing qualified leads' AS metric,
        'Not Applicable for MQLs'  as intuit_opportunity,
        'Not Applicable for MQLs' as created_by,
        'Not Applicable for MQLs' as owned_by,
        'Not Applicable for MQLs' as market_segment,
        'Not Applicable for MQLs' as team_segment,
        0 AS total_locations,
        0 AS total_payments_amount,
        COUNT(*) AS total_leads
    FROM `shopify-dw.sales.salesforce_leads` AS ble
    LEFT JOIN `shopify-dw.raw_salesforce_banff.lead` bl
    -- LEFT JOIN `sdp-prd-commercial.raw_salesforce_banff.from_longboat_leads` AS bl
        ON bl.id = ble.lead_id
    LEFT JOIN `sdp-prd-commercial.raw_salesforce_trident.from_longboat_leads` AS tl
        ON tl.id = ble.lead_id
    LEFT JOIN `shopify-dw.utils.countries` AS cd
        ON cd.country_name = SPLIT(ble.region, ' - ')[SAFE_OFFSET(1)] -- ble country_name
    LEFT JOIN sales_header_dimension AS shd
        ON LOWER(ble.lead_source) = shd.lead_source
    WHERE TRUE 
        AND ble.primary_product_interest = 'POS Pro'
        -- FILTER FOR VALID MQLs
        AND bl.isdeleted = false
        AND ble.lead_source NOT LIKE '%partner%'
        AND (
            COALESCE(bl.Annual_Offline_Revenue__c, tl.annual_revenue_band) IN (
                '$250K - $400k',
                '$250,000 to $500,000',
                '$250,000 to $1,000,000',
                '$400k - $500k',
                '$500,000 to $1,000,000',
                '$500k - $1M',
                '$1,000,000+',
                '$1,000,000 to $10,000,000',
                '$10,000,000+',
                '$250,000 to $500,000',
                '$500,000 to $1,000,000',
                '$250,000 to $1,000,000',
                '$1,000,000 to $10,000,000',
                '$1,000,000+',
                '$0 to $250,000',
                '$50,000 to $250,000',
                '$10,000,000+',
                '$5,000 to $50,000',
                'Up to $5,000'
            )
            OR (
                cd.country_code IN ('ES', 'IT') AND COALESCE(bl.Annual_Offline_Revenue__c, tl.annual_revenue_band) IN ('$50,000 to $250,000, revenue_between_50000_250000')
            )
        )
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)

-- We originally pulled the inbound calls from hive.raw_google_sheets.retail_inbound_calls but this source appeared to be missing information.
-- The solution was to count all inbound call SQLs as MQLs since any SQL would have to be an MQL first. 
, inbound_call_mql as (
    SELECT
        region_code,
        country_name,
        country_code,
        created_date AS datetime,
        'Not Applicable for MQLs' AS current_opportunity_stage,
        'inbound call' AS lead_source,
        lead_source_bucket,
        'Not Applicable for MQLs' AS opportunity_type,
        'Not Applicable for MQLs' AS product_name,
        CASE 
            WHEN number_of_locations = 1 THEN '01. 1'
            WHEN number_of_locations >= 2 and number_of_locations <= 5 THEN '02. 2-5'
            WHEN number_of_locations >= 6 and number_of_locations <= 10 THEN '03. 6-10'
            WHEN number_of_locations > 10 THEN '04. 10+'
            ELSE '06. Unknown'  
        END AS location_band,
        'Not Available for Inbound Calls' AS revenue_band,
        '01. Marketing qualified leads' AS metric,
        'Not Applicable for MQLs'  as intuit_opportunity,
        'Not Applicable for MQLs' as created_by,
        'Not Applicable for MQLs' as owned_by,
        'Not Applicable for MQLs' as market_segment,
        'Not Applicable for MQLs' as team_segment,
        COALESCE(number_of_locations, 0) AS total_locations,
        COALESCE(retail_payments_amount, 0) AS total_payments_amount,
        COUNT(*) AS total_leads
    FROM retail_sales_opportunities
    WHERE lead_source = 'inbound sales inquiry'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
)

, union_all AS (
  SELECT * FROM marketing_qualified_leads
  UNION ALL
  SELECT * FROM inbound_call_mql
  UNION ALL
  SELECT * FROM sales_qualified_leads
  UNION ALL 
  SELECT * FROM sales_accepted_leads
  UNION ALL 
  SELECT * FROM closed_won_opportunities
  UNION ALL 
  SELECT * FROM fwas
) 

SELECT * FROM union_all
WHERE DATE(datetime) >=  DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 14 MONTH), MONTH)
  AND DATE(datetime) <= DATE_TRUNC(CURRENT_DATE(), MONTH) + INTERVAL 3 MONTH - INTERVAL 1 DAY
