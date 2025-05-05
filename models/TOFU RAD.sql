CREATE OR REPLACE TABLE `sdp-for-analysts-platform.rev_ops_prod.modelled_rev_ops_leads` AS

WITH

referring_partner AS (
SELECT
  lead_id,
  referring_partner_id[offset] AS partner_id,
  referring_partner_name[offset] AS partner_name,
  referral_timestamp[offset] AS referral_timestamp,
  referral_index[offset] AS referral_index
FROM `shopify-dw.scratch.derekfeher_modelled_leads_referring_partner_fix`,
UNNEST(referring_partner_id) AS partner_id WITH OFFSET AS offset
),

######
#start of the leads model
######

leads_from_salesforce AS (
SELECT
lead_id
FROM `shopify-dw.sales.sales_leads` leads
-- LEFT JOIN `shopify-dw.raw_partners.partner_leads` pl
LEFT JOIN `shopify-dw.base.base__partner_leads` pl
    ON pl.salesforce_reference_id = leads.lead_id
),

leads_from_trident AS (
SELECT    
    leads.lead_id
FROM `shopify-dw.sales.sales_leads` leads
  JOIN `shopify-dw.static_datasets.raw_salesforce_trident_leads` trident
    ON trident.id = leads.trident_sfdc_id
WHERE product_eligibility_segment IS NULL
),

leads_from_trident_retail AS (
SELECT    
    leads.lead_id
FROM `shopify-dw.sales.sales_leads` leads
  JOIN `shopify-dw.static_datasets.raw_salesforce_trident_leads` trident
    ON trident.id = leads.trident_sfdc_id
WHERE product_eligibility_segment = 'Retail'
),

consolidated_leads_id AS (
SELECT *
FROM leads_from_salesforce

UNION DISTINCT

SELECT * 
FROM leads_from_trident

UNION DISTINCT

SELECT * 
FROM leads_from_trident_retail

UNION DISTINCT

SELECT lead_id
FROM referring_partner
),

fixing_raw_partners_partner_leads_step1 AS (
SELECT 
    leads.*,
    row_number() over (partition by salesforce_reference_id,membership_id order by updated_at desc) as rn
FROM `shopify-dw.base.base__partner_leads` leads
JOIN consolidated_leads_id id
    ON leads.salesforce_reference_id = id.lead_id
WHERE
salesforce_reference_id NOT IN ('00Q8V00001YIahtUAD')
),

fixing_raw_partners_partner_leads AS (
SELECT
    organization_id, #this is shopify_partner_id
    membership_id,
    business_name,
    business_website,
    country,
    product,
    salesforce_reference_id,
    shop_id
FROM fixing_raw_partners_partner_leads_step1
WHERE rn=1
),

partner_sales_assistance AS ( #fixed, eventually get rid of this CTE and plug normally as a left join
SELECT
id AS lead_id,
PartnerInvolvement__c AS partner_requested_sales_assistance
FROM `shopify-dw.raw_salesforce_banff.lead` 
WHERE PartnerInvolvement__c IS NOT NULL
),

sales_assistance AS (
SELECT
id AS lead_id,
Marketing_Qualified_Lead_Date__c
-- case when Sales_Accepted_Lead_Date__c is not null then 'yes' else 'no' end AS sales_assistance
FROM `shopify-dw.raw_salesforce_banff.lead` 
-- WHERE Sales_Accepted_Lead_Date__c IS NOT NULL
),

payment_partners AS (
SELECT
distinct partner_id
FROM (SELECT 
  partner_id,
  program_membership
FROM 
  `sdp-for-analysts-platform.rev_ops_prod.derekfeher_intermediate_partner_dimension_current`,
  UNNEST(all_program_memberships) AS program_membership)
WHERE program_membership LIKE '%payment%'
GROUP BY ALL
),

###############################################################
# Partner Manager at the time of closing the opp
###############################################################

partner_manager_type2 AS ( #needs to be updated, this only references partners_internal dash
SELECT *
FROM `shopify-dw.scratch.derekfeher_modelled_partners_partner_manager_historical`
),

lead_owner_name_and_role AS ( #we might need to make this a bit more comprehensive since it only looks at banff.
SELECT
leads.lead_id,
leads.owner_id,
leads.owner_role AS lead_owner_role,
user.name AS lead_owner_name
FROM `shopify-dw.base.base__salesforce_banff_leads` leads
LEFT JOIN `shopify-dw.base.base__salesforce_banff_users` user
    ON user.user_id = leads.owner_id
),

lead_status_date AS (
SELECT 
lead_id,
end_date AS current_status_date,
FROM `shopify-dw.scratch.andrewyu_modelled_sfdc_lead_status_name_type2`
QUALIFY ROW_NUMBER() OVER (PARTITION BY lead_id ORDER BY end_date DESC) = 1
),

partner_lead_status_date AS (
SELECT 
lead_id,
end_date AS current_status_date,
FROM `shopify-dw.scratch.derekfeher_modelled_sfdc_lead_status_name_type2`
QUALIFY ROW_NUMBER() OVER (PARTITION BY lead_id ORDER BY end_date DESC) = 1
),

all_partner_leads AS (
SELECT 
    leads.lead_id,
    leads.lead_source,
    leads.lead_source_original,
    fllb.routing_segment__c AS routing_segment,
    fllb.Is_Shopify_Customer__c AS is_shopify_customer,
    fllb.Org_Id__c AS organization_id,
    leads.primary_product_interest,
    leads.primary_product_interest_at_lead_creation,
    lowner.lead_owner_name,
    lowner.lead_owner_role,
    COALESCE(fllb.Retail_Locations__c, fllt.number_of_locations) AS retail_locations,
    leads.opportunity_created_at AS opportunity_at, #replacing sales_accepted_lead_date
    leads.new_sales_ready_at, #this is the date that DS use for MQLs
    leads.disqualified_at,
    leads.marketing_nurture_at,
    leads.sales_accepted_lead_at,
    leads.status,
    leads.disqualified_reason,
    lsd.current_status_date,
    COALESCE(fllb.company, fllt.company) AS lead_company_name, #referring lead name (company name referred)
    CASE
        -- Special case for AMER with United States
        WHEN REGEXP_EXTRACT(leads.region, r'^AMER - United States') IS NOT NULL THEN 'United States'
        -- Regular case for other regions and countries
        ELSE REGEXP_EXTRACT(leads.region, r' - (.+)$')
    END AS country_name,
    CASE
        -- Extract the region
        WHEN REGEXP_EXTRACT(leads.region, r'^([^ -]+) -') IS NOT NULL THEN REGEXP_EXTRACT(leads.region, r'^([^ -]+) -')
        ELSE NULL
    END AS region,
    c.opportunity_sub_region,
    c.market_maturity,
    leads.annual_online_revenue,
    leads.annual_offline_revenue,
    leads.third_party_enriched_revenue,
    'NULL' AS temp_2025_gmv_bands,
    'NULL' AS market_segment_25q1,
    leads.converted_account_id,
    leads.converted_contact_id,
    bl.Lead_Grade_Segment__c AS lead_grade,
    date(NULL) AS Marketing_Qualified_Lead_Date__c,
    leads.created_at,
    COALESCE(fllb.LastActivityDate,fllt.last_activity_date) AS last_activity_date,
    CAST(cpl.sf_close_date AS TIMESTAMP) AS closed_won_date, # REPLACE HERE AND ADD THE OPPORTUNITY INFORMATION
    opp.current_stage_name,
    opp.annual_offline_revenue_usd AS opp_annual_offline_revenue_usd,
    opp.annual_online_revenue_verified_usd AS opp_annual_online_revenue_verified_usd,
    opp.annual_online_revenue_verified_usd + opp.annual_offline_revenue_usd AS opp_annual_total_revenue_verified_usd,
    opp.total_revenue_usd AS opp_total_revenue_usd,
    leads.converted_opportunity_id,
    COALESCE(pmt2_plus.partner_manager,pmt2_partners.partner_manager,pmt2_agency.partner_manager,pmt2_retail.partner_manager) as current_partner_manager,
    leads.owner_role,
    pd.partner_name, #changed from business_name
    COALESCE(rp.partner_id, pl.organization_id, SAFE_CAST(opp.winning_partner_id AS INT64)) AS shopify_partner_id,
    MAX(CASE WHEN pmt2_plus.program_handle LIKE 'plus-%' THEN 'Yes' ELSE 'No' END) OVER (PARTITION BY leads.lead_id, pd.partner_id) AS plus_program,
    SAFE_CAST(rp.referral_timestamp AS STRING) AS referral_timestamp,
    SAFE_CAST(rp.referral_index AS STRING) AS referral_index,
    sa.partner_requested_sales_assistance as sales_assistance,
    SAFE_CAST(pl.shop_id AS STRING) AS shop_id,
    pd.region AS partner_region,
    pd.country_name AS partner_country,
    pd.partner_sub_region,
    pd.partner_management_model,
    pd.partner_type,
    campaign.name as campaign_name
    #partner_recruit.recruit_cohort,
    #partner_recruit.current_recruit_funnel_stage,
    #partner_recruit.qualified_date,
    #partner_recruit.is_qualified
FROM consolidated_leads_id id
LEFT JOIN `shopify-dw.sales.sales_leads` leads
    ON leads.lead_id = id.lead_id
LEFT JOIN `shopify-dw.raw_salesforce_banff.lead` bl
    ON id.lead_id  = bl.Id
LEFT JOIN partner_sales_assistance sa
    ON id.lead_id = sa.lead_id
LEFT JOIN partner_lead_status_date lsd
    ON id.lead_id = lsd.lead_id
LEFT JOIN lead_owner_name_and_role lowner
    ON lowner.lead_id = id.lead_id
LEFT JOIN fixing_raw_partners_partner_leads pl #only being brought for country, I have to find a better replacement
    ON pl.salesforce_reference_id = id.lead_id
LEFT JOIN `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` cpl
    ON leads.converted_opportunity_id = cpl.opportunity_id
        AND metric = 'Closed Won'
        AND value_type = 'Product Deal Count'
        AND dataset = ' Actuals'
LEFT JOIN `shopify-dw.sales.sales_opportunities` opp
    ON leads.converted_opportunity_id = opp.opportunity_id
LEFT JOIN `shopify-dw.raw_salesforce_banff.lead` fllb
    ON fllb.id = leads.lead_id
LEFT JOIN `shopify-dw.static_datasets.raw_salesforce_trident_leads` fllt
    on fllt.id = leads.lead_id
LEFT JOIN `shopify-dw.scratch.derekfeher_modelled_partnership_country_dimension` c
    ON c.country_code = pl.country
LEFT JOIN referring_partner rp
    ON rp.lead_id = leads.lead_id
LEFT JOIN `sdp-for-analysts-platform.rev_ops_prod.derekfeher_intermediate_partner_dimension_current` pd
    ON COALESCE(rp.partner_id, SAFE_CAST(opp.winning_partner_id AS INT64)) = pd.partner_id
LEFT JOIN `shopify-dw.raw_salesforce_banff.campaignmember` cm
on cm.leadid = bl.id
LEFT JOIN `shopify-dw.raw_salesforce_banff.campaign` campaign
on campaign.id = cm.CampaignId    
###############################################################
# Partner Recruitment Funnel
###############################################################
#LEFT JOIN `sdp-for-analysts-platform.rev_ops_prod.derekfeher_intermediate_partner_dimension_current` partner_recruit
#    ON COALESCE(rp.partner_id, SAFE_CAST(soc.winning_partner_id AS INT64)) = partner_recruit.partner_id
#        AND DATE(leads.created_at) >= partner_recruit.qualified_date

###############################################################
# Partner Manager at the time of closing the opp
###############################################################
LEFT JOIN partner_manager_type2 AS pmt2_partners
    ON pd.partner_id = pmt2_partners.partner_id
      AND SAFE_CAST(lsd.current_status_date AS DATE) = pmt2_partners.date
      AND lower(pmt2_partners.program_handle) = 'partners'
LEFT JOIN partner_manager_type2 AS pmt2_plus
    ON pd.partner_id = pmt2_plus.partner_id
      AND SAFE_CAST(lsd.current_status_date AS DATE) = pmt2_plus.date
      AND lower(pmt2_plus.program_handle) LIKE 'plus-partners%'
LEFT JOIN partner_manager_type2 AS pmt2_agency
    ON pd.partner_id = pmt2_agency.partner_id
      AND SAFE_CAST(lsd.current_status_date AS DATE) = pmt2_agency.date
      AND lower(pmt2_agency.program_handle) LIKE 'agency%'
LEFT JOIN partner_manager_type2 AS pmt2_retail
    ON pd.partner_id = pmt2_retail.partner_id
      AND SAFE_CAST(lsd.current_status_date AS DATE) = pmt2_retail.date
      AND lower(pmt2_retail.program_handle) LIKE 'retail_partnerships%'    
/*
LEFT JOIN partner_manager_type2 AS pmt2_payment
    ON pd.partner_id = pmt2_payment.partner_id
      AND SAFE_CAST(lsd.current_status_date AS DATE) = pmt2_payment.date
      AND lower(pmt2_payment.program_handle) LIKE '%payment%' 
*/
),

all_marketing_leads AS (
SELECT 
    leads.lead_id,
    leads.lead_source,
    leads.lead_source_original,
    fllb.routing_segment__c AS routing_segment,
    fllb.Is_Shopify_Customer__c AS is_shopify_customer,
    fllb.Org_Id__c AS organization_id,
    leads.primary_product_interest,
    leads.primary_product_interest_at_lead_creation,
    lowner.lead_owner_name,
    lowner.lead_owner_role,
    COALESCE(fllb.Retail_Locations__c, fllt.number_of_locations) AS retail_locations,
    leads.opportunity_created_at AS opportunity_at, #replacing sales_accepted_lead_date
    leads.new_sales_ready_at, #this is the date that DS use for MQLs
    leads.disqualified_at,
    leads.marketing_nurture_at,
    leads.sales_accepted_lead_at,
    leads.status,
    leads.disqualified_reason,
    lsd.current_status_date,
    COALESCE(fllb.company, fllt.company) AS lead_company_name, #referring lead name (company name referred)
    CASE
        -- Special case for AMER with United States
        WHEN REGEXP_EXTRACT(leads.region, r'^AMER - United States') IS NOT NULL THEN 'United States'
        -- Regular case for other regions and countries
        ELSE REGEXP_EXTRACT(leads.region, r' - (.+)$')
    END AS country_name,
    CASE
        -- Extract the region
        WHEN REGEXP_EXTRACT(leads.region, r'^([^ -]+) -') IS NOT NULL THEN REGEXP_EXTRACT(leads.region, r'^([^ -]+) -')
        ELSE NULL
    END AS region,
    'NULL' AS opportunity_sub_region,
    'NULL' AS market_maturity,
    leads.annual_online_revenue,
    leads.annual_offline_revenue,
    leads.third_party_enriched_revenue,
    CASE
        WHEN bl.Annual_Online_Revenue_Verified__c BETWEEN 0 AND 1999999 THEN '0-2M'
        WHEN IFNULL(bl.Annual_Online_Revenue_Verified__c,0) BETWEEN 2000000 AND 4999999 THEN '2-5M' --'SMB Acq'
        WHEN IFNULL(bl.Annual_Online_Revenue_Verified__c,0) BETWEEN 5000000 AND 39999999 THEN '5-40M' --'Mid Market'
        WHEN IFNULL(bl.Annual_Online_Revenue_Verified__c,0) BETWEEN 40000000 AND 199999999 THEN '40-200M' --'Large Accounts'
        WHEN IFNULL(bl.Annual_Online_Revenue_Verified__c,0) > 200000000 THEN '200M+' --'Enterprise'
        ELSE 'Unknown'
    END AS temp_2025_gmv_bands,
    market_segment_25q1,
    leads.converted_account_id,
    leads.converted_contact_id,
    bl.Lead_Grade_Segment__c AS lead_grade,
    date(bl.Marketing_Qualified_Lead_Date__c) as Marketing_Qualified_Lead_Date__c,
    leads.created_at,
    COALESCE(fllb.LastActivityDate,fllt.last_activity_date) AS last_activity_date,
    CAST(cpl.sf_close_date AS TIMESTAMP) AS closed_won_date, # REPLACE HERE AND ADD THE OPPORTUNITY INFORMATION
    opp.current_stage_name,
    opp.annual_offline_revenue_usd AS opp_annual_offline_revenue_usd,
    opp.annual_online_revenue_verified_usd AS opp_annual_online_revenue_verified_usd,
    opp.annual_online_revenue_verified_usd + opp.annual_offline_revenue_usd AS opp_annual_total_revenue_verified_usd,
    opp.total_revenue_usd AS opp_total_revenue_usd,
    leads.converted_opportunity_id,
    'NULL' as current_partner_manager,
    leads.owner_role,
    'NULL' AS partner_name, #changed from business_name
    SAFE_CAST(opp.winning_partner_id AS INT64) AS shopify_partner_id,
    'NULL' AS plus_program,
    'NULL' AS referral_timestamp,
    'NULL' AS referral_index,
    case when sa.Marketing_Qualified_Lead_Date__c is not null then 'Yes' else 'No' end AS sales_assistance,
    'NULL' AS shop_id,
    'NULL' AS partner_region,
    'NULL' AS partner_country,
    'NULL' AS partner_sub_region,
    'NULL' AS partner_management_model,
    'NULL' AS partner_type,
    campaign.name as campaign_name
    #partner_recruit.recruit_cohort,
    #partner_recruit.current_recruit_funnel_stage,
    #partner_recruit.qualified_date,
    #partner_recruit.is_qualified
FROM consolidated_leads_id id
LEFT JOIN `shopify-dw.sales.sales_leads` leads
    ON leads.lead_id = id.lead_id
LEFT JOIN `shopify-dw.raw_salesforce_banff.lead` bl
    ON id.lead_id  = bl.Id
LEFT JOIN sales_assistance sa
    ON id.lead_id = sa.lead_id
LEFT JOIN lead_status_date lsd
    ON id.lead_id = lsd.lead_id
LEFT JOIN lead_owner_name_and_role lowner
    ON lowner.lead_id = id.lead_id
-- LEFT JOIN fixing_raw_partners_partner_leads pl #only being brought for country, I have to find a better replacement
--     ON pl.salesforce_reference_id = id.lead_id
LEFT JOIN `sdp-for-analysts-platform.rev_ops_prod.modelled_rad` cpl
    ON leads.converted_opportunity_id = cpl.opportunity_id
        AND metric = 'Closed Won'
        AND value_type = 'Product Deal Count'
        AND dataset = ' Actuals'
LEFT JOIN `shopify-dw.sales.sales_opportunities` opp
    ON leads.converted_opportunity_id = opp.opportunity_id
LEFT JOIN `shopify-dw.raw_salesforce_banff.lead` fllb
    ON fllb.id = leads.lead_id
LEFT JOIN `shopify-dw.static_datasets.raw_salesforce_trident_leads` fllt
    on fllt.id = leads.lead_id
LEFT JOIN (SELECT account_id, market_segment_25q1, team_segment_25q1 FROM `sdp-for-analysts-platform.rev_ops_prod.salesforce_accounts`) account
    on account.account_id = leads.converted_account_id
LEFT JOIN `shopify-dw.raw_salesforce_banff.campaignmember` cm
    on cm.leadid = bl.id
LEFT JOIN `shopify-dw.raw_salesforce_banff.campaign` campaign
    on campaign.id = cm.CampaignId
-- LEFT JOIN `shopify-dw.scratch.derekfeher_modelled_partnership_country_dimension` c
--     ON c.country_code = pl.country
-- LEFT JOIN referring_partner rp
--     ON rp.lead_id = leads.lead_id
-- LEFT JOIN `sdp-for-analysts-platform.rev_ops_prod.derekfeher_intermediate_partner_dimension_current` pd
--     ON COALESCE(rp.partner_id, SAFE_CAST(opp.winning_partner_id AS INT64)) = pd.partner_id
WHERE leads.lead_id IS NOT NULL AND leads.lead_source_original NOT IN ('Partner POS Referral','Partner Referral', 'ZoomInfo', 'LinkedIn Sales Navigator', 'Outbound', 'Business Development Rep', 'Lead List')
)

, pql as (
SELECT 
    'PQL' as metric_name,
    apl.lead_id,
    apl.lead_source,
    apl.lead_source_original,
    apl.routing_segment,
    apl.is_shopify_customer,
    apl.organization_id,
    apl.primary_product_interest,
    apl.primary_product_interest_at_lead_creation,
    apl.lead_owner_name,
    apl.lead_owner_role,
    apl.retail_locations,
    apl.opportunity_at, #replacing sales_accepted_lead_date
    apl.new_sales_ready_at,
    -- CASE WHEN apl.new_sales_ready_at IS NOT NULL THEN apl.lead_id ELSE NULL END AS pql_id,
    CASE WHEN apl.new_sales_ready_at IS NOT NULL THEN apl.lead_id ELSE NULL END AS qualified_lead_id,
    CASE WHEN apl.disqualified_at IS NOT NULL THEN apl.lead_id ELSE NULL END AS disqualified_id,
    CASE WHEN apl.marketing_nurture_at IS NOT NULL THEN apl.lead_id ELSE NULL END AS marketing_nurture_id,
    CASE WHEN apl.new_sales_ready_at IS NULL THEN apl.lead_id ELSE NULL END AS unworked_id,
    apl.disqualified_at,
    apl.marketing_nurture_at,
    apl.sales_accepted_lead_at,
    apl.status,
    apl.disqualified_reason,
    apl.current_status_date,
    DATE_DIFF(CURRENT_DATE(), SAFE_CAST(apl.current_status_date AS DATE), DAY) AS days_in_current_status,
    apl.lead_company_name, #referring lead name (company name referred)
    apl.country_name,
    apl.region,
    apl.opportunity_sub_region,
    apl.market_maturity,
    apl.annual_online_revenue,
    apl.annual_offline_revenue,
    apl.third_party_enriched_revenue,
    apl.temp_2025_gmv_bands,
    market_segment_25q1,
    apl.converted_account_id,
    apl.converted_contact_id,
    apl.lead_grade,
    apl.Marketing_Qualified_Lead_Date__c,
    apl.created_at,
    apl.last_activity_date,
    apl.current_stage_name,
    ifnull(apl.opp_annual_offline_revenue_usd,0) as opp_annual_offline_revenue_usd,
    ifnull(apl.opp_annual_online_revenue_verified_usd,0) as opp_annual_online_revenue_verified_usd,
    apl.opp_annual_total_revenue_verified_usd,
    apl.opp_total_revenue_usd,
    apl.closed_won_date,
    apl.converted_opportunity_id,
    COALESCE(pdmtype2.partner_development_manager_name,apl.current_partner_manager) AS current_partner_manager, #update
    COALESCE(prmtype2.partner_recruiter_name,pd.partner_recruiter) AS partner_recruiter,
    COALESCE(apl.partner_name, pd.partner_name) as partner_name, #changed from business_name
    shopify_partner_id, #add the account_id IF possible
    pd.partner_account_id,
    apl.plus_program,
    CASE WHEN payment.partner_id IS NOT NULL THEN 'Yes' ELSE 'No' END AS payment_program,
    bl.Referrer_Name__c AS referrer_name,
    bl.Referrer_Email__c AS referrer_email,
    apl.referral_timestamp,
    apl.referral_index,
    apl.sales_assistance,
    apl.shop_id,
    apl.region AS partner_region,
    apl.country_name AS partner_country,
    apl.partner_sub_region,
    apl.partner_management_model,
    apl.partner_type,
    apl.campaign_name
    #apl.recruit_cohort,
    #apl.current_recruit_funnel_stage,
    #apl.qualified_date,
    #apl.is_qualified
FROM all_partner_leads apl
LEFT JOIN `sdp-for-analysts-platform.rev_ops_prod.derekfeher_intermediate_partner_dimension_current` pd
    ON apl.shopify_partner_id = pd.partner_id
LEFT JOIN `shopify-dw.scratch.derekfeher_intermediate_partner_development_manager_sfdc_type2` pdmtype2
    ON pdmtype2.accountid = pd.partner_account_id
        AND DATE(apl.created_at) BETWEEN pdmtype2.start_date AND pdmtype2.end_date
LEFT JOIN `shopify-dw.scratch.derekfeher_intermediate_partner_recruiter_sfdc_type2` prmtype2
    ON prmtype2.accountid = pd.partner_account_id
        AND DATE(apl.created_at) BETWEEN prmtype2.start_date AND prmtype2.end_date
LEFT JOIN `shopify-dw.raw_salesforce_banff.lead` bl
    ON bl.id = apl.lead_id
LEFT JOIN payment_partners payment
    ON payment.partner_id = apl.shopify_partner_id
WHERE lead_id IS NOT NULL AND (apl.lead_source IN ('Partner POS Referral','Partner Referral')
OR apl.lead_source_original IN ('Partner POS Referral','Partner Referral'))
QUALIFY ROW_NUMBER() OVER (PARTITION BY apl.lead_id, shopify_partner_id) = 1
)

, mql as (
SELECT 
    'MQL' as metric_name,
    apl.lead_id,
    apl.lead_source,
    apl.lead_source_original,
    apl.routing_segment,
    apl.is_shopify_customer,
    apl.organization_id,
    apl.primary_product_interest,
    apl.primary_product_interest_at_lead_creation,
    apl.lead_owner_name,
    apl.lead_owner_role,
    apl.retail_locations,
    apl.opportunity_at, #replacing sales_accepted_lead_date
    apl.new_sales_ready_at,
    -- CASE WHEN apl.new_sales_ready_at IS NOT NULL THEN apl.lead_id ELSE NULL END AS mql_id,
    CASE WHEN apl.new_sales_ready_at IS NOT NULL THEN apl.lead_id ELSE NULL END AS qualified_lead_id,
    CASE WHEN apl.disqualified_at IS NOT NULL THEN apl.lead_id ELSE NULL END AS disqualified_id,
    CASE WHEN apl.marketing_nurture_at IS NOT NULL THEN apl.lead_id ELSE NULL END AS marketing_nurture_id,
    CASE WHEN apl.new_sales_ready_at IS NULL THEN apl.lead_id ELSE NULL END AS unworked_id,
    apl.disqualified_at,
    apl.marketing_nurture_at,
    apl.sales_accepted_lead_at,
    apl.status,
    apl.disqualified_reason,
    apl.current_status_date,
    DATE_DIFF(CURRENT_DATE(), SAFE_CAST(apl.current_status_date AS DATE), DAY) AS days_in_current_status,
    apl.lead_company_name, #referring lead name (company name referred)
    apl.country_name,
    apl.region,
    apl.opportunity_sub_region,
    apl.market_maturity,
    apl.annual_online_revenue,
    apl.annual_offline_revenue,
    apl.third_party_enriched_revenue,
    COALESCE(apl.temp_2025_gmv_bands, 'Unknown') AS temp_2025_gmv_bands,
    market_segment_25q1,
    apl.converted_account_id,
    apl.converted_contact_id,
    apl.lead_grade,
    apl.Marketing_Qualified_Lead_Date__c,
    apl.created_at,
    apl.last_activity_date,
    apl.current_stage_name,
    apl.opp_annual_offline_revenue_usd,
    ifnull(apl.opp_annual_offline_revenue_usd,0) as opp_annual_offline_revenue_usd,
    ifnull(apl.opp_annual_online_revenue_verified_usd,0) as opp_annual_online_revenue_verified_usd,
    apl.opp_total_revenue_usd,
    apl.closed_won_date,
    apl.converted_opportunity_id,
    COALESCE(pdmtype2.partner_development_manager_name,apl.current_partner_manager) AS current_partner_manager, #update
    COALESCE(prmtype2.partner_recruiter_name,pd.partner_recruiter) AS partner_recruiter,
    COALESCE(apl.partner_name, pd.partner_name) as partner_name, #changed from business_name
    shopify_partner_id, #add the account_id IF possible
    pd.partner_account_id,
    apl.plus_program,
    CASE WHEN payment.partner_id IS NOT NULL THEN 'Yes' ELSE 'No' END AS payment_program,
    bl.Referrer_Name__c AS referrer_name,
    bl.Referrer_Email__c AS referrer_email,
    apl.referral_timestamp,
    apl.referral_index,
    apl.sales_assistance,
    apl.shop_id,
    apl.region AS partner_region,
    apl.country_name AS partner_country,
    apl.partner_sub_region,
    apl.partner_management_model,
    apl.partner_type,
    apl.campaign_name
    #apl.recruit_cohort,
    #apl.current_recruit_funnel_stage,
    #apl.qualified_date,
    #apl.is_qualified
FROM all_marketing_leads apl
LEFT JOIN `sdp-for-analysts-platform.rev_ops_prod.derekfeher_intermediate_partner_dimension_current` pd
    ON apl.shopify_partner_id = pd.partner_id
LEFT JOIN `shopify-dw.scratch.derekfeher_intermediate_partner_development_manager_sfdc_type2` pdmtype2
    ON pdmtype2.accountid = pd.partner_account_id
--         AND DATE(apl.created_at) BETWEEN pdmtype2.start_date AND pdmtype2.end_date
LEFT JOIN `shopify-dw.scratch.derekfeher_intermediate_partner_recruiter_sfdc_type2` prmtype2
    ON prmtype2.accountid = pd.partner_account_id
--         AND DATE(apl.created_at) BETWEEN prmtype2.start_date AND prmtype2.end_date
LEFT JOIN `shopify-dw.raw_salesforce_banff.lead` bl
    ON bl.id = apl.lead_id
LEFT JOIN payment_partners payment
    ON payment.partner_id = apl.shopify_partner_id
WHERE lead_id IS NOT NULL AND apl.lead_source_original NOT IN ('Partner POS Referral','Partner Referral', 'ZoomInfo', 'LinkedIn Sales Navigator', 'Outbound', 'Business Development Rep', 'Lead List')
QUALIFY ROW_NUMBER() OVER (PARTITION BY apl.lead_id, shopify_partner_id) = 1
)

select * from pql 
union all 
select * from mql
