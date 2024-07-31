CREATE OR REPLACE TABLE shopify-dw.scratch.modelled_86_orphaned_shops_plus_pt2
AS

WITH inactive_plans as (
   select bda.shop_id
   from `shopify-dw.raw_billing.billing_deal_agreements` bda
   join `shopify-dw.raw_billing.billing_deals` bd on bd.id = bda.billing_deal_id
   where (bd.terminated_on is not null or date(bd.terminated_on) < current_date)
   and (bda.terminated_on is not null or date(bda.terminated_on) < current_date)
   and not bda.is_deleted
   and not bd.is_deleted
)


, active_plans as (
   select bda.shop_id
   from `shopify-dw.raw_billing.billing_deal_agreements` bda
   join `shopify-dw.raw_billing.billing_deals` bd on bd.id = bda.billing_deal_id
   where (bd.terminated_on is null or date(bd.terminated_on) >= current_date)
   and (bda.terminated_on is null or date(bda.terminated_on) >= current_date)
)


, plans as (
   select bda.shop_id
   from `shopify-dw.raw_billing.billing_deal_agreements` bda
   where terminated_on is null or date(terminated_on) >= current_date   
)


, base_query AS
(
select _reason, shop_url, shop_id, name, base_price
from (
   select 'Something is wrong' _reason, s.shop_id as shop_id, 'https://app.shopify.com/services/internal/shops/' || cast(s.shop_id as string) shop_url, s.name, sub.base_price
   from `shopify-dw.base.base__shopify_shops` s
   -- hive.raw_shopify.shops s (old table)
   inner join `shopify-dw.raw_billing.billing_accounts` a on s.billing_account_id = a.id
   -- hive.raw_shopify.billing_accounts (old table)
   join `shopify-dw.raw_billing.subscriptions` sub on a.current_subscription_id = sub.id and sub.plan_name = 'shopify_plus'
   -- hive.raw_shopify.subscriptions (old table)
   where s.shop_id in (select shop_id from inactive_plans)
   and s.shop_id not in (select shop_id from active_plans)
   and s.email not like '%@shopify%'

   union all
  
   select 'No billing deal at all' _reason, s.shop_id as shop_id, 'https://app.shopify.com/services/internal/shops/' || cast(s.shop_id as string)  shop_url, s.name, sub.base_price
   from  `shopify-dw.base.base__shopify_shops` s
   -- join hive.raw_shopify.subscriptions sub on s.active_subscription_id = sub.id and sub.plan_name = 'shopify_plus'
   inner join `shopify-dw.raw_billing.billing_accounts` a
   on s.billing_account_id = a.id
   join `shopify-dw.raw_billing.subscriptions` sub
   on a.current_subscription_id = sub.id and sub.plan_name = 'shopify_plus'
   where s.shop_id not in (select shop_id from plans)
   and s.email not like '%@shopify%'
) order by base_price desc
),


dropped_duplicates AS
(
SELECT
  bq.shop_url
 ,bq.shop_id
 ,bq.name
 ,bq.base_price
 -- ,ARRAY_JOIN(ARRAY_AGG(bq._reason), ', ') AS reasons (old query)
 ,STRING_AGG(bq._reason, ', ') AS reasons


FROM base_query AS bq


GROUP BY
  bq.shop_url
 ,bq.shop_id
 ,bq.name
 ,bq.base_price
)


SELECT
  dd.*
--  ,md.current_merchant_success_manager AS current_msm
,md.merchant_success_manager AS current_msm


FROM dropped_duplicates AS dd

-- limit 100
-- INNER JOIN `shopify-data-bigquery-global.finance.shop_dimension` AS sd
INNER JOIN `sdp-for-analysts-platform.rev_ops_prod.modelled_shop_dimension` AS sd
 ON sd.shop_id = dd.shop_id
INNER JOIN `shopify-dw.sales.salesforce_accounts` AS md
 ON md.account_id = sd.account_id
--  INNER JOIN `shopify-data-bigquery-global.plus.merchant_dimension` AS md
-- -- change above table to `sdp-prd-finance-data-science.base.merchant_dimension` after SDP migration, this table is deprecated so using salesforce_accounts intead
--  ON md.merchant_id = sd.current_merchant_id
