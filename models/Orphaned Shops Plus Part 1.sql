CREATE OR REPLACE TABLE shopify-dw.scratch.modelled_86_orphaned_shops_plus_pt1
AS

with inactive_plans as (
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


select _reason, shop_url, name, base_price
from
(
   select 'Something is wrong' _reason, s.shop_id, 'https://app.shopify.com/services/internal/shops/' || cast(s.shop_id as string) shop_url, s.name, sub.base_price
   from `shopify-dw.base.base__shopify_shops` s
   inner join `shopify-dw.raw_billing.billing_accounts` a on s.billing_account_id = a.id
   join `shopify-dw.raw_billing.subscriptions` sub on a.current_subscription_id = sub.id and sub.plan_name = 'shopify_plus'
   where s.shop_id in (select shop_id from inactive_plans)
   and s.shop_id not in (select shop_id from active_plans)
   and s.email not like '%@shopify%'

   union all

   select 'No billing deal at all' _reason, s.shop_id, 'https://app.shopify.com/services/internal/shops/' || cast(s.shop_id as string)  shop_url, s.name, sub.base_price
   from `shopify-dw.base.base__shopify_shops` s
   -- join hive.raw_shopify.subscriptions sub on s.active_subscription_id = sub.id and sub.plan_name = 'shopify_plus'
   inner join `shopify-dw.raw_billing.billing_accounts` a on s.billing_account_id = a.id
   join `shopify-dw.raw_billing.subscriptions` sub on a.current_subscription_id = sub.id and sub.plan_name = 'shopify_plus'
   WHERE s.shop_id not in (select shop_id from plans)
   and s.email not like '%@shopify%'
) order by base_price desc
