CREATE OR REPLACE TABLE shopify-dw.scratch.modelled_86_expired_but_non_terminated_billing_deals
AS

WITH n_shops as (
 select billing_deal_id, count(*) n_active_shops
 from `shopify-dw.raw_billing.billing_deal_agreements` bda
 where not is_deleted
 and (terminated_on is null or date(terminated_on) > current_date)
 group by billing_deal_id
)

, renewed as (
 select original_deal_id
 from `shopify-data-bigquery-global.raw_shopify.from_longboat_billing_deals`
 where not is_deleted
 and original_deal_id is not null
 -- and (
 --   terminated_on is null or try_cast(end_date as date) >= current_date
 -- )
)

select 'https://app.shopify.com/services/internal/deals/' || cast(bd.id as string) billing_deal, name, start_date, terminated_on, coalesce(n_active_shops, 0) n_active_shops
from `shopify-dw.raw_billing.billing_deals` bd
left join n_shops on billing_deal_id = bd.id
where bd.terminated_on is null
and date(terminated_on) < current_date
and not is_deleted
and bd.id not in (
 select original_deal_id from renewed
)
order by n_active_shops desc
