CREATE OR REPLACE TABLE shopify-dw.scratch.modelled_84_nondev_stores_for_deal
AS

select sh.name, sh.permanent_domain, bda.shop_id, bda.reason, bda.billing_deal_id
from `shopify-dw.raw_billing.billing_deal_agreements` bda
join `shopify-dw.base.base__shopify_shops` sh on sh.shop_id = bda.shop_id
where bda.reason <> 'Development/Staging shop'
and bda.terminated_on is NULL
and bda.is_deleted = false
