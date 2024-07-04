SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell') as ops_market_segment, 1 as sort_id, 'Created Opp Volume' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type, 
  COUNT(DISTINCT opportunity_id)/13 as Total, 
  COUNT(DISTINCT CASE WHEN ops_lead_source = 'Inbound' THEN opportunity_id END)/13 as Marketing, 
  COUNT(DISTINCT CASE WHEN ops_lead_source = 'Partner' THEN opportunity_id END)/13 as Partner, 
  COUNT(DISTINCT CASE WHEN ops_lead_source = 'Outbound' THEN opportunity_id END)/13 as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset IN ('Actuals') AND value_type IN ('Opportunity Deal Count') AND metric IN ('Created')  AND ops_market_segment IN ('Enterprise','Large Accounts','Mid Market','Cross-Sell','SMB','Retail') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell') as ops_market_segment, 1 as sort_id, 'Created Opp Volume' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type, 
  SUM(value)/13 as Total, 
  SUM(CASE WHEN ops_lead_source = 'Inbound' THEN value END)/13 as Marketing, 
  SUM(CASE WHEN ops_lead_source = 'Partner' THEN value END)/13 as Partner, 
  SUM(CASE WHEN ops_lead_source = 'Outbound' THEN value END)/13 as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset IN ('Targets') AND value_type IN ('Product Deal Count') AND metric IN ('Created')  AND ops_market_segment IN ('Enterprise','Large Accounts','Mid Market','Cross-Sell','SMB','Retail') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell') as ops_market_segment, 1 as sort_id, 'Created Opp Volume' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type, 
  SUM(value)/13 as Total, 
  SUM(CASE WHEN ops_lead_source = 'Inbound' THEN value END)/13 as Marketing, 
  SUM(CASE WHEN ops_lead_source = 'Partner' THEN value END)/13 as Partner, 
  SUM(CASE WHEN ops_lead_source = 'Outbound' THEN value END)/13 as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset IN ('Targets','Actuals') AND metric IN ('Created') AND value_type IN ('Total Revenue') AND ops_market_segment IN ('Enterprise','Large Accounts','Mid Market','Cross-Sell','SMB','Retail') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell'), 2 as sort_id, 'Qualified Opp Total Revenue Avg ($)' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type,
  SUM(value)/COUNT(DISTINCT opportunity_id) as Total, 
  SUM(CASE WHEN ops_lead_source = 'Inbound' THEN value END)/COUNT(DISTINCT CASE WHEN ops_lead_source = 'Inbound' THEN opportunity_id END) as Marketing, 
  SUM(CASE WHEN ops_lead_source = 'Partner' THEN value END)/COUNT(DISTINCT CASE WHEN ops_lead_source = 'Partner' THEN opportunity_id END) as Partner, 
  SUM(CASE WHEN ops_lead_source = 'Outbound' THEN value END)/COUNT(DISTINCT CASE WHEN ops_lead_source = 'Outbound' THEN opportunity_id END) as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE ((dataset IN ('Actuals','Targets') AND ops_market_segment IN ('Enterprise','Large Accounts','Cross-Sell','SMB','Retail'))
OR (dataset IN ('Actuals','Targets') AND ops_market_segment IN ('Mid Market')))
 AND metric IN ('Qualified SAL') AND value_type IN ('Total Revenue','Incremental Product Profit') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell'), 3 as sort_id, 'Created to Qualified (days)' as metric, '*' as label, CONCAT(year,'-',quarter), value_type,
  AVG(DATE_DIFF(qualified_sal_date,sf_created_date,DAY)) as Total, 
  AVG(CASE WHEN ops_lead_source = 'Inbound' THEN DATE_DIFF(qualified_sal_date,sf_created_date,DAY) END) as Marketing, 
  AVG(CASE WHEN ops_lead_source = 'Partner' THEN DATE_DIFF(qualified_sal_date,sf_created_date,DAY) END) as Partner, 
  AVG(CASE WHEN ops_lead_source = 'Outbound' THEN DATE_DIFF(qualified_sal_date,sf_created_date,DAY) END) as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset = 'Actuals' AND metric IN ('Pipeline SAL') AND value_type = 'Opportunity Deal Count' AND ops_market_segment IN ('Enterprise','Large Accounts','Mid Market','Cross-Sell','SMB','Retail') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell'), 4 as sort_id, 'Qualified to Win (days)' as metric, '*' as label, CONCAT(year,'-',quarter), value_type,
  AVG(DATE_DIFF(sf_close_date,qualified_sal_date,DAY)) as Total, 
  AVG(CASE WHEN ops_lead_source = 'Inbound' THEN DATE_DIFF(sf_close_date,qualified_sal_date,DAY) END) as Marketing, 
  AVG(CASE WHEN ops_lead_source = 'Partner' THEN DATE_DIFF(sf_close_date,qualified_sal_date,DAY) END) as Partner, 
  AVG(CASE WHEN ops_lead_source = 'Outbound' THEN DATE_DIFF(sf_close_date,qualified_sal_date,DAY) END) as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset = 'Actuals' AND metric IN ('Closed Won') AND value_type = 'Opportunity Deal Count' AND ops_market_segment IN ('Enterprise','Large Accounts','Mid Market','Cross-Sell','SMB','Retail') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell'), 5 as sort_id, 'Time to Win (days)' as metric, '*' as label, CONCAT(year,'-',quarter), value_type,
  AVG(DATE_DIFF(sf_close_date,sf_created_date,DAY)) as Total, 
  AVG(CASE WHEN ops_lead_source = 'Inbound' THEN DATE_DIFF(sf_close_date,sf_created_date,DAY) END) as Marketing, 
  AVG(CASE WHEN ops_lead_source = 'Partner' THEN DATE_DIFF(sf_close_date,sf_created_date,DAY) END) as Partner, 
  AVG(CASE WHEN ops_lead_source = 'Outbound' THEN DATE_DIFF(sf_close_date,sf_created_date,DAY) END) as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset = 'Actuals' AND metric IN ('Closed Won') AND value_type = 'Opportunity Deal Count' AND ops_market_segment IN ('Enterprise','Large Accounts','Mid Market','Cross-Sell','SMB','Retail') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell'), 6 as sort_id, 'Win Rate' as metric, '*' as label, CONCAT(year,'-',quarter), value_type,
  COUNT(DISTINCT CASE WHEN metric = 'Closed Won' THEN opportunity_id END) / COUNT(DISTINCT opportunity_id) as Total, 
  CASE WHEN COUNT(DISTINCT CASE WHEN ops_lead_source = 'Inbound' THEN opportunity_id END) > 0 THEN COUNT(DISTINCT CASE WHEN ops_lead_source = 'Inbound' AND metric = 'Closed Won' THEN opportunity_id END) / COUNT(DISTINCT CASE WHEN ops_lead_source = 'Inbound' THEN opportunity_id END) END as Marketing, 
  CASE WHEN COUNT(DISTINCT CASE WHEN ops_lead_source = 'Partner' THEN opportunity_id END) > 0 THEN COUNT(DISTINCT CASE WHEN ops_lead_source = 'Partner' AND metric = 'Closed Won' THEN opportunity_id END) / COUNT(DISTINCT CASE WHEN ops_lead_source = 'Partner' THEN opportunity_id END) END as Partner, 
  CASE WHEN COUNT(DISTINCT CASE WHEN ops_lead_source = 'Outbound' THEN opportunity_id END) > 0 THEN COUNT(DISTINCT CASE WHEN ops_lead_source = 'Outbound' AND metric = 'Closed Won' THEN opportunity_id END) / COUNT(DISTINCT CASE WHEN ops_lead_source = 'Outbound' THEN opportunity_id END) END as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset = 'Actuals' AND metric IN ('Closed Won','Closed Lost') AND value_type = 'Opportunity Deal Count' AND ops_market_segment IN ('Enterprise','Large Accounts','Mid Market','Cross-Sell','SMB','Retail') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell'), 7 as sort_id, 'Closed Won' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type,
  SUM(value) as Total, 
  SUM(CASE WHEN ops_lead_source = 'Inbound' THEN value END) as Marketing, 
  SUM(CASE WHEN ops_lead_source = 'Partner' THEN value END) as Partner, 
  SUM(CASE WHEN ops_lead_source = 'Outbound' THEN value END) as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE ((dataset IN ('Actuals','Targets') AND ops_market_segment IN ('Enterprise','Large Accounts','Cross-Sell','SMB','Retail'))
OR (dataset IN ('Actuals','Targets') AND ops_market_segment IN ('Mid Market')))
  AND metric IN ('Closed Won') AND value_type IN ('Total Revenue','Incremental Product Profit') AND ops_market_segment IN ('Enterprise','Large Accounts','Mid Market','Cross-Sell','SMB','Retail') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell'), 8 as sort_id, 'Closed Won Avg' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type,
  SUM(value) / COUNT(DISTINCT opportunity_id) as Total, 
  SUM(CASE WHEN ops_lead_source = 'Inbound' THEN value END) / COUNT(DISTINCT CASE WHEN ops_lead_source = 'Inbound' THEN opportunity_id END) as Marketing, 
  SUM(CASE WHEN ops_lead_source = 'Partner' THEN value END) / COUNT(DISTINCT CASE WHEN ops_lead_source = 'Partner' THEN opportunity_id END) as Partner, 
  SUM(CASE WHEN ops_lead_source = 'Outbound' THEN value END) / COUNT(DISTINCT CASE WHEN ops_lead_source = 'Outbound' THEN opportunity_id END) as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE ((dataset IN ('Actuals','Targets') AND ops_market_segment IN ('Enterprise','Large Accounts','Cross-Sell','SMB','Retail'))
OR (dataset IN ('Actuals','Targets') AND ops_market_segment IN ('Mid Market')))
  AND metric IN ('Closed Won') AND value_type IN ('Total Revenue','Incremental Product Profit') AND ops_market_segment IN ('Enterprise','Large Accounts','Mid Market','Cross-Sell','SMB','Retail') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell'), 9 as sort_id, 'Heat Count Capacity' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type,
  SUM(value) as Total, 
  0 as Marketing, 
  0 as Partner, 
  0 as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset IN ('Head Count') AND ops_market_segment IN ('Enterprise','Large Accounts','Cross-Sell','SMB','Retail','Mid Market') AND metric IN ('Head Count Capacity')  AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell') as ops_market_segment, 1 as sort_id, 'Created Opp Volume' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type, 
  COUNT(DISTINCT opportunity_id)/13 as Total, 
  COUNT(DISTINCT CASE WHEN ops_lead_source = 'Inbound' THEN opportunity_id END)/13 as Marketing, 
  COUNT(DISTINCT CASE WHEN ops_lead_source = 'Partner' THEN opportunity_id END)/13 as Partner, 
  COUNT(DISTINCT CASE WHEN ops_lead_source = 'Outbound' THEN opportunity_id END)/13 as Sales, 'GLOBAL'
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset IN ('Actuals') AND value_type IN ('Opportunity Deal Count') AND metric IN ('Created')  AND ops_market_segment IN ('Enterprise','Large Accounts','Mid Market','Cross-Sell','SMB','Retail') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell') as ops_market_segment, 1 as sort_id, 'Created Opp Volume' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type, 
  SUM(value)/13 as Total, 
  SUM(CASE WHEN ops_lead_source = 'Inbound' THEN value END)/13 as Marketing, 
  SUM(CASE WHEN ops_lead_source = 'Partner' THEN value END)/13 as Partner, 
  SUM(CASE WHEN ops_lead_source = 'Outbound' THEN value END)/13 as Sales, 'GLOBAL'
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset IN ('Targets') AND value_type IN ('Product Deal Count') AND metric IN ('Created')  AND ops_market_segment IN ('Enterprise','Large Accounts','Mid Market','Cross-Sell','SMB','Retail') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell') as ops_market_segment, 1 as sort_id, 'Created Opp Volume' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type, 
  SUM(value)/13 as Total, 
  SUM(CASE WHEN ops_lead_source = 'Inbound' THEN value END)/13 as Marketing, 
  SUM(CASE WHEN ops_lead_source = 'Partner' THEN value END)/13 as Partner, 
  SUM(CASE WHEN ops_lead_source = 'Outbound' THEN value END)/13 as Sales, 'GLOBAL'
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset IN ('Targets','Actuals') AND metric IN ('Created') AND value_type IN ('Total Revenue') AND ops_market_segment IN ('Enterprise','Large Accounts','Mid Market','Cross-Sell','SMB','Retail') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell'), 2 as sort_id, 'Qualified Opp Total Revenue Avg ($)' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type,
  SUM(value)/COUNT(DISTINCT opportunity_id) as Total, 
  SUM(CASE WHEN ops_lead_source = 'Inbound' THEN value END)/COUNT(DISTINCT CASE WHEN ops_lead_source = 'Inbound' THEN opportunity_id END) as Marketing, 
  SUM(CASE WHEN ops_lead_source = 'Partner' THEN value END)/COUNT(DISTINCT CASE WHEN ops_lead_source = 'Partner' THEN opportunity_id END) as Partner, 
  SUM(CASE WHEN ops_lead_source = 'Outbound' THEN value END)/COUNT(DISTINCT CASE WHEN ops_lead_source = 'Outbound' THEN opportunity_id END) as Sales, 'GLOBAL'
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE ((dataset IN ('Actuals','Targets') AND ops_market_segment IN ('Enterprise','Large Accounts','Cross-Sell','SMB','Retail'))
OR (dataset IN ('Actuals','Targets') AND ops_market_segment IN ('Mid Market')))
 AND metric IN ('Qualified SAL') AND value_type IN ('Total Revenue','Incremental Product Profit') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell'), 3 as sort_id, 'Created to Qualified (days)' as metric, '*' as label, CONCAT(year,'-',quarter), value_type,
  AVG(DATE_DIFF(qualified_sal_date,sf_created_date,DAY)) as Total, 
  AVG(CASE WHEN ops_lead_source = 'Inbound' THEN DATE_DIFF(qualified_sal_date,sf_created_date,DAY) END) as Marketing, 
  AVG(CASE WHEN ops_lead_source = 'Partner' THEN DATE_DIFF(qualified_sal_date,sf_created_date,DAY) END) as Partner, 
  AVG(CASE WHEN ops_lead_source = 'Outbound' THEN DATE_DIFF(qualified_sal_date,sf_created_date,DAY) END) as Sales, 'GLOBAL'
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset = 'Actuals' AND metric IN ('Pipeline SAL') AND value_type = 'Opportunity Deal Count' AND ops_market_segment IN ('Enterprise','Large Accounts','Mid Market','Cross-Sell','SMB','Retail') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell'), 4 as sort_id, 'Qualified to Win (days)' as metric, '*' as label, CONCAT(year,'-',quarter), value_type,
  AVG(DATE_DIFF(sf_close_date,qualified_sal_date,DAY)) as Total, 
  AVG(CASE WHEN ops_lead_source = 'Inbound' THEN DATE_DIFF(sf_close_date,qualified_sal_date,DAY) END) as Marketing, 
  AVG(CASE WHEN ops_lead_source = 'Partner' THEN DATE_DIFF(sf_close_date,qualified_sal_date,DAY) END) as Partner, 
  AVG(CASE WHEN ops_lead_source = 'Outbound' THEN DATE_DIFF(sf_close_date,qualified_sal_date,DAY) END) as Sales, 'GLOBAL'
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset = 'Actuals' AND metric IN ('Closed Won') AND value_type = 'Opportunity Deal Count' AND ops_market_segment IN ('Enterprise','Large Accounts','Mid Market','Cross-Sell','SMB','Retail') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell'), 5 as sort_id, 'Time to Win (days)' as metric, '*' as label, CONCAT(year,'-',quarter), value_type,
  AVG(DATE_DIFF(sf_close_date,sf_created_date,DAY)) as Total, 
  AVG(CASE WHEN ops_lead_source = 'Inbound' THEN DATE_DIFF(sf_close_date,sf_created_date,DAY) END) as Marketing, 
  AVG(CASE WHEN ops_lead_source = 'Partner' THEN DATE_DIFF(sf_close_date,sf_created_date,DAY) END) as Partner, 
  AVG(CASE WHEN ops_lead_source = 'Outbound' THEN DATE_DIFF(sf_close_date,sf_created_date,DAY) END) as Sales, 'GLOBAL'
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset = 'Actuals' AND metric IN ('Closed Won') AND value_type = 'Opportunity Deal Count' AND ops_market_segment IN ('Enterprise','Large Accounts','Mid Market','Cross-Sell','SMB','Retail') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell'), 6 as sort_id, 'Win Rate' as metric, '*' as label, CONCAT(year,'-',quarter), value_type,
  COUNT(DISTINCT CASE WHEN metric = 'Closed Won' THEN opportunity_id END) / COUNT(DISTINCT opportunity_id) as Total, 
  CASE WHEN COUNT(DISTINCT CASE WHEN ops_lead_source = 'Inbound' THEN opportunity_id END) > 0 THEN COUNT(DISTINCT CASE WHEN ops_lead_source = 'Inbound' AND metric = 'Closed Won' THEN opportunity_id END) / COUNT(DISTINCT CASE WHEN ops_lead_source = 'Inbound' THEN opportunity_id END) END as Marketing, 
  CASE WHEN COUNT(DISTINCT CASE WHEN ops_lead_source = 'Partner' THEN opportunity_id END) > 0 THEN COUNT(DISTINCT CASE WHEN ops_lead_source = 'Partner' AND metric = 'Closed Won' THEN opportunity_id END) / COUNT(DISTINCT CASE WHEN ops_lead_source = 'Partner' THEN opportunity_id END) END as Partner, 
  CASE WHEN COUNT(DISTINCT CASE WHEN ops_lead_source = 'Outbound' THEN opportunity_id END) > 0 THEN COUNT(DISTINCT CASE WHEN ops_lead_source = 'Outbound' AND metric = 'Closed Won' THEN opportunity_id END) / COUNT(DISTINCT CASE WHEN ops_lead_source = 'Outbound' THEN opportunity_id END) END as Sales, 'GLOBAL'
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset = 'Actuals' AND metric IN ('Closed Won','Closed Lost') AND value_type = 'Opportunity Deal Count' AND ops_market_segment IN ('Enterprise','Large Accounts','Mid Market','Cross-Sell','SMB','Retail') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell'), 7 as sort_id, 'Closed Won' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type,
  SUM(value) as Total, 
  SUM(CASE WHEN ops_lead_source = 'Inbound' THEN value END) as Marketing, 
  SUM(CASE WHEN ops_lead_source = 'Partner' THEN value END) as Partner, 
  SUM(CASE WHEN ops_lead_source = 'Outbound' THEN value END) as Sales, 'GLOBAL'
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE ((dataset IN ('Actuals','Targets') AND ops_market_segment IN ('Enterprise','Large Accounts','Cross-Sell','SMB','Retail'))
OR (dataset IN ('Actuals','Targets') AND ops_market_segment IN ('Mid Market')))
  AND metric IN ('Closed Won') AND value_type IN ('Total Revenue','Incremental Product Profit') AND ops_market_segment IN ('Enterprise','Large Accounts','Mid Market','Cross-Sell','SMB','Retail') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell'), 8 as sort_id, 'Closed Won Avg' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type,
  SUM(value) / COUNT(DISTINCT opportunity_id) as Total, 
  SUM(CASE WHEN ops_lead_source = 'Inbound' THEN value END) / COUNT(DISTINCT CASE WHEN ops_lead_source = 'Inbound' THEN opportunity_id END) as Marketing, 
  SUM(CASE WHEN ops_lead_source = 'Partner' THEN value END) / COUNT(DISTINCT CASE WHEN ops_lead_source = 'Partner' THEN opportunity_id END) as Partner, 
  SUM(CASE WHEN ops_lead_source = 'Outbound' THEN value END) / COUNT(DISTINCT CASE WHEN ops_lead_source = 'Outbound' THEN opportunity_id END) as Sales, 'GLOBAL'
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE ((dataset IN ('Actuals','Targets') AND ops_market_segment IN ('Enterprise','Large Accounts','Cross-Sell','SMB','Retail'))
OR (dataset IN ('Actuals','Targets') AND ops_market_segment IN ('Mid Market')))
  AND metric IN ('Closed Won') AND value_type IN ('Total Revenue','Incremental Product Profit') AND ops_market_segment IN ('Enterprise','Large Accounts','Mid Market','Cross-Sell','SMB','Retail') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(market_segment,'SMB','Cross-Sell'), 9 as sort_id, 'Heat Count Capacity' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type,
  SUM(value) as Total, 
  0 as Marketing, 
  0 as Partner, 
  0 as Sales, 'GLOBAL'
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset IN ('Head Count') AND ops_market_segment IN ('Enterprise','Large Accounts','Cross-Sell','SMB','Retail','Mid Market') AND metric IN ('Head Count Capacity') AND team_segment NOT LIKE '%N3%'
GROUP BY ALL
UNION ALL
SELECT 'N3 Retail', 1 as sort_id, 'Created Opp Volume' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type, 
  COUNT(DISTINCT opportunity_id)/13 as Total, 
  COUNT(DISTINCT CASE WHEN ops_lead_source = 'Inbound' THEN opportunity_id END)/13 as Marketing, 
  COUNT(DISTINCT CASE WHEN ops_lead_source = 'Partner' THEN opportunity_id END)/13 as Partner, 
  COUNT(DISTINCT CASE WHEN ops_lead_source = 'Outbound' THEN opportunity_id END)/13 as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset IN ('Actuals') AND value_type IN ('Opportunity Deal Count') AND metric IN ('Created')  AND team_segment LIKE '%N3 Retail%'
GROUP BY ALL
UNION ALL
SELECT 'N3 Retail', 1 as sort_id, 'Created Opp Volume' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type, 
  SUM(value)/13 as Total, 
  SUM(CASE WHEN ops_lead_source = 'Inbound' THEN value END)/13 as Marketing, 
  SUM(CASE WHEN ops_lead_source = 'Partner' THEN value END)/13 as Partner, 
  SUM(CASE WHEN ops_lead_source = 'Outbound' THEN value END)/13 as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset IN ('Targets') AND value_type IN ('Product Deal Count') AND metric IN ('Created')  AND team_segment LIKE '%N3 Retail%'
GROUP BY ALL
UNION ALL
SELECT 'N3 Retail', 1 as sort_id, 'Created Opp Volume' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type, 
  SUM(value)/13 as Total, 
  SUM(CASE WHEN ops_lead_source = 'Inbound' THEN value END)/13 as Marketing, 
  SUM(CASE WHEN ops_lead_source = 'Partner' THEN value END)/13 as Partner, 
  SUM(CASE WHEN ops_lead_source = 'Outbound' THEN value END)/13 as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset IN ('Targets','Actuals') AND metric IN ('Created') AND value_type IN ('Total Revenue') AND team_segment LIKE '%N3 Retail%'
GROUP BY ALL
UNION ALL
SELECT 'N3 Retail', 2 as sort_id, 'Qualified Opp Total Revenue Avg ($)' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type,
  SUM(value)/COUNT(DISTINCT opportunity_id) as Total, 
  SUM(CASE WHEN ops_lead_source = 'Inbound' THEN value END)/COUNT(DISTINCT CASE WHEN ops_lead_source = 'Inbound' THEN opportunity_id END) as Marketing, 
  SUM(CASE WHEN ops_lead_source = 'Partner' THEN value END)/COUNT(DISTINCT CASE WHEN ops_lead_source = 'Partner' THEN opportunity_id END) as Partner, 
  SUM(CASE WHEN ops_lead_source = 'Outbound' THEN value END)/COUNT(DISTINCT CASE WHEN ops_lead_source = 'Outbound' THEN opportunity_id END) as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset IN ('Actuals','Targets') AND team_segment LIKE '%N3 Retail%'
 AND metric IN ('Qualified SAL') AND value_type IN ('Total Revenue','Incremental Product Profit')
GROUP BY ALL
UNION ALL
SELECT 'N3 Retail', 3 as sort_id, 'Created to Qualified (days)' as metric, '*' as label, CONCAT(year,'-',quarter), value_type,
  AVG(DATE_DIFF(qualified_sal_date,sf_created_date,DAY)) as Total, 
  AVG(CASE WHEN ops_lead_source = 'Inbound' THEN DATE_DIFF(qualified_sal_date,sf_created_date,DAY) END) as Marketing, 
  AVG(CASE WHEN ops_lead_source = 'Partner' THEN DATE_DIFF(qualified_sal_date,sf_created_date,DAY) END) as Partner, 
  AVG(CASE WHEN ops_lead_source = 'Outbound' THEN DATE_DIFF(qualified_sal_date,sf_created_date,DAY) END) as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset = 'Actuals' AND metric IN ('Created SAL') AND value_type = 'Opportunity Deal Count' AND team_segment LIKE '%N3 Retail%'
GROUP BY ALL
UNION ALL
SELECT 'N3 Retail', 4 as sort_id, 'Qualified to Win (days)' as metric, '*' as label, CONCAT(year,'-',quarter), value_type,
  AVG(DATE_DIFF(sf_close_date,qualified_sal_date,DAY)) as Total, 
  AVG(CASE WHEN ops_lead_source = 'Inbound' THEN DATE_DIFF(sf_close_date,qualified_sal_date,DAY) END) as Marketing, 
  AVG(CASE WHEN ops_lead_source = 'Partner' THEN DATE_DIFF(sf_close_date,qualified_sal_date,DAY) END) as Partner, 
  AVG(CASE WHEN ops_lead_source = 'Outbound' THEN DATE_DIFF(sf_close_date,qualified_sal_date,DAY) END) as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset = 'Actuals' AND metric IN ('Closed Won') AND value_type = 'Opportunity Deal Count' AND team_segment LIKE '%N3 Retail%'
GROUP BY ALL
UNION ALL
SELECT REPLACE(ops_market_segment,'SMB','Cross-Sell'), 5 as sort_id, 'Time to Win (days)' as metric, '*' as label, CONCAT(year,'-',quarter), value_type,
  AVG(DATE_DIFF(sf_close_date,sf_created_date,DAY)) as Total, 
  AVG(CASE WHEN ops_lead_source = 'Inbound' THEN DATE_DIFF(sf_close_date,sf_created_date,DAY) END) as Marketing, 
  AVG(CASE WHEN ops_lead_source = 'Partner' THEN DATE_DIFF(sf_close_date,sf_created_date,DAY) END) as Partner, 
  AVG(CASE WHEN ops_lead_source = 'Outbound' THEN DATE_DIFF(sf_close_date,sf_created_date,DAY) END) as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset = 'Actuals' AND metric IN ('Closed Won') AND value_type = 'Opportunity Deal Count' AND team_segment LIKE '%N3 Retail%'
GROUP BY ALL
UNION ALL
SELECT 'N3 Retail', 6 as sort_id, 'Win Rate' as metric, '*' as label, CONCAT(year,'-',quarter), value_type,
  COUNT(DISTINCT CASE WHEN metric = 'Closed Won' THEN opportunity_id END) / COUNT(DISTINCT opportunity_id) as Total, 
  CASE WHEN COUNT(DISTINCT CASE WHEN ops_lead_source = 'Inbound' THEN opportunity_id END) > 0 THEN COUNT(DISTINCT CASE WHEN ops_lead_source = 'Inbound' AND metric = 'Closed Won' THEN opportunity_id END) / COUNT(DISTINCT CASE WHEN ops_lead_source = 'Inbound' THEN opportunity_id END) END as Marketing, 
  CASE WHEN COUNT(DISTINCT CASE WHEN ops_lead_source = 'Partner' THEN opportunity_id END) > 0 THEN COUNT(DISTINCT CASE WHEN ops_lead_source = 'Partner' AND metric = 'Closed Won' THEN opportunity_id END) / COUNT(DISTINCT CASE WHEN ops_lead_source = 'Partner' THEN opportunity_id END) END as Partner, 
  CASE WHEN COUNT(DISTINCT CASE WHEN ops_lead_source = 'Outbound' THEN opportunity_id END) > 0 THEN COUNT(DISTINCT CASE WHEN ops_lead_source = 'Outbound' AND metric = 'Closed Won' THEN opportunity_id END) / COUNT(DISTINCT CASE WHEN ops_lead_source = 'Outbound' THEN opportunity_id END) END as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset = 'Actuals' AND metric IN ('Closed Won','Closed Lost') AND value_type = 'Opportunity Deal Count' AND team_segment LIKE '%N3 Retail%'
GROUP BY ALL
UNION ALL
SELECT 'N3 Retail', 7 as sort_id, 'Closed Won' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type,
  SUM(value) as Total, 
  SUM(CASE WHEN ops_lead_source = 'Inbound' THEN value END) as Marketing, 
  SUM(CASE WHEN ops_lead_source = 'Partner' THEN value END) as Partner, 
  SUM(CASE WHEN ops_lead_source = 'Outbound' THEN value END) as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset IN ('Actuals','Targets') AND team_segment LIKE '%N3 Retail%'
  AND metric IN ('Closed Won') AND value_type IN ('Incremental Product Profit')
GROUP BY ALL
UNION ALL
SELECT 'N3 Retail', 8 as sort_id, 'Closed Won Avg' as metric, dataset as label, CONCAT(year,'-',quarter) as yr_qtr, value_type,
  SUM(value) / COUNT(DISTINCT opportunity_id) as Total, 
  SUM(CASE WHEN ops_lead_source = 'Inbound' THEN value END) / COUNT(DISTINCT CASE WHEN ops_lead_source = 'Inbound' THEN opportunity_id END) as Marketing, 
  SUM(CASE WHEN ops_lead_source = 'Partner' THEN value END) / COUNT(DISTINCT CASE WHEN ops_lead_source = 'Partner' THEN opportunity_id END) as Partner, 
  SUM(CASE WHEN ops_lead_source = 'Outbound' THEN value END) / COUNT(DISTINCT CASE WHEN ops_lead_source = 'Outbound' THEN opportunity_id END) as Sales, sales_region
FROM `sdp-for-analysts-platform.rev_ops_prod.modelled_rad`
WHERE dataset IN ('Actuals','Targets') AND team_segment LIKE '%N3 Retail%'
  AND metric IN ('Closed Won') AND value_type IN ('Incremental Product Profit')
GROUP BY ALL

