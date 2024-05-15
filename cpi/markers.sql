-- New script in dw-prod-rds-master.cr9dekxonyuj.us-east-1.rds.amaz.
-- Date: Jan 31, 2024
-- Time: 8:58:35 PM
SELECT *
FROM epi_netsuite.subscription_change_orders
WHERE action_id = 'MODIFY_PRICING' --AND(lower(memo) ILIKE 'cpi' OR memo IS NULL )
  AND requester_id = '-4.0'
  AND "_fivetran_deleted" IS DISTINCT
FROM TRUE --AND movement_in_arr_tagging_id =  'CPI'
SELECT *
FROM epi_netsuite.subscription_line_revisions slr
  LEFT JOIN epi_netsuite.subscription_change_orders sco ON sco.change_order_id = slr.change_order_id
  AND sco."_fivetran_deleted" IS DISTINCT
FROM TRUE
WHERE 1 = 1
  AND slr."_fivetran_deleted" IS DISTINCT
FROM TRUE --                            AND sco.action_id IN ( 'TERMINATE' )
  --                            AND sco.approval_status_id = 'APPROVED'
  --                            AND sco.status_id <> 'VOIDED'
  AND action_id = 'MODIFY_PRICING'
  AND(
    lower(memo) ILIKE 'cpi'
    OR memo IS NULL
  )
SELECT column_name,
  data_type,
  table_schema,
  table_name
FROM information_schema.columns
WHERE table_schema = 'epi_netsuite' --    AND 
  --    lower(table_name ) LIKE '%movement%'
  AND lower(column_name) like '%new_%'
SELECT *
FROM epi_netsuite.master_tagging_category
SELECT *
FROM epi_netsuite.billing_subscription_lines slr
LIMIT 5
SELECT *
FROM epi_netsuite.billing_subscription_lines
LIMIT 1000