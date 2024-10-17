select ryzlan.sp_populate_adaptive_exports_churn_mig_renewal_type('2019-01-01', '2024-05-31');

--insert below to metadata tab
select a.*,
  null::Text
from ryzlan.SST_adaptive_customer_metadata a --left join ufdm_blue.customer_detail b on a.master_customer_id = b.epi_universal_id
;
--insert below to ending arr tab
select a.snapshot_date,
  a.c_full_name,
  coalesce(a.end_customer, 'a') as end_customer --,null::Text as end_customer
,
  a.master_customer_id,
  a.baseline_currency,
  a.baseline_arr_local_currency,
  a.arr_usd_ccfx_sst as arr_usd_ccfx,
  a.product_family as sku,
  a.subsidiary_entity_name,
  'Combined' as flag,
  digital_maturity,
  ICP,
  industry,
  lapsed_flag --select lapsed_flag
from ryzlan.SST_adaptive_ending_ARR a
where lapsed_flag is not null --where snapshot_date = '2024-02-29'
  --left join ufdm_blue.customer_detail c on a.master_customer_id = c.epi_universal_id
;
select a.snapshot_date,
  a.c_full_name,
  coalesce(a.end_customer, 'a'),
  a.master_customer_id,
  a.baseline_currency,
  a.baseline_arr_local_currency,
  a.arr_usd_ccfx_sst as arr_usd_ccfx,
  a.product_family,
  a.subsidiary_entity_name,
  'Split' as type,
  digital_maturity,
  ICP,
  industry,
  lapsed_flag --select count(*)
from ryzlan.SST_adaptive_ending_ARR_split a --where snapshot_date = '2024-02-29'
  --left join ufdm_blue.customer_detail c on a.master_customer_id = c.epi_universal_id
;
--insert below to movements tab
with temp as (
  select *,
    customer_arr_change_ccfx as arr_usd_ccfx_sst
  from ryzlan.SST_adaptive_customer_bridge_movements
  union all
  select *,
    product_arr_change_ccfx as arr_usd_ccfx_sst
  from ryzlan.SST_adaptive_product_bridge_pg_movements
  union all
  select *,
    product_arr_change_ccfx as arr_usd_ccfx_sst
  from ryzlan.SST_adaptive_product_bridge_ps_movements
)
select snapshot_date,
  c_full_name,
  end_customer,
  master_customer_id,
  baseline_currency,
  baseline_arr_local_currency,
  arr_usd_ccfx_sst as arr_usd_ccfx,
  sku,
  subsidiary_entity_name,
  "Bridge_Account",
  "Type",
  Pathways,
  renewal_type
from temp
where 1 = 1 --and snapshot_date >= '2024-01-31'
  --and coalesce(renewal_type,'') = ''
order by snapshot_date;
-- with temp as (
--   select *,
--     customer_arr_change_ccfx as arr_usd_ccfx_sst
--   from sandbox_pd.SST_adaptive_customer_bridge_movements_core
--   union all
--   select *,
--     customer_arr_change_ccfx as arr_usd_ccfx_sst
--   from sandbox_pd.SST_adaptive_customer_bridge_movements_cloud
--   union all
--   select *,
--     product_arr_change_ccfx as arr_usd_ccfx_sst
--   from sandbox_pd.SST_adaptive_product_bridge_pg_movements_cloud_license
--   union all
--   select *,
--     product_arr_change_ccfx as arr_usd_ccfx_sst
--   from sandbox_pd.SST_adaptive_product_bridge_ps_movements_cloud_license
-- )
-- select snapshot_date,
--   c_full_name,
--   end_customer,
--   master_customer_id,
--   baseline_currency,
--   baseline_arr_local_currency,
--   arr_usd_ccfx_sst as arr_usd_ccfx,
--   sku,
--   subsidiary_entity_name,
--   "Bridge_Account",
--   "Type",
--   pathways,
--   renewal_type
-- from temp --where snapshot_date = '2024-02-29'
-- order by 1;