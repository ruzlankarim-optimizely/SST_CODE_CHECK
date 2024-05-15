select sandbox_pd.sp_populate_adaptive_exports_new('2019-01-01', '2024-04-30');
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
  industry
from sandbox_pd.SST_adaptive_ending_ARR a;
select a.*,
  null::Text
from sandbox_pd.SST_adaptive_customer_metadata a --left join ufdm_blue.customer_detail b on a.master_customer_id = b.epi_universal_id
;
with temp as (
  select *,
    customer_arr_change_ccfx as arr_usd_ccfx_sst
  from sandbox_pd.SST_adaptive_customer_bridge_movements
  union all
  select *,
    product_arr_change_ccfx as arr_usd_ccfx_sst
  from sandbox_pd.SST_adaptive_product_bridge_pg_movements
  union all
  select *,
    product_arr_change_ccfx as arr_usd_ccfx_sst
  from sandbox_pd.SST_adaptive_product_bridge_ps_movements
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
  Pathways
from temp
where 1 = 1
order by snapshot_date;
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
  industry --select count(*)
from sandbox_pd.SST_adaptive_ending_ARR_split a;
with temp as (
  select *,
    customer_arr_change_ccfx as arr_usd_ccfx_sst
  from sandbox_pd.SST_adaptive_customer_bridge_movements_core
  union all
  select *,
    customer_arr_change_ccfx as arr_usd_ccfx_sst
  from sandbox_pd.SST_adaptive_customer_bridge_movements_cloud
  union all
  select *,
    product_arr_change_ccfx as arr_usd_ccfx_sst
  from sandbox_pd.SST_adaptive_product_bridge_pg_movements_cloud_license
  union all
  select *,
    product_arr_change_ccfx as arr_usd_ccfx_sst
  from sandbox_pd.SST_adaptive_product_bridge_ps_movements_cloud_license
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
  pathways
from temp
order by 1;