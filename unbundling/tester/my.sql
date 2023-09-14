CREATE OR REPLACE FUNCTION ryzlan.sp_populate_adaptive_exports_mod(snapshot_date_from date, snapshot_date_to date) RETURNS void LANGUAGE plpgsql AS $function$ BEGIN --####################################################
  --Ending ARR
  --####################################################
  RAISE NOTICE 'Running temp_adaptive_ending_ARR...';
drop table if exists temp_adaptive_ending_ARR;
create temp table temp_adaptive_ending_ARR as with temp_subentity as (
  select *,
    row_number () over (
      partition by mcid,
      snapshot_date
      order by total_arr desc
    ) as rnk
  from (
      select snapshot_date,
        mcid,
        subsidiary_entity_name,
        sum(baseline_arr_local_currency) as total_arr,
        sum(arr) as arr_total,
        max(c_name) as c_name --select *
      from ryzlan.sku_sst
      where snapshot_date between snapshot_date_from and snapshot_date_to
        and coalesce(baseline_arr_local_currency, 0) > 0
      group by snapshot_date,
        mcid,
        subsidiary_entity_name
    ) a
),
temp_multi_currencies as (
  select snapshot_date,
    mcid,
    updated_product_group
  from ryzlan.sku_sst
  where snapshot_date between snapshot_date_from and snapshot_date_to
    and coalesce(baseline_arr_local_currency, 0) > 0
  group by snapshot_date,
    mcid,
    updated_product_group
  having count(distinct base_currency) > 1
),
temp1 as (
  select snapshot_date --, null as c_full_name
,
    mcid as master_customer_id,
    base_currency as baseline_currency,
    overage_flag,
    sum(baseline_arr_local_currency) as baseline_arr_local_currency,
    sum(arr) as arr_total,
    null as arr_usd_ccfx,
    updated_product_group,
    max(c_name) as c_name
  from ryzlan.sku_sst
  where snapshot_date between snapshot_date_from and snapshot_date_to
    and coalesce(baseline_arr_local_currency, 0) > 0
  group by snapshot_date,
    mcid,
    base_currency,
    updated_product_group,
    overage_flag
)
select a.snapshot_date,
  a.c_name,
  c.name as c_full_name,
  null as end_customer,
  a.master_customer_id,
  a.baseline_currency,
  a.baseline_arr_local_currency,
  a.arr_usd_ccfx,
  CASE
    WHEN overage_flag ILIKE '%Y%' THEN 'Campaign Overages'
    ELSE a.updated_product_group
  END AS product_family,
  b.subsidiary_entity_name,
  case
    when d.mcid is not null then 1
    else 0
  end as mcid_with_multiple_currencies,
  a.arr_total,
  a.overage_flag
from temp1 a
  left join temp_subentity b on a.master_customer_id = b.mcid
  and a.snapshot_date = b.snapshot_date
  and b.rnk = 1
  left join (
    select epi_universal_id,
      name,
      row_number() over(
        partition by epi_universal_id
        order by is_active desc
      ) as rnk
    from ufdm_blue.customer_detail
  ) c on a.master_customer_id = c.epi_universal_id
  and c.rnk = 1
  left join temp_multi_currencies d on a.master_customer_id = d.mcid
  and a.snapshot_date = d.snapshot_date
  and a.updated_product_group = d.updated_product_group --order by a.baseline_arr_local_currency
;
RAISE NOTICE 'Running SST_adaptive_ending_ARR ...';
--1.Ending ARR results
drop table if exists ryzlan.SST_adaptive_ending_ARR_mod;
create table ryzlan.SST_adaptive_ending_ARR_mod as
select snapshot_date,
  master_customer_id as c_name,
  c_full_name,
  end_customer,
  case
    when coalesce(master_customer_id, '') = '' then 'Mockups'
    when lower(coalesce(master_customer_id, '')) = 'blank' then 'Mockups'
    else master_customer_id
  end as master_customer_id,
  baseline_currency,
  baseline_arr_local_currency,
  arr_usd_ccfx --          ,case when product_family is null or product_family = '' or product_family = 'New' or product_family ilike '%applicable%'
--                    then ''
--                else product_family
--           end
,
coalesce(product_family, '') as product_family,
subsidiary_entity_name,
c_name as c_name_real,
arr_total as arr_usd_ccfx_sst,
  overage_flag
from temp_adaptive_ending_ARR
where coalesce(baseline_arr_local_currency, 0) > 0;
--####################################################
--Customer metadata
--####################################################
--3.Customer Metadata results
RAISE NOTICE 'Running SST_adaptive_customer_metadata...';
drop table if exists ryzlan.SST_adaptive_customer_metadata_mod;
create table ryzlan.SST_adaptive_customer_metadata_mod as with temp1 as (
  select master_customer_id,
    subsidiary_entity_name,
    row_number() over (
      partition by master_customer_id
      order by snapshot_date desc
    ) as rnk
  from ryzlan.SST_adaptive_ending_ARR_mod
  where subsidiary_entity_name is not null
),
temp2 as (
  select master_customer_id,
    c_name_real as c_name,
    row_number() over (
      partition by master_customer_id
      order by snapshot_date desc
    ) as rnk
  from ryzlan.SST_adaptive_ending_ARR_mod
  where c_name_real is not null
),
temp3 as (
  select distinct master_customer_id
  from ryzlan.SST_adaptive_ending_ARR_mod
)
select c.c_name,
  case
    when coalesce(a.master_customer_id, '') = '' then 'Mockups'
    when lower(coalesce(a.master_customer_id, '')) = 'blank' then 'Mockups'
    else a.master_customer_id
  end as master_customer_id,
  case
    when cd.name is null then 'Mock Ups'
    else cd.name
  end as c_full_name,
  null as end_customer,
  case
    when coalesce(cd.segment, '') = '' then 'Mid-Market'
    else cd.segment
  end as segment,
  case
    when coalesce(cd.territory, '') = '' then 'NA-Mid-Market East'
    else cd.territory
  end as territory,
  case
    when coalesce(cd.region, '') = '' then 'North America'
    else cd.region
  end as region,
  b.subsidiary_entity_name
from temp3 a
  left join temp1 b on a.master_customer_id = b.master_customer_id
  and b.rnk = 1
  left join temp2 c on a.master_customer_id = c.master_customer_id
  and c.rnk = 1
  left join (
    select epi_universal_id,
      name,
      segment,
      territory,
      region,
      row_number() over(
        partition by epi_universal_id
        order by is_active desc
      ) as rnk
    from ufdm_blue.customer_detail
  ) cd on a.master_customer_id = cd.epi_universal_id
  and c.rnk = 1;
--####################################################
--movements data
--####################################################
-- RAISE NOTICE 'Running SST_adaptive_product_bridge_movements...';
-- drop table if exists ryzlan.SST_adaptive_product_bridge_movements_mod;
-- create table ryzlan.SST_adaptive_product_bridge_movements_mod as
-- select b.current_period as snapshot_date,
--   mcid as c_full_name,
--   null as end_customer --,mcid as master_customer_id
-- ,
--   case
--     when coalesce(mcid, '') = '' then 'Mockups'
--     when lower(coalesce(mcid, '')) = 'blank' then 'Mockups'
--     else mcid
--   end as master_customer_id,
--   a.currency_code as baseline_currency,
--   product_arr_change_lcu as baseline_arr_local_currency,
--   null as arr_usd_ccfx,
--   case
--     when coalesce(prior_product_family, current_product_family, '') = '' then 'Recurring: Cloud: Other Bookings: Other Bookings'
--     else coalesce(prior_product_family, current_product_family, '')
--   end as sku,
--   subsidiary_entity_name,
--   replace(product_bridge, 'N/A', 'Flat') as "Bridge_Account",
--   'Account Name Product Bridge' as "Type",
--   product_arr_change_ccfx
-- from ryzlan.sst_product_bridge_with_proposal a
--   join ufdm_grey.periods b on a.evaluation_period = b.evaluation_period
-- where 1 = 1 --and a.product_bridge not in ('Flat','N/A')
--   and product_arr_change_lcu <> 0 --and mcid is not null and mcid <> ''
--   and b.current_period between snapshot_date_from and snapshot_date_to
--   and b.evaluation_period not ilike '%W%'
-- order by b.current_period;
RAISE NOTICE 'Running SST_adaptive_product_bridge_pg movements...';
drop table if exists ryzlan.SST_adaptive_product_bridge_pg_movements_mod;
create table ryzlan.SST_adaptive_product_bridge_pg_movements_mod as
select b.current_period as snapshot_date,
  mcid as c_full_name,
  null as end_customer --,mcid as master_customer_id
,
  case
    when coalesce(mcid, '') = '' then 'Mockups'
    when lower(coalesce(mcid, '')) = 'blank' then 'Mockups'
    else mcid
  end as master_customer_id,
  a.currency_code as baseline_currency,
  product_arr_change_lcu as baseline_arr_local_currency,
  null as arr_usd_ccfx,
  case
    when coalesce(prior_product_group, current_product_group, '') = '' then ''
    else coalesce(prior_product_group, current_product_group, '')
  end as sku,
  subsidiary_entity_name,
  replace(product_bridge, 'N/A', 'Flat') as "Bridge_Account",
  'Account Name Product Group Bridge' as "Type",
  product_arr_change_ccfx
from ryzlan.sst_product_bridge_product_group_mod a
  join ufdm_grey.periods b on a.evaluation_period = b.evaluation_period
where 1 = 1 --and a.product_bridge not in ('Flat','N/A')
  and product_arr_change_lcu <> 0 --and mcid is not null and mcid <> ''
  and b.current_period between snapshot_date_from and snapshot_date_to
  and b.evaluation_period not ilike '%W%'
order by b.current_period;
RAISE NOTICE 'Running SST_adaptive_product_bridge_ps_movements...';
drop table if exists ryzlan.SST_adaptive_product_bridge_ps_movements_mod;
CREATE TABLE ryzlan.SST_adaptive_product_bridge_ps_movements_mod as
select b.current_period as snapshot_date,
  mcid as c_full_name,
  null as end_customer --,mcid as master_customer_id
,
  case
    when coalesce(mcid, '') = '' then 'Mockups'
    when lower(coalesce(mcid, '')) = 'blank' then 'Mockups'
    else mcid
  end as master_customer_id,
  a.currency_code as baseline_currency,
  product_arr_change_lcu as baseline_arr_local_currency,
  null as arr_usd_ccfx,
  case
    when coalesce(
      prior_product_solution,
      current_product_solution,
      ''
    ) = '' then ''
    else coalesce(
      prior_product_solution,
      current_product_solution,
      ''
    )
  end as sku,
  subsidiary_entity_name,
  replace(product_bridge, 'N/A', 'Flat') as "Bridge_Account",
  'Account Name Solution Bridge' as "Type",
  product_arr_change_ccfx
from ryzlan.sst_product_bridge_product_solution_mod a
  join ufdm_grey.periods b on a.evaluation_period = b.evaluation_period
where 1 = 1 --and a.product_bridge not in ('Flat','N/A')
  and product_arr_change_lcu <> 0 --and mcid is not null and mcid <> ''
  and b.current_period between snapshot_date_from and snapshot_date_to
  and b.evaluation_period not ilike '%W%'
order by b.current_period;
-- RAISE NOTICE 'Running SST_adaptive_product_bridge_pf_ps movements...';
-- drop table if exists ryzlan.SST_adaptive_product_bridge_pf_ps_movements_mod;
-- create table ryzlan.SST_adaptive_product_bridge_pf_ps_movements_mod as
-- select b.current_period as snapshot_date,
--   mcid as c_full_name,
--   null as end_customer --,mcid as master_customer_id
-- ,
--   case
--     when coalesce(mcid, '') = '' then 'Mockups'
--     when lower(coalesce(mcid, '')) = 'blank' then 'Mockups'
--     else mcid
--   end as master_customer_id,
--   a.currency_code as baseline_currency,
--   product_arr_change_lcu as baseline_arr_local_currency,
--   null as arr_usd_ccfx,
--   case
--     when coalesce(
--       prior_product_family_solution,
--       current_product_family_solution,
--       ''
--     ) = '' then ''
--     else coalesce(
--       prior_product_family_solution,
--       current_product_family_solution,
--       ''
--     )
--   end as sku,
--   subsidiary_entity_name,
--   replace(product_bridge, 'N/A', 'Flat') as "Bridge_Account",
--   'Account Name Product Family to Product Solution Bridge - new' as "Type",
--   product_arr_change_ccfx
-- from ryzlan.sst_product_bridge_product_family_solution_with_proposal a
--   join ufdm_grey.periods b on a.evaluation_period = b.evaluation_period
-- where 1 = 1 --and a.product_bridge not in ('Flat','N/A')
--   and product_arr_change_lcu <> 0 --and mcid is not null and mcid <> ''
--   and b.current_period between snapshot_date_from and snapshot_date_to
--   and b.evaluation_period not ilike '%W%'
-- order by b.current_period;
RAISE NOTICE 'Running SST_adaptive_customer_bridge_movements...';
drop table if exists ryzlan.SST_adaptive_customer_bridge_movements_mod;
create table ryzlan.SST_adaptive_customer_bridge_movements_mod as
select b.current_period as snapshot_date,
  mcid as c_full_name,
  null as end_customer --,mcid as master_customer_id
,
  case
    when coalesce(mcid, '') = '' then 'Mockups'
    when lower(coalesce(mcid, '')) = 'blank' then 'Mockups'
    else mcid
  end as master_customer_id,
  a.baseline_currency as baseline_currency,
  a.customer_arr_change_lcu as baseline_arr_local_currency,
  null as arr_usd_ccfx,
  '' as sku,
  subsidiary_entity_name,
  customer_bridge as "Bridge_Account",
  'Account Name Customer Bridge' as "Type",
  a.customer_arr_change_ccfx --,a.evaluation_period
  --select *
from ryzlan.sst_customer_bridge_mod a
  join ufdm_grey.periods b on a.evaluation_period = b.evaluation_period
where 1 = 1 --and a.customer_bridge not in ('Flat','N/A')
  and customer_arr_change_lcu <> 0
  and b.current_period between snapshot_date_from and snapshot_date_to
  and b.evaluation_period not ilike '%W%' --and mcid is not null and mcid <> ''
order by 1;
--update sku based on product bridge
drop table if exists temp_CB_sku;
create temp table temp_CB_sku as with temp_cb as (
  select distinct snapshot_date,
    master_customer_id
  from ryzlan.SST_adaptive_customer_bridge_movements_mod
),
temp_pb as (
  select b.master_customer_id,
    b.snapshot_date,
    a.sku,
    sum(coalesce(a.product_arr_change_ccfx, 0)) as product_arr_change_ccfx
  from ryzlan.SST_adaptive_product_bridge_movements_mod a
    join temp_cb b on a.master_customer_id = b.master_customer_id
    and a.snapshot_date = b.snapshot_date
  group by b.master_customer_id,
    b.snapshot_date,
    sku
),
temp_pb_max as (
  select *,
    rank() over (
      partition by master_customer_id,
      snapshot_date
      order by product_arr_change_ccfx desc
    ) as rnk
  from temp_pb
)
select *
from temp_pb_max
where rnk = 1;
update ryzlan.SST_adaptive_customer_bridge_movements_mod a
set sku = b.sku
from temp_CB_sku b
where a.master_customer_id = b.master_customer_id
  and a.snapshot_date = b.snapshot_date;
END;
$function$;