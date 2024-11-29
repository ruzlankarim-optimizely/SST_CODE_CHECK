---##########################################
--
---##########################################
drop table if exists tmp_commerce_connect_split;
create temporary table tmp_commerce_connect_split as
select id,
a.snapshot_date,
c_name,
parent_customer_ns_id,
end_customer_ns_id,
parent_customer,
end_customer,
parent_master_customer_id,
end_customer_master_customer_id,
parent_salesforce_id,
end_customer_salesforce_id,
line_type,
baseline_currency,
subsidiary_base_currency,
recurring_amount,
baseline_mrr_local_currency * d.arr_percentage::numeric as baseline_mrr_local_currency,
baseline_arr_local_currency * d.arr_percentage::numeric as baseline_arr_local_currency,
ccfx_date,
mefx_date,
fx_rate_ccfx,
mrr_usd_ccfx * d.arr_percentage::numeric AS mrr_usd_ccfx,
arr_usd_ccfx * d.arr_percentage::numeric AS arr_usd_ccfx,
fx_rate_mefx,
mrr_usd_mefx * d.arr_percentage::numeric AS mrr_usd_mefx,
arr_usd_mefx * d.arr_percentage::numeric AS arr_usd_mefx,
fx_rate_actualfx,
mrr_usd_actualfx * d.arr_percentage::numeric AS mrr_usd_actualfx,
arr_usd_actualfx * d.arr_percentage::numeric AS arr_usd_actualfx,
bill_freq,
term_months,
date_start,
date_end,
date_termination,
subline_id,
reference_number,
line_number,
revision_number,
change_order,
status,
catalog_type,
case
  when d.sku = '' then a.sku
  else d.sku
end as sku,
sku_name,
product_name,
product_group,
product_family,
arr_source,
sco_action_id,
sco_memo,
sco_modification_type,
subsidiary_entity_name,
legacy_org,
a.mcid,
created_date,
modified_date,
snapshot_date_revised,
new_product_solution,
new_product_line,
a.updated_product_group,
new_product,
new_line_of_business,
new_line_of_business_sub_category,
modified_comments,
temp_product_group_li,
temp_product_solution_li,
migration_from,
migration_to
from ufdm.arr a
  cross join (
    select 'Commerce Connect' as updated_product_group,
      'Commerce Connect Subscription' as product_group_li,
      'ALLOCA-CMPPASSSUB' as sku,
      '0.70' as arr_percentage
    union
    select 'Commerce Connect' as updated_product_group,
      'Commerce Connect M&S' as product_group_li,
      'ALLOCA-CMPPASSM&S' as sku,
      '0.70' as arr_percentage
    union
    select 'Commerce Connect' as updated_product_group,
      'Commerce Connect' as product_group_li,
      'ALLOCA-CMPPASS' as sku,
      '0.70' as arr_percentage
    union
    select 'Commerce Connect' as updated_product_group,
      'Commerce Connect Subscription' as product_group_li,
      '' as sku,
      '0.30' as arr_percentage
    union
    select 'Commerce Connect' as updated_product_group,
      'Commerce Connect M&S' as product_group_li,
      '' as sku,
      '0.30' as arr_percentage
    union
    select 'Commerce Connect' as updated_product_group,
      'Commerce Connect' as product_group_li,
      '' as sku,
      '0.30' as arr_percentage
  ) b
where 1 = 1
  and (
    coalesce(a.updated_product_group, '') = 'Commerce Connect'
  )
  and a.temp_product_group_li = b.product_group_li
  and coalesce(a.arr, 0) > 0 --and a.snapshot_date = '2020-02-29'
  --and a.mcid in ('65598904-2267-e311-a1f4-0050568d2da8','ba07dd74-a785-e111-92d4-0050568d002c','8d27baba-9f34-e911-a962-000d3a441525')
;
update tmp_commerce_connect_split a
set updated_product_group = b."Product Group",
  new_product_solution = b."Product Solution",
  temp_product_group_li = b."TEMP Product Group -- LI",
  temp_product_solution_li = b."TEMP Product Solution -- LI",
  migration_from = b."Mig From Name",
  migration_to = b."Mig to Name"
from sandbox_pd.product_hierarchy_15082024 b
where a.sku = b."Product Code"
  and a.sku in (
    'ALLOCA-CMPPASS',
    'ALLOCA-CMPPASSM&S',
    'ALLOCA-CMPPASS'
  );
--delete multi product group records
delete from ufdm.arr a
using sandbox_pd.product_hierarchy_15082024 b
where 1 = 1
and a.sku = b."Product Code"
  and (
    coalesce(a.updated_product_group, '') = 'Commerce Connect'
  )
  and b."Included in ARR" = 'Y'
  and coalesce(a.arr, 0) > 0;
--insert split records
insert into ufdm.arr (
    id,
snapshot_date,
c_name,
parent_customer_ns_id,
end_customer_ns_id,
parent_customer,
end_customer,
parent_master_customer_id,
end_customer_master_customer_id,
parent_salesforce_id,
end_customer_salesforce_id,
line_type,
baseline_currency,
subsidiary_base_currency,
recurring_amount,
baseline_mrr_local_currency,
baseline_arr_local_currency,
ccfx_date,
mefx_date,
fx_rate_ccfx,
mrr_usd_ccfx,
arr_usd_ccfx,
fx_rate_mefx,
mrr_usd_mefx,
arr_usd_mefx,
fx_rate_actualfx,
mrr_usd_actualfx,
arr_usd_actualfx,
bill_freq,
term_months,
date_start,
date_end,
date_termination,
subline_id,
reference_number,
line_number,
revision_number,
change_order,
status,
catalog_type,
sku,
sku_name,
product_name,
product_group,
product_family,
arr_source,
sco_action_id,
sco_memo,
sco_modification_type,
subsidiary_entity_name,
legacy_org,
mcid,
created_date,
modified_date,
snapshot_date_revised,
new_product_solution,
new_product_line,
updated_product_group,
new_product,
new_line_of_business,
new_line_of_business_sub_category,
modified_comments,
temp_product_group_li,
temp_product_solution_li,
migration_from,
migration_to
  )
select id,
snapshot_date,
c_name,
parent_customer_ns_id,
end_customer_ns_id,
parent_customer,
end_customer,
parent_master_customer_id,
end_customer_master_customer_id,
parent_salesforce_id,
end_customer_salesforce_id,
line_type,
baseline_currency,
subsidiary_base_currency,
recurring_amount,
baseline_mrr_local_currency,
baseline_arr_local_currency,
ccfx_date,
mefx_date,
fx_rate_ccfx,
mrr_usd_ccfx,
arr_usd_ccfx,
fx_rate_mefx,
mrr_usd_mefx,
arr_usd_mefx,
fx_rate_actualfx,
mrr_usd_actualfx,
arr_usd_actualfx,
bill_freq,
term_months,
date_start,
date_end,
date_termination,
subline_id,
reference_number,
line_number,
revision_number,
change_order,
status,
catalog_type,
sku,
sku_name,
product_name,
product_group,
product_family,
arr_source,
sco_action_id,
sco_memo,
sco_modification_type,
subsidiary_entity_name,
legacy_org,
mcid,
created_date,
modified_date,
snapshot_date_revised,
new_product_solution,
new_product_line,
updated_product_group,
new_product,
new_line_of_business,
new_line_of_business_sub_category,
modified_comments,
temp_product_group_li,
temp_product_solution_li,
migration_from,
migration_to
from tmp_commerce_connect_split;