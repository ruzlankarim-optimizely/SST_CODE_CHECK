---##########################################
--
---##########################################
drop table if exists tmp_commerce_connect_split;
create temporary table tmp_commerce_connect_split as
select snapshot_date,
  ultimate_parent_id,
  ultimate_parent_name,
  duns_name,
  duns_number,
  parent_duns_name,
  parent_duns_number,
  domesticultimatedunsnumber,
  globalultimatedunsnumber,
  new_product_solution,
  new_product_line,
  a.updated_product_group,
  new_product,
  new_line_of_business,
  new_line_of_business_sub_category,
  c_name,
  parent_ns_id,
  end_ns_id,
  name,
  parent_name,
  end_name,
  mcid,
  parent_mcid,
  end_mcid,
  subsidiary_entity_name,
  overage_flag,
  segment,
  region,
  product_family,
  case
    when b.sku = '' then a.sku
    else b.sku
  end as sku,
  base_currency,
  cc_fx_rate,
  fx_date,
  arr * b.arr_percentage::numeric as arr,
  baseline_arr_local_currency * b.arr_percentage::numeric as baseline_arr_local_currency,
  dw_modified_date,
  dw_created_date,
  parent_sf_id,
  parent_sf_name,
  record_source,
  modified_comments,
  cohort_actions,
  id,
  reference_number,
  updated_product_group_manual,
  updated_product_solution_manual,
  icp_account,
  lob,
  lob_sub_category,
  temp_product_solution_li,
  temp_product_group_li,
  temp_product_line_li,
  exists_in_customer_detail,
  industry,
  sub_industry,
  digital_maturity,
  under_audit,
  migration_from,
  migration_to --, updated_product_group, product_group_li, sku, arr_percentage
from sandbox_pd.sst_adhoc a
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
from sandbox_pd.product_hierarchy_11072024_1 b
where a.sku = b."Product Code"
  and a.sku in (
    'ALLOCA-CMPPASS',
    'ALLOCA-CMPPASSM&S',
    'ALLOCA-CMPPASS'
  );
--delete multi product group records
delete from sandbox_pd.sst_adhoc a
where 1 = 1
  and (
    coalesce(a.updated_product_group, '') = 'Commerce Connect'
  )
  and coalesce(a.arr, 0) > 0;
--insert split records
insert into sandbox_pd.sst_adhoc (
    snapshot_date,
    ultimate_parent_id,
    ultimate_parent_name,
    duns_name,
    duns_number,
    parent_duns_name,
    parent_duns_number,
    domesticultimatedunsnumber,
    globalultimatedunsnumber,
    new_product_solution,
    new_product_line,
    updated_product_group,
    new_product,
    new_line_of_business,
    new_line_of_business_sub_category,
    c_name,
    parent_ns_id,
    end_ns_id,
    name,
    parent_name,
    end_name,
    mcid,
    parent_mcid,
    end_mcid,
    subsidiary_entity_name,
    overage_flag,
    segment,
    region,
    product_family,
    sku,
    base_currency,
    cc_fx_rate,
    fx_date,
    arr,
    baseline_arr_local_currency,
    dw_modified_date,
    dw_created_date,
    parent_sf_id,
    parent_sf_name,
    record_source,
    modified_comments,
    cohort_actions,
    id,
    reference_number,
    updated_product_group_manual,
    updated_product_solution_manual,
    icp_account,
    lob,
    lob_sub_category,
    temp_product_solution_li,
    temp_product_group_li,
    temp_product_line_li,
    exists_in_customer_detail,
    industry,
    sub_industry,
    digital_maturity,
    under_audit,
    migration_from,
    migration_to,
    acquire_product_group,
    acquire_product_solution,
    acquire_product_group_li,
    acquire_product_solution_li
  )
select snapshot_date,
  ultimate_parent_id,
  ultimate_parent_name,
  duns_name,
  duns_number,
  parent_duns_name,
  parent_duns_number,
  domesticultimatedunsnumber,
  globalultimatedunsnumber,
  new_product_solution,
  new_product_line,
  updated_product_group,
  new_product,
  new_line_of_business,
  new_line_of_business_sub_category,
  c_name,
  parent_ns_id,
  end_ns_id,
  name,
  parent_name,
  end_name,
  mcid,
  parent_mcid,
  end_mcid,
  subsidiary_entity_name,
  overage_flag,
  segment,
  region,
  product_family,
  sku,
  base_currency,
  cc_fx_rate,
  fx_date,
  arr,
  baseline_arr_local_currency,
  dw_modified_date,
  dw_created_date,
  parent_sf_id,
  parent_sf_name,
  record_source,
  modified_comments,
  cohort_actions,
  id,
  reference_number,
  updated_product_group_manual,
  updated_product_solution_manual,
  icp_account,
  lob,
  lob_sub_category,
  temp_product_solution_li,
  temp_product_group_li,
  temp_product_line_li,
  exists_in_customer_detail,
  industry,
  sub_industry,
  digital_maturity,
  under_audit,
  migration_from,
  migration_to,
  null,
  null,
  null,
  null
from tmp_commerce_connect_split;