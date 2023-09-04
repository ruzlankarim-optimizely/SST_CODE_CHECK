CREATE OR REPLACE FUNCTION ryzlan.sp_populate_sst_updates_manual_after_sensitivity_analysis_with_() RETURNS void LANGUAGE plpgsql AS $function$ BEGIN
delete from ryzlan.sku_sst
WHERE 1 = 1
  and base_currency IS NOT NULL --and mcid = '7434e983-09b5-db11-8952-0018717a8c82' and snapshot_date ='2020-07-31'
  AND coalesce(ARR, 0) < 0
  and baseline_arr_local_currency < 0;
---##########################################
--APPLY SHELL CUSTOMER FIXES
---##########################################
update ryzlan.sku_sst
set arr = '218333.33',
  baseline_arr_local_currency = '218333.33',
  modified_comments = concat(
    coalesce(modified_comments, ''),
    '==> arr and lcu updated from ',
    arr::text
  ) --select * from ryzlan.sku_sst
where mcid = '8ef0bf48-f16c-72ce-6356-41f25b5aaaf2'
  and snapshot_date between '2022-09-30' and '2022-09-30'
  and arr > 0
  and arr <> '218333.33';
update ryzlan.sku_sst
set arr = '218333.33',
  baseline_arr_local_currency = '218333.33',
  modified_comments = concat(
    coalesce(modified_comments, ''),
    '==> arr and lcu updated from ',
    arr::text
  ) --select * from ryzlan.sku_sst
where mcid = '8ef0bf48-f16c-72ce-6356-41f25b5aaaf2'
  and snapshot_date between '2022-10-31' and '2022-12-31'
  and arr > 0
  and arr <> '218333.33';
update ryzlan.sku_sst
set arr = '252333.33',
  baseline_arr_local_currency = '252333.33',
  modified_comments = concat(
    coalesce(modified_comments, ''),
    '==> arr and lcu updated from ',
    arr::text
  ) --select * from ryzlan.sku_sst
where mcid = '8ef0bf48-f16c-72ce-6356-41f25b5aaaf2'
  and snapshot_date between '2023-01-31' and '2023-06-30'
  and arr > 0
  and arr <> '252333.33';
---##########################################
--UPDATE baseline vs arr discrepencies
---##########################################
update ryzlan.sku_sst a
set baseline_arr_local_currency = (a.arr / b.fx_rate),
  modified_comments = concat(
    coalesce(modified_comments, ';'),
    'lcu update from ',
    baseline_arr_local_currency::Text,
    ' to ',
    (a.arr / b.fx_rate)::Text
  )
from (
    select trans_cur,
      fx_rate
    from ufdm_grey.arr_fx_rates
    where fx_type = 'ccfx'
  ) b --select * from ryzlan.sku_sst a, (select trans_cur,fx_rate from ufdm_grey.arr_fx_rates where fx_type = 'ccfx') b
where a.base_currency = b.trans_cur
  and coalesce(a.arr, 0) - (
    coalesce(a.baseline_arr_local_currency, 0) * b.fx_rate
  ) not between -1 and 1;
---##########################################
-- update product group and product solution based on SKU
---##########################################
/*
 SELECT a.snapshot_date,a.sku, count(*)
 --distinct a.sku,a.new_product,a.new_product_solution,a.new_line_of_business,a.new_product_line,a.updated_product_group,tmjs.*
 FROM ryzlan.sku_sst a
 LEFT JOIN ufdm_grey.product_hierarchy_mappings tmjs ON tmjs."Product Code" = a.sku
 where a.new_product is null
 group by a.snapshot_date,a.sku
 ;
 
 SELECT distinct a.sku,a.new_product,a.new_product_solution,a.new_line_of_business,a.new_product_line,a.updated_product_group,tmjs.*
 FROM ryzlan.sku_sst a
 LEFT JOIN ufdm_grey.product_hierarchy_mappings tmjs ON tmjs."Product Code" = a.sku
 where a.updated_product_group is null
 ;
 */
update ryzlan.sku_sst a
set new_product = null,
  updated_product_group = null,
  new_product_line = null,
  new_product_solution = null,
  new_line_of_business = null,
  new_line_of_business_sub_category = null,
  modified_comments = concat(
    coalesce(modified_comments, ''),
    '==> new_product_hierarchy columns updated to blank for fopti'
  ) --select distinct a.sku,a.product_family,a.new_product,a.new_product_solution,a.new_line_of_business,a.new_product_line,a.updated_product_group,record_source from ryzlan.sku_sst a
WHERE 1 = 1
  and product_family in ('Full Stack', 'Web')
  and updated_product_group is not null;
update ryzlan.sku_sst a
set new_product = tmjs."NEW: Product",
  updated_product_group = tmjs."Updated: Product Group",
  new_product_line = tmjs."NEW:  Product Line",
  new_product_solution = tmjs."NEW: Product Solution",
  new_line_of_business = tmjs."NEW: Line of Business",
  new_line_of_business_sub_category = tmjs."NEW: Line of Business Subcategory",
  modified_comments = concat(
    coalesce(modified_comments, ''),
    '==> new_product_hierarchy columns updated from blank based on SKU'
  )
FROM ufdm_grey.product_hierarchy_mappings tmjs
WHERE tmjs."Product Code" = a.sku
  AND a.updated_product_group is null;
---##########################################
-- update product group and product solution based on new mapping tables
---##########################################
drop table if exists tmp_pf_split;
create temporary table tmp_pf_split as
select a.snapshot_date,
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
  b.pg_mapping_1 as updated_product_group,
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
  a.product_family,
  base_currency,
  cc_fx_rate,
  fx_date,
  arr,
  baseline_arr_local_currency,
  arr * (b.pg_arr_percentage::numeric) as arr_new,
  baseline_arr_local_currency * (b.pg_arr_percentage::numeric) as baseline_arr_local_currency_new,
  dw_modified_date,
  dw_created_date,
  parent_sf_id,
  parent_sf_name,
  record_source,
  modified_comments,
  cohort_actions,
  id,
  pg_mapping_1,
  pg_arr_percentage,
  subsidairy
from ryzlan.sku_sst a
  cross join ufdm_grey.sst_product_family_porduct_group_mappings_manual b
where a.product_family = b.product_family
  and a.product_family in (
    'Recurring: Cloud: Other Bookings: Other Bookings'
  )
  and (coalesce(updated_product_group, '') = '')
  and coalesce(arr, 0) > 0
union all
select a.snapshot_date,
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
  b.pg_mapping_1 as updated_product_group,
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
  a.product_family,
  base_currency,
  cc_fx_rate,
  fx_date,
  arr,
  baseline_arr_local_currency,
  arr * (b.pg_arr_percentage::numeric) as arr_new,
  baseline_arr_local_currency * (b.pg_arr_percentage::numeric) as baseline_arr_local_currency_new,
  dw_modified_date,
  dw_created_date,
  parent_sf_id,
  parent_sf_name,
  record_source,
  modified_comments,
  cohort_actions,
  id,
  pg_mapping_1,
  pg_arr_percentage,
  subsidairy
from ryzlan.sku_sst a
  cross join ufdm_grey.sst_product_family_porduct_group_mappings_manual b
where a.product_family = b.product_family
  and a.product_family in (
    'Recurring: Subscription License',
    'Non-Recurring: Perpetual License'
  )
  and (coalesce(updated_product_group, '') = '')
  and coalesce(arr, 0) > 0
  and coalesce(a.subsidiary_entity_name, '') ilike '%insite%'
  and b.subsidairy = 'Insite'
union all
select a.snapshot_date,
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
  b.pg_mapping_1 as updated_product_group,
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
  a.product_family,
  base_currency,
  cc_fx_rate,
  fx_date,
  arr,
  baseline_arr_local_currency,
  arr * (b.pg_arr_percentage::numeric) as arr_new,
  baseline_arr_local_currency * (b.pg_arr_percentage::numeric) as baseline_arr_local_currency_new,
  dw_modified_date,
  dw_created_date,
  parent_sf_id,
  parent_sf_name,
  record_source,
  modified_comments,
  cohort_actions,
  id,
  pg_mapping_1,
  pg_arr_percentage,
  subsidairy
from ryzlan.sku_sst a
  cross join ufdm_grey.sst_product_family_porduct_group_mappings_manual b
where a.product_family = b.product_family
  and a.product_family in (
    'Recurring: Subscription License',
    'Non-Recurring: Perpetual License'
  )
  and (coalesce(updated_product_group, '') = '')
  and coalesce(arr, 0) > 0
  and coalesce(a.subsidiary_entity_name, '') not ilike '%insite%'
  and b.subsidairy = 'Not Insite';
--update 1 to 1 mappings
update ryzlan.sku_sst a
set updated_product_group = b.pg_mapping_1,
  modified_comments = concat(
    coalesce(modified_comments, ''),
    '==> updated_product_group updated from blank'
  )
from ufdm_grey.sst_product_family_porduct_group_mappings_manual b
where 1 = 1
  and a.product_family = b.product_family
  and (coalesce(updated_product_group, '') = '')
  and coalesce(arr, 0) > 0
  and a.product_family not in (
    'Recurring: Cloud: Other Bookings: Other Bookings',
    'Recurring: Subscription License',
    'Non-Recurring: Perpetual License'
  );
--delete multi product group records
delete from ryzlan.sku_sst a
where 1 = 1
  and (coalesce(updated_product_group, '') = '')
  and coalesce(arr, 0) > 0
  and a.product_family in (
    'Recurring: Cloud: Other Bookings: Other Bookings',
    'Recurring: Subscription License',
    'Non-Recurring: Perpetual License'
  );
--insert split records
insert into ryzlan.sku_sst (
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
    cohort_actions
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
  base_currency,
  cc_fx_rate,
  fx_date,
  arr_new,
  baseline_arr_local_currency_new,
  dw_modified_date,
  dw_created_date,
  parent_sf_id,
  parent_sf_name,
  record_source,
  concat(
    coalesce(modified_comments, ''),
    '; updated product group changed from null to ',
    updated_product_group,
    case
      when subsidairy = 'Not Insite'
      or product_family in (
        'Recurring: Cloud: Other Bookings: Other Bookings'
      ) then concat(
        '; arr updated from ',
        arr::Text,
        ' to ',
        arr_new::text,
        '; baseline_arr_local_currency updated from ',
        baseline_arr_local_currency::Text,
        ' to ',
        baseline_arr_local_currency_new::text
      )
      else ''
    end
  ) as modified_comments,
  cohort_actions
from tmp_pf_split;
--finally update product solution based on product group mappings
/*
 select distinct a.updated_product_group,b.*
 from sandbox_pd.sst_pg_pf_updates a
 join ufdm_grey.sst_product_group_porduct_solution_mappings_manual b
 on a.updated_product_group = b.product_group
 where 1=1 and (coalesce(new_product_solution,'') = '')
 and coalesce(arr,0) > 0
 ;
 */
update ryzlan.sku_sst a
set new_product_solution = b.product_solution,
  modified_comments = concat(
    coalesce(modified_comments, ''),
    '==> new_product_solution updated from blank'
  )
from ufdm_grey.sst_product_group_porduct_solution_mappings_manual b
where 1 = 1
  and a.updated_product_group = b.product_group
  and (coalesce(new_product_solution, '') = '')
  and coalesce(arr, 0) > 0;
--
delete --select arr,baseline_arr_local_currency,sku,product_family,*
from ryzlan.sku_sst
where 1 = 1
  and snapshot_date between '2021-01-31' and '2021-07-31'
  and mcid = '5a8496b5-17ed-06d0-9cf9-664634366539'
  and (
    (
      sku = 'CLCO2-PVPP'
      and arr > 17000
    )
    or (
      sku = 'CLCO2'
      and arr < 90000
    )
  );
--     delete
--         --select arr,baseline_arr_local_currency,sku,product_family,*
--     from ryzlan.sku_sst
--     where 1=1
--       and snapshot_date between '2021-01-31' and '2021-01-31'
--       and mcid = '6e2dddfc-4f35-837f-6912-ed2b9bc5dab0'
--       and ( (sku = 'CLCO2-PVPP' and arr > 17000) or (sku = 'CLCO2' and arr < 90000))
--     ;
drop table if exists tmp_mcid_mappings_vamsi;
create temp table tmp_mcid_mappings_vamsi as
select '9c3d7c94-4fcf-25c3-8f32-bfcdb1c4135e' as mcid_new,
  '2205022046' as mcid_old
union all
select '4b37b7f8-9989-ddf2-f483-70981c90d9ff',
  '577690255'
union all
select 'b20aa31e-b447-90ed-39dc-c462f797c4b0',
  '226558773'
union all
select '88d24466-8dd5-d2f4-1188-fb4ad9848829',
  '300527710'
union all
select '3131b01a-1eaa-db11-8952-0018717a8c82',
  'da86f6ba-8c01-e411-a67d-0050568d2da8'
union all
select '75ed44e1-a3ce-bd51-f03a-b0fcb5932aee',
  '3212741224'
union all
select 'f56678df-f192-bb48-a8c9-6cbfab90cdc7',
  '2002000414'
union all
select '174f4865-a8c2-35d5-1785-c03775b3c4b9',
  '2839070157'
union all
select '79753210-8c38-ac0c-87ba-372b81a43902',
  '10155050976'
union all
select 'ecccdfa3-8444-b1db-9af8-b662824397cb',
  '10205350479'
union all
select '2b716d30-8c9b-45a6-7f27-c32cb711666b',
  '12259010309'
union all
select '516af03e-6b40-fa4b-eed3-94cf018265fc',
  '12373912567'
union all
select '88ac9cdf-bc80-c476-29cb-fc09edd690f1',
  '13230780019'
union all
select '44e143ba-7840-1493-f226-9685bdfa219f',
  '1809350501'
union all
select 'abf09198-59ad-e5ba-a004-1eba2ee117e7',
  '19950585'
union all
select '02bf49cc-f681-1efa-b544-70fd8c217838',
  '2002000089'
union all
select 'ac3a3278-e9c1-e611-80f1-fc15b426ff90',
  '222ed82b-53b1-e211-9907-0050568d002c'
union all
select '72d16811-4c77-5078-f5a0-570b39a4b348',
  '2562510400'
union all
select '48fc0e42-8a9e-0c29-0127-a3fb732470b9',
  '3261430067'
union all
select 'ee6c7b56-8355-001e-5e6d-87394854c174',
  '3736420181'
union all
select 'e3b0258f-48a3-7a38-cef7-45bffee0ad42',
  '3744411361'
union all
select '255a8bfe-e122-4ea9-6df2-a654cf50e696',
  '3848171733'
union all
select 'd677f73e-a24f-3492-160a-5e272a7dd8f1',
  '4666041069'
union all
select '49e1b7db-d1f0-f776-4106-e73ee5d5e82e',
  '4758484551'
union all
select 'cd37a35a-7e8f-df11-a236-0018717a8c82',
  '4c977ce6-9246-ac3b-23fa-dc711e5e651f'
union all
select 'badebfef-e93c-21f6-c852-72f6759e722b',
  '5444685741'
union all
select '49f9a9f0-b8b3-c26d-5c3f-6c2013762dd9',
  '5763640713'
union all
select '34d0b34a-4bfc-7bdf-07d4-3c83f691a99d',
  '6131884851'
union all
select 'a9dae5d9-1aa8-c763-79d4-89697d3e7f31',
  '7766623933'
union all
select '5aa442dc-03bf-59d4-8fb2-7fb270d7cd3b',
  '7955584735'
union all
select '06e9e8c6-b263-d2f9-8e0b-9da3fc37f793',
  '8081092737'
union all
select '44fc1d48-d3ee-e8e2-3caa-87b09f8bfb0d',
  '8091130321'
union all
select 'a9055dda-27c7-1497-3bf5-f05e287a438c',
  '8342874567'
union all
select 'eab47761-4870-b6e5-8d40-420ab18a648f',
  '8442657002'
union all
select 'a0dd5276-385e-084e-5d1e-5dbfd3ce2074',
  '8443856239'
union all
select 'e52847a0-5887-17bc-4d6b-c0ff19e59cb6',
  '9414726664'
union all
select '977f1355-4133-80e2-eb64-9f355f644334',
  '9859685907'
union all
select '2a664eb7-cff0-8532-9b1f-fb43b588465c',
  'a664eb7-cff0-8532-9b1f-fb43b588465c'
union all
select '3b69a094-5c1a-ea11-a811-000d3a228515',
  'ace38ab1-43ae-5f52-686a-1564781f56a4'
union all
select 'd1264cda-8659-7d18-0dc5-7f0ed8b63654',
  'f103e73a-334e-e411-9f63-0050568d2da8';
update ryzlan.sku_sst a
set mcid = b.mcid_new,
  end_mcid = case
    when end_mcid = b.mcid_old then b.mcid_new
    else end_mcid
  end,
  parent_mcid = case
    when parent_mcid = b.mcid_old then b.mcid_new
    else parent_mcid
  end,
  modified_comments = concat(
    coalesce(modified_comments, ''),
    'mcid updated from ',
    a.mcid,
    ' to ',
    b.mcid_new
  )
from tmp_mcid_mappings_vamsi b --select distinct b.*,a.parent_mcid,a.end_mcid from tmp_mcid_mappings_vamsi b,ryzlan.sku_sst a
where a.mcid = b.mcid_old;
END;
$function$;