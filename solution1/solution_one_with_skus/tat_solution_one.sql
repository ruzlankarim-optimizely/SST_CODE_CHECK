-- #######################################################
-- TAT CODE
-- #######################################################


DROP table sandbox.tat_with_sku;
CREATE TABLE sandbox.tat_with_sku as (
with edit_tat as (
  select *
  from ufdm.tat_upload_data tat
  where tat.is_deleted IS DISTINCT
  FROM 1
    and "Overage Y/N" is distinct
  from 'Y'
    and not (
      date_trunc('month', snapshot_date) = '2021-12-01'::DATE
      AND product_family ilike '%Campaign%'
    )
),
tat_change_1 as (
  select distinct mcid,
    snapshot_date,
    product_family,
    currency,
    sum(arr) over(
      partition by mcid,
      snapshot_date,
      product_family,
      currency
    ) as arr,
    sum(arr_usd_ccfx) over(
      partition by mcid,
      snapshot_date,
      product_family,
      currency
    ) as arr_usd_ccfx
  from edit_tat
),
tat_change_2 as (
  select distinct tc1.mcid,
    tc1.snapshot_date,
    sum(tc1.arr) over(
      partition by tc1.mcid,
      tc1.snapshot_date
    ) as "ARR:Local Currency",
    sum(tc1.arr_usd_ccfx) over(
      partition by tc1.mcid,
      tc1.snapshot_date
    ) as "ARR:USD CCFX"
  from tat_change_1 tc1
  where arr >= 0
) --Now take the sandbox.drag_ration table and append ending ARR to 
,
tat_change_3 as (
  select sdr3."customer_name_d&b",
    sdr3.parent_customer,
    sdr3.parent_master_customer_id,
    sdr3.customer_name,
    sdr3.end_customer,
    sdr3.mcid,
    sdr3."Overage Y/N",
    sdr3."NS ID",
    sdr3.subsidiary_name,
    sdr3.currency,
    sdr3.snapshot_date,
    sdr3.fx_rate_ccfx,
    sdr3.ccfx_date,
    sdr3.mcid_old,
    sdr3.is_deleted,
    sdr3.modified_comments,
    sdr3."MAX Snapshot Date of TAT",
    sdr3.product_family_arr,
    sdr3.sku,
    sdr3."Original Ratio",
    sdr3."Date to Drag to Under Scenario 1",
    sdr3."Date to Drag Under Scenario 2",
    sdr3."Product Family Transition",
    sdr3."New Ratio Per Date for TAT",
    sdr3."Sum of Ratios Per MCID and Snapshot Date",
    tc2."ARR:Local Currency",
    tc2."ARR:USD CCFX"
  from sandbox.drag_ration_with_sku sdr3
    inner join tat_change_2 tc2 on tc2.mcid = sdr3.mcid
    and tc2.snapshot_date = sdr3.snapshot_date --For Scenario 1 
  where sdr3.snapshot_date >= sdr3."Date to Drag to Under Scenario 1"::DATE
) --select 
--    *
--from 
--    tat_change_3 
--where 
--    mcid = 'd75409e0-c8f2-e711-811d-70106faa0841'
--select 
--    *
--from 
--    ufdm.tat_upload_data tud 
--where 
--    mcid = '03c69ff6-a949-ea11-a812-000d3a228882'
--and 
--    date_trunc('MONTH', snapshot_date) = '2021-02-01' 
--This is the final tat to be changed. It has the same structure as original TAT 
,
tat_change_4 as (
  select tc3."customer_name_d&b",
    tc3.parent_customer,
    tc3.parent_master_customer_id,
    tc3.customer_name,
    tc3.end_customer,
    tc3.mcid,
    tc3."Overage Y/N",
    tc3."NS ID",
    tc3.subsidiary_name,
    tc3.product_family_arr as product_family,
    tc3.sku ,
    --new product family of TAT 
    tc3.currency,
    tc3.snapshot_date,
    (
      tc3."ARR:Local Currency" * tc3."New Ratio Per Date for TAT"
    ) as arr,
    -- new local currency arr of TAT 
    tc3.fx_rate_ccfx,
    (
      tc3."ARR:USD CCFX" * tc3."New Ratio Per Date for TAT"
    ) as arr_usd_ccfx,
    --new arr_usd_ccfx of TAT 
    tc3.ccfx_date,
    tc3.mcid_old,
    tc3.is_deleted,
    tc3.modified_comments
  from tat_change_3 tc3
) --select 
--    *
--from 
--    tat_change_4 
--where 
--    mcid = 'abf9133d-75e4-e411-9afb-0050568d2da8'
--select 
--    *,
--    sum(arr) over(partition by mcid, snapshot_date) as sum_arr,
--    sum(arr_usd_ccfx) over(partition by mcid, snapshot_date) as sum_arr_usd_ccfx
--from 
--    tat_change_4 
--where 
--    mcid = '1f6370ef-dbaf-e311-a1cd-0050568d2da8'
--    and 
--    date_trunc('MONTH', snapshot_date) = '2019-01-01'
--Make sure ending ARRs match 
--Running Tests 
--,   test_1 as 
--(
--select 
--    distinct mcid as mcid_nt, 
--    snapshot_date as snapshot_date_nt, 
--    sum(arr_usd_ccfx) over(partition by mcid, snapshot_date) as sum_new_tat_ccfx,
--    sum(arr) over(partition by mcid, snapshot_date) as sum_new_tat_lc
--from 
--    tat_change_4
--)
--
--select 
--    t1.mcid_nt,
--    t1.snapshot_date_nt,
--    t1.sum_new_tat_ccfx,
--    t1.sum_new_tat_lc, 
--    tc2."ARR:USD CCFX",
--    tc2."ARR:Local Currency"
--from 
--    test_1 t1
--inner join 
--    tat_change_2 tc2
--          on 
--                t1.mcid_nt = tc2.mcid
--                and 
--                t1.snapshot_date_nt = tc2.snapshot_date 
--where 
--    abs(tc2."ARR:USD CCFX"-t1.sum_new_tat_ccfx) > 1
--Now union to TAT that does not change. Take all the mcids and dates that are not present in sandbox ratio
,
tat_no_change as (
  select st7."customer_name_d&b",
    st7.parent_customer,
    st7.parent_master_customer_id,
    st7.customer_name,
    st7.end_customer,
    st7.mcid,
    st7."Overage Y/N",
    st7."NS ID",
    st7.subsidiary_name,
    st7.product_family,
    sdr2.sku ,
    st7.currency,
    st7.snapshot_date,
    st7.arr,
    st7.fx_rate_ccfx,
    st7.arr_usd_ccfx,
    st7.ccfx_date,
    st7.mcid_old,
    st7.is_deleted,
    st7.modified_comments
  from edit_tat st7
    left join sandbox.drag_ration_with_sku sdr2 on st7.mcid = sdr2.mcid
    and st7.snapshot_date = sdr2.snapshot_date --For Scenario 1 
  where sdr2.mcid is null
) --select 
--    *
--from 
--    tat_no_change 
--where 
--    mcid = '819e438e-610b-e111-9d3b-0050568d002c'
--and 
--    date_trunc('MONTH', snapshot_date) = '2021-12-01'
--Tests: Use this mcid 
--select 
--    *
--from 
--    tat_no_change 
--where 
--    mcid = '0059ab5d-4608-c5a9-57f7-0d4b79c42cba'
--order by 
--    snapshot_date 
--select 
--    *
--from 
--    sandbox.drag_ration_3 dr 
--where 
--    mcid = '0059ab5d-4608-c5a9-57f7-0d4b79c42cba'
--Now union the 2 tables 
,
combined_table_1 as (
  (
    select tc4."customer_name_d&b",
      tc4.parent_customer,
      tc4.parent_master_customer_id,
      tc4.customer_name,
      tc4.end_customer,
      tc4.mcid,
      tc4."Overage Y/N",
      tc4."NS ID",
      tc4.subsidiary_name,
      tc4.product_family,
      tc4.sku , 
      --new product family of TAT 
      tc4.currency,
      tc4.snapshot_date,
      tc4.arr,
      -- new local currency arr of TAT 
      tc4.fx_rate_ccfx,
      tc4.arr_usd_ccfx,
      --new arr_usd_ccfx of TAT 
      tc4.ccfx_date,
      tc4.mcid_old,
      tc4.is_deleted,
      tc4.modified_comments
    from tat_change_4 tc4
  )
  union all
  (
    select tcn."customer_name_d&b",
      tcn.parent_customer,
      tcn.parent_master_customer_id,
      tcn.customer_name,
      tcn.end_customer,
      tcn.mcid,
      tcn."Overage Y/N",
      tcn."NS ID",
      tcn.subsidiary_name,
      tcn.product_family,
      tcn.sku , 
      tcn.currency,
      tcn.snapshot_date,
      tcn.arr,
      tcn.fx_rate_ccfx,
      tcn.arr_usd_ccfx,
      tcn.ccfx_date,
      tcn.mcid_old,
      tcn.is_deleted,
      tcn.modified_comments
    from tat_no_change tcn
  )
) --select 
--    *
--from 
--    combined_table_1
,
new_prod_tat as (
  select ct1."customer_name_d&b",
    ct1.parent_customer,
    ct1.parent_master_customer_id,
    ct1.customer_name,
    ct1.end_customer,
    ct1.mcid,
    ct1."Overage Y/N",
    ct1."NS ID",
    ct1.subsidiary_name,
    ct1.product_family,
    ct1.sku ,
    ct1.currency,
    ct1.snapshot_date,
    ct1.arr,
    ct1.fx_rate_ccfx,
    ct1.arr_usd_ccfx,
    ct1.ccfx_date,
    ct1.mcid_old,
    ct1.is_deleted,
    ct1.modified_comments
  from combined_table_1 ct1
  order by ct1.mcid,
    ct1.snapshot_date
) --New TAT table: Sol 1
--
select *
from new_prod_tat

);
-- where mcid = 'd75409e0-c8f2-e711-811d-70106faa0841' --select 
  --    distinct mcid,
  --    snapshot_date,
  --    sum(arr_usd_ccfx) as sum_a,
  --    sum(arr) as sum_b
  --from 
  --    new_prod_tat
  --where 
  --    mcid = '1f6370ef-dbaf-e311-a1cd-0050568d2da8'
  --    and 
  --    date_trunc('MONTH', snapshot_date) = '2019-01-01'
  --group by 
  --    1,2
  --select 
  --    count(distinct snapshot_date)
  --from 
  --    with edit_tat as  tud 
  --Test: Take ARR by MCID and Date of Combined_Table and compare to old TAT. It should match 
  --
-- ,
--   test_1 as (
--     select distinct mcid as mcid_nt,
--       snapshot_date as snapshot_date_nt,
--       sum(arr_usd_ccfx) over(partition by mcid, snapshot_date) as sum_new_tat_ccfx,
--       sum(arr) over(partition by mcid, snapshot_date) as sum_new_tat_lc
--     from combined_table_1 --where 
--       --    is_DELETED = 0 
--   )
-- select t1.mcid_nt,
--   t1.snapshot_date_nt,
--   t1.sum_new_tat_ccfx,
--   t1.sum_new_tat_lc,
--   tc2."ARR:USD CCFX",
--   tc2."ARR:Local Currency"
-- from test_1 t1
--   inner join tat_change_2 tc2 on t1.mcid_nt = tc2.mcid
--   and t1.snapshot_date_nt = tc2.snapshot_date
-- where abs(tc2."ARR:USD CCFX" - t1.sum_new_tat_ccfx) > 1