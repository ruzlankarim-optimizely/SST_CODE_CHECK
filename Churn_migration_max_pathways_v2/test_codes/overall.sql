-- overall
with base_df as (
select
    product_bridge as base_bridge ,
    sum(product_arr_change_ccfx) as base_arr
from ufdm_archive.sst_pb_product_solution_cloud_license_lcoked_18032025_0244
--     ufdm_archive.sst_pb_product_group_cloud_license_lcoked_18032025_0244
--     ufdm_archive.sst_product_bridge_product_group_lcoked_18032025_0244
--     ufdm_archive.sst_pb_product_solution_cloud_license_lcoked_18022025_1547
-- ufdm_archive.sst_customer_bridge_lcoked_10122024_1354
group by 1

)
, test_df as (
select
    product_bridge as test_bridge ,
    sum(product_arr_change_ccfx) as test_arr
from sandbox.sst_product_solution_bridge_rollup_cm_cloud
--     sandbox.sst_product_group_bridge_cloud
--     sandbox.sst_product_group_bridge
--     sandbox.sst_product_solution_bridge_rollup_cm_cloud
group by 1

)

, middle_df as (
select
    coalesce (a.base_bridge , b.test_bridge ) as bridge,
    coalesce(a.base_arr ,0) as base_arr ,
    coalesce(b.test_arr ,0) as test_arr ,
    (coalesce(a.base_arr ,0) - coalesce(b.test_arr ,0) ) as varience_arr
from base_df as a
full join test_df as b
on a.base_bridge = b.test_bridge

)

    select
        *
    from middle_df
    where varience_arr <> 0;
-------- CUSTOMER BRIDGE
-- overall
with base_df as (
select
    customer_bridge as base_bridge ,
    sum(customer_arr_change_lcu) as base_arr
from ufdm_archive.sst_customer_bridge_lcoked_18032025_0244
-- ufdm_archive.sst_customer_bridge_lcoked_10122024_1354
group by 1

)
, test_df as (
select
    customer_bridge as test_bridge ,
    sum(customer_arr_change_lcu) as test_arr
from sandbox.sst_customer_bridge_rollover_cm
group by 1

)

, middle_df as (
select
    coalesce (a.base_bridge , b.test_bridge ) as bridge,
    coalesce(a.base_arr ,0) as base_arr ,
    coalesce(b.test_arr ,0) as test_arr ,
    (coalesce(a.base_arr ,0) - coalesce(b.test_arr ,0) ) as varience_arr
from base_df as a
full join test_df as b
on a.base_bridge = b.test_bridge

)

    select
        *
    from middle_df
    where varience_arr <> 0
;