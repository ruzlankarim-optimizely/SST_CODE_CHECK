-- Granular level

with base_df as (
select
    evaluation_period,
    mcid,
    customer_bridge as base_bridge ,
    sum(customer_arr_change_ccfx) as base_arr
from ufdm_archive.sst_customer_bridge_core_lcoked_17122024_0238
group by 1,2,3

)
, test_df as (
select
    evaluation_period,
    mcid,
    customer_bridge as test_bridge ,
    sum(customer_arr_change_ccfx) as test_arr
from sandbox.sst_customer_bridge_rollover_cm_core
group by 1,2,3

)

, middle_df as (
select
    coalesce(a.evaluation_period,b.evaluation_period) as evaluation_period ,
    coalesce(a.mcid, b.mcid) as mcid ,
    coalesce (a.base_bridge , b.test_bridge ) as bridge,
    coalesce(a.base_arr ,0) as base_arr ,
    coalesce(b.test_arr ,0) as test_arr ,
    (coalesce(a.base_arr ,0) - coalesce(b.test_arr ,0) ) as varience_arr
from base_df as a
full join test_df as b
on a.base_bridge = b.test_bridge
and a.mcid = b.mcid
and a.evaluation_period = b.evaluation_period

)
select
    a.*
from(
    select
        * ,
        sum(varience_arr) over(partition by evaluation_period, mcid ) as sum_variance
    from middle_df
--     where varience_arr <> 0
    ) as a
where sum_variance <> 0 and  sum_variance <> 0.01 and  sum_variance <> -0.01;
-- overall
with base_df as (
select
    customer_bridge as base_bridge ,
    sum(customer_arr_change_ccfx) as base_arr
from ufdm_archive.sst_customer_bridge_core_lcoked_17122024_0238
-- ufdm_archive.sst_customer_bridge_lcoked_10122024_1354
group by 1

)
, test_df as (
select
    customer_bridge as test_bridge ,
    sum(customer_arr_change_ccfx) as test_arr
from sandbox.sst_customer_bridge_rollover_cm_core
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


-- examples
   mcid = 'c03b1919-67c7-e411-9afb-0050568d2da8'
and evaluation_period = '2024M07'
   has cross sell only on CB
   but has cross migration and downsell migration on PG
   so CB CM does cross sell negative  and cross sell migration positive to get the total sum same

   mcid = '670920c7-185f-e411-9afb-0050568d2da8'
and evaluation_period = '2022M06'
   upsell only in CB
   upsell mig and Downgrade Migraion and downsell migration in
mcid = '1e617738-3698-e211-9907-0050568d002c'
and evaluation_period = '2023M11'
upsell same issue
   mcid = '44072f7b-2056-e911-a960-000d3a3a3a80'
   and evaluation_period= '2022M05'

670920c7-185f-e411-9afb-0050568d2da8
2022M06

--- customer without CM
select
    evaluation_period ,
    mcid ,
    prior_period_customer_arr_usd_ccfx,
    current_period_customer_arr_usd_ccfx,
    customer_arr_change_ccfx,
    customer_bridge,
    pathways
from ufdm_archive.sst_customer_bridge_cloud_lcoked_17122024_0238
where mcid = '670920c7-185f-e411-9afb-0050568d2da8'
   and evaluation_period= '2022M06';

-- customer CM test
select
    evaluation_period ,
    mcid ,
    prior_period_customer_arr_usd_ccfx,
    current_period_customer_arr_usd_ccfx,
    customer_arr_change_ccfx,
    customer_bridge,
    pathways
from sandbox.sst_customer_bridge_rollover_cm_cloud
where mcid = '670920c7-185f-e411-9afb-0050568d2da8'
    and evaluation_period= '2022M06';

-- product group CM
select
    evaluation_period,
    DATE(TO_DATE(evaluation_period, 'YYYY"M"MM')+ interval '1 month - 1 day') as snapshot_date,
    mcid,
    concat(current_product_group,'-',prior_product_group) as product_group,
    product_bridge,
    pathways,
    product_arr_change_ccfx,
    product_arr_change_lcu,
    currency_code
from ufdm_archive.sst_product_group_cloud_churn_mig_lcoked_17122024_0238
where 1 = 1
and mcid = '670920c7-185f-e411-9afb-0050568d2da8'
    and evaluation_period= '2022M06';

-- mig joiner
select * from sandbox.customer_product_migration_joiner
where mcid = '670920c7-185f-e411-9afb-0050568d2da8'
    and evaluation_period= '2022M06';
