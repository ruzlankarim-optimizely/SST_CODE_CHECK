-- Granular level

with base_df as (
select
    evaluation_period,
    mcid,
    customer_bridge as base_bridge ,
    sum(customer_arr_change_ccfx) as base_arr
from ufdm_archive.sst_customer_bridge_lcoked_10122024_1354
group by 1,2,3

)
, test_df as (
select
    evaluation_period,
    mcid,
    customer_bridge as test_bridge ,
    sum(customer_arr_change_ccfx) as test_arr
from sandbox.sst_customer_bridge_rollover_cm
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
from ufdm_archive.sst_customer_bridge_churn_mig_lcoked_10122024_1354
-- ufdm_archive.sst_customer_bridge_lcoked_10122024_1354
group by 1

)
, test_df as (
select
    customer_bridge as test_bridge ,
    sum(customer_arr_change_ccfx) as test_arr
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


-- examples
    d74620b9-768f-dd11-a26e-0018717a8c82
    2023M03
    --base
    upsell = 93090.57
    -- test
    upsell = 84660.62
    upsell mig = 101520.52
    var = -93090.57
--- customer without CM
select
    evaluation_period ,
    mcid ,
    prior_period_customer_arr_usd_ccfx,
    current_period_customer_arr_usd_ccfx,
    customer_arr_change_ccfx,
    customer_bridge,
    pathways
from ufdm_archive.sst_customer_bridge_lcoked_10122024_1354
where mcid = 'd74620b9-768f-dd11-a26e-0018717a8c82'
and evaluation_period = '2023M03';

-- customer CM test
select
    evaluation_period ,
    mcid ,
    prior_period_customer_arr_usd_ccfx,
    current_period_customer_arr_usd_ccfx,
    customer_arr_change_ccfx,
    customer_bridge,
    pathways
from sandbox.sst_customer_bridge_rollover_cm
where mcid = '218a418d-453d-e411-9f63-0050568d2da8'
and evaluation_period = '2022M10';

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
from ufdm_archive.sst_product_group_churn_mig_lcoked_10122024_1354
where 1 = 1
and mcid = '218a418d-453d-e411-9f63-0050568d2da8'
and evaluation_period = '2022M10';

-- mig joiner
select * from sandbox.customer_product_migration_joiner
where mcid = 'd74620b9-768f-dd11-a26e-0018717a8c82'
and evaluation_period = '2023M03';
