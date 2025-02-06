-- sst
Select
    snapshot_date ,
    mcid ,
    sku,
    updated_product_group ,
    new_product_solution,
    migration_from,
    migration_to,
    arr
from ufdm_archive.sst_adhoc_lcoked_10122024_1354
where mcid = '1b026b3d-992b-e111-9eb3-0050568d002c'
and snapshot_date in ('2023-11-30' , '2023-12-31');

-- Classifier
select *
from ufdm_archive.churn_migration_classifiers_lcoked_10122024_1354
where mcid = 'c03b1919-67c7-e411-9afb-0050568d2da8'
and evaluation_period = '2024M07';
--
-- select *
-- from sandbox_pd.churn_migration_classifiers2
-- where mcid = '89c9b1c2-ac43-56ad-2651-a9e0538a6712'
-- and evaluation_period = '2022M04';


-- classifier Joiner
    select
        *
    from sandbox.solution_classifier_migration_joiner
    where mcid = 'c03b1919-67c7-e411-9afb-0050568d2da8'
and evaluation_period = '2024M07';


-- product Solution
select
    evaluation_period,
    mcid,
    currency_code ,
    prior_product_solution ,
    current_product_solution ,
    product_arr_change_ccfx,
    product_bridge ,
    pathways
from sandbox.sst_product_solution_bridge_rollup_cm
where mcid = 'c03b1919-67c7-e411-9afb-0050568d2da8'
and evaluation_period = '2024M07';


--product solution cm
select
    evaluation_period,
    mcid,
    currency_code ,
    prior_product_solution ,
    current_product_solution ,
    product_arr_change_ccfx ,
    product_bridge ,
    pathways
from ufdm_archive.sst_product_bridge_product_solution_lcoked_10122024_1354
where mcid = 'c03b1919-67c7-e411-9afb-0050568d2da8'
and evaluation_period = '2024M07';
-- product Group cm
select
    evaluation_period,
    mcid,
    currency_code ,
    prior_product_group ,
    current_product_group ,
    product_arr_change_ccfx ,
    product_bridge ,
    pathways
from ufdm_archive.sst_product_group_churn_mig_lcoked_10122024_1354
where mcid = 'c03b1919-67c7-e411-9afb-0050568d2da8'
and evaluation_period = '2024M07';

-- product group

select
    evaluation_period,
    mcid,
    currency_code ,
    prior_product_group ,
    current_product_group ,
    product_arr_change_ccfx ,
    product_bridge ,
    pathways
from ufdm_archive.sst_product_bridge_product_group_lcoked_10122024_1354
where mcid = 'c03b1919-67c7-e411-9afb-0050568d2da8'
and evaluation_period = '2024M07';

--  TESTING
-- Granular level

with base_df as (
select
    evaluation_period,
    mcid,
    product_bridge as base_bridge ,
    sum(product_arr_change_ccfx) as base_arr
from ufdm_archive.sst_product_bridge_product_solution_lcoked_10122024_1354
group by 1,2,3

)
, test_df as (
select
    evaluation_period,
    mcid,
    product_bridge as test_bridge ,
    sum(product_arr_change_ccfx) as test_arr
from sandbox.sst_product_solution_bridge_rollup_cm
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
where sum_variance <> 0 and  sum_variance <> 0.01 and  sum_variance <> -0.01 and sum_variance not between 1 and -1 ;


-- overall
with base_df as (
select
    product_bridge as base_bridge ,
    sum(product_arr_change_ccfx) as base_arr
from ufdm_archive.sst_product_bridge_product_solution_lcoked_10122024_1354
-- ufdm_archive.sst_customer_bridge_lcoked_10122024_1354
group by 1

)
, test_df as (
select
    product_bridge as test_bridge ,
    sum(product_arr_change_ccfx) as test_arr
from sandbox.sst_product_solution_bridge_rollup_cm
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