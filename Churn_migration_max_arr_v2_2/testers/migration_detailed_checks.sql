--- for product group

with before as
    (
        select
            DATE(TO_DATE(evaluation_period, 'YYYY"M"MM')+ interval '1 month - 1 day') as snapshot_date,
            mcid,
            product_bridge,
            coalesce(current_product_solution,prior_product_solution) as product_group,
            sum(product_arr_change_ccfx) as arr_before,
            sum(product_arr_change_lcu) as lcu_before
        from ufdm_archive.sst_product_solution_cloud_churn_mig_lcoked_18022025_1547
        where 1=1
--         and product_bridge ilike '%migr%'
        and product_arr_change_ccfx <> 0
        group by 1,2,3,4
    )
, after as
    (
        select
            DATE(TO_DATE(evaluation_period, 'YYYY"M"MM')+ interval '1 month - 1 day') as snapshot_date,
            mcid,
            product_bridge,
            coalesce(current_product_solution,prior_product_solution) as product_group,
            sum(product_arr_change_ccfx) as arr_after,
            sum(product_arr_change_lcu) as lcu_after
        from sandbox.sst_product_solution_bridge_rollup_cm_cloud
        where 1=1
--         and product_bridge ilike '%migr%'
        and product_arr_change_ccfx <> 0
        and evaluation_period <> '2025M02'
        group by 1,2,3,4
    )
, combo as
    (
        select
            distinct snapshot_date,mcid, product_bridge,product_group
        from before
        union all
        select
            distinct snapshot_date,mcid,product_bridge,product_group
        from after
    )
select *,
       case when product_bridge in ('Up Sell - migration','Cross-sell - migration') then 'Positive Migration'
         when product_bridge in ('Downgrade - migration','Downsell - migration') then 'Negative Migration'
    end as migration_type
from(
select
    distinct on (c.snapshot_date,c.mcid,c.product_bridge,c.product_group)
    c.snapshot_date,
    c.mcid,
    coalesce(c.product_bridge, b.product_bridge) as product_bridge ,
    c.product_group,
    coalesce(b.arr_before,0) as arr_before,
    coalesce(a.arr_after,0) as arr_after,
    round(coalesce(a.arr_after,0) - coalesce(b.arr_before,0)) as diff_arr
--     coalesce(b.lcu_before,0) as lcu_before,
--     coalesce(a.lcu_after,0) as lcu_after,
--     round(coalesce(a.lcu_after,0) - coalesce(b.lcu_before,0)) as diff_lcu

from combo c
left join before b
on c.snapshot_date = b.snapshot_date
and c.mcid = b.mcid
and c.product_bridge = b.product_bridge
and c.product_group = b.product_group
left join after a
on c.snapshot_date = a.snapshot_date
and c.mcid = a.mcid
and c.product_bridge = a.product_bridge
and c.product_group = a.product_group
where abs(round(coalesce(a.lcu_after,0) - coalesce(b.lcu_before,0))) not between 0 and 1
order by c.snapshot_date,c.mcid,c.product_bridge,c.product_group
    ) as a
where product_bridge  not in ('Lapsed Renewal', 'Winback');


-- Customer bridge
-------- CUSTOMER BRIDGE
-- overall
with base_df as (
select
    mcid ,
    evaluation_period,
    customer_bridge as base_bridge ,
    sum(customer_arr_change_lcu) as base_arr
from ufdm_archive.sst_customer_bridge_lcoked_18032025_0244
-- ufdm_archive.sst_customer_bridge_lcoked_10122024_1354
group by 1,2,3

)
, test_df as (
select
    mcid ,
    evaluation_period,
    customer_bridge as test_bridge ,
    sum(customer_arr_change_lcu) as test_arr
from sandbox.sst_customer_bridge_rollover_cm
group by 1,2,3

)

, middle_df as (
select
    coalesce(a.mcid , b.mcid ) as mcid ,
    coalesce(a.evaluation_period, b.evaluation_period) as evaluation_period ,
    coalesce (a.base_bridge , b.test_bridge ) as bridge,
    split_part(coalesce (a.base_bridge , b.test_bridge ), '- migration',1) as bridge_part,
    case when coalesce (a.base_bridge , b.test_bridge ) ilike '%- migration' then 'migration' else null end as migration_part,
    coalesce(a.base_arr ,0) as base_arr ,
    coalesce(b.test_arr ,0) as test_arr ,
    (coalesce(a.base_arr ,0) - coalesce(b.test_arr ,0) ) as varience_arr
from base_df as a
full join test_df as b
on a.base_bridge = b.test_bridge
and a.mcid = b.mcid
and a.evaluation_period = b.evaluation_period

)

select *
from (
select
        * ,
        varience_arr + lead_var as varr_diff ,
        varience_arr + lag_var as varr_diff2

from (
    select
--         mcid , evaluation_period, bridge_part , sum(varience_arr) sum_var
        *,
        lag(varience_arr) over(partition by mcid , evaluation_period order by bridge_part) as lag_var,
        lead(varience_arr) over(partition by mcid , evaluation_period order by bridge_part ) as lead_var
--         dense_rank() over (partition by mcid, evaluation_period , bridge_part, migration_part) as rnk
    from middle_df
    where varience_arr <> 0
-- group by mcid , evaluation_period, bridge_part
              ) as a
     ) as a
where varr_diff2 <> 0 or varr_diff <> 0
-- where sum_var <> 0
;