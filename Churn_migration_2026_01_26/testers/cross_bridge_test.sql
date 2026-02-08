with cb_new_total as
    (
        select
            DATE(TO_DATE(evaluation_period, 'YYYY"M"MM')+ interval '1 month - 1 day') as snapshot_date,
            mcid,
            sum(product_arr_change_ccfx) as arr_change_ccfx,
            sum(product_arr_change_lcu) as arr_change_lcu,
            sum(product_arr_change_ccfx) filter ( where product_bridge IN ('Cross-sell - migration') ) as "Cross-sell - migration",
            sum(product_arr_change_ccfx) filter ( where product_bridge IN ('Downgrade - migration') ) as "Downgrade - migration",
            sum(product_arr_change_ccfx) filter ( where product_bridge IN ('Downsell - migration') ) as "Downsell - migration",
            sum(product_arr_change_ccfx) filter ( where product_bridge IN ('Up Sell - migration') ) as "Up Sell - migration",
            sum(product_arr_change_ccfx) filter ( where product_arr_change_ccfx > 0 ) as "cb_Pos_migration",
            sum(product_arr_change_ccfx) filter ( where product_arr_change_ccfx < 0 ) as "cb_Neg_migration"
        from sandbox.sst_product_solution_bridge_rollup_cm_cloud
        where product_bridge ilike '%migr%'
        group by 1,2
    )
, pg_new_total as
    (
        select
            DATE(TO_DATE(evaluation_period, 'YYYY"M"MM')+ interval '1 month - 1 day') as snapshot_date,
            mcid,
            sum(product_arr_change_ccfx) as arr_change_ccfx,
            sum(product_arr_change_lcu) as arr_change_lcu,
            sum(product_arr_change_ccfx) filter ( where product_bridge IN ('Cross-sell - migration') ) as "Cross-sell - migration",
            sum(product_arr_change_ccfx) filter ( where product_bridge IN ('Downgrade - migration') ) as "Downgrade - migration",
            sum(product_arr_change_ccfx) filter ( where product_bridge IN ('Downsell - migration') ) as "Downsell - migration",
            sum(product_arr_change_ccfx) filter ( where product_bridge IN ('Up Sell - migration') ) as "Up Sell - migration",
            sum(product_arr_change_ccfx) filter ( where product_arr_change_ccfx > 0  ) as "PG_pos_migration",
            sum(product_arr_change_ccfx) filter (where product_arr_change_ccfx < 0 ) as "PG_neg_migration"
        from sandbox.sst_product_group_bridge_cloud
        where product_bridge ilike '%migr%'
        group by 1,2
    )
, mcid_snap as
    (
        select
            mcid,
            snapshot_date
        from cb_new_total
        union all
        select
            mcid,
            snapshot_date
        from pg_new_total
    )
, final AS
    (
        select
            distinct on (ms.mcid,ms.snapshot_date)
            ms.snapshot_date,
            ms.mcid,
            cn.arr_change_ccfx as customer_arr_change_ccfx_new,
            cn."Cross-sell - migration" as "NEW cust Cross-sell - migration",
            cn."Downgrade - migration" as "NEW cust Downgrade - migration",
            cn."Downsell - migration" as "NEW cust Downsell - migration",
            cn."Up Sell - migration" as "NEW cust Upsell - migration",
            cn."cb_Pos_migration",
            cn."cb_Neg_migration",
            pgn.arr_change_ccfx as product_arr_change_ccfx_new,
            pgn."Cross-sell - migration" as "NEW pg Cross-sell - migration",
            pgn."Downgrade - migration" as "NEW pg Downgrade - migration",
            pgn."Downsell - migration" as "NEW pg Downsell - migration",
            pgn."Up Sell - migration" as "NEW pg Upsell - migration",
            pgn."PG_pos_migration",
            pgn."PG_neg_migration",
            abs(coalesce(cn.arr_change_ccfx,0)) - abs(coalesce(pgn.arr_change_ccfx,0)) as diff_cb_pg_new,
            abs(coalesce(cn."cb_Pos_migration",0)) - abs(coalesce(pgn."PG_pos_migration",0)) as diff_positive_mig,
            abs(coalesce(cn."cb_Neg_migration",0)) - abs(coalesce(pgn."PG_neg_migration",0)) as diff_negative_mig
        from mcid_snap ms
        left join cb_new_total cn
        on ms.mcid = cn.mcid
        and ms.snapshot_date = cn.snapshot_date
        left join pg_new_total pgn
        on ms.mcid = pgn.mcid
        and ms.snapshot_date = pgn.snapshot_date
        order by ms.mcid,ms.snapshot_date
    )
select *
from (
    select
        *,
        case when diff_cb_pg_new > 0 AND abs(diff_cb_pg_new) not between 0 and 1 then 'Wrong' else 'Right' end as total_level_flag,
        case when diff_positive_mig > 0 and abs(diff_positive_mig) not between 0 and 1 then 'Wrong' else 'Right' end as postive_migration_flag,
        case when diff_negative_mig > 0 and abs(diff_negative_mig) not between 0 and 1 then 'Wrong' else 'Right' end as negative_migration_flag,
        case when (abs(coalesce("NEW cust Cross-sell - migration",0)) = abs(coalesce("NEW pg Cross-sell - migration",0))) AND (abs(abs(coalesce("NEW cust Cross-sell - migration",0)) - abs(coalesce("NEW pg Cross-sell - migration",0))) between 0 and 1) then 'OK'
             when abs(coalesce("NEW cust Cross-sell - migration",0)) > abs(coalesce("NEW pg Cross-sell - migration",0)) then 'Cross-sell migration greater in CB'
             else 'OK' end as "Cross-sell migration flag",
        case when (abs(coalesce("NEW cust Upsell - migration",0)) = abs(coalesce("NEW pg Upsell - migration",0))) AND (abs(abs(coalesce("NEW cust Upsell - migration",0)) - abs(coalesce("NEW pg Upsell - migration",0))) between 0 and 1) then 'OK'
             when abs(coalesce("NEW cust Upsell - migration",0)) > abs(coalesce("NEW pg Upsell - migration",0)) then 'Upsell migration greater in CB'
             else 'OK' end as "Upsell migration flag",
        case when (abs(coalesce("NEW cust Downsell - migration",0)) = abs(coalesce("NEW pg Downsell - migration",0))) AND (abs(abs(coalesce("NEW cust Downsell - migration",0)) - abs(coalesce("NEW pg Downsell - migration",0))) between 0 and 1) then 'OK'
             when abs(coalesce("NEW cust Downsell - migration",0)) > abs(coalesce("NEW pg Downsell - migration",0)) then 'Downsell migration greater in CB'
             else 'OK' end as "Downsell migratin flag",
        case when (abs(coalesce("NEW cust Downgrade - migration",0)) = abs(coalesce("NEW pg Downgrade - migration",0))) AND (abs(abs(coalesce("NEW cust Downgrade - migration",0)) - abs(coalesce("NEW pg Downgrade - migration",0))) between 0 and 1) then 'OK'
             when abs(coalesce("NEW cust Downgrade - migration",0)) > abs(coalesce("NEW pg Downgrade - migration",0)) then 'Cross-sell migration greater in CB'
             else 'OK' end as "Downgrade migration flag"
    from final
    order by abs(diff_cb_pg_new) desc
     ) as a
;

---- Customer Bridge level
with cb_new_total as
    (
        select
            DATE(TO_DATE(evaluation_period, 'YYYY"M"MM')+ interval '1 month - 1 day') as snapshot_date,
            mcid,
            sum(customer_arr_change_ccfx) as arr_change_ccfx,
            sum(customer_arr_change_lcu) as arr_change_lcu,
            sum(customer_arr_change_ccfx) filter ( where customer_bridge IN ('Cross-sell - migration') ) as "Cross-sell - migration",
            sum(customer_arr_change_ccfx) filter ( where customer_bridge IN ('Downgrade - migration') ) as "Downgrade - migration",
            sum(customer_arr_change_ccfx) filter ( where customer_bridge IN ('Downsell - migration') ) as "Downsell - migration",
            sum(customer_arr_change_ccfx) filter ( where customer_bridge IN ('Up Sell - migration') ) as "Up Sell - migration",
            sum(customer_arr_change_ccfx) filter ( where customer_arr_change_ccfx > 0 ) as "cb_Pos_migration",
            sum(customer_arr_change_ccfx) filter ( where customer_arr_change_ccfx < 0 ) as "cb_Neg_migration"
        from sandbox.sst_customer_bridge_rollover_cm
        where customer_bridge ilike '%migr%'
        group by 1,2
    )
, pg_new_total as
    (
        select
            DATE(TO_DATE(evaluation_period, 'YYYY"M"MM')+ interval '1 month - 1 day') as snapshot_date,
            mcid,
            sum(product_arr_change_ccfx) as arr_change_ccfx,
            sum(product_arr_change_lcu) as arr_change_lcu,
            sum(product_arr_change_ccfx) filter ( where product_bridge IN ('Cross-sell - migration') ) as "Cross-sell - migration",
            sum(product_arr_change_ccfx) filter ( where product_bridge IN ('Downgrade - migration') ) as "Downgrade - migration",
            sum(product_arr_change_ccfx) filter ( where product_bridge IN ('Downsell - migration') ) as "Downsell - migration",
            sum(product_arr_change_ccfx) filter ( where product_bridge IN ('Up Sell - migration') ) as "Up Sell - migration",
            sum(product_arr_change_ccfx) filter ( where product_arr_change_ccfx > 0  ) as "PG_pos_migration",
            sum(product_arr_change_ccfx) filter (where product_arr_change_ccfx < 0 ) as "PG_neg_migration"
        from sandbox.sst_product_solution_bridge_rollup_cm_cloud --sandbox.sst_product_group_bridge
        where product_bridge ilike '%migr%'
        group by 1,2
    )
, mcid_snap as
    (
        select
            mcid,
            snapshot_date
        from cb_new_total
        union all
        select
            mcid,
            snapshot_date
        from pg_new_total
    )
, final AS
    (
        select
            distinct on (ms.mcid,ms.snapshot_date)
            ms.snapshot_date,
            ms.mcid,
            cn.arr_change_ccfx as customer_arr_change_ccfx_new,
            cn."Cross-sell - migration" as "NEW cust Cross-sell - migration",
            cn."Downgrade - migration" as "NEW cust Downgrade - migration",
            cn."Downsell - migration" as "NEW cust Downsell - migration",
            cn."Up Sell - migration" as "NEW cust Upsell - migration",
            cn."cb_Pos_migration",
            cn."cb_Neg_migration",
            pgn.arr_change_ccfx as product_arr_change_ccfx_new,
            pgn."Cross-sell - migration" as "NEW pg Cross-sell - migration",
            pgn."Downgrade - migration" as "NEW pg Downgrade - migration",
            pgn."Downsell - migration" as "NEW pg Downsell - migration",
            pgn."Up Sell - migration" as "NEW pg Upsell - migration",
            pgn."PG_pos_migration",
            pgn."PG_neg_migration",
            abs(coalesce(cn.arr_change_ccfx,0)) - abs(coalesce(pgn.arr_change_ccfx,0)) as diff_cb_pg_new,
            abs(coalesce(cn."cb_Pos_migration",0)) - abs(coalesce(pgn."PG_pos_migration",0)) as diff_positive_mig,
            abs(coalesce(cn."cb_Neg_migration",0)) - abs(coalesce(pgn."PG_neg_migration",0)) as diff_negative_mig
        from mcid_snap ms
        left join cb_new_total cn
        on ms.mcid = cn.mcid
        and ms.snapshot_date = cn.snapshot_date
        left join pg_new_total pgn
        on ms.mcid = pgn.mcid
        and ms.snapshot_date = pgn.snapshot_date
        order by ms.mcid,ms.snapshot_date
    )
select
    *,
    case when diff_cb_pg_new > 0 AND abs(diff_cb_pg_new) not between 0 and 1 then 'Wrong' else 'Right' end as total_level_flag,
    case when diff_positive_mig > 0 and abs(diff_positive_mig) not between 0 and 1 then 'Wrong' else 'Right' end as postive_migration_flag,
    case when diff_negative_mig > 0 and abs(diff_negative_mig) not between 0 and 1 then 'Wrong' else 'Right' end as negative_migration_flag,
    case when (abs(coalesce("NEW cust Cross-sell - migration",0)) = abs(coalesce("NEW pg Cross-sell - migration",0))) AND (abs(abs(coalesce("NEW cust Cross-sell - migration",0)) - abs(coalesce("NEW pg Cross-sell - migration",0))) between 0 and 1) then 'OK'
         when abs(coalesce("NEW cust Cross-sell - migration",0)) > abs(coalesce("NEW pg Cross-sell - migration",0)) then 'Cross-sell migration greater in CB'
         else 'OK' end as "Cross-sell migration flag",
    case when (abs(coalesce("NEW cust Upsell - migration",0)) = abs(coalesce("NEW pg Upsell - migration",0))) AND (abs(abs(coalesce("NEW cust Upsell - migration",0)) - abs(coalesce("NEW pg Upsell - migration",0))) between 0 and 1) then 'OK'
         when abs(coalesce("NEW cust Upsell - migration",0)) > abs(coalesce("NEW pg Upsell - migration",0)) then 'Upsell migration greater in CB'
         else 'OK' end as "Upsell migration flag",
    case when (abs(coalesce("NEW cust Downsell - migration",0)) = abs(coalesce("NEW pg Downsell - migration",0))) AND (abs(abs(coalesce("NEW cust Downsell - migration",0)) - abs(coalesce("NEW pg Downsell - migration",0))) between 0 and 1) then 'OK'
         when abs(coalesce("NEW cust Downsell - migration",0)) > abs(coalesce("NEW pg Downsell - migration",0)) then 'Downsell migration greater in CB'
         else 'OK' end as "Downsell migratin flag",
    case when (abs(coalesce("NEW cust Downgrade - migration",0)) = abs(coalesce("NEW pg Downgrade - migration",0))) AND (abs(abs(coalesce("NEW cust Downgrade - migration",0)) - abs(coalesce("NEW pg Downgrade - migration",0))) between 0 and 1) then 'OK'
         when abs(coalesce("NEW cust Downgrade - migration",0)) > abs(coalesce("NEW pg Downgrade - migration",0)) then 'Cross-sell migration greater in CB'
         else 'OK' end as "Downgrade migration flag"
from final
order by abs(diff_cb_pg_new) desc;



670920c7-185f-e411-9afb-0050568d2da8
2022M06
-------------------
with cb_new_total as
    (
        select
            DATE(TO_DATE(evaluation_period, 'YYYY"M"MM')+ interval '1 month - 1 day') as snapshot_date,
            mcid,
            sum(customer_arr_change_ccfx) as arr_change_ccfx,
            sum(customer_arr_change_lcu) as arr_change_lcu,
            sum(customer_arr_change_ccfx) filter ( where customer_bridge IN ('Cross-sell - migration') ) as "Cross-sell - migration",
            sum(customer_arr_change_ccfx) filter ( where customer_bridge IN ('Downgrade - migration') ) as "Downgrade - migration",
            sum(customer_arr_change_ccfx) filter ( where customer_bridge IN ('Downsell - migration') ) as "Downsell - migration",
            sum(customer_arr_change_ccfx) filter ( where customer_bridge IN ('Up Sell - migration') ) as "Up Sell - migration",
            sum(customer_arr_change_ccfx) filter ( where customer_arr_change_ccfx > 0 ) as "cb_Pos_migration",
            sum(customer_arr_change_ccfx) filter ( where customer_arr_change_ccfx < 0 ) as "cb_Neg_migration"
        from sandbox.sst_customer_bridge_rollover_cm
        where customer_bridge ilike '%migr%'
        group by 1,2
    )
, pg_new_total as
    (
        select
            DATE(TO_DATE(evaluation_period, 'YYYY"M"MM')+ interval '1 month - 1 day') as snapshot_date,
            mcid,
            sum(product_arr_change_ccfx) as arr_change_ccfx,
            sum(product_arr_change_lcu) as arr_change_lcu,
            sum(product_arr_change_ccfx) filter ( where product_bridge IN ('Cross-sell - migration') ) as "Cross-sell - migration",
            sum(product_arr_change_ccfx) filter ( where product_bridge IN ('Downgrade - migration') ) as "Downgrade - migration",
            sum(product_arr_change_ccfx) filter ( where product_bridge IN ('Downsell - migration') ) as "Downsell - migration",
            sum(product_arr_change_ccfx) filter ( where product_bridge IN ('Up Sell - migration') ) as "Up Sell - migration",
            sum(product_arr_change_ccfx) filter ( where product_arr_change_ccfx > 0  ) as "PG_pos_migration",
            sum(product_arr_change_ccfx) filter (where product_arr_change_ccfx < 0 ) as "PG_neg_migration"
        from sandbox.sst_product_group_bridge
        where product_bridge ilike '%migr%'
        group by 1,2
    )
, mcid_snap as
    (
        select
            mcid,
            snapshot_date
        from cb_new_total
        union all
        select
            mcid,
            snapshot_date
        from pg_new_total
    )
, final AS
    (
        select
            distinct on (ms.mcid,ms.snapshot_date)
            ms.snapshot_date,
            ms.mcid,
            cn.arr_change_ccfx as customer_arr_change_ccfx_new,
            cn."Cross-sell - migration" as "NEW cust Cross-sell - migration",
            cn."Downgrade - migration" as "NEW cust Downgrade - migration",
            cn."Downsell - migration" as "NEW cust Downsell - migration",
            cn."Up Sell - migration" as "NEW cust Upsell - migration",
            cn."cb_Pos_migration",
            cn."cb_Neg_migration",
            pgn.arr_change_ccfx as product_arr_change_ccfx_new,
            pgn."Cross-sell - migration" as "NEW pg Cross-sell - migration",
            pgn."Downgrade - migration" as "NEW pg Downgrade - migration",
            pgn."Downsell - migration" as "NEW pg Downsell - migration",
            pgn."Up Sell - migration" as "NEW pg Upsell - migration",
            pgn."PG_pos_migration",
            pgn."PG_neg_migration",
            abs(coalesce(cn.arr_change_ccfx,0)) - abs(coalesce(pgn.arr_change_ccfx,0)) as diff_cb_pg_new,
            abs(coalesce(cn."cb_Pos_migration",0)) - abs(coalesce(pgn."PG_pos_migration",0)) as diff_positive_mig,
            abs(coalesce(cn."cb_Neg_migration",0)) - abs(coalesce(pgn."PG_neg_migration",0)) as diff_negative_mig
        from mcid_snap ms
        left join cb_new_total cn
        on ms.mcid = cn.mcid
        and ms.snapshot_date = cn.snapshot_date
        left join pg_new_total pgn
        on ms.mcid = pgn.mcid
        and ms.snapshot_date = pgn.snapshot_date
        order by ms.mcid,ms.snapshot_date
    )
select
    *,
    case when diff_cb_pg_new > 0 AND abs(diff_cb_pg_new) not between 0 and 1 then 'Wrong' else 'Right' end as total_level_flag,
    case when diff_positive_mig > 0 and abs(diff_positive_mig) not between 0 and 1 then 'Wrong' else 'Right' end as postive_migration_flag,
    case when diff_negative_mig > 0 and abs(diff_negative_mig) not between 0 and 1 then 'Wrong' else 'Right' end as negative_migration_flag,
    case when (abs(coalesce("NEW cust Cross-sell - migration",0)) = abs(coalesce("NEW pg Cross-sell - migration",0))) AND (abs(abs(coalesce("NEW cust Cross-sell - migration",0)) - abs(coalesce("NEW pg Cross-sell - migration",0))) between 0 and 1) then 'OK'
         when abs(coalesce("NEW cust Cross-sell - migration",0)) > abs(coalesce("NEW pg Cross-sell - migration",0)) then 'Cross-sell migration greater in CB'
         else 'OK' end as "Cross-sell migration flag",
    case when (abs(coalesce("NEW cust Upsell - migration",0)) = abs(coalesce("NEW pg Upsell - migration",0))) AND (abs(abs(coalesce("NEW cust Upsell - migration",0)) - abs(coalesce("NEW pg Upsell - migration",0))) between 0 and 1) then 'OK'
         when abs(coalesce("NEW cust Upsell - migration",0)) > abs(coalesce("NEW pg Upsell - migration",0)) then 'Upsell migration greater in CB'
         else 'OK' end as "Upsell migration flag",
    case when (abs(coalesce("NEW cust Downsell - migration",0)) = abs(coalesce("NEW pg Downsell - migration",0))) AND (abs(abs(coalesce("NEW cust Downsell - migration",0)) - abs(coalesce("NEW pg Downsell - migration",0))) between 0 and 1) then 'OK'
         when abs(coalesce("NEW cust Downsell - migration",0)) > abs(coalesce("NEW pg Downsell - migration",0)) then 'Downsell migration greater in CB'
         else 'OK' end as "Downsell migratin flag",
    case when (abs(coalesce("NEW cust Downgrade - migration",0)) = abs(coalesce("NEW pg Downgrade - migration",0))) AND (abs(abs(coalesce("NEW cust Downgrade - migration",0)) - abs(coalesce("NEW pg Downgrade - migration",0))) between 0 and 1) then 'OK'
         when abs(coalesce("NEW cust Downgrade - migration",0)) > abs(coalesce("NEW pg Downgrade - migration",0)) then 'Cross-sell migration greater in CB'
         else 'OK' end as "Downgrade migration flag"
from final
order by abs(diff_cb_pg_new) desc;



--- customer without CM
select
    evaluation_period ,
    mcid ,
    prior_period_customer_arr_usd_ccfx,
    current_period_customer_arr_usd_ccfx,
    customer_arr_change_ccfx,
    customer_bridge,
    pathways
from ufdm_archive.sst_customer_bridge_core_lcoked_17122024_0238
where mcid = 'c03b1919-67c7-e411-9afb-0050568d2da8'
    and evaluation_period= '2022M07';

-- customer CM test
select
    evaluation_period ,
    mcid ,
    prior_period_customer_arr_usd_ccfx,
    current_period_customer_arr_usd_ccfx,
    customer_arr_change_ccfx,
    customer_bridge,
    pathways
from sandbox.sst_customer_bridge_rollover_cm_core
where mcid = 'c03b1919-67c7-e411-9afb-0050568d2da8'
    and evaluation_period= '2022M07';

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
from ufdm_archive.sst_product_group_churn_mig_lcoked_17122024_0238
where 1 = 1
and mcid = 'c03b1919-67c7-e411-9afb-0050568d2da8'
    and evaluation_period= '2022M07';