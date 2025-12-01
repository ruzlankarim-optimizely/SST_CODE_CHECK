/*///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                    ##### COHORT DETECTION for new migration pathway implementation  #####                             */


--- for customer bridge

with before as (
    select
        DATE(TO_DATE(evaluation_period, 'YYYY"M"MM') + interval '1 month - 1 day') as snapshot_date,
        mcid,
        customer_bridge,
        sum(customer_arr_change_ccfx) as arr_before,
        sum(customer_arr_change_lcu) as lcu_before
    from ufdm_archive.sst_customer_bridge_churn_mig_lcoked_18032025_0244
    where 1=1
--      and customer_bridge ilike '%migr%'
      and customer_arr_change_ccfx <> 0
    group by 1,2,3
),
after as (
    select
        DATE(TO_DATE(evaluation_period, 'YYYY"M"MM') + interval '1 month - 1 day') as snapshot_date,
        mcid,
        customer_bridge,
        sum(customer_arr_change_ccfx) as arr_after,
        sum(customer_arr_change_lcu) as lcu_after
    from sandbox.sst_customer_bridge_rollover_cm
    where 1=1
--      and customer_bridge ilike '%migr%'
      and customer_arr_change_ccfx <> 0
    group by 1,2,3
),
combo as (
    select distinct snapshot_date, mcid, customer_bridge
    from before
    union all
    select distinct snapshot_date, mcid, customer_bridge
    from after
),
base as (
    select
        distinct on (c.snapshot_date, c.mcid, c.customer_bridge)
        c.snapshot_date,
        c.mcid,
        c.customer_bridge,
        coalesce(b.arr_before, 0) as arr_before,
        coalesce(a.arr_after, 0) as arr_after,
        round(coalesce(a.arr_after, 0) - coalesce(b.arr_before, 0)) as diff_arr,
        coalesce(b.lcu_before, 0) as lcu_before,
        coalesce(a.lcu_after, 0) as lcu_after,
        round(coalesce(a.lcu_after, 0) - coalesce(b.lcu_before, 0)) as diff_lcu,
        case
            when c.customer_bridge in ('Up Sell - migration','Cross-sell - migration') then 'Positive Migration'
            when c.customer_bridge in ('Downgrade - migration','Downsell - migration') then 'Negative Migration'
        end as migration_type
    from combo c
    left join before b
      on c.snapshot_date = b.snapshot_date
     and c.mcid = b.mcid
     and c.customer_bridge = b.customer_bridge
    left join after a
      on c.snapshot_date = a.snapshot_date
     and c.mcid = a.mcid
     and c.customer_bridge = a.customer_bridge
    order by c.snapshot_date, c.mcid, c.customer_bridge, abs(round(coalesce(a.arr_after, 0) - coalesce(b.arr_before, 0))) desc
),
sst_with_new_mig_from_to as (
    select
        snapshot_date,
        mcid,
        reference_number,
        sku,
        temp_product_group_li,
        temp_product_solution_li,
        a.migration_from as sst_mig_from,
        b."Mig From Name" as new_mig_from,
        a.migration_to as sst_mig_to,
        b."Mig to Name" as new_mig_to,
        updated_product_group,
        new_product_solution,
        arr,
        baseline_arr_local_currency,
        base_currency
    from ufdm.sst as a
    left join sandbox_pd.tmjs_24032025 as b
      on trim(lower(a.sku)) = trim(lower(b."Product Code"))
),
cdp_leg as (
    select distinct
        mcid,
        snapshot_date
    from sst_with_new_mig_from_to
    where sku = 'CLEN2-CDP-LEGACY'
),
all_sku_mapping as (
    select
        distinct a.sku,
        b."Product Code",
        a.migration_from as sst_mig_from,
        a.migration_to as sst_mig_to,
        b."Mig From Name" as tmjs_mig_from,
        b."Mig to Name" as tmjs_mig_to
    from ufdm.sst as a
    full join sandbox_pd.tmjs_24032025 as b
    on trim(lower(a.sku)) = trim(lower(b."Product Code"))
),
skus_lost_mig_from as (
    select
        mcid,
        snapshot_date,
        string_agg(distinct sku,', ') as sku_list
    from sst_with_new_mig_from_to
    where sku in (select distinct sku from all_sku_mapping where all_sku_mapping.sst_mig_from is not null and tmjs_mig_from is null)
    group by 1,2
),
skus_gained_mig_from as (
    select
        mcid,
        snapshot_date,
        string_agg(distinct sku,', ') as sku_list
    from sst_with_new_mig_from_to
    where sku in (select distinct sku from all_sku_mapping where all_sku_mapping.sst_mig_from is null and tmjs_mig_from is not null)
    group by 1,2
),
skus_lost_mig_to as (
    select mcid,
           snapshot_date,
           string_agg(distinct sku,', ') as sku_list
    from sst_with_new_mig_from_to
    where sku in (select distinct sku from all_sku_mapping where all_sku_mapping.sst_mig_to is not null and tmjs_mig_to is null)
    group by 1,2
),
skus_gained_mig_to as (
    select mcid,
           snapshot_date,
           string_agg(distinct sku,', ') as sku_list
    from sst_with_new_mig_from_to
    where sku in (select distinct sku from all_sku_mapping where all_sku_mapping.sst_mig_to is null and tmjs_mig_to is not null)
    group by 1,2
),
rollup_bridge_change as (
    select distinct a.mcid, a.snapshot_date
    from (
        select mcid, DATE(TO_DATE(evaluation_period, 'YYYY"M"MM') + interval '1 month - 1 day') as snapshot_date
        from ufdm_archive.sst_product_group_churn_mig_lcoked_18032025_0244
        where product_bridge ilike '%migr%'
    ) a
    inner join (
        select mcid, DATE(TO_DATE(evaluation_period, 'YYYY"M"MM') + interval '1 month - 1 day') as snapshot_date
        from sandbox.sst_product_group_bridge
        where product_bridge ilike '%migr%'
    ) b
    on a.mcid = b.mcid and a.snapshot_date = b.snapshot_date
    where not exists (
        select 1
        from sandbox.sst_product_group_bridge_cloud c
        where c.product_bridge ilike '%migr%'
        and c.mcid = a.mcid
        and DATE(TO_DATE(c.evaluation_period, 'YYYY"M"MM') + interval '1 month - 1 day') = a.snapshot_date
    )
    and not exists (
        select 1
        from sandbox.sst_product_solution_bridge_rollup_cm_cloud d
        where d.customer_bridge ilike '%migr%'
        and d.mcid = a.mcid
        and DATE(TO_DATE(d.evaluation_period, 'YYYY"M"MM') + interval '1 month - 1 day') = a.snapshot_date
    )
    and exists (
        select 1
        from ufdm_archive.sst_customer_bridge_churn_mig_lcoked_18032025_0244 e
        where e.customer_bridge ilike '%migr%'
        and e.mcid = a.mcid
        and DATE(TO_DATE(e.evaluation_period, 'YYYY"M"MM') + interval '1 month - 1 day') = a.snapshot_date
    )
    and not exists (
        select 1
        from sandbox.sst_customer_bridge_rollover_cm f
        where f.customer_bridge ilike '%migr%'
        and f.mcid = a.mcid
        and DATE(TO_DATE(f.evaluation_period, 'YYYY"M"MM') + interval '1 month - 1 day') = a.snapshot_date
    )
),
base_with_flag as (
    select
        b.*,
        case
            when abs(b.diff_arr) between 0 and 1 then
                'Matched'
            when abs(b.diff_arr) not between 0 and 1 and cl.mcid is not null then
                'CLEN2-CDP-LEGACY : In prior pathway, not in current'
            when abs(b.diff_arr) not between 0 and 1 and lmf.mcid is not null then
                'Migration changing due to pathway changes'
            when abs(b.diff_arr) not between 0 and 1 and gmf.mcid is not null then
                'Migration changing due to pathway changes'
            when abs(b.diff_arr) not between 0 and 1 and lmt.mcid is not null then
                'Migration changing due to pathway changes'
            when abs(b.diff_arr) not between 0 and 1 and gmt.mcid is not null then
                'Migration changing due to pathway changes'
            when abs(b.diff_arr) not between 0 and 1 and rbc.mcid is not null then
                'Rollup Change: Non split to split bridge'
        end as cohort_flag,
        case
            when abs(b.diff_arr) not between 0 and 1 and cl.mcid is not null then
                'CLEN2-CDP-LEGACY'
            when abs(b.diff_arr) not between 0 and 1 and lmf.mcid is not null then
                lmf.sku_list
            when abs(b.diff_arr) not between 0 and 1 and gmf.mcid is not null then
                gmf.sku_list
            when abs(b.diff_arr) not between 0 and 1 and lmt.mcid is not null then
                lmt.sku_list
            when abs(b.diff_arr) not between 0 and 1 and gmt.mcid is not null then
                gmt.sku_list
        end as sku_list,
        abs(b.diff_arr) as abs_diff_arr
    from base b
    left join (
        -- Find mcid, snapshot_date or snapshot_date - 1 month
        select distinct mcid, snapshot_date
        from cdp_leg
        union
        select distinct mcid,DATE_TRUNC('month', snapshot_date + interval '1 month') + interval '1 month - 1 day' -- allow 1 month earlier matching
        from cdp_leg
    ) cl
      on b.mcid = cl.mcid
     and b.snapshot_date = cl.snapshot_date
    left join (
        select distinct mcid, snapshot_date,sku_list
        from skus_lost_mig_from
        union
        select distinct mcid,DATE_TRUNC('month', snapshot_date + interval '1 month') + interval '1 month - 1 day',sku_list -- allow 1 month earlier matching
        from skus_lost_mig_from
        ) lmf
      on b.mcid = lmf.mcid
     and b.snapshot_date = lmf.snapshot_date
    left join (
        select distinct mcid,snapshot_date,sku_list
        from skus_gained_mig_from
        union
        select distinct mcid,DATE_TRUNC('month', snapshot_date + interval '1 month') + interval '1 month - 1 day',sku_list
        from skus_gained_mig_from
    ) gmf
     on b.mcid = gmf.mcid
    and b.snapshot_date = gmf.snapshot_date
    left join (
        select
            distinct mcid,snapshot_date,sku_list
        from skus_lost_mig_to
        union
        select distinct mcid,DATE_TRUNC('month', snapshot_date + interval '1 month') + interval '1 month - 1 day', sku_list
        from skus_lost_mig_to
    ) lmt
     on b.mcid = lmt.mcid
    and b.snapshot_date = lmt.snapshot_date
    left join (
        select
            distinct mcid,snapshot_date,sku_list
        from skus_gained_mig_to
        union
        select
            distinct mcid,DATE_TRUNC('month', snapshot_date + interval '1 month') + interval '1 month - 1 day', sku_list
        from skus_gained_mig_to
    ) gmt
     on b.mcid = gmt.mcid
    and b.snapshot_date = gmt.snapshot_date
    left join (
        select
            distinct mcid, snapshot_date
        from rollup_bridge_change
    ) rbc
  on b.mcid = rbc.mcid and b.snapshot_date = rbc.snapshot_date

)
select
    distinct on (mcid,snapshot_date,customer_bridge)
    *
--     count(mcid) over(partition by snapshot_date,mcid,customer_bridge)
from base_with_flag
order by mcid,snapshot_date,customer_bridge;