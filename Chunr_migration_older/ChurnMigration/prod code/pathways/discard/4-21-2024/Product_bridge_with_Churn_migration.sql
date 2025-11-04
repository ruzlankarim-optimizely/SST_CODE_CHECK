
create temp table sst_product_bridge_product_group as select * from ufdm.sst_product_bridge_product_group limit 1 ;
truncate table sst_product_bridge_product_group ;

CREATE OR REPLACE FUNCTION public.sp_populate_sst_product_bridge_product_group('2023M06' text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$

BEGIN

--     create temp table sst_product_bridge_product_group as
-- select *
-- from ufdm.sst_product_bridge_product_group
-- limit 1;
-- truncate table sst_product_bridge_product_group;
DELETE from sst_product_bridge_product_group
where evaluation_period = '2023M06';
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--SST product Bridge
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists prior_period_customer_arr_tmp;
create temp table prior_period_customer_arr_tmp as
SELECT snapshot_date,
    a.mcid as master_customer_id,
    updated_product_group as product_family,
    a.base_currency as baseline_currency,
    max(coalesce(a.end_name, a.parent_name)) as end_customer,
    sum(arr) AS arr_usd_ccfx,
    sum(baseline_arr_local_currency) AS arr_lcu
FROM ufdm.sst a
WHERE 1 = 1
    AND snapshot_date = (
        SELECT prior_period
        from ufdm_grey.periods
        WHERE evaluation_period = '2023M06'
    )
    AND a.overage_flag IS DISTINCT
FROM 'Y'
GROUP BY 1,
    2,
    3,
    4;
drop table if exists current_period_customer_arr_tmp;
create temp table current_period_customer_arr_tmp as
SELECT snapshot_date,
    a.mcid as master_customer_id,
    updated_product_group as product_family,
    a.base_currency as baseline_currency,
    max(coalesce(a.end_name, a.parent_name)) as end_customer,
    sum(arr) AS arr_usd_ccfx,
    sum(baseline_arr_local_currency) AS arr_lcu
FROM ufdm.sst a
WHERE 1 = 1
    AND snapshot_date = (
        SELECT current_period
        from ufdm_grey.periods
        WHERE evaluation_period = '2023M06'
    )
    AND a.overage_flag IS DISTINCT
FROM 'Y'
GROUP BY 1,
    2,
    3,
    4;
drop table if exists customer_level_arr_tmp;
create temp table customer_level_arr_tmp as
SELECT c1.master_customer_id AS current_cust_id,
    c2.master_customer_id AS prior_cust_id,
    c1.end_customer as current_end_customer,
    c2.end_customer as prior_end_customer,
    c2.snapshot_date AS prior_period,
    c1.snapshot_date AS current_period,
    c1.baseline_currency as current_baseline_currency,
    c2.baseline_currency as prior_baseline_currency,
    COALESCE(c1.baseline_currency, c2.baseline_currency) AS baseline_currency,
    c2.product_family AS prior_product_family,
    c1.product_family AS current_product_family,
    coalesce(c1.arr_usd_ccfx, 0) AS current_arr_usd_ccfx,
    coalesce(c2.arr_usd_ccfx, 0) AS prior_arr_usd_ccfx,
    coalesce(c1.arr_lcu, 0) AS current_arr_lcu,
    coalesce(c2.arr_lcu, 0) AS prior_arr_lcu --c3.arr AS prior2_arr, --WIP
FROM current_period_customer_arr_tmp c1
    FULL OUTER JOIN prior_period_customer_arr_tmp c2 ON c1.master_customer_id = c2.master_customer_id
    and c1.product_family = c2.product_family
    and c1.baseline_currency = c2.baseline_currency;
------------------------------------------
-- Evaluate
------------------------------------------
drop table if exists arr_product_bridge_tmp;
create temp table arr_product_bridge_tmp AS
SELECT per.evaluation_period,
    cla.prior_period,
    cla.current_period,
    cla.current_cust_id as current_master_customer_id,
    cla.prior_cust_id as prior_master_customer_id,
    coalesce(cla.current_cust_id, cla.prior_cust_id) as mcid,
    cla.current_product_family as current_product_family,
    cla.prior_product_family as prior_product_family,
    cla.current_end_customer,
    cla.prior_end_customer,
    cla.baseline_currency,
    round(
        (coalesce(cla.current_arr_usd_ccfx::numeric, 0)),
        2
    ) as current_arr_usd_ccfx,
    round((coalesce(cla.prior_arr_usd_ccfx::numeric, 0)), 2) as prior_arr_usd_ccfx,
    round(
        (
            coalesce(cla.current_arr_usd_ccfx::numeric, 0) - coalesce(cla.prior_arr_usd_ccfx::numeric, 0)
        ),
        2
    ) AS product_arr_change_ccfx,
    round((coalesce(cla.current_arr_lcu::numeric, 0)), 2) as current_arr_lcu,
    round((coalesce(cla.prior_arr_lcu::numeric, 0)), 2) as prior_arr_lcu,
    round(
        (
            coalesce(cla.current_arr_lcu::numeric, 0) - coalesce(cla.prior_arr_lcu::numeric, 0)
        ),
        2
    ) AS product_arr_change_lcu,
    CASE
        WHEN (
            (
                coalesce (cla.prior_arr_usd_ccfx, 0) = 0
                or coalesce (cla.prior_arr_usd_ccfx, 0) = 0.00
            ) --OR (cla.prior_arr_usd_ccfx = 0 and clp.prior_product_family_agg is null))
            AND cla.current_arr_usd_ccfx > 0
        ) THEN 'New'
        WHEN cla.current_arr_usd_ccfx - cla.prior_arr_usd_ccfx BETWEEN -1 and 1 THEN 'Flat'
        WHEN cla.current_arr_usd_ccfx - cla.prior_arr_usd_ccfx > 1 THEN 'Up Sell'
        WHEN cla.current_arr_usd_ccfx - cla.prior_arr_usd_ccfx < - 1
        AND cla.current_arr_usd_ccfx > 0 THEN 'Partial Churn' -- different products, lower ARR
        WHEN cla.prior_arr_usd_ccfx > 0
        AND (
            cla.current_arr_usd_ccfx = 0
            OR cla.current_arr_usd_ccfx IS NULL
        ) THEN 'Churn'
        ELSE 'N/A'
    END AS product_bridge
FROM customer_level_arr_tmp cla
    CROSS JOIN ufdm_grey.periods per
WHERE 1 = 1
    AND per.evaluation_period = '2023M06';
--Downgrade
update arr_product_bridge_tmp
set product_bridge = 'Downgrade'
where product_bridge = 'Partial Churn';
--Cross-sell
WITH PG_F_C AS (
    SELECT mcid,
        COUNT(distinct product_bridge) as product_family_count
    FROM arr_product_bridge_tmp
    WHERE current_arr_usd_ccfx > 0
        and evaluation_period = '2023M06'
    group by mcid,
        evaluation_period
)
UPDATE arr_product_bridge_tmp AS t
SET product_bridge = CASE
        WHEN pfc.product_family_count > 1 THEN 'Cross-sell'
        ELSE product_bridge
    END
FROM PG_F_C AS pfc
WHERE t.mcid = pfc.mcid
    AND t.product_bridge = 'New'
    and t.evaluation_period = '2023M06';
-------
--Downsell
WITH PG_F_C AS (
    SELECT mcid,
        COUNT(distinct product_bridge) as product_family_count
    FROM arr_product_bridge_tmp
    WHERE prior_arr_usd_ccfx > 0
        and evaluation_period = '2023M06'
    group by mcid,
        evaluation_period
) --  select * from PG_F_C
UPDATE arr_product_bridge_tmp AS t
SET product_bridge = CASE
        WHEN pfc.product_family_count > 1 THEN 'Downsell'
        ELSE product_bridge
    END
FROM PG_F_C AS pfc
WHERE t.mcid = pfc.mcid
    AND t.product_bridge = 'Churn'
    and t.evaluation_period = '2023M06';
--#############################################
--WIP/WINBACK
--#############################################
drop table if exists arr_new_products_tmp;
create temp table arr_new_products_tmp AS
select a.current_master_customer_id as mcid,
    a.current_product_family as product_family,
    a.current_period as snapshot_date,
    a.current_arr_usd_ccfx as arr_at_new,
    a.current_arr_lcu as arr_lcu_at_new,
    baseline_currency
from arr_product_bridge_tmp a
where product_bridge in ('New', 'Cross-sell');
--get most recent postivie arr for above new product which should have been churned
drop table if exists arr_churned_products_tmp;
create temp table arr_churned_products_tmp AS with temp as (
    select b.snapshot_date,
        b.mcid,
        b.updated_product_group as product_family,
        a.baseline_currency,
        a.snapshot_date as snapshot_date_at_new,
        sum(b.arr) as arr_at_churn,
        sum(b.baseline_arr_local_currency) as arr_lcu_at_churn,
        sum(a.arr_at_new) as arr_at_new,
        sum(a.arr_lcu_at_new) as arr_lcu_at_new,
        row_number() over (
            partition by b.mcid,
            b.updated_product_group
            order by b.snapshot_date desc
        ) as rnk
    from arr_new_products_tmp a
        join (
            select sb.mcid,
                sb.snapshot_date,
                sb.overage_flag,
                sb.updated_product_group,
                sb.base_currency,
                sum(arr) as arr,
                sum(baseline_arr_local_currency) as baseline_arr_local_currency
            from ufdm.sst sb
            group by 1,
                2,
                3,
                4,
                5
        ) b on a.mcid = b.mcid --and a.product_family = b.product_family
        and a.product_family = b.updated_product_group
        and a.baseline_currency = b.base_currency
    where b.snapshot_date < a.snapshot_date
        and b.overage_flag ilike '%N%'
        and b.arr > 0
    group by 1,
        2,
        3,
        4,
        5
)
select *,
(
        DATE_PART('year', snapshot_date_at_new::date) - DATE_PART('year', snapshot_date::date)
    ) * 12 + (
        DATE_PART('month', snapshot_date_at_new::date) - DATE_PART('month', snapshot_date::date)
    ) as months_diff,
case
        when arr_at_new > arr_at_churn then 'Upsell'
        else case
            when extract(
                day
                from snapshot_date_at_new::timestamp - (snapshot_date + INTERVAL '1 month')::date
            ) <= 90 then 'Winback ST'
            else 'Winback LT'
        end
    end as product_bridge_new,
    arr_at_new - arr_at_churn as arr_diff,
    arr_lcu_at_new - arr_lcu_at_churn as arr_lcu_diff,
    extract(
        day
        from snapshot_date_at_new::timestamp - (snapshot_date + INTERVAL '1 month')::date
    ) as days_diff,
    snapshot_date as churn_period
from temp
where rnk = 1
    and extract(
        day
        from snapshot_date_at_new::timestamp - (snapshot_date + INTERVAL '1 month')::date
    ) < 186;
-- create index if not exists nci_arr_churned_products_tmp_tmp_composite on arr_churned_products_tmp(mcid,product_family,baseline_currency,snapshot_date_at_new) include(arr_at_new, arr_at_churn);
INSERT INTO sst_product_bridge_product_group (
        evaluation_period,
        prior_period,
        current_period,
        current_end_customer,
        prior_end_customer,
        mcid,
        current_master_customer_id,
        Prior_master_customer_id,
        current_product_group,
        prior_product_group,
        --         "name",
        prior_period_product_arr_usd_ccfx,
        current_period_product_arr_usd_ccfx,
        product_arr_change_ccfx,
        prior_period_product_arr_lcu,
        current_period_product_arr_lcu,
        product_arr_change_lcu,
        product_bridge,
        Winback_period_days,
        Wip_Flag,
        price_increase_amount,
        subsidiary_entity_name,
        churn_period,
        currency_code
    )
SELECT a.evaluation_period,
    a.prior_period,
    a.current_period,
    a.current_end_customer,
    a.prior_end_customer,
    a.mcid,
    a.current_master_customer_id,
    a.Prior_master_customer_id,
    a.current_product_family,
    a.prior_product_family,
    --         "name",
    round(a.prior_arr_usd_ccfx::numeric, 2) AS prior_period_customer_arr_usd_ccfx,
    --round(a.current_arr_usd_ccfx::numeric,2) AS current_period_customer_arr_usd_ccfx,
    case
        when b.mcid is not null then case
            when b.arr_at_new > b.arr_at_churn then b.arr_at_churn
            else b.arr_at_new
        end --round(b.arr_at_churn::numeric,2)
        else round(a.current_arr_usd_ccfx::numeric, 2)
    end as current_period_customer_arr_usd_ccfx,
    case
        when b.mcid is not null then case
            when b.arr_at_new > b.arr_at_churn then b.arr_at_churn
            else b.arr_at_new
        end
        else a.product_arr_change_ccfx
    end as product_arr_change_ccfx,
    ------------------------lcu----------------------------
    round(a.prior_arr_lcu::numeric, 2) AS prior_period_product_arr_lcu,
    case
        when b.mcid is not null then case
            when b.arr_lcu_at_new > b.arr_lcu_at_churn then b.arr_lcu_at_churn
            else b.arr_lcu_at_new
        end --round(b.arr_lcu_at_churn::numeric,2)
        else round(a.current_arr_lcu::numeric, 2)
    end as current_period_product_arr_lcu,
    case
        when b.mcid is not null then case
            when b.arr_lcu_at_new > b.arr_lcu_at_churn then b.arr_lcu_at_churn
            else b.arr_lcu_at_new
        end
        else a.product_arr_change_lcu
    end as product_arr_change_lcu,
    case
        when b.mcid is not null then case
            when b.days_diff <= 90 then 'Winback ST'
            else 'Winback LT'
        end --b.product_bridge_new --'Winback' --/WIP
        else a.product_bridge
    end as product_bridge,
    b.days_diff as Winback_period_days,
    case
        when b.days_diff <= 90 then 'Y'
        else 'N'
    end as Wip_Flag,
    null::numeric as price_increase_amount,
    null::text as subsidiary_entity_name,
    b.churn_period,
    a.baseline_currency
FROM arr_product_bridge_tmp a
    left join arr_churned_products_tmp b on a.current_master_customer_id = b.mcid
    and a.current_product_family = b.product_family
    and a.baseline_currency = b.baseline_currency
    and a.current_period = b.snapshot_date_at_new
union all
SELECT a.evaluation_period,
    a.prior_period,
    a.current_period,
    a.current_end_customer,
    a.prior_end_customer,
    a.mcid,
    a.current_master_customer_id,
    a.Prior_master_customer_id,
    a.current_product_family,
    a.prior_product_family,
    --         "name",
    round(a.prior_arr_usd_ccfx::numeric, 2) AS prior_period_customer_arr_usd_ccfx,
    b.arr_diff as current_period_customer_arr_usd_ccfx,
    b.arr_diff,
    round(a.prior_arr_lcu::numeric, 2) AS prior_period_product_arr_lcu,
    b.arr_lcu_diff as current_period_product_arr_lcu,
    b.arr_lcu_diff,
    'Up Sell' as product_bridge,
    null as Winback_period_days,
    null as Wip_Flag,
    null::numeric as price_increase_amount,
    null::text as subsidiary_entity_name,
    null::date as churn_period,
    a.baseline_currency
FROM arr_product_bridge_tmp a
    join arr_churned_products_tmp b on a.current_master_customer_id = b.mcid
    and a.current_product_family = b.product_family
    and a.baseline_currency = b.baseline_currency
    and a.current_period = b.snapshot_date_at_new
where b.arr_at_new > b.arr_at_churn;
--    RAISE NOTICE 'Running customer bridge update on sst product bridge...';
--update customer bridge and subsidiary entity
update sst_product_bridge_product_group a
set customer_bridge = b.customer_bridge
from ufdm.sst_customer_bridge b
where 1 = 1
    and a.evaluation_period = b.evaluation_period
    and a.mcid = b.mcid
    and a.evaluation_period = '2023M06';
--    RAISE NOTICE 'Running subsidiary entity name insert on sst product bridge...';
drop table if exists sub_entity_tmp;
create temp table sub_entity_tmp as --update subsidiary_entity_name
with mcid_list as (
    select distinct mcid as master_customer_id
    from arr_product_bridge_tmp
    where evaluation_period = '2023M06'
),
total_arr as (
    select a.mcid as mcid,
        a.snapshot_date,
        a.subsidiary_entity_name,
        sum(a.arr) as total_arr
    from ufdm.sst a
        join mcid_list b on a.mcid = b.master_customer_id
        and a.snapshot_date in (
            SELECT prior_period
            from ufdm_grey.periods
            WHERE evaluation_period = '2023M06'
            union
            SELECT current_period
            from ufdm_grey.periods
            WHERE evaluation_period = '2023M06'
        )
    group by a.mcid,
        a.snapshot_date,
        a.subsidiary_entity_name
),
sub_entity as (
    select *,
        row_number () over (
            partition by mcid
            order by total_arr desc
        ) as rnk
    from total_arr
)
select *
from sub_entity
where rnk = 1;
--    RAISE NOTICE 'Running sub entity update on sst product bridge...';
create index nci_sub_entity_tmp_mcid on sub_entity_tmp(mcid);
update sst_product_bridge_product_group a
set subsidiary_entity_name = b.subsidiary_entity_name
from sub_entity_tmp b
where a.mcid = b.mcid
    and a.evaluation_period = '2023M06';
--    RAISE NOTICE 'Running Price Increase update on sst product bridge...';
--Price Increase updates
update sst_product_bridge_product_group
set product_bridge = 'CPI'
where product_bridge = 'Up Sell'
    and prior_period_product_arr_usd_ccfx > 0
    and (
        (
            product_arr_change_ccfx / prior_period_product_arr_usd_ccfx
        ) * 100
    )::numeric <= case
        when evaluation_period < '2023-01-01' then 5.5
        else 10.5
    end
    and evaluation_period = '2023M06';
-- Run Churn migration bridge code before all 
-- Make copy of base table 
DROP TABLE IF EXISTS sst_product_bridge_product_group_temp;
create temp table sst_product_bridge_product_group_temp as
select *
from sst_product_bridge_product_group
where evaluation_period = '2023M06';
--- take bridge values for particular time frame of  Churn migration bridge 
DROP TABLE IF EXISTS sst_product_bridge_pathways_cm_all;
create TEMP TABLE sst_product_bridge_pathways_cm_all as
select *
from ryzlan.sst_product_bridge_product_family_pathways_CM_all
where evaluation_period = '2023M06';
-- take churn migration classifiers code for particular time frame 
DROP TABLE IF EXISTS churn_migration_classifiers;
create temp table churn_migration_classifiers as
select *
from sandbox.churn_migration_classifiers
where evaluation_period = '2023M06';
Drop table if exists churn_migration_classifiers_pg;
Create temp table churn_migration_classifiers_pg as (
    WITH initial_table_4 as (
        SELECT it3.*,
            -- take this up a level
            case
                when it3."Movement Classification" is not null
                and it3.product_arr_change_ccfx > 0 then '+'
                when it3."Movement Classification" is not null
                and it3.product_arr_change_ccfx < 0 then '-'
                else null
            end as "Movement Type-PF",
            --type of migration movement
            --PG Information
            rt.product_arr_change_ccfx as pg_arr_change,
            rt.product_arr_change_lcu as pg_arr_change_lcu,
            rt.product_bridge as pg_bridge
        from churn_migration_classifiers it3
            left join sst_product_bridge_product_group_temp rt on it3.evaluation_period = rt.evaluation_period
            and coalesce(
                it3.prior_product_group,
                it3.current_product_group
            ) = coalesce(rt.current_product_group, rt.prior_product_group)
            and it3.mcid = rt.mcid
            and it3.currency_code = rt.currency_code
    ),
    initial_table_5 as (
        select *,
            --
            case
                when pg_arr_change > 0 then '+'
                when pg_arr_change < 0 then '-'
                else null
            end as "Movement Type-PG"
        from initial_table_4
    ),
    initial_table_6 as (
        select *,
            --What is the PG movement? + or is it -
            --if neg, look back at pf movements and sum all - migration movements
            case
                when "Movement Type-PG" = '-'
                and "Movement Type-PF" = '-' then sum(product_arr_change_ccfx) filter(
                    where "Movement Type-PF" = '-'
                ) over(
                    partition by mcid,
                    evaluation_period,
                    currency_code,
                    coalesce(current_product_group, prior_product_group)
                )
                when "Movement Type-PG" = '+'
                and "Movement Type-PF" = '+' then sum(product_arr_change_ccfx) filter(
                    where "Movement Type-PF" = '+'
                ) over(
                    partition by mcid,
                    evaluation_period,
                    currency_code,
                    coalesce(current_product_group, prior_product_group)
                )
                else null
            end as "Sum of Positive or Negative Movements-PG"
        from initial_table_5
    ),
    initial_table_7 as (
        select *,
            --if neg take the max (if pos take min) between the two PG movement and sum and tag the PG bridge movement as migration
            case
                when "Sum of Positive or Negative Movements-PG" is not null
                and "Movement Type-PG" = '-' then greatest(
                    pg_arr_change,
                    "Sum of Positive or Negative Movements-PG"
                )
                when "Sum of Positive or Negative Movements-PG" is not null
                and "Movement Type-PG" = '+' then least(
                    pg_arr_change,
                    "Sum of Positive or Negative Movements-PG"
                )
                else null
            end as "Min/Max PF Level movement" --Bring in the product solution columns as well -- to roll it up on the PS level
        from initial_table_6
    ),
    initial_table_8 as (
        select *,
            --if neg take the max (if pos take min) between the two PG movement and sum and tag the PG bridge movement as migration
            --            "Min/Max PF Level movement",
            case
                --Positive
                when "Movement Type-PG" = '+'
                and "Movement Type-PF" is not null
                and "Min/Max PF Level movement" >= pg_arr_change then "Min/Max PF Level movement"
                when "Movement Type-PG" = '+'
                and "Movement Type-PF" is not null
                and "Min/Max PF Level movement" < pg_arr_change then "Min/Max PF Level movement"
                when "Movement Type-PG" = '-'
                and "Movement Type-PF" is not null
                and "Min/Max PF Level movement" <= pg_arr_change then "Min/Max PF Level movement"
                when "Movement Type-PG" = '-'
                and "Movement Type-PF" is not null
                and "Min/Max PF Level movement" > pg_arr_change then "Min/Max PF Level movement"
            end as "PG Migration: Rolled Up Amount",
            case
                when "Movement Type-PG" = '+'
                and "Movement Type-PF" is not null
                and "Min/Max PF Level movement" < pg_arr_change then pg_arr_change - "Min/Max PF Level movement"
                when "Movement Type-PG" = '-'
                and "Movement Type-PF" is not null
                and "Min/Max PF Level movement" > pg_arr_change then pg_arr_change - "Min/Max PF Level movement"
                else null
            end as "PG Leftover: Rolled Up Amount" --Bring in the product solution columns as well -- to roll it up on the PS level
        from initial_table_7
    ),
    initial_table_9 as (
        select *,
            --if neg take the max (if pos take min) between the two PG movement and sum and tag the PG bridge movement as migration
            --            "Min/Max PF Level movement",
            --Positive
            --            "PG Migration: Rolled Up Amount",
            --            "PG Leftover: Rolled Up Amount",
            (
                pg_arr_change_lcu *(
                    "PG Migration: Rolled Up Amount" /case
                        when pg_arr_change = 0
                        or pg_arr_change is null then 1
                        else pg_arr_change
                    end
                )
            ) as "PG Migration: Rolled Up Amount LCU",
            (
                pg_arr_change_lcu * (
                    "PG Leftover: Rolled Up Amount" / case
                        when pg_arr_change = 0
                        or pg_arr_change is null then 1
                        else pg_arr_change
                    end
                )
            ) as "PG Leftover: Rolled Up Amount LCU",
            case
                when "PG Migration: Rolled Up Amount" is not null then "Movement Classification"
                else null
            end as "PG Migration: Classification",
            case
                when "PG Leftover: Rolled Up Amount" is not null then pg_bridge
                else null
            end as "PG Leftover: Classification"
        from initial_table_8
    )
    select *
    from initial_table_9
);

-- make sub tables to mod the base table
CREATE TABLE sandbox.churn_migration_test_pg AS
SELECT DISTINCT mcid,
    evaluation_period,
    currency_code,
    current_product_group,
    prior_product_group,
    pg_bridge,
    "PG Migration: Rolled Up Amount",
    "PG Leftover: Rolled Up Amount",
    "PG Migration: Rolled Up Amount LCU",
    "PG Leftover: Rolled Up Amount LCU",
    "PG Migration: Classification",
    "PG Leftover: Classification"
FROM churn_migration_classifiers_pg
WHERE mcid NOT IN ('-');

DROP TABLE IF EXISTS sandbox.PG_migration_default;
CREATE TABLE sandbox.PG_migration_default AS
SELECT a.*,
    "PG Migration: Rolled Up Amount",
    "PG Leftover: Rolled Up Amount",
    "PG Migration: Rolled Up Amount LCU",
    "PG Leftover: Rolled Up Amount LCU",
    pg_bridge,
    "PG Migration: Classification"
FROM sst_product_bridge_product_group_temp AS a
    JOIN sandbox.churn_migration_test_pg AS b ON a.mcid = b.mcid
    AND a.evaluation_period = b.evaluation_period
    AND a.currency_code = b.currency_code
    AND a.current_product_group = b.current_product_group
    AND a.prior_product_group = b.prior_product_group
    AND a.product_bridge = b.pg_bridge
WHERE lower("PG Migration: Classification") ILIKE ('%migration%')
    AND "PG Leftover: Rolled Up Amount" IS NULL;

DROP TABLE IF EXISTS sandbox.PG_migration_split;
CREATE TABLE sandbox.PG_migration_split AS
SELECT a.*,
    "PG Migration: Rolled Up Amount",
    "PG Leftover: Rolled Up Amount",
    "PG Migration: Rolled Up Amount LCU",
    "PG Leftover: Rolled Up Amount LCU",
    "PG Migration: Classification",
    "PG Leftover: Classification"
FROM sst_product_bridge_product_group_temp AS a
    JOIN sandbox.churn_migration_test_pg AS b ON a.mcid = b.mcid
    AND a.evaluation_period = b.evaluation_period
    AND a.currency_code = b.currency_code
    AND a.current_product_group = b.current_product_group
    AND a.prior_product_group = b.prior_product_group
    AND a.product_bridge = b.pg_bridge --AND round(a.product_arr_change_ccfx)  = round(b.pg_arr_change) 
WHERE "PG Migration: Classification" ILIKE ('%migration')
    AND "PG Leftover: Rolled Up Amount" IS NOT NULL;

DELETE FROM sst_product_bridge_product_group_temp AS a 
USING sandbox.PG_migration_default AS b
WHERE a.mcid = b.mcid
    AND a.evaluation_period = b.evaluation_period
    AND a.currency_code = b.currency_code
    AND a.current_product_group = b.current_product_group
    AND a.prior_product_group = b.prior_product_group
    AND a.product_bridge = b.product_bridge;

INSERT INTO sst_product_bridge_product_group_temp AS a (
        evaluation_period,
        prior_period,
        current_period,
        current_end_customer,
        prior_end_customer,
        mcid,
        current_master_customer_id,
        prior_master_customer_id,
        current_product_group,
        prior_product_group,
        currency_code,
        prior_period_product_arr_usd_ccfx,
        current_period_product_arr_usd_ccfx,
        product_arr_change_ccfx,
        prior_period_product_arr_lcu,
        current_period_product_arr_lcu,
        product_arr_change_lcu,
        product_bridge,
        winback_period_days,
        wip_flag,
        price_increase_amount,
        subsidiary_entity_name,
        churn_period,
        customer_bridge
    )
SELECT evaluation_period,
    prior_period,
    current_period,
    current_end_customer,
    prior_end_customer,
    mcid,
    current_master_customer_id,
    prior_master_customer_id,
    current_product_group,
    prior_product_group,
    currency_code,
    prior_period_product_arr_usd_ccfx,
    current_period_product_arr_usd_ccfx,
    product_arr_change_ccfx,
    prior_period_product_arr_lcu,
    current_period_product_arr_lcu,
    product_arr_change_lcu,
    --    product_bridge ,
    COALESCE("PG Migration: Classification", product_bridge),
    winback_period_days,
    wip_flag,
    price_increase_amount,
    subsidiary_entity_name,
    churn_period,
    customer_bridge
FROM sandbox.PG_migration_default AS b
WHERE mcid = b.mcid
    AND evaluation_period = b.evaluation_period
    AND currency_code = b.currency_code
    AND current_product_group = b.current_product_group
    AND prior_product_group = b.prior_product_group
    AND product_bridge = b.product_bridge;

DELETE FROM sst_product_bridge_product_group_temp AS a 
USING sandbox.PG_migration_split AS b
WHERE a.mcid = b.mcid
    AND a.evaluation_period = b.evaluation_period
    AND a.currency_code = b.currency_code
    AND a.current_product_group = b.current_product_group
    AND a.prior_product_group = b.prior_product_group
    AND a.product_bridge = b.product_bridge;

INSERT INTO sst_product_bridge_product_group_temp AS a (
        evaluation_period,
        prior_period,
        current_period,
        current_end_customer,
        prior_end_customer,
        mcid,
        current_master_customer_id,
        prior_master_customer_id,
        current_product_group,
        prior_product_group,
        currency_code,
        prior_period_product_arr_usd_ccfx,
        current_period_product_arr_usd_ccfx,
        product_arr_change_ccfx,
        prior_period_product_arr_lcu,
        current_period_product_arr_lcu,
        product_arr_change_lcu,
        product_bridge,
        winback_period_days,
        wip_flag,
        price_increase_amount,
        subsidiary_entity_name,
        churn_period,
        customer_bridge
    )
SELECT evaluation_period,
    prior_period,
    current_period,
    current_end_customer,
    prior_end_customer,
    mcid,
    current_master_customer_id,
    prior_master_customer_id,
    current_product_group,
    prior_product_group,
    currency_code,
    prior_period_product_arr_usd_ccfx * abs(
        "PG Migration: Rolled Up Amount" / product_arr_change_ccfx
    ),
    current_period_product_arr_usd_ccfx * abs(
        "PG Migration: Rolled Up Amount" / product_arr_change_ccfx
    ),
    --  product_arr_change_ccfx ,
    "PG Migration: Rolled Up Amount",
    prior_period_product_arr_lcu * abs(
        "PG Migration: Rolled Up Amount LCU" / product_arr_change_lcu
    ),
    current_period_product_arr_lcu * abs(
        "PG Migration: Rolled Up Amount LCU" / product_arr_change_lcu
    ),
    "PG Migration: Rolled Up Amount LCU",
    --    product_bridge ,
    COALESCE("PG Migration: Classification", product_bridge),
    winback_period_days,
    wip_flag,
    price_increase_amount,
    subsidiary_entity_name,
    churn_period,
    customer_bridge
FROM sst_product_bridge_product_group_temp AS b
WHERE mcid = b.mcid
    AND evaluation_period = b.evaluation_period
    AND currency_code = b.currency_code
    AND current_product_group = b.current_product_group
    AND prior_product_group = b.prior_product_group
    AND product_bridge = b.product_bridge;


INSERT INTO sst_product_bridge_product_group_temp AS a (
        evaluation_period,
        prior_period,
        current_period,
        current_end_customer,
        prior_end_customer,
        mcid,
        current_master_customer_id,
        prior_master_customer_id,
        current_product_group,
        prior_product_group,
        currency_code,
        prior_period_product_arr_usd_ccfx,
        current_period_product_arr_usd_ccfx,
        product_arr_change_ccfx,
        prior_period_product_arr_lcu,
        current_period_product_arr_lcu,
        product_arr_change_lcu,
        product_bridge,
        winback_period_days,
        wip_flag,
        price_increase_amount,
        subsidiary_entity_name,
        churn_period,
        customer_bridge
    )
SELECT evaluation_period,
    prior_period,
    current_period,
    current_end_customer,
    prior_end_customer,
    mcid,
    current_master_customer_id,
    prior_master_customer_id,
    current_product_group,
    prior_product_group,
    currency_code,
    prior_period_product_arr_usd_ccfx * abs(
        "PG Leftover: Rolled Up Amount" / product_arr_change_ccfx
    ),
    current_period_product_arr_usd_ccfx * abs(
        "PG Leftover: Rolled Up Amount" / product_arr_change_ccfx
    ),
    --  product_arr_change_ccfx ,
    -- change this to default once and then to migrated value
    "PG Leftover: Rolled Up Amount",
    prior_period_product_arr_lcu * abs(
        "PG Leftover: Rolled Up Amount LCU" / product_arr_change_lcu
    ),
    current_period_product_arr_lcu * abs(
        "PG Leftover: Rolled Up Amount LCU" / product_arr_change_lcu
    ),
    "PG Leftover: Rolled Up Amount LCU",
    --    default_value_lcu ,
    --    product_bridge ,
    COALESCE("PG Leftover: Classification", product_bridge),
    winback_period_days,
    wip_flag,
    price_increase_amount,
    subsidiary_entity_name,
    churn_period,
    customer_bridge
FROM sandbox.PG_migration_split AS b
WHERE mcid = b.mcid
    AND evaluation_period = b.evaluation_period
    AND currency_code = b.currency_code
    AND current_product_group = b.current_product_group
    AND prior_product_group = b.prior_product_group
    AND product_bridge = b.product_bridge;



-- select * from churn_migration_classifiers_pg where evaluation_period = '2023M06' and mcid = '218a418d-453d-e411-9f63-0050568d2da8';
END;
$function$
;
