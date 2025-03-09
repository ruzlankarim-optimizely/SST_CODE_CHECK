DROP TABLE IF EXISTS sandbox.sst_customer_bridge_rollover_cm;
CREATE TABLE sandbox.sst_customer_bridge_rollover_cm AS
SELECT *
FROM sandbox_pd.sst_customer_bridge scb;


-- product group rollup

Drop table if exists sandbox.customer_product_migration_joiner;
create table sandbox.customer_product_migration_joiner as (
with base_product_group as (
select
    evaluation_period,
    DATE(TO_DATE(evaluation_period, 'YYYY"M"MM')+ interval '1 month - 1 day') as snapshot_date,
    mcid,
--     concat(current_product_group,'-',prior_product_group) as product_group,
    product_bridge,
    pathways,
    product_arr_change_ccfx,
    product_arr_change_lcu,
    currency_code
from sandbox.sst_product_group_bridge
where 1 = 1
-- and mcid IN ('218a418d-453d-e411-9f63-0050568d2da8')
-- and evaluation_period = '2022M10'
)
, cb_part as (
    select
    evaluation_period,
    DATE(TO_DATE(evaluation_period, 'YYYY"M"MM')+ interval '1 month - 1 day') as snapshot_date,
    mcid,
    customer_bridge,
    customer_arr_change_ccfx,
    customer_arr_change_lcu,
    baseline_currency
from sandbox.sst_customer_bridge_rollover_cm
where 1=1
-- and mcid IN ('218a418d-453d-e411-9f63-0050568d2da8')
-- and evaluation_period = '2022M10'
)
,pg_sub_part as (
select
    evaluation_period,
    mcid ,
--     product_group ,
    split_part(product_bridge, '- migration',1) as bridge_part ,
    case when product_bridge ilike '%- migration' then 'migration' else null end as migration_part,
    pathways,
    product_arr_change_ccfx,
    product_arr_change_lcu ,
    currency_code
from base_product_group
)
, pg_part as (
select
    evaluation_period ,
    mcid,
    currency_code ,
--     product_group ,
    bridge_part ,
    migration_part ,
    pathways ,
    sum(product_arr_change_ccfx ) as product_arr_change_ccfx,
    sum(product_arr_change_lcu) as product_arr_change_lcu
from pg_sub_part
where migration_part is not null
group by 1,2,3,4,5,6
)


, joint_part as (
select
    b.evaluation_period,
    b.mcid,
    b.currency_code,
--     b.product_group ,
    b.bridge_part,
    b.migration_part,
    b.pathways,
    b.product_arr_change_ccfx,
    b.product_arr_change_lcu,
    a.customer_bridge ,
    a.customer_arr_change_ccfx ,
    a.customer_arr_change_lcu
from pg_part as b
left  join cb_part  as a
    on a.evaluation_period = b.evaluation_period
    and a.mcid = b.mcid
    and a.baseline_currency = b.currency_code
    and lower(trim(a.customer_bridge)) = lower(trim(b.bridge_part))
-- where a.mcid = 'd7b8de84-3337-e711-810c-3863bb365008'
-- and a.evaluation_period = '2022M03'
)
--    select * from joint_part

, logic_base as (
select
    a.*,
    case when Migration_rolled_up_amount is not null then concat(customer_bridge,' - migration') end as Migration_classification ,
    case when Migration_split_amount is not null then customer_bridge end as Migration_leftover_classification
from (
select
    a.*,
    case when full_amount_migration = True or split_case_migration  = true then
        case when round(abs(customer_arr_change_ccfx)) < round(abs(product_arr_change_ccfx)) then customer_arr_change_ccfx else product_arr_change_ccfx end
        else Null end as Migration_rolled_up_amount,
    case when full_amount_migration = True or split_case_migration  = true then
        case when round(abs(customer_arr_change_ccfx)) < round(abs(product_arr_change_ccfx)) then customer_arr_change_lcu else product_arr_change_lcu end
    else Null end as Migration_rolled_up_amount_lcu,
    Case when full_amount_migration = False and split_case_migration = True then (customer_arr_change_ccfx - product_arr_change_ccfx ) else Null end as Migration_split_amount, -- logic to handle negative values
    Case when full_amount_migration = False and split_case_migration = True then (customer_arr_change_lcu  - product_arr_change_lcu ) else Null end as Migration_split_amount_lcu
from (
    select
        a.* ,
        case when (round(abs(product_arr_change_ccfx) - abs(customer_arr_change_ccfx)) = 0) or round(abs(customer_arr_change_ccfx)) < round(abs(product_arr_change_ccfx))  then True else False end as full_amount_migration ,
        case when round(abs(customer_arr_change_ccfx)) > round(abs(product_arr_change_ccfx)) then True else False end as split_case_migration
    from joint_part as a
    where customer_bridge is not null
) as a) as a

)



,double_classification_setup as (
select
    a.*,
    count(mcid) over(partition by evaluation_period , mcid, customer_bridge, a.total_migration_amount_ccfx) as counting_migration,
    dense_rank() over (partition by evaluation_period, mcid order by customer_arr_change_ccfx)  + dense_rank() over (partition by evaluation_period , mcid order by customer_arr_change_ccfx desc) - 1 AS count_movements
from (
    select
        *,
        sum(Migration_rolled_up_amount) over(partition by evaluation_period, mcid, customer_bridge , customer_arr_change_ccfx ) as total_migration_amount_ccfx ,
        sum(Migration_rolled_up_amount_lcu) over(partition by evaluation_period, mcid,customer_bridge,customer_arr_change_lcu ) as total_migration_amount_lcu
    from logic_base

    ) as a
)
, flagging_table as (
select
    a.*,
    case when Migration_leftover_classification is not null
    and counting_migration > 1
    and abs(total_migration_amount_ccfx) < abs(customer_arr_change_ccfx)
    then round((customer_arr_change_ccfx - total_migration_amount_ccfx)/ counting_migration )
    else Null
    end as new_leftover_value_ccfx,
    case when Migration_leftover_classification is not null
    and counting_migration > 1
    and abs(total_migration_amount_ccfx) < abs(customer_arr_change_ccfx)
    then round((customer_arr_change_lcu - total_migration_amount_lcu)/ counting_migration )
    else Null
    end as new_leftover_value_lcu,

    case when (counting_migration > 1 OR counting_migration >= count_movements)
    and (counting_migration <> count_movements )
    and abs(total_migration_amount_ccfx) > abs(customer_arr_change_ccfx)
    then Case when (migration_rolled_up_amount/ total_migration_amount_ccfx ) * 2 = 1 then round((migration_rolled_up_amount / total_migration_amount_ccfx) * (total_migration_amount_ccfx - customer_arr_change_ccfx))
    else round((total_migration_amount_ccfx - customer_arr_change_ccfx) * (migration_rolled_up_amount/ total_migration_amount_ccfx)) end
    else Null end as substracted_amount_ccfx,

    case when (counting_migration > 1 OR counting_migration >= count_movements)
    and (counting_migration <> count_movements )
    and abs(total_migration_amount_ccfx) > abs(customer_arr_change_ccfx)
    then Case when (migration_rolled_up_amount_lcu/ total_migration_amount_lcu ) * 2 = 1 then round((migration_rolled_up_amount_lcu / total_migration_amount_lcu) * (total_migration_amount_lcu - customer_arr_change_lcu))
    else round((total_migration_amount_lcu - customer_arr_change_lcu) * (migration_rolled_up_amount_lcu/ total_migration_amount_lcu)) end
    else Null end as substracted_amount_lcu,

    case when Migration_classification is not null and counting_migration > 1 and abs(total_migration_amount_ccfx ) = abs(customer_arr_change_ccfx) then True else False end double_classification_first_case,
    case when Migration_leftover_classification is not null and counting_migration > 1 and abs(total_migration_amount_ccfx ) < abs(customer_arr_change_ccfx) then True else False end double_classification_second_case ,
    case when (counting_migration > 1 or counting_migration >= count_movements ) and (counting_migration <> count_movements ) and abs(total_migration_amount_ccfx ) > abs(customer_arr_change_ccfx) then True else False end as double_classification_third_case
from double_classification_setup as a
)
-- select * from flagging_table
-- select
--     evaluation_period,
--     mcid ,
--     pathways ,
--     Migration_classification ,
--     Migration_leftover_classification ,
--     customer_arr_change_ccfx,
--     Migration_rolled_up_amount ,
--     Migration_split_amount ,
--     total_migration_amount_ccfx,
--     counting_migration,
--     count_movements,
--     new_leftover_value_ccfx,
--     substracted_amount_ccfx,
--     double_classification_first_case,
--     double_classification_second_case,
--     double_classification_third_case
--     from flagging_table
--     where mcid = 'd74620b9-768f-dd11-a26e-0018717a8c82'
-- and evaluation_period = '2023M03';

SELECT
       evaluation_period,
       mcid,
       currency_code AS baseline_currency,
       --     product_group,
       customer_arr_change_ccfx,
       customer_arr_change_lcu,
       customer_bridge,
       pathways,
       CASE WHEN double_classification_third_case = TRUE THEN Migration_rolled_up_amount - coalesce(substracted_amount_ccfx, 0 ) ELSE Migration_rolled_up_amount END AS Migration_rolled_up_amount,
       CASE WHEN double_classification_third_case = TRUE THEN Migration_rolled_up_amount_lcu - coalesce(substracted_amount_lcu, 0 ) ELSE Migration_rolled_up_amount_lcu END AS Migration_rolled_up_amount_lcu,
       CASE WHEN double_classification_second_case = TRUE THEN new_leftover_value_ccfx ELSE
            CASE WHEN (double_classification_first_case = TRUE OR double_classification_third_case = TRUE) THEN NULL ELSE Migration_split_amount END
       END AS Migration_split_amount,
       CASE WHEN double_classification_second_case = TRUE THEN new_leftover_value_lcu ELSE
           CASE WHEN double_classification_first_case = TRUE OR double_classification_third_case = TRUE THEN NULL ELSE Migration_split_amount_lcu END
           END AS Migration_split_amount_lcu,
       Migration_classification  ,
       CASE WHEN double_classification_second_case = TRUE THEN Migration_leftover_classification ELSE
           CASE WHEN double_classification_first_case = TRUE OR double_classification_third_case = TRUE THEN NULL ELSE Migration_leftover_classification END
           END AS Migration_leftover_classification
FROM flagging_table
-- where mcid = 'd74620b9-768f-dd11-a26e-0018717a8c82'
-- and evaluation_period = '2023M03'
);

-- select * from sandbox.customer_product_migration_joiner
-- where mcid = 'bce8ec4d-6934-e511-9afb-0050568d2da8'
-- and evaluation_period = '2024M01';

-- Migration default categories
Drop table if exists sandbox.customer_churn_migration_default_cases ;
create table sandbox.customer_churn_migration_default_cases as
    Select
        a.*,
        b.pathways as migration_pathways,
        b.migration_rolled_up_amount ,
        b.migration_rolled_up_amount_lcu,
        b.migration_split_amount ,
        b.migration_split_amount_lcu,
        b.migration_classification ,
        b.migration_leftover_classification
    from sandbox.sst_customer_bridge_rollover_cm    as a
    join sandbox.customer_product_migration_joiner  as b
    on a.mcid = b.mcid
    and a.evaluation_period = b.evaluation_period
    and a.baseline_currency = b.baseline_currency
    and lower(trim(a.customer_bridge)) = lower(trim(b.customer_bridge))
    Where migration_classification ilike ('%migration')
    and migration_leftover_classification is null
;

-- Migration Split Categories
Drop table if exists sandbox.customer_churn_migration_split_cases ;
create table sandbox.customer_churn_migration_split_cases as
    Select
        a.*,
        b.pathways as migration_pathways,
        b.migration_rolled_up_amount ,
        b.migration_rolled_up_amount_lcu,
        b.migration_split_amount ,
        b.migration_split_amount_lcu,
        b.migration_classification ,
        b.migration_leftover_classification
    from sandbox.sst_customer_bridge_rollover_cm    as a
    join sandbox.customer_product_migration_joiner  as b
    on a.mcid = b.mcid
    and a.evaluation_period = b.evaluation_period
    and a.baseline_currency = b.baseline_currency
    and lower(trim(a.customer_bridge)) = lower(trim(b.customer_bridge))
    Where migration_classification ilike ('%migration')
    and migration_leftover_classification is not null
;

--select * from sandbox.customer_churn_migration_split_cases

-- Deleting default cases from main table

Delete from sandbox.sst_customer_bridge_rollover_cm as a
Using sandbox.customer_churn_migration_default_cases as b
where a.mcid = b.mcid
and a.evaluation_period = b.evaluation_period
and a.baseline_currency = b.baseline_currency
and lower(trim(a.customer_bridge)) = lower(trim(b.customer_bridge));

--Inserting default cases with new Classification and pathawys

Insert into sandbox.sst_customer_bridge_rollover_cm as a (
evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    "name",
    baseline_currency,
    subsidiary_entity_name,
    prior_period_customer_arr_usd_ccfx,
    current_period_customer_arr_usd_ccfx,
    customer_arr_change_ccfx,
    prior_period_customer_arr_lcu,
    current_period_customer_lcu,
    customer_arr_change_lcu,
    customer_bridge,
    winback_period_days,
    wip_flag,
    pathways
)
Select
      evaluation_period,
      prior_period,
      current_period,
      current_master_customer_id,
      prior_master_customer_id,
      mcid,
      "name",
      baseline_currency,
      subsidiary_entity_name,
    prior_period_customer_arr_usd_ccfx * abs(
    b.migration_rolled_up_amount / CASE
      WHEN customer_arr_change_ccfx = 0 THEN 1
      ELSE customer_arr_change_ccfx
    END
  ) as prior_period_customer_arr_usd_ccfx,
current_period_customer_arr_usd_ccfx * abs(
    b.migration_rolled_up_amount / CASE
      WHEN customer_arr_change_ccfx = 0 THEN 1
      ELSE customer_arr_change_ccfx
    END) as current_period_customer_arr_usd_ccfx,
    b.migration_rolled_up_amount as customer_arr_change_ccfx,
    prior_period_customer_arr_lcu * abs(
    b.migration_rolled_up_amount_lcu / CASE
      WHEN customer_arr_change_lcu = 0 THEN 1
      ELSE customer_arr_change_lcu
    END
  ) as prior_period_customer_arr_lcu,
  current_period_customer_lcu * abs(
    b.migration_rolled_up_amount_lcu / CASE
      WHEN customer_arr_change_lcu = 0 THEN 1
      ELSE customer_arr_change_lcu
    END
  ) as current_period_customer_lcu,
  b.migration_rolled_up_amount_lcu as customer_arr_change_lcu,
  COALESCE(b.migration_classification, customer_bridge),
  winback_period_days,
  wip_flag,
  coalesce(b.migration_pathways, Null) as pathways

from sandbox.customer_churn_migration_default_cases as b
where mcid = b.mcid
and evaluation_period = b.evaluation_period
and baseline_currency = b.baseline_currency
and customer_bridge = b.customer_bridge;



Delete from sandbox.sst_customer_bridge_rollover_cm as a
Using sandbox.customer_churn_migration_split_cases as b
where a.mcid = b.mcid
and a.evaluation_period = b.evaluation_period
and a.baseline_currency = b.baseline_currency
and lower(trim(a.customer_bridge)) = lower(trim(b.customer_bridge));



Insert into sandbox.sst_customer_bridge_rollover_cm as a (
evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    "name",
    baseline_currency,
    subsidiary_entity_name,
    prior_period_customer_arr_usd_ccfx,
    current_period_customer_arr_usd_ccfx,
    customer_arr_change_ccfx,
    prior_period_customer_arr_lcu,
    current_period_customer_lcu,
    customer_arr_change_lcu,
    customer_bridge,
    winback_period_days,
    wip_flag,
    pathways
)
Select
      evaluation_period,
      prior_period,
      current_period,
      current_master_customer_id,
      prior_master_customer_id,
      mcid,
      "name",
      baseline_currency,
      subsidiary_entity_name,
    prior_period_customer_arr_usd_ccfx * abs(
    b.migration_rolled_up_amount / CASE
      WHEN customer_arr_change_ccfx = 0 THEN 1
      ELSE customer_arr_change_ccfx
    END
  ) as prior_period_customer_arr_usd_ccfx,
current_period_customer_arr_usd_ccfx * abs(
    b.migration_rolled_up_amount / CASE
      WHEN customer_arr_change_ccfx = 0 THEN 1
      ELSE customer_arr_change_ccfx
    END) as current_period_customer_arr_usd_ccfx,
    b.migration_rolled_up_amount as customer_arr_change_ccfx,
    prior_period_customer_arr_lcu * abs(
    b.migration_rolled_up_amount_lcu / CASE
      WHEN customer_arr_change_lcu = 0 THEN 1
      ELSE customer_arr_change_lcu
    END
  ) as prior_period_customer_arr_lcu,
  current_period_customer_lcu * abs(
    b.migration_rolled_up_amount_lcu/ CASE
      WHEN customer_arr_change_lcu = 0 THEN 1
      ELSE customer_arr_change_lcu
    END
  ) as current_period_customer_lcu,
  b.migration_rolled_up_amount_lcu as customer_arr_change_lcu,
  COALESCE(b.migration_classification, customer_bridge),
  winback_period_days,
  wip_flag,
  coalesce(b.migration_pathways, Null) as pathways

from sandbox.customer_churn_migration_split_cases as b
where mcid = b.mcid
and evaluation_period = b.evaluation_period
and baseline_currency = b.baseline_currency
and customer_bridge = b.customer_bridge;



Insert into sandbox.sst_customer_bridge_rollover_cm as a (
evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    "name",
    baseline_currency,
    subsidiary_entity_name,
    prior_period_customer_arr_usd_ccfx,
    current_period_customer_arr_usd_ccfx,
    customer_arr_change_ccfx,
    prior_period_customer_arr_lcu,
    current_period_customer_lcu,
    customer_arr_change_lcu,
    customer_bridge,
    winback_period_days,
    wip_flag,
    pathways
)
Select
      evaluation_period,
      prior_period,
      current_period,
      current_master_customer_id,
      prior_master_customer_id,
      mcid,
      "name",
      baseline_currency,
      subsidiary_entity_name,
    prior_period_customer_arr_usd_ccfx * abs(
    b.migration_split_amount / CASE
      WHEN customer_arr_change_ccfx = 0 THEN 1
      ELSE customer_arr_change_ccfx
    END
  ) as prior_period_customer_arr_usd_ccfx,
current_period_customer_arr_usd_ccfx * abs(
    b.migration_split_amount / CASE
      WHEN customer_arr_change_ccfx = 0 THEN 1
      ELSE customer_arr_change_ccfx
    END) as current_period_customer_arr_usd_ccfx,
    b.migration_split_amount as customer_arr_change_ccfx,
    prior_period_customer_arr_lcu * abs(
    b.migration_split_amount_lcu / CASE
      WHEN customer_arr_change_lcu = 0 THEN 1
      ELSE customer_arr_change_lcu
    END
  ) as prior_period_customer_arr_lcu,
  current_period_customer_lcu * abs(
    b.migration_split_amount_lcu / CASE
      WHEN customer_arr_change_lcu = 0 THEN 1
      ELSE customer_arr_change_lcu
    END
  ) as current_period_customer_lcu,
  b.migration_split_amount_lcu as customer_arr_change_lcu,
  COALESCE(b.migration_leftover_classification, customer_bridge),
  winback_period_days,
  wip_flag,
  null as pathways

from sandbox.customer_churn_migration_split_cases as b
where mcid = b.mcid
and evaluation_period = b.evaluation_period
and baseline_currency = b.baseline_currency
and customer_bridge = b.customer_bridge;

-- Grouping common bridge movements
drop table if exists sandbox.temp_arr_table;
create table sandbox.temp_arr_table as
SELECT evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  "name",
  baseline_currency,
  subsidiary_entity_name,
  customer_bridge,
  winback_period_days,
  wip_flag,
  pathways,
  sum(current_period_customer_arr_usd_ccfx) AS current_period_customer_arr_usd_ccfx,
  sum(prior_period_customer_arr_usd_ccfx) AS prior_period_customer_arr_usd_ccfx,
  sum(customer_arr_change_ccfx) AS customer_arr_change_ccfx,
  sum(current_period_customer_lcu) AS current_period_customer_lcu,
  sum(prior_period_customer_arr_lcu) AS prior_period_customer_arr_lcu,
  sum(customer_arr_change_lcu) AS customer_arr_change_lcu
FROM sandbox.sst_customer_bridge_rollover_cm
GROUP BY 1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11,
  12,
  13;
TRUNCATE TABLE sandbox.sst_customer_bridge_rollover_cm;
INSERT INTO sandbox.sst_customer_bridge_rollover_cm(
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    "name",
    baseline_currency,
    subsidiary_entity_name,
    prior_period_customer_arr_usd_ccfx,
    current_period_customer_arr_usd_ccfx,
    customer_arr_change_ccfx,
    prior_period_customer_arr_lcu,
    current_period_customer_lcu,
    customer_arr_change_lcu,
    customer_bridge,
    winback_period_days,
    wip_flag,
    pathways
  )
select evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  "name",
  baseline_currency,
  subsidiary_entity_name,
  prior_period_customer_arr_usd_ccfx,
  current_period_customer_arr_usd_ccfx,
  customer_arr_change_ccfx,
  prior_period_customer_arr_lcu,
  current_period_customer_lcu,
  customer_arr_change_lcu,
  customer_bridge,
  winback_period_days,
  wip_flag,
  pathways
FROM sandbox.temp_arr_table;