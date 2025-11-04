DROP TABLE IF EXISTS sandbox.sst_product_solution_bridge_rollup_cm_cloud;
CREATE TABLE sandbox.sst_product_solution_bridge_rollup_cm_cloud AS
SELECT *
FROM sandbox_pd.sst_product_bridge_product_solution_cloud_license
;



drop table if exists temp_pg_bridge;
create temp table temp_pg_bridge as
    select
    evaluation_period,
    mcid,
    currency_code ,
    case when product_arr_change_ccfx > 0 then '+' else
        case when product_arr_change_ccfx < 0 then '-' else null end
        end as pos_neg_flag ,
    current_product_group,
    prior_product_group ,
    concat(prior_product_group,'=',current_product_group) as product_groups,
    product_bridge,
    pathways,
    product_arr_change_ccfx,
    product_arr_change_lcu
from sandbox.sst_product_group_bridge_cloud
where 1 = 1

-- and product_bridge ilike '%- migration'
;

drop table if exists temp_ps_bridge;
create temp table temp_ps_bridge as
    Select
        a.evaluation_period ,
        a.mcid ,
        a.currency_code,
        case when a.product_arr_change_ccfx > 0 then '+' else
        case when a.product_arr_change_ccfx < 0 then '-' else null end
        end as pos_neg_flag ,
        a.prior_product_solution ,
        a.current_product_solution ,
        concat(a.prior_product_solution ,'=', a.current_product_solution) as product_solution,
        a.product_bridge ,
        a.product_arr_change_ccfx,
        a.product_arr_change_lcu
    from sandbox.sst_product_solution_bridge_rollup_cm_cloud as a
    join (select evaluation_period, mcid , currency_code  from temp_pg_bridge group by 1,2,3) as b
    on a.evaluation_period = b.evaluation_period
    and a.mcid = b.mcid
    and a.currency_code = b.currency_code
where product_bridge in (
--         'Flat',
--         'New',
        'Up Sell',
--         'Churn',
        'Cross-sell',
        'Downsell',
--         'Price Uplift',
        'Downgrade'
    )
-- and    a.mcid = '0a9a1cc5-ddaf-e911-a96a-000d3a4416ab'
-- and a.evaluation_period = '2024M01'
;


Drop table if exists sandbox.solution_classifier_migration_joiner_cloud;
create table sandbox.solution_classifier_migration_joiner_cloud as (
with  classifier as (
select
    evaluation_period,
    mcid ,
    currency_code ,
    prior_product_group  ,
    current_product_group ,
    prior_product_solution,
    current_product_solution ,
    product_arr_change_ccfx ,
    product_arr_change_lcu ,
    split_part("Movement Classification", '--',1) as bridge_path,
    split_part("Movement Classification", '--',2) as migration_pathways
from sandbox.churn_migration_classifiers_max_value_v2_2_split
-- where mcid = '50661331-d24e-e811-813c-70106fa6f451'
-- 	and evaluation_period = '2024M04'

)
, pg_bridge_mod as (
select
    a.evaluation_period,
    a.mcid,
    a.currency_code,
    a.pathways,
    a.pos_neg_flag,
    a.prior_product_group,
    a.current_product_group,
    b.prior_product_solution ,
    b.current_product_solution ,
    a.product_bridge,
    a.product_arr_change_ccfx,
    a.product_arr_change_lcu
from temp_pg_bridge as a
join classifier as b
on a.evaluation_period = b.evaluation_period
and a.mcid = b.mcid
and a.currency_code = b.currency_code
and lower(trim(a.pathways)) = lower(trim(b.migration_pathways))
and coalesce(a.current_product_group , a.prior_product_group ) = coalesce(b.current_product_group , b.prior_product_group)
)

--    select * from temp_ps_bridge
--     where mcid = '50661331-d24e-e811-813c-70106fa6f451'
-- 	and evaluation_period = '2024M04';


, initial_table_4 as (
select
    a.evaluation_period ,
    a.mcid,
    a.currency_code ,
    a.pos_neg_flag ,
    a.pathways ,
    concat(a.prior_product_group,'=',a.current_product_group) as product_groups ,
    a.product_bridge as pg_bridge ,
    a.product_arr_change_ccfx as pg_arr_change ,
    a.product_arr_change_lcu as pg_arr_change_lcu ,
    b.product_solution as ps_solutions ,
    b.product_bridge as ps_bridge ,
    b.product_arr_change_ccfx as ps_arr_change ,
    b.product_arr_change_lcu as ps_arr_change_lcu
from pg_bridge_mod as a
left join temp_ps_bridge as b
on a.evaluation_period = b.evaluation_period
and a.mcid = b.mcid
and a.currency_code = b.currency_code
and coalesce(a.current_product_solution , a.prior_product_solution ) = coalesce(b.current_product_solution , b.prior_product_solution)
and a.pos_neg_flag = b.pos_neg_flag
and lower(trim(split_part(a.product_bridge , '- migration',1))) = lower(trim(b.product_bridge))
and ( a.product_bridge ilike '%migration'
--     or lower(trim(split_part(a.product_bridge , '- migration',1))) = lower(trim(b.product_bridge))
       )
)

--    select * from initial_table_4
--    where mcid = '50661331-d24e-e811-813c-70106fa6f451'
-- 	and evaluation_period = '2024M04';

, initial_table_group as (
    select
        evaluation_period ,
        mcid ,
        currency_code ,
        pathways ,
        pos_neg_flag ,
        product_groups ,
        pg_bridge ,
        pg_arr_change ,
        pg_arr_change_lcu,
        ps_solutions ,
        ps_bridge ,
        ps_arr_change ,
        ps_arr_change_lcu
    from initial_table_4
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)
--    select * from initial_table_group
-- where  mcid = '6cb082b5-52a2-dd11-a48c-0018717a8c82'
-- and evaluation_period = '2024M05';



, mid_logic_part as (
select
    evaluation_period ,
    mcid ,
    currency_code ,
    product_groups ,
    pg_arr_change ,
    pg_arr_change_lcu ,
    pathways as migration_pathways,
    ps_solutions ,
    ps_arr_change,
    ps_arr_change_lcu,
    ps_bridge,
--     flags ,
--     case when (full_amount_migration is false and split_case_migration is true) and flags is true  then true else full_amount_migration end as full_amount_migration,
--     case when (full_amount_migration is false and split_case_migration is true) and flags is true  then false else split_case_migration end as split_case_migration
    full_amount_migration,
    split_case_migration
from (
    select
        a.* ,
        case when (round(abs(pg_arr_change) - abs(ps_arr_change)) = 0) or round(abs(ps_arr_change)) < round(abs(pg_arr_change))  then True else False end as full_amount_migration ,
        case when round(abs(ps_arr_change)) > round(abs(pg_arr_change))  then True else False end as split_case_migration
--         case when (ps_arr_change > 0 and a.product_arr_change_ccfx < 0) or (ps_arr_change < 0 and a.product_arr_change_ccfx > 0) then true else false end as flags
    from initial_table_group as a
     ) as a

)

--    select * from mid_logic_part
--                where mcid = 'f7cea80b-de83-e911-a964-000d3a441cb0'
-- and evaluation_period = '2023M01'

, logic_base as (
select
    a.*,
    case when Migration_rolled_up_amount is not null then concat(ps_bridge ,' - migration') end as Migration_classification ,
    case when Migration_split_amount is not null then ps_bridge end as Migration_leftover_classification
from (
    select
        a.* ,
        case when full_amount_migration = True or split_case_migration  = true then
        case when round(abs(ps_arr_change)) < round(abs(pg_arr_change )) then ps_arr_change else pg_arr_change end
        else Null end as Migration_rolled_up_amount,
        case when full_amount_migration = True or split_case_migration  = true then
        case when round(abs(ps_arr_change)) < round(abs(pg_arr_change)) then ps_arr_change_lcu else pg_arr_change_lcu end
        else Null end as Migration_rolled_up_amount_lcu,
        Case when full_amount_migration = False and split_case_migration = True then (ps_arr_change - pg_arr_change ) else Null end as Migration_split_amount, -- logic to handle negative values
        Case when full_amount_migration = False and split_case_migration = True then (ps_arr_change_lcu  - pg_arr_change_lcu ) else Null end as Migration_split_amount_lcu
    from mid_logic_part as a
 ) as a
)


-- select
-- *
-- from logic_base
-- where mcid = 'f7cea80b-de83-e911-a964-000d3a441cb0'
-- and evaluation_period = '2023M01';


, double_classification_setup as (
select
    *,
    count(mcid) over(partition by evaluation_period , mcid ,ps_solutions, ps_bridge , a.total_migration_amount_ccfx) as counting_migration ,
    dense_rank() over (partition by evaluation_period, mcid, ps_bridge,ps_arr_change, total_migration_amount_ccfx order by ps_solutions)  + dense_rank() over (partition by evaluation_period, mcid, ps_bridge,ps_arr_change, total_migration_amount_ccfx order by ps_solutions desc) - 1 AS count_movements

from (
select
    * ,
    sum(Migration_rolled_up_amount) over(partition by evaluation_period , mcid ,ps_solutions, ps_bridge , ps_arr_change) as total_migration_amount_ccfx,
    sum(Migration_rolled_up_amount_lcu) over(partition by evaluation_period , mcid ,ps_solutions, ps_bridge , ps_arr_change_lcu) as total_migration_amount_lcu
from logic_base
     ) as a
)

, flagging_table as (
    select
    a.*,
    case when Migration_leftover_classification is not null
    and counting_migration > 1
    and abs(total_migration_amount_ccfx) < abs(ps_arr_change)
    then round((ps_arr_change - total_migration_amount_ccfx)/ counting_migration )
    else Null
    end as new_leftover_value_ccfx,
    case when Migration_leftover_classification is not null
    and counting_migration > 1
    and abs(total_migration_amount_ccfx) < abs(ps_arr_change)
    then round((ps_arr_change_lcu - total_migration_amount_lcu)/ counting_migration )
    else Null
    end as new_leftover_value_lcu,

    case when (counting_migration > 1 OR counting_migration >= count_movements)
    and (counting_migration <> count_movements )
    and abs(total_migration_amount_ccfx) > abs(ps_arr_change)
    then Case when (migration_rolled_up_amount/ total_migration_amount_ccfx ) * 2 = 1 then round((migration_rolled_up_amount / total_migration_amount_ccfx) * (total_migration_amount_ccfx - ps_arr_change))
    else round((total_migration_amount_ccfx - ps_arr_change) * (migration_rolled_up_amount/ total_migration_amount_ccfx)) end
    else Null end as substracted_amount_ccfx,

    case when (counting_migration > 1 OR counting_migration >= count_movements)
    and (counting_migration <> count_movements )
    and abs(total_migration_amount_ccfx) > abs(ps_arr_change)
    then Case when (migration_rolled_up_amount_lcu/ total_migration_amount_lcu ) * 2 = 1 then round((migration_rolled_up_amount_lcu / total_migration_amount_lcu) * (total_migration_amount_lcu - ps_arr_change_lcu))
    else round((total_migration_amount_lcu - ps_arr_change_lcu) * (migration_rolled_up_amount_lcu/ total_migration_amount_lcu)) end
    else Null end as substracted_amount_lcu,

    case when Migration_classification is not null and counting_migration > 1 and abs(total_migration_amount_ccfx ) = abs(ps_arr_change) then True else False end double_classification_first_case,
    case when Migration_leftover_classification is not null and counting_migration > 1 and abs(total_migration_amount_ccfx ) < abs(ps_arr_change) then True else False end double_classification_second_case ,
    case when (counting_migration > 1 or counting_migration >= count_movements ) and (counting_migration <> count_movements ) and abs(total_migration_amount_ccfx ) > abs(ps_arr_change) then True else False end as double_classification_third_case
from double_classification_setup as a

)
-- select * from flagging_table
--     where mcid = 'f7cea80b-de83-e911-a964-000d3a441cb0'
-- and evaluation_period = '2023M01';


select
    evaluation_period,
    mcid ,
    currency_code ,
    ps_solutions as product_solutions ,
    ps_arr_change as product_arr_change_ccfx ,
    ps_arr_change_lcu as product_arr_change_lcu ,
    ps_bridge  as product_bridge ,
    migration_pathways  as pathways ,
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
from flagging_table

);

-- select * from sandbox.solution_classifier_migration_joiner_cloud
-- where Migration_classification ilike('%migration%');

drop table if exists sandbox.churn_migration_limitation_ps_cloud;
create table sandbox.churn_migration_limitation_ps_cloud as
    with base as (
        Select
            mcid ,
            evaluation_period ,
            DATE(TO_DATE(evaluation_period, 'YYYY"M"MM')+ interval '1 month - 1 day') as  eval_period,
            currency_code ,
            product_solutions ,
            product_bridge,
            Migration_classification,
            Migration_leftover_classification,
            pathways,
            sum(product_arr_change_ccfx) as product_arr_change_ccfx ,
            sum(product_arr_change_lcu) as product_arr_change_lcu ,
            sum(Migration_rolled_up_amount) as Migration_rolled_up_amount ,
            sum(Migration_rolled_up_amount_lcu) as Migration_rolled_up_amount_lcu ,
            sum(Migration_split_amount) as Migration_split_amount ,
            sum(Migration_split_amount_lcu) as Migration_split_amount_lcu
        from sandbox.solution_classifier_migration_joiner_cloud
        group by 1,2,3,4,5,6,7,8,9
    )
    ,negative_base as (
        select
            *
        from base
        where Migration_classification ilike('%migration%')
        and Migration_rolled_up_amount < 0
    )
    , positive_base as (
        SELECT
            *,
            rank() over(
              PARTITION BY mcid,
              currency_code
              ORDER BY eval_period
            ) AS rnk
        FROM base
        WHERE Migration_classification ILIKE ('%migration%')
        AND Migration_rolled_up_amount > 0
    )
    Select
        mcid,
        evaluation_period ,
        currency_code,
        eval_period,
        product_solutions,
        product_bridge,
        pathways,
        product_arr_change_ccfx ,
        product_arr_change_lcu ,
        Migration_rolled_up_amount,
        Migration_rolled_up_amount_lcu ,
        Migration_split_amount,
        Migration_split_amount_lcu,
        migration_classification,
        migration_leftover_classification
    from positive_base
    where rnk = 1
    UNION all
    select
        mcid,
        evaluation_period ,
        currency_code,
        eval_period,
        product_solutions,
        product_bridge,
        pathways ,
        product_arr_change_ccfx ,
        product_arr_change_lcu ,
        Migration_rolled_up_amount,
        Migration_rolled_up_amount_lcu ,
        Migration_split_amount,
        Migration_split_amount_lcu,
        migration_classification,
        migration_leftover_classification
    from negative_base;


-- Migration Default Catagories
Drop table if exists sandbox.solution_churn_migration_default_cases_cloud ;
create table sandbox.solution_churn_migration_default_cases_cloud as
    Select
        a.*,
        b.product_solutions ,
        b.pathways as migration_pathways ,
        b.migration_rolled_up_amount ,
        b.migration_rolled_up_amount_lcu,
        b.migration_split_amount ,
        b.migration_split_amount_lcu,
        b.migration_classification ,
        b.migration_leftover_classification
    from sandbox.sst_product_solution_bridge_rollup_cm_cloud as a
    join sandbox.churn_migration_limitation_ps_cloud as b
    on a.evaluation_period = b.evaluation_period
    and a.mcid = b.mcid
    and a.currency_code = b.currency_code
    and lower(trim(a.product_bridge)) = lower(trim(b.product_bridge))
    and concat(prior_product_solution, '=',current_product_solution) = b.product_solutions
    where migration_classification ilike ('%migration')
    and migration_leftover_classification is null;

-- Migration Spit Categories
Drop table if exists sandbox.solution_churn_migration_split_cases_cloud ;
create table sandbox.solution_churn_migration_split_cases_cloud as
    Select
        a.*,
        b.product_solutions ,
        b.pathways as migration_pathways ,
        b.migration_rolled_up_amount ,
        b.migration_rolled_up_amount_lcu,
        b.migration_split_amount ,
        b.migration_split_amount_lcu,
        b.migration_classification ,
        b.migration_leftover_classification
    from sandbox.sst_product_solution_bridge_rollup_cm_cloud as a
    join sandbox.churn_migration_limitation_ps_cloud as b
    on a.evaluation_period = b.evaluation_period
    and a.mcid = b.mcid
    and a.currency_code = b.currency_code
    and lower(trim(a.product_bridge)) = lower(trim(b.product_bridge))
    and concat(prior_product_solution, '=',current_product_solution) = b.product_solutions
    where migration_classification ilike ('%migration')
    and migration_leftover_classification is not null;

-- Deleting default cases

Delete from sandbox.sst_product_solution_bridge_rollup_cm_cloud as a
Using sandbox.solution_churn_migration_default_cases_cloud as b
where a.mcid = b.mcid
and a.evaluation_period = b.evaluation_period
and a.currency_code  = b.currency_code
and concat(a.prior_product_solution, '=',a.current_product_solution) = b.product_solutions
and lower(trim(a.product_bridge)) = lower(trim(b.product_bridge));

-- Inserting Default Migration cases
INSERT INTO sandbox.sst_product_solution_bridge_rollup_cm_cloud AS a (
    evaluation_period,
    prior_period,
    current_period,
    current_end_customer,
    prior_end_customer,
    mcid,
    current_master_customer_id,
    prior_master_customer_id,
    current_product_solution,
    prior_product_solution,
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
    customer_bridge,
    pathways
  )
SELECT evaluation_period,
  prior_period,
  current_period,
  current_end_customer,
  prior_end_customer,
  mcid,
  current_master_customer_id,
  prior_master_customer_id,
  current_product_solution,
  prior_product_solution,
  currency_code,
  prior_period_product_arr_usd_ccfx * abs(
    migration_rolled_up_amount / product_arr_change_ccfx
  ) as prior_period_product_arr_usd_ccfx,
  current_period_product_arr_usd_ccfx * abs(
    migration_rolled_up_amount / product_arr_change_ccfx
  ) as current_period_product_arr_usd_ccfx,
  --  product_arr_change_ccfx ,
  migration_rolled_up_amount as product_arr_change_ccfx,
  prior_period_product_arr_lcu * abs(
    migration_rolled_up_amount_lcu / product_arr_change_lcu
  ) as prior_period_product_arr_lcu,
  current_period_product_arr_lcu * abs(
    migration_rolled_up_amount_lcu / product_arr_change_lcu
  ) as current_period_product_arr_lcu,
  migration_rolled_up_amount_lcu as product_arr_change_lcu,
  COALESCE(b.migration_classification, product_bridge) as product_bridge,
  winback_period_days,
  wip_flag,
  price_increase_amount,
  subsidiary_entity_name,
  churn_period,
  customer_bridge,
  coalesce(b.migration_pathways , null) as pathways
FROM sandbox.solution_churn_migration_default_cases_cloud AS b
WHERE mcid = b.mcid
  AND evaluation_period = b.evaluation_period
  AND currency_code = b.currency_code
  AND concat(prior_product_solution, '=',current_product_solution) = b.product_solutions
  AND lower(trim(product_bridge)) = lower(trim(b.product_bridge));

-- deleting split cases
Delete from sandbox.sst_product_solution_bridge_rollup_cm_cloud as a
Using sandbox.solution_churn_migration_split_cases_cloud as b
where a.mcid = b.mcid
and a.evaluation_period = b.evaluation_period
and a.currency_code  = b.currency_code
and concat(a.prior_product_solution, '=',a.current_product_solution) = b.product_solutions
and lower(trim(a.product_bridge)) = lower(trim(b.product_bridge));

-- Insertig Split cases twice
    -- inserting migration cases
INSERT INTO sandbox.sst_product_solution_bridge_rollup_cm_cloud AS a (
    evaluation_period,
    prior_period,
    current_period,
    current_end_customer,
    prior_end_customer,
    mcid,
    current_master_customer_id,
    prior_master_customer_id,
    current_product_solution,
    prior_product_solution,
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
    customer_bridge,
    pathways
  )
SELECT evaluation_period,
  prior_period,
  current_period,
  current_end_customer,
  prior_end_customer,
  mcid,
  current_master_customer_id,
  prior_master_customer_id,
  current_product_solution,
  prior_product_solution,
  currency_code,
  prior_period_product_arr_usd_ccfx * abs(
    migration_rolled_up_amount / CASE
      WHEN product_arr_change_ccfx = 0 THEN 1
      ELSE product_arr_change_ccfx
    END
  ) as prior_period_product_arr_usd_ccfx,
  current_period_product_arr_usd_ccfx * abs(
    migration_rolled_up_amount / CASE
      WHEN product_arr_change_ccfx = 0 THEN 1
      ELSE product_arr_change_ccfx
    END
  ) as current_period_product_arr_usd_ccfx,
  --  product_arr_change_ccfx ,
  migration_rolled_up_amount as product_arr_change_ccfx ,
  prior_period_product_arr_lcu * abs(
    migration_rolled_up_amount_lcu / CASE
      WHEN product_arr_change_lcu = 0 THEN 1
      ELSE product_arr_change_lcu
    END
  ) as prior_period_product_arr_lcu,
  current_period_product_arr_lcu * abs(
    migration_rolled_up_amount_lcu / CASE
      WHEN product_arr_change_lcu = 0 THEN 1
      ELSE product_arr_change_lcu
    END
  ) as current_period_product_arr_lcu,
  migration_rolled_up_amount_lcu as product_arr_change_lcu,
  --    product_bridge ,
  COALESCE(b.migration_classification, product_bridge) as product_bridge,
  winback_period_days,
  wip_flag,
  price_increase_amount,
  subsidiary_entity_name,
  churn_period,
  customer_bridge,
  coalesce(b.migration_pathways , null) as pathways
FROM sandbox.solution_churn_migration_split_cases_cloud AS b
WHERE mcid = b.mcid
  AND evaluation_period = b.evaluation_period
  AND currency_code = b.currency_code
  AND concat(prior_product_solution, '=',current_product_solution) = b.product_solutions
  AND lower(trim(product_bridge)) = lower(trim(b.product_bridge));

-- inserting split cases

INSERT INTO sandbox.sst_product_solution_bridge_rollup_cm_cloud AS a (
    evaluation_period,
    prior_period,
    current_period,
    current_end_customer,
    prior_end_customer,
    mcid,
    current_master_customer_id,
    prior_master_customer_id,
    current_product_solution,
    prior_product_solution,
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
    customer_bridge,
    pathways
  )
SELECT evaluation_period,
  prior_period,
  current_period,
  current_end_customer,
  prior_end_customer,
  mcid,
  current_master_customer_id,
  prior_master_customer_id,
  current_product_solution,
  prior_product_solution,
  currency_code,
  prior_period_product_arr_usd_ccfx * abs(
    migration_split_amount / CASE
      WHEN product_arr_change_ccfx = 0 THEN 1
      ELSE product_arr_change_ccfx
    END
  ) as prior_period_product_arr_usd_ccfx,
  current_period_product_arr_usd_ccfx * abs(
    migration_split_amount / CASE
      WHEN product_arr_change_ccfx = 0 THEN 1
      ELSE product_arr_change_ccfx
    END
  ) as current_period_product_arr_usd_ccfx,
  --  product_arr_change_ccfx ,
  -- change this to default once and then to migrated value
  migration_split_amount as product_arr_change_ccfx,
  prior_period_product_arr_lcu * abs(
    migration_split_amount_lcu / CASE
      WHEN product_arr_change_lcu = 0 THEN 1
      ELSE product_arr_change_lcu
    END
  ) as prior_period_product_arr_lcu,
  current_period_product_arr_lcu * abs(
    migration_split_amount_lcu / CASE
      WHEN product_arr_change_lcu = 0 THEN 1
      ELSE product_arr_change_lcu
    END
  ) as current_period_product_arr_lcu,
  migration_split_amount_lcu as product_arr_change_lcu ,
  --    default_value_lcu ,
  --    product_bridge ,
  COALESCE(b.migration_leftover_classification, product_bridge) as product_bridge,
  winback_period_days,
  wip_flag,
  price_increase_amount,
  subsidiary_entity_name,
  churn_period,
  customer_bridge,
  null as pathways
FROM sandbox.solution_churn_migration_split_cases_cloud AS b
WHERE mcid = b.mcid
  AND evaluation_period = b.evaluation_period
  AND currency_code = b.currency_code
  AND concat(prior_product_solution, '=',current_product_solution) = b.product_solutions
  AND lower(trim(product_bridge)) = lower(trim(b.product_bridge));


drop table if exists sandbox.temp_solution_cm_table_cloud;
create table sandbox.temp_solution_cm_table_cloud as
SELECT evaluation_period,
  prior_period,
  current_period,
  current_end_customer,
  prior_end_customer,
  mcid,
  current_master_customer_id,
  prior_master_customer_id,
  current_product_solution,
  prior_product_solution,
  currency_code,
  product_bridge,
  winback_period_days,
  wip_flag,
  price_increase_amount,
  subsidiary_entity_name,
  churn_period,
  customer_bridge,
  pathways,
  sum(current_period_product_arr_usd_ccfx) AS current_period_product_arr_usd_ccfx,
  sum(prior_period_product_arr_usd_ccfx) AS prior_period_product_arr_usd_ccfx,
  sum(product_arr_change_ccfx) AS product_arr_change_ccfx,
  sum(current_period_product_arr_lcu) AS current_period_product_arr_lcu,
  sum(prior_period_product_arr_lcu) AS prior_period_product_arr_lcu,
  sum(product_arr_change_lcu) AS product_arr_change_lcu
FROM sandbox.sst_product_solution_bridge_rollup_cm_cloud
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
  13,
  14,
  15,
  16,
  17,
  18,
  19;
TRUNCATE TABLE sandbox.sst_product_solution_bridge_rollup_cm_cloud;
INSERT INTO sandbox.sst_product_solution_bridge_rollup_cm_cloud(
    evaluation_period,
    prior_period,
    current_period,
    current_end_customer,
    prior_end_customer,
    mcid,
    current_master_customer_id,
    prior_master_customer_id,
    current_product_solution,
    prior_product_solution,
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
    customer_bridge,
    pathways
  )
select evaluation_period,
  prior_period,
  current_period,
  current_end_customer,
  prior_end_customer,
  mcid,
  current_master_customer_id,
  prior_master_customer_id,
  current_product_solution,
  prior_product_solution,
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
  customer_bridge,
  pathways
FROM sandbox.temp_solution_cm_table_cloud;