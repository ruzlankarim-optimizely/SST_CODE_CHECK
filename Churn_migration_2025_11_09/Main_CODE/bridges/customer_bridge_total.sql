DROP TABLE IF EXISTS sandbox.sst_customer_bridge_total_mig;
CREATE TABLE sandbox.sst_customer_bridge_total_mig AS
SELECT *
FROM ufdm_archive.sst_customer_bridge_total_temp_lcoked_251209_0226
--     sandbox_pd.sst_customer_bridge
    scb;


-- product group rollup

Drop table if exists sandbox.customer_product_migration_joiner;
create table sandbox.customer_product_migration_joiner as (
with    base_product_group AS (
  SELECT
    evaluation_period,
    DATE(TO_DATE(evaluation_period, 'YYYY"M"MM') + INTERVAL '1 month - 1 day') AS snapshot_date,
    mcid,
    product_bridge,
    pathways,
    product_arr_change_ccfx,
    product_arr_change_lcu,
    currency_code,
    CASE
      WHEN product_arr_change_ccfx > 0 THEN '+'
      WHEN product_arr_change_ccfx < 0 THEN '-'
      ELSE 'flat'
    END AS pos_neg_flag
  FROM ufdm_archive.sst_pb_ps_cloud_license_mig_lcoked_251209_0226
),

pg_sub_part AS (
  SELECT
    evaluation_period,
    mcid,
    -- everything before “- migration”
    split_part(product_bridge, '- migration', 1) AS bridge_part,
    -- flag only if text ends in “- migration”
    CASE
      WHEN product_bridge ILIKE '%- migration' THEN 'migration'
      ELSE NULL
    END AS migration_part,
    pathways,
    product_arr_change_ccfx,
    product_arr_change_lcu,
    currency_code,
    pos_neg_flag
  FROM base_product_group
),

pg_part AS (
  SELECT
    evaluation_period,
    mcid,
    currency_code,
    bridge_part,
    migration_part,
    pathways,
    pos_neg_flag,
    SUM(product_arr_change_ccfx) AS product_arr_change_ccfx,
    SUM(product_arr_change_lcu)  AS product_arr_change_lcu
  FROM pg_sub_part
  WHERE migration_part IS NOT NULL
  GROUP BY
    evaluation_period,
    mcid,
    currency_code,
    bridge_part,
    migration_part,
    pathways,
    pos_neg_flag
),

-- 2) Pull customer_bridge buckets, limiting to the four categories:
cb_part AS (
  SELECT
    evaluation_period,
    DATE(TO_DATE(evaluation_period, 'YYYY"M"MM') + INTERVAL '1 month - 1 day') AS snapshot_date,
    mcid,
    customer_bridge,
    customer_arr_change_ccfx,
    customer_arr_change_lcu,
    baseline_currency,
    CASE
      WHEN customer_arr_change_ccfx > 0 THEN '+'
      WHEN customer_arr_change_ccfx < 0 THEN '-'
      ELSE 'flat'
    END AS pos_neg_flag
  FROM ufdm_archive.sst_customer_bridge_total_temp_lcoked_251209_0226
  WHERE customer_bridge IN ('Up Sell', 'Cross-sell', 'Downsell', 'Downgrade')
),

-- 3) Join customer to product (only same mcid, same period, same currency, same sign):
InitialJoin AS (
  SELECT
    cb.evaluation_period              AS cb_evaluation_period,
    cb.snapshot_date,
    cb.mcid,
    cb.customer_bridge,
    cb.customer_arr_change_ccfx,
    cb.customer_arr_change_lcu,
    cb.baseline_currency,
    cb.pos_neg_flag                   AS cb_pos_neg_flag,
    pg.currency_code,
    pg.bridge_part,
    pg.migration_part,
    pg.pathways,
    pg.pos_neg_flag                   AS pg_pos_neg_flag,
    pg.product_arr_change_ccfx,
    pg.product_arr_change_lcu,

    -- guard against divide-by-zero (NULL if customer_arr_change_ccfx = 0)
    CASE
      WHEN cb.customer_arr_change_ccfx = 0 THEN NULL
      ELSE
        100.0 * ABS(
          ABS(cb.customer_arr_change_ccfx) - ABS(pg.product_arr_change_ccfx)
        ) / ABS(cb.customer_arr_change_ccfx)
    END AS arr_change_difference

  FROM cb_part cb
  JOIN pg_part pg
    ON cb.mcid = pg.mcid
   AND cb.evaluation_period = pg.evaluation_period
   AND cb.baseline_currency = pg.currency_code
   AND cb.pos_neg_flag = pg.pos_neg_flag
  WHERE pg.pathways IS NOT NULL
),

-- 4) Rank each joined row by smallest ARR gap within (mcid, period, currency, pathways):
RankedJoins AS (
  SELECT
    ij.*,
    CASE
      WHEN ij.arr_change_difference <= 3 THEN 1
      ELSE 2
    END AS flg_1,  -- 1 if ≤3% gap

    CASE
      WHEN LOWER(TRIM(ij.customer_bridge)) = LOWER(TRIM(ij.bridge_part)) THEN 1
      ELSE 2
    END AS flg_2,  -- 1 if exact text match

    ROW_NUMBER() OVER (
      PARTITION BY
        ij.mcid,
        ij.cb_evaluation_period,
        ij.baseline_currency,
        ij.pathways,
        ij.customer_bridge,
        ij.customer_arr_change_ccfx
      ORDER BY
        ij.arr_change_difference
    ) AS best_match_rank

  FROM InitialJoin ij
),

-- 5a) Pick out any row where flg_1 = 1 (close match):
first_case AS (
  SELECT *
  FROM RankedJoins
  WHERE flg_1 = 1
),

-- 5b) Fallback: for any customer_bucket that never got a flg_1=1, collect all rows:
second_part AS (
  SELECT *
  FROM RankedJoins rj
  WHERE NOT EXISTS (
    SELECT 1
    FROM RankedJoins rj2
    WHERE rj2.flg_1 = 1
      AND rj2.mcid = rj.mcid
      AND rj2.cb_evaluation_period = rj.cb_evaluation_period
      AND rj2.baseline_currency = rj.baseline_currency
      AND rj2.customer_arr_change_ccfx = rj.customer_arr_change_ccfx
      AND rj2.customer_bridge = rj.customer_bridge
  )
),

-- 5c) Further remove from fallback any product_bucket that was used as flg_1=1 by anyone:
second_part_two AS (
  SELECT *
  FROM second_part rj
  WHERE NOT EXISTS (
    SELECT 1
    FROM RankedJoins rj2
    WHERE rj2.flg_1 = 1
      AND rj2.mcid = rj.mcid
      AND rj2.cb_evaluation_period = rj.cb_evaluation_period
      AND rj2.currency_code = rj.currency_code
      AND rj2.pathways = rj.pathways
      AND rj2.bridge_part = rj.bridge_part
      AND rj2.product_arr_change_ccfx = rj.product_arr_change_ccfx
  )
),

-- 6) Return all “first_case” (flg_1=1) + all “fallbacks” (second_part_two)
final AS (
  SELECT *
  FROM first_case

  UNION

  SELECT *
  FROM second_part_two
),

-- 7) De-duplicate on (cb_evaluation_period, snapshot_date, mcid, customer_bridge, customer_arr_change_ccfx, cb_pos_neg_flag, currency_code)
deduped AS (
  SELECT
    f.*,
    ROW_NUMBER() OVER (
      PARTITION BY
        f.cb_evaluation_period,
        f.snapshot_date,
        f.mcid,
        f.customer_bridge,
        f.customer_arr_change_ccfx,
        f.cb_pos_neg_flag,
        f.currency_code
      ORDER BY
        f.flg_1 ASC,              -- prefer flg_1 = 1
        f.flg_2 ASC,              -- then prefer flg_2 = 1
        f.arr_change_difference   -- then the smallest difference
    ) AS dedup_customer_side ,
      ROW_NUMBER() OVER (
      PARTITION BY
        f.cb_evaluation_period,
        f.mcid,
        f.bridge_part,
        f.pathways,
        f.product_arr_change_ccfx,
        f.pg_pos_neg_flag,
        f.currency_code
      ORDER BY
        f.flg_1 ASC,              -- prefer flg_1 = 1
        f.flg_2 ASC,              -- then prefer flg_2 = 1
        f.arr_change_difference   -- then the smallest difference
    ) AS dedup_product_side
  FROM final f
--   where mcid = '1b026b3d-992b-e111-9eb3-0050568d002c' and cb_evaluation_period = '2024M05'
)

-- 8) Final output: only rows where rn = 1 (unique per specified keys)
, finalised_join as (
SELECT
  deduped.cb_evaluation_period ,
  deduped.snapshot_date,
  deduped.mcid,
  deduped.customer_bridge,
  deduped.customer_arr_change_ccfx,
  deduped.customer_arr_change_lcu,
  deduped.baseline_currency,
  deduped.cb_pos_neg_flag,
  deduped.currency_code,
  deduped.bridge_part,
  deduped.migration_part,
  deduped.pathways,
  deduped.pg_pos_neg_flag,
  deduped.product_arr_change_ccfx,
  deduped.product_arr_change_lcu,
  deduped.arr_change_difference,
  deduped.flg_1,
  deduped.flg_2,
  deduped.best_match_rank
FROM deduped
WHERE
--     deduped.dedup_customer_side = 1 and
   deduped.dedup_product_side = 1
ORDER BY
  deduped.cb_evaluation_period,
  deduped.snapshot_date,
  deduped.mcid,
  deduped.customer_bridge
)
--    select * from mid_joiner;

   , joint_part AS (
    SELECT
        rj.cb_evaluation_period AS evaluation_period,
        rj.mcid,
        rj.currency_code,
        rj.bridge_part,
        rj.migration_part,
        rj.pathways,
        rj.product_arr_change_ccfx,
        rj.product_arr_change_lcu,
        rj.customer_bridge,
        rj.customer_arr_change_ccfx,
        rj.customer_arr_change_lcu
    FROM
        finalised_join rj
--     WHERE
--         flg_1 = 1
--         rj.rn =1
--         rj.arr_change_difference = rj.min_arr_difference or rj.rn <= 2
--        (rj.has_close_arr_match = 0 OR LOWER(TRIM(rj.customer_bridge)) = LOWER(TRIM(rj.bridge_part)))
)
--     select * from joint_part where mcid = '6521c9b4-60e4-e411-9afb-0050568d2da8' and evaluation_period ='2021M12';
-- ,


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
-- select * from logic_base


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
-- select * from flagging_table ;

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
    from sandbox.sst_customer_bridge_total_mig  as a
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
    from sandbox.sst_customer_bridge_total_mig  as a
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

Delete from sandbox.sst_customer_bridge_total_mig as a 
Using sandbox.customer_churn_migration_default_cases as b
where a.mcid = b.mcid
and a.evaluation_period = b.evaluation_period
and a.baseline_currency = b.baseline_currency
and lower(trim(a.customer_bridge)) = lower(trim(b.customer_bridge));

--Inserting default cases with new Classification and pathawys

Insert into sandbox.sst_customer_bridge_total_mig as a  (
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



Delete from sandbox.sst_customer_bridge_total_mig as a 
Using sandbox.customer_churn_migration_split_cases as b
where a.mcid = b.mcid
and a.evaluation_period = b.evaluation_period
and a.baseline_currency = b.baseline_currency
and lower(trim(a.customer_bridge)) = lower(trim(b.customer_bridge));



Insert into sandbox.sst_customer_bridge_total_mig as a  (
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



Insert into sandbox.sst_customer_bridge_total_mig as a  (
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
FROM sandbox.sst_customer_bridge_total_mig
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
TRUNCATE TABLE sandbox.sst_customer_bridge_total_mig;
INSERT INTO sandbox.sst_customer_bridge_total_mig ( 
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