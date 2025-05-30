DROP TABLE IF EXISTS sandbox.sst_product_group_bridge_cloud;
CREATE TABLE sandbox.sst_product_group_bridge_cloud AS
SELECT *
FROM ufdm_archive.sst_pb_product_group_cloud_license_lcoked_18022025_1547;

--#############################################
--CHURN MIGRATION
--#############################################
--SELECT * FROM arr_product_bridge_tmp WHERE mcid = 'f3909c43-53c3-e611-80f1-c4346bac4838'
----
Drop table if exists churn_migration_classifiers_pg;
CREATE temp table churn_migration_classifiers_pg as (
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
      concat(
        rt.current_product_group,
        '-',
        rt.prior_product_group
      ) AS pg_ps,
      rt.product_arr_change_ccfx as pg_arr_change,
      rt.product_arr_change_lcu as pg_arr_change_lcu,
      rt.product_bridge as pg_bridge
    FROM sandbox.churn_migration_classifiers_max_value it3
      left join sandbox.sst_product_group_bridge_cloud rt -- replace with product group bridge
      -- Here
      on it3.evaluation_period = rt.evaluation_period
      and COALESCE (
        it3.prior_product_group,
        it3.current_product_group
      ) = COALESCE (
        rt.prior_product_group,
        rt.current_product_group
      )
      and it3.mcid = rt.mcid
      and it3.currency_code = rt.currency_code --      AND it3.product_arr_change_ccfx <> 0
    WHERE rt.product_bridge IN (
        'Flat',
        'New',
        'Up Sell',
        'Churn',
        'Cross-sell',
        'Downsell',
        'Price Uplift',
        'Downgrade'
      ) --      AND it3.mcid = 'bbeaf423-e118-e211-83c1-0050568d002c'
      --         'f677c904-1faa-db11-8952-0018717a8c82'
      --
      --      AND it3.evaluation_period = var_period
  ) --  SELECT
,
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
          prior_product_group,
          current_product_group,
          prior_pathways,
          current_pathways --          COALESCE (prior_product_group , current_product_group)
          --          ,COALESCE (prior_pathways,current_pathways)
        )
        when "Movement Type-PG" = '+'
        and "Movement Type-PF" = '+' then sum(product_arr_change_ccfx) filter(
          where "Movement Type-PF" = '+'
        ) over(
          partition by mcid,
          evaluation_period,
          currency_code,
          prior_product_group,
          current_product_group,
          prior_pathways,
          current_pathways --          COALESCE (prior_product_group ,current_product_group)
          --          ,COALESCE (prior_pathways,current_pathways)
        )
        else null
      end as "Sum of Positive or Negative Movements-PG"
    from initial_table_5
  ) --  SELECT * FROM initial_table_6
,
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
  ) --    SELECT
  --        * ,
  --        sum(1 )
  --    FROM initial_table_7
,
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
        when "PG Migration: Rolled Up Amount" is not NULL
        AND pg_bridge <> 'Flat'
        AND pg_bridge <> 'Price Uplift'
        AND pg_bridge <> 'New'
        AND pg_bridge <> 'Churn' THEN --  THEN case
        --   when pg_bridge = 'New'
        --   or pg_bridge = 'Churn' then "Movement Classification"
        --   else
        concat(
          pg_bridge,
          ' - ',
          split_part("Movement Classification", ' - ', 2)
        ) -- end
        else null
      end as "PG Migration: Classification",
      case
        when "PG Leftover: Rolled Up Amount" is not null
        AND pg_bridge <> 'Flat'
        AND pg_bridge <> 'Price Uplift'
        AND pg_bridge <> 'New'
        AND pg_bridge <> 'Churn' then pg_bridge
        else null
      end as "PG Leftover: Classification"
    from initial_table_8
  ),
  adding_classification AS (
    SELECT *,
      split_part("PG Migration: Classification", ' -- ', 1) bridge_part,
      split_part("PG Migration: Classification", ' -- ', 2) pathways_part
    FROM initial_table_9
    WHERE "PG Migration: Classification" IS NOT NULL
  ) --  SELECT
  --  current_pathways  , prior_pathways  ,
  --  product_arr_change_ccfx  , "Movement Classification" , pg_arr_change  ,pg_bridge,  "PG Migration: Classification" , "PG Leftover: Classification" , bridge_part ,pathways_part
  --  FROM adding_classification
,
  double_classification_fix AS (
    select *,
      sum("PG Migration: Rolled Up Amount") over(
        PARTITION BY evaluation_period,
        mcid,
        bridge_part,
        pg_ps,
        --        pathways_part,
        pg_arr_change
      ) AS total_migration_amount_ccfx,
      sum("PG Migration: Rolled Up Amount LCU") over(
        PARTITION BY evaluation_period,
        mcid,
        bridge_part,
        pg_ps,
        --        pathways_part,
        pg_arr_change
      ) AS total_migration_amount_lcu
    from adding_classification
  ),
  double_classification_fix_2 AS (
    SELECT *,
      count(mcid) OVER(
        PARTITION BY evaluation_period,
        mcid,
        pg_ps,
        bridge_part,
        --        pathways_part,
        total_migration_amount_ccfx
      ) AS count_migrations,
      --      DENSE_RANK() OVER (PARTITION BY evaluation_period  , mcid ,bridge_part,pg_arr_change,"PG Migration: Rolled Up Amount" ORDER BY pathways_part ASC ) +
      --      DENSE_RANK() OVER (PARTITION BY  evaluation_period  , mcid ,bridge_part,"PG Migration: Rolled Up Amount" ORDER BY pathways_part DESC)  - 1 AS test_migrations ,
      --    count(  CASE WHEN "Movement Classification" IS NOT NULL THEN "Movement Classification"  END  ) over(PARTITION BY
      --    evaluation_period  ,
      --    mcid ,
      --    pg_bridge  ,
      --    pg_arr_change
      --    ) AS count_movements ,
      --    DENSE_RANK() OVER (PARTITION BY evaluation_period  , mcid ,pg_bridge,pg_arr_change   ORDER BY pg_ps ASC ) AS part_1 ,
      --    DENSE_RANK() OVER (PARTITION BY  evaluation_period  , mcid, pg_bridge,pg_arr_change ORDER BY pg_ps  DESC) AS part_2,
      DENSE_RANK() OVER (
        PARTITION BY evaluation_period,
        mcid,
        pg_bridge,
        pg_arr_change,
        total_migration_amount_ccfx
        ORDER BY pg_ps ASC
      ) + DENSE_RANK() OVER (
        PARTITION BY evaluation_period,
        mcid,
        pg_bridge,
        pg_arr_change,
        total_migration_amount_ccfx
        ORDER BY pg_ps DESC
      ) - 1 AS count_movements --    DENSE_RANK() OVER (PARTITION BY evaluation_period  ,
      --    mcid ,
      --    current_product_solution,
      --    prior_product_solution ,
      --    pg_bridge  ,
      --    pg_arr_change   ORDER BY pg_arr_change ASC ) +
      --    DENSE_RANK() OVER (PARTITION BY evaluation_period  ,
      --    mcid ,
      --    current_product_solution,
      --    prior_product_solution ,
      --    pg_bridge  ,
      --    pg_arr_change ORDER BY pg_arr_change DESC) - 1 AS count_movements
    FROM double_classification_fix
  ),
  --    SELECT * FROM double_classification_fix WHERE "PG Migration: Classification" IS NOT NUL
  double_classification_marker AS (
    SELECT *,
      CASE
        WHEN "PG Leftover: Classification" IS NOT NULL
        AND count_migrations > 1
        AND abs(total_migration_amount_ccfx) < abs(pg_arr_change) THEN round(
          (pg_arr_change - total_migration_amount_ccfx) / count_migrations
        )
        ELSE NULL
      END AS new_leftover_value_ccfx,
      CASE
        WHEN "PG Leftover: Classification" IS NOT NULL
        AND count_migrations > 1
        AND abs(total_migration_amount_ccfx) < abs(pg_arr_change) THEN round(
          (pg_arr_change_lcu - total_migration_amount_lcu) / count_migrations
        )
        ELSE NULL
      END AS new_leftover_value_lcu,
      CASE
        WHEN (
          count_migrations > 1
          OR count_migrations >= count_movements
        )
        AND (count_migrations <> count_movements)
        AND abs(total_migration_amount_ccfx) > abs(pg_arr_change) THEN CASE
          WHEN (
            "PG Migration: Rolled Up Amount" / total_migration_amount_ccfx
          ) * 2 = 1 THEN (
            "PG Migration: Rolled Up Amount" / total_migration_amount_ccfx
          ) * pg_arr_change
          ELSE round(
            (total_migration_amount_ccfx - pg_arr_change) *(
              "PG Migration: Rolled Up Amount" / total_migration_amount_ccfx
            )
          )
        END
        ELSE NULL
      END AS subtracted_amount_ccfx,
      CASE
        WHEN (
          count_migrations > 1
          OR count_migrations >= count_movements
        )
        AND (count_migrations <> count_movements)
        AND abs(total_migration_amount_lcu) > abs(pg_arr_change_lcu) THEN CASE
          WHEN (
            "PG Migration: Rolled Up Amount LCU" / total_migration_amount_lcu
          ) * 2 = 1 THEN (
            "PG Migration: Rolled Up Amount LCU" / total_migration_amount_lcu
          ) * pg_arr_change_lcu
          ELSE round(
            (total_migration_amount_lcu - pg_arr_change_lcu) *(
              "PG Migration: Rolled Up Amount LCU" / total_migration_amount_lcu
            )
          )
        END
        ELSE NULL
      END AS subtracted_amount_lcu,
      CASE
        WHEN "PG Leftover: Classification" IS NOT NULL
        AND count_migrations > 1
        AND abs(total_migration_amount_ccfx) = abs(pg_arr_change) THEN TRUE
        ELSE FALSE
      END AS double_classification_first_case_flag,
      CASE
        WHEN "PG Leftover: Classification" IS NOT NULL
        AND count_migrations > 1
        AND abs(total_migration_amount_ccfx) < abs(pg_arr_change) THEN TRUE
        ELSE FALSE
      END AS double_migration_second_case,
      CASE
        WHEN (
          count_migrations > 1
          OR count_migrations >= count_movements
        )
        AND (count_migrations <> count_movements)
        AND abs(total_migration_amount_ccfx) > abs(pg_arr_change) THEN TRUE
        ELSE FALSE
      END AS double_migration_third_case
    FROM double_classification_fix_2
  ) --          SELECT * FROM double_classification_marker
  SELECT evaluation_period,
    current_period ,
    prior_period ,
    mcid,
    current_product_group,
    prior_product_group,
    current_product_solution,
    prior_product_solution,
    current_pathways,
    prior_pathways,
    currency_code,
    current_period_product_arr_usd_ccfx,
    prior_period_product_arr_usd_ccfx,
    product_arr_change_ccfx,
    current_period_product_arr_lcu,
    prior_period_product_arr_lcu,
    product_arr_change_lcu,
    active_flag_count,
    flag_descriptions,
    "Movement Classification",
    "Movement Type-PF",
    pg_arr_change,
    pg_arr_change_lcu,
    pg_bridge,
    "Movement Type-PG",
    "Sum of Positive or Negative Movements-PG",
    "Min/Max PF Level movement",
    CASE
      WHEN double_migration_third_case = TRUE THEN "PG Migration: Rolled Up Amount" - subtracted_amount_ccfx
      ELSE "PG Migration: Rolled Up Amount"
    END AS "PG Migration: Rolled Up Amount",
    CASE
      WHEN double_migration_second_case = TRUE THEN new_leftover_value_ccfx
      ELSE CASE
        WHEN double_classification_first_case_flag = TRUE
        OR double_migration_third_case = TRUE THEN NULL
        ELSE "PG Leftover: Rolled Up Amount"
      END
    END AS "PG Leftover: Rolled Up Amount",
    CASE
      WHEN double_migration_third_case = TRUE THEN "PG Migration: Rolled Up Amount LCU" - subtracted_amount_lcu
      ELSE "PG Migration: Rolled Up Amount LCU"
    END AS "PG Migration: Rolled Up Amount LCU",
    CASE
      WHEN double_migration_second_case = TRUE THEN new_leftover_value_lcu
      ELSE CASE
        WHEN double_classification_first_case_flag = TRUE
        OR double_migration_third_case = TRUE THEN NULL
        ELSE "PG Leftover: Rolled Up Amount LCU"
      END
    END AS "PG Leftover: Rolled Up Amount LCU",
    "PG Migration: Classification",
    CASE
      WHEN double_migration_second_case = TRUE THEN "PG Leftover: Classification"
      ELSE CASE
        WHEN double_classification_first_case_flag = TRUE
        OR double_migration_third_case = TRUE THEN NULL
        ELSE "PG Leftover: Classification"
      END
    END AS "PG Leftover: Classification"
  FROM double_classification_marker --WHERE mcid = '1b026b3d-992b-e111-9eb3-0050568d002c'
    --  WHERE pg_bridge IS NOT NULL
);
DROP TABLE IF EXISTS sandbox.churn_migration_test_pg;
CREATE TABLE sandbox.churn_migration_test_pg AS WITH base AS (
  SELECT mcid,
    evaluation_period,
    TO_CHAR(
      (
        CASE
          WHEN current_period IS NULL
          AND prior_period IS NOT NULL THEN date(
            date_trunc('month', prior_period) + INTERVAL '1 month' + INTERVAL '1 month - 1 day'
          )
          ELSE current_period
        END
      )::DATE,
      'YYYY-MM'
    ) AS eval_period,
    currency_code,
    current_product_group,
    prior_product_group,
    pg_bridge,
    "PG Migration: Classification",
    "PG Leftover: Classification",
    sum ("PG Migration: Rolled Up Amount") AS "PG Migration: Rolled Up Amount",
    sum ("PG Leftover: Rolled Up Amount") AS "PG Leftover: Rolled Up Amount",
    sum ("PG Migration: Rolled Up Amount LCU") AS "PG Migration: Rolled Up Amount LCU",
    sum ("PG Leftover: Rolled Up Amount LCU") AS "PG Leftover: Rolled Up Amount LCU"
  FROM churn_migration_classifiers_pg
  WHERE mcid NOT IN ('-')
  GROUP BY 1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9
),
negative_base AS (
  SELECT *
  FROM base
  WHERE "PG Migration: Classification" ILIKE ('%migration%')
    AND "PG Migration: Rolled Up Amount" < 0
),
positve_base AS (
  SELECT *,
    rank() over(
      PARTITION BY mcid,
      currency_code
      order by eval_period
    ) AS rnk
  FROM base
  WHERE "PG Migration: Classification" ILIKE ('%migration%')
    AND "PG Migration: Rolled Up Amount" > 0
)
SELECT mcid,
  evaluation_period,
  eval_period,
  currency_code,
  current_product_group,
  prior_product_group,
  pg_bridge,
  "PG Migration: Classification",
  "PG Leftover: Classification",
  "PG Migration: Rolled Up Amount",
  "PG Leftover: Rolled Up Amount",
  "PG Migration: Rolled Up Amount LCU",
  "PG Leftover: Rolled Up Amount LCU"
FROM positve_base
WHERE rnk = 1
UNION ALL
SELECT *
FROM negative_base;
DROP TABLE IF EXISTS sandbox.PG_migration_default;
CREATE TABLE sandbox.PG_migration_default AS
SELECT a.*,
  "PG Migration: Rolled Up Amount",
  "PG Leftover: Rolled Up Amount",
  "PG Migration: Rolled Up Amount LCU",
  "PG Leftover: Rolled Up Amount LCU",
  pg_bridge,
  "PG Migration: Classification"
FROM sandbox.sst_product_group_bridge_cloud AS a
  JOIN sandbox.churn_migration_test_pg AS b ON a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.currency_code = b.currency_code
  AND COALESCE (
    a.prior_product_group,
    a.current_product_group
  ) = COALESCE(b.prior_product_group, b.current_product_group) --  AND a.prior_product_family = b.prior_product_group
  AND a.product_bridge = b.pg_bridge
WHERE lower("PG Migration: Classification") ILIKE ('%migration%')
  AND "PG Leftover: Rolled Up Amount" IS NULL;
--
DROP TABLE IF EXISTS sandbox.PG_migration_split;
CREATE TABLE sandbox.PG_migration_split AS
SELECT a.*,
  "PG Migration: Rolled Up Amount",
  "PG Leftover: Rolled Up Amount",
  "PG Migration: Rolled Up Amount LCU",
  "PG Leftover: Rolled Up Amount LCU",
  "PG Migration: Classification",
  "PG Leftover: Classification"
FROM sandbox.sst_product_group_bridge_cloud AS a
  JOIN sandbox.churn_migration_test_pg AS b ON a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.currency_code = b.currency_code
  AND COALESCE (
    a.prior_product_group,
    a.current_product_group
  ) = COALESCE(b.prior_product_group, b.current_product_group) --  AND a.prior_product_family = b.prior_product_group
  AND a.product_bridge = b.pg_bridge --AND round(a.product_arr_change_ccfx)  = round(b.pg_arr_change)
WHERE "PG Migration: Classification" ILIKE ('%migration%')
  AND "PG Leftover: Rolled Up Amount" IS NOT NULL;
--
DELETE FROM sandbox.sst_product_group_bridge_cloud AS a USING sandbox.PG_migration_default AS b
WHERE a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.currency_code = b.currency_code
  AND COALESCE (
    a.prior_product_group,
    a.current_product_group
  ) = COALESCE(
    b.prior_product_group,
    b.current_product_group
  )
  AND a.product_bridge = b.product_bridge;
--
--  SELECT * FROM sandbox.PG_migration_default
INSERT INTO sandbox.sst_product_group_bridge_cloud AS a (
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
  COALESCE("PG Migration: Classification", product_bridge),
  winback_period_days,
  wip_flag,
  price_increase_amount,
  subsidiary_entity_name,
  churn_period,
  customer_bridge,
  null as pathways
FROM sandbox.PG_migration_default AS b
WHERE mcid = b.mcid
  AND evaluation_period = b.evaluation_period
  AND currency_code = b.currency_code
  AND COALESCE (
    prior_product_group,
    current_product_group
  ) = COALESCE(
    b.prior_product_group,
    b.current_product_group
  )
  AND product_bridge = b.product_bridge;
--
--
DELETE FROM sandbox.sst_product_group_bridge_cloud AS a USING sandbox.PG_migration_split AS b
WHERE a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.currency_code = b.currency_code
  AND COALESCE (
    a.prior_product_group,
    a.current_product_group
  ) = COALESCE(
    b.prior_product_group,
    b.current_product_group
  )
  AND a.product_bridge = b.product_bridge;
--
--
INSERT INTO sandbox.sst_product_group_bridge_cloud AS a (
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
  current_product_group,
  prior_product_group,
  currency_code,
  prior_period_product_arr_usd_ccfx * abs(
    "PG Migration: Rolled Up Amount" / CASE
      WHEN product_arr_change_ccfx = 0 THEN 1
      ELSE product_arr_change_ccfx
    END
  ),
  current_period_product_arr_usd_ccfx * abs(
    "PG Migration: Rolled Up Amount" / CASE
      WHEN product_arr_change_ccfx = 0 THEN 1
      ELSE product_arr_change_ccfx
    END
  ),
  --  product_arr_change_ccfx ,
  "PG Migration: Rolled Up Amount",
  prior_period_product_arr_lcu * abs(
    "PG Migration: Rolled Up Amount LCU" / CASE
      WHEN product_arr_change_lcu = 0 THEN 1
      ELSE product_arr_change_lcu
    END
  ),
  current_period_product_arr_lcu * abs(
    "PG Migration: Rolled Up Amount LCU" / CASE
      WHEN product_arr_change_lcu = 0 THEN 1
      ELSE product_arr_change_lcu
    END
  ),
  "PG Migration: Rolled Up Amount LCU",
  --    product_bridge ,
  COALESCE("PG Migration: Classification", product_bridge),
  winback_period_days,
  wip_flag,
  price_increase_amount,
  subsidiary_entity_name,
  churn_period,
  customer_bridge,
  null as pathways
FROM sandbox.PG_migration_split AS b
WHERE mcid = b.mcid
  AND evaluation_period = b.evaluation_period
  AND currency_code = b.currency_code
  AND COALESCE (
    prior_product_group,
    current_product_group
  ) = COALESCE(
    b.prior_product_group,
    b.current_product_group
  )
  AND product_bridge = b.product_bridge;
INSERT INTO sandbox.sst_product_group_bridge_cloud AS a (
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
  current_product_group,
  prior_product_group,
  currency_code,
  prior_period_product_arr_usd_ccfx * abs(
    "PG Leftover: Rolled Up Amount" / CASE
      WHEN product_arr_change_ccfx = 0 THEN 1
      ELSE product_arr_change_ccfx
    END
  ),
  current_period_product_arr_usd_ccfx * abs(
    "PG Leftover: Rolled Up Amount" / CASE
      WHEN product_arr_change_ccfx = 0 THEN 1
      ELSE product_arr_change_ccfx
    END
  ),
  --  product_arr_change_ccfx ,
  -- change this to default once and then to migrated value
  "PG Leftover: Rolled Up Amount",
  prior_period_product_arr_lcu * abs(
    "PG Leftover: Rolled Up Amount LCU" / CASE
      WHEN product_arr_change_lcu = 0 THEN 1
      ELSE product_arr_change_lcu
    END
  ),
  current_period_product_arr_lcu * abs(
    "PG Leftover: Rolled Up Amount LCU" / CASE
      WHEN product_arr_change_lcu = 0 THEN 1
      ELSE product_arr_change_lcu
    END
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
  customer_bridge,
  null as pathways
FROM sandbox.PG_migration_split AS b
WHERE mcid = b.mcid
  AND evaluation_period = b.evaluation_period
  AND currency_code = b.currency_code
  AND COALESCE (
    prior_product_group,
    current_product_group
  ) = COALESCE(
    b.prior_product_group,
    b.current_product_group
  )
  AND product_bridge = b.product_bridge;
--
--
--
UPDATE sandbox.sst_product_group_bridge_cloud AS a
SET product_bridge = split_part(product_bridge, ' -- ', 1),
  pathways = split_part(product_bridge, ' -- ', 2)
WHERE product_bridge ILIKE '%migration --%';
drop table if exists sandbox.temp_arr_table;
create table sandbox.temp_arr_table as
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
FROM sandbox.sst_product_group_bridge_cloud
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
TRUNCATE TABLE sandbox.sst_product_group_bridge_cloud;
INSERT INTO sandbox.sst_product_group_bridge_cloud(
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
  customer_bridge,
  pathways
FROM sandbox.temp_arr_table;