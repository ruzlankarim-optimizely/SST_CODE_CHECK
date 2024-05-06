--#############################################
--CHURN MIGRATION
--#############################################
DROP TABLE IF EXISTS churn_migration_classifiers;
create temp table churn_migration_classifiers as
select *
from sandbox.churn_migration_classifiers
where evaluation_period = var_period;
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
      rt.product_arr_change_ccfx as pg_arr_change,
      rt.product_arr_change_lcu as pg_arr_change_lcu,
      rt.product_bridge as pg_bridge
    from churn_migration_classifiers it3
      left join arr_product_bridge_tmp rt -- replace with product group bridge
      -- Here 
      on it3.evaluation_period = rt.evaluation_period
      and COALESCE (
        it3.prior_product_group,
        it3.current_product_group
      ) = COALESCE (
        rt.prior_product_family,
        rt.current_product_family
      )
      and it3.mcid = rt.mcid
      and it3.currency_code = rt.baseline_currency
      AND it3.product_arr_change_ccfx <> 0 --         WHERE it3.mcid = 'bbeaf423-e118-e211-83c1-0050568d002c' 
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
          prior_product_family_class,
          current_product_family_class --          COALESCE (prior_product_group , current_product_group) 
          --          ,COALESCE (prior_product_family_class,current_product_family_class)
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
          prior_product_family_class,
          current_product_family_class --          COALESCE (prior_product_group ,current_product_group) 
          --          ,COALESCE (prior_product_family_class,current_product_family_class)
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
        AND pg_bridge <> 'Price Uplift' THEN case
          when pg_bridge = 'New'
          or pg_bridge = 'Churn' then "Movement Classification"
          else concat(
            pg_bridge,
            ' - ',
            split_part("Movement Classification", ' - ', 2)
          )
        end
        else null
      end as "PG Migration: Classification",
      case
        when "PG Leftover: Rolled Up Amount" is not null
        AND pg_bridge <> 'Flat'
        AND pg_bridge <> 'Price Uplift' then pg_bridge
        else null
      end as "PG Leftover: Classification"
    from initial_table_8
  ),
  double_classification_fix AS (
    select *,
      sum(pg_arr_change) OVER (
        PARTITION BY evaluation_period,
        mcid,
        pg_bridge,
        pg_arr_change
      ) AS product_bridge_sum,
      sum("PG Migration: Rolled Up Amount") over(
        PARTITION BY evaluation_period,
        mcid,
        "PG Leftover: Classification",
        pg_arr_change
      ) AS total_migration_amount_ccfx,
      sum("PG Migration: Rolled Up Amount LCU") over(
        PARTITION BY evaluation_period,
        mcid,
        "PG Leftover: Classification",
        pg_arr_change
      ) AS total_migration_amount_lcu,
      count(mcid) OVER(
        PARTITION BY evaluation_period,
        mcid,
        pg_bridge,
        "PG Leftover: Classification",
        pg_arr_change
      ) AS count_migrations,
      (
        "PG Migration: Rolled Up Amount" + "PG Leftover: Rolled Up Amount"
      ) AS total_leftover_amount_ccfx,
      (
        "PG Migration: Rolled Up Amount LCU" + "PG Leftover: Rolled Up Amount LCU"
      ) AS total_leftover_amount_lcu --      sum("PG Migration: Rolled Up Amount") over(PARTITION BY evaluation_period , mcid , "PG Leftover: Classification",
      --        pg_arr_change) AS total_migration_amount 
    from initial_table_9
  ),
  double_classification_marker AS (
    SELECT *,
      (
        (
          total_leftover_amount_ccfx - total_migration_amount_ccfx
        ) / count_migrations
      ) AS new_leftover_value_ccfx,
      (
        (
          total_leftover_amount_lcu - total_migration_amount_lcu
        ) / count_migrations
      ) AS new_leftover_value_lcu,
      CASE
        WHEN "PG Leftover: Classification" IS NOT NULL
        AND product_bridge_sum > total_leftover_amount_ccfx
        AND count_migrations > 1 THEN TRUE
        ELSE FALSE
      END AS double_classification_marker_flag
    FROM double_classification_fix --    WHERE mcid IN ('1b026b3d-992b-e111-9eb3-0050568d002c' , '3ac6acec-1eaa-db11-8952-0018717a8c82',  '446b7906-ad5e-e111-9125-0050568d002c', '97abfc08-bbc8-e511-8123-c4346baccd14' , '99635c65-20cc-c9eb-89c3-576b1ca33dd1')
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
    prior_period_product_arr_usd_ccfx,
    current_period_product_arr_usd_ccfx,
    product_arr_change_ccfx,
    prior_period_product_arr_lcu,
    current_period_product_arr_lcu,
    product_arr_change_lcu,
    product_bridge,
    prior_product_group,
    current_product_group,
    current_product_family_class,
    prior_product_family_class,
    "Downgraded a Licenses  Product in Current Date",
    "Downgraded a Everweb  Product in Current Date",
    "Downgraded a Ektron  Product in Current Date",
    "Downgraded a Personalized Find  Product in Current Date",
    "Downgraded a Visitor Int  Product in Current Date",
    "Churned a Licenses Product in Current Date",
    "Churned a Everweb Product in Current Date",
    "Churned a Ektron Product in Current Date",
    "Churned a Personalized Find Product in Current Date",
    "Churned a Visitor Int Product in Current Date",
    "Added a Cloud Product in Current Date",
    "Added a CMS Product in Current Date",
    "Added a Content Graph Product in Current Date",
    "Added a ODP Product in Current Date",
    "Increased a Cloud Product in Current Date",
    "Increased a CMS Product in Current Date",
    "Increased a Content Graph Product in Current Date",
    "Increased a ODP Product in Current Date",
    "Cloud Product in Current Date with ARR",
    "CMS  Product in Current Date with ARR",
    "Content Graph Product in Current Date with ARR",
    "ODP Product in Current Date with ARR",
    "Licenses Product in Previous Date with ARR",
    "Everweb Product in Previous Date with ARR",
    "Ektron Product in Previous Date with ARR",
    "Personalized Find Product in Previous Date with ARR",
    "Visitor Int Product in Previous Date with ARR",
    "Movement Classification",
    "Movement Type-PF",
    pg_arr_change,
    pg_arr_change_lcu,
    pg_bridge,
    "Movement Type-PG",
    "Sum of Positive or Negative Movements-PG",
    "Min/Max PF Level movement",
    "PG Migration: Rolled Up Amount",
    CASE
      WHEN double_classification_marker_flag = TRUE THEN CASE
        WHEN new_leftover_value_ccfx > 0 THEN new_leftover_value_ccfx
        ELSE NULL
      END
      ELSE "PG Leftover: Rolled Up Amount"
    END AS "PG Leftover: Rolled Up Amount",
    "PG Migration: Rolled Up Amount LCU",
    CASE
      WHEN double_classification_marker_flag = TRUE THEN CASE
        WHEN new_leftover_value_lcu > 0 THEN new_leftover_value_lcu
        ELSE NULL
      END
      ELSE "PG Leftover: Rolled Up Amount LCU"
    END AS "PG Leftover: Rolled Up Amount LCU",
    "PG Migration: Classification",
    CASE
      WHEN double_classification_marker_flag = TRUE THEN CASE
        WHEN new_leftover_value_ccfx > 0 THEN "PG Leftover: Classification"
        ELSE NULL
      END
      ELSE "PG Leftover: Classification"
    END AS "PG Leftover: Classification"
  FROM double_classification_marker --WHERE mcid = '1b026b3d-992b-e111-9eb3-0050568d002c' 
    --  WHERE pg_bridge IS NOT NULL 
);
DROP TABLE IF EXISTS sandbox.churn_migration_test_pg;
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
FROM arr_product_bridge_tmp AS a
  JOIN sandbox.churn_migration_test_pg AS b ON a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.baseline_currency = b.currency_code
  AND COALESCE (
    a.prior_product_family,
    a.current_product_family
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
FROM arr_product_bridge_tmp AS a
  JOIN sandbox.churn_migration_test_pg AS b ON a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.baseline_currency = b.currency_code
  AND COALESCE (
    a.prior_product_family,
    a.current_product_family
  ) = COALESCE(b.prior_product_group, b.current_product_group) --  AND a.prior_product_family = b.prior_product_group
  AND a.product_bridge = b.pg_bridge --AND round(a.product_arr_change_ccfx)  = round(b.pg_arr_change) 
WHERE "PG Migration: Classification" ILIKE ('%migration%')
  AND "PG Leftover: Rolled Up Amount" IS NOT NULL;
--
DELETE FROM arr_product_bridge_tmp AS a USING sandbox.PG_migration_default AS b
WHERE a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.baseline_currency = b.baseline_currency
  AND COALESCE (
    a.prior_product_family,
    a.current_product_family
  ) = COALESCE(
    b.prior_product_family,
    b.current_product_family
  )
  AND a.product_bridge = b.product_bridge;
--
--  SELECT * FROM sandbox.PG_migration_default
INSERT INTO arr_product_bridge_tmp AS a (
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    current_product_family,
    prior_product_family,
    current_end_customer,
    prior_end_customer,
    baseline_currency,
    current_arr_usd_ccfx,
    prior_arr_usd_ccfx,
    product_arr_change_ccfx,
    current_arr_lcu,
    prior_arr_lcu,
    product_arr_change_lcu,
    product_bridge
  )
SELECT evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  current_product_family,
  prior_product_family,
  current_end_customer,
  prior_end_customer,
  baseline_currency,
  prior_arr_usd_ccfx * abs(
    "PG Migration: Rolled Up Amount" / product_arr_change_ccfx
  ),
  current_arr_usd_ccfx * abs(
    "PG Migration: Rolled Up Amount" / product_arr_change_ccfx
  ),
  --  product_arr_change_ccfx ,
  "PG Migration: Rolled Up Amount",
  prior_arr_lcu * abs(
    "PG Migration: Rolled Up Amount LCU" / product_arr_change_lcu
  ),
  current_arr_lcu * abs(
    "PG Migration: Rolled Up Amount LCU" / product_arr_change_lcu
  ),
  "PG Migration: Rolled Up Amount LCU",
  COALESCE("PG Migration: Classification", product_bridge)
FROM sandbox.PG_migration_default AS b
WHERE mcid = b.mcid
  AND evaluation_period = b.evaluation_period
  AND baseline_currency = b.baseline_currency
  AND COALESCE (
    prior_product_family,
    current_product_family
  ) = COALESCE(
    b.prior_product_family,
    b.current_product_family
  )
  AND product_bridge = b.product_bridge;
--
--
DELETE FROM arr_product_bridge_tmp AS a USING sandbox.PG_migration_split AS b
WHERE a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.baseline_currency = b.baseline_currency
  AND COALESCE (
    a.prior_product_family,
    a.current_product_family
  ) = COALESCE(
    b.prior_product_family,
    b.current_product_family
  )
  AND a.product_bridge = b.product_bridge;
--
--
INSERT INTO arr_product_bridge_tmp AS a (
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    current_product_family,
    prior_product_family,
    current_end_customer,
    prior_end_customer,
    baseline_currency,
    current_arr_usd_ccfx,
    prior_arr_usd_ccfx,
    product_arr_change_ccfx,
    current_arr_lcu,
    prior_arr_lcu,
    product_arr_change_lcu,
    product_bridge
  )
SELECT evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  current_product_family,
  prior_product_family,
  current_end_customer,
  prior_end_customer,
  baseline_currency,
  prior_arr_usd_ccfx * abs(
    "PG Migration: Rolled Up Amount" / product_arr_change_ccfx
  ),
  current_arr_usd_ccfx * abs(
    "PG Migration: Rolled Up Amount" / product_arr_change_ccfx
  ),
  --  product_arr_change_ccfx ,
  "PG Migration: Rolled Up Amount",
  prior_arr_lcu * abs(
    "PG Migration: Rolled Up Amount LCU" / product_arr_change_lcu
  ),
  current_arr_lcu * abs(
    "PG Migration: Rolled Up Amount LCU" / product_arr_change_lcu
  ),
  "PG Migration: Rolled Up Amount LCU",
  --    product_bridge ,
  COALESCE("PG Migration: Classification", product_bridge)
FROM sandbox.PG_migration_split AS b
WHERE mcid = b.mcid
  AND evaluation_period = b.evaluation_period
  AND baseline_currency = b.baseline_currency
  AND COALESCE (
    prior_product_family,
    current_product_family
  ) = COALESCE(
    b.prior_product_family,
    b.current_product_family
  )
  AND product_bridge = b.product_bridge;
INSERT INTO arr_product_bridge_tmp AS a (
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    current_product_family,
    prior_product_family,
    current_end_customer,
    prior_end_customer,
    baseline_currency,
    current_arr_usd_ccfx,
    prior_arr_usd_ccfx,
    product_arr_change_ccfx,
    current_arr_lcu,
    prior_arr_lcu,
    product_arr_change_lcu,
    product_bridge
  )
SELECT evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  current_product_family,
  prior_product_family,
  current_end_customer,
  prior_end_customer,
  baseline_currency,
  prior_arr_usd_ccfx * abs(
    "PG Leftover: Rolled Up Amount" / product_arr_change_ccfx
  ),
  current_arr_usd_ccfx * abs(
    "PG Leftover: Rolled Up Amount" / product_arr_change_ccfx
  ),
  --  product_arr_change_ccfx ,
  -- change this to default once and then to migrated value
  "PG Leftover: Rolled Up Amount",
  prior_arr_lcu * abs(
    "PG Leftover: Rolled Up Amount LCU" / product_arr_change_lcu
  ),
  current_arr_lcu * abs(
    "PG Leftover: Rolled Up Amount LCU" / product_arr_change_lcu
  ),
  "PG Leftover: Rolled Up Amount LCU",
  --    default_value_lcu ,
  --    product_bridge ,
  COALESCE("PG Leftover: Classification", product_bridge)
FROM sandbox.PG_migration_split AS b
WHERE mcid = b.mcid
  AND evaluation_period = b.evaluation_period
  AND baseline_currency = b.baseline_currency
  AND COALESCE (
    prior_product_family,
    current_product_family
  ) = COALESCE(
    b.prior_product_family,
    b.current_product_family
  )
  AND product_bridge = b.product_bridge;
ALTER TABLE arr_product_bridge_tmp
ADD pathways VARCHAR(255);
--
--
--
UPDATE arr_product_bridge_tmp AS a
SET product_bridge = split_part(product_bridge, ' -- ', 1),
  pathways = split_part(product_bridge, ' -- ', 2)
WHERE product_bridge ILIKE '%migration --%';
drop table if exists sandbox.temp_arr_table;
create table sandbox.temp_arr_table as
SELECT evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  current_product_family,
  prior_product_family,
  current_end_customer,
  prior_end_customer,
  baseline_currency,
  product_bridge,
  pathways,
  sum(current_arr_usd_ccfx) AS current_arr_usd_ccfx,
  sum(prior_arr_usd_ccfx) AS prior_arr_usd_ccfx,
  sum(product_arr_change_ccfx) AS product_arr_change_ccfx,
  sum(current_arr_lcu) AS current_arr_lcu,
  sum(prior_arr_lcu) AS prior_arr_lcu,
  sum(product_arr_change_lcu) AS product_arr_change_lcu
FROM arr_product_bridge_tmp
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
TRUNCATE TABLE arr_product_bridge_tmp;
INSERT INTO arr_product_bridge_tmp(
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    current_product_family,
    prior_product_family,
    current_end_customer,
    prior_end_customer,
    baseline_currency,
    product_bridge,
    pathways,
    current_arr_usd_ccfx,
    prior_arr_usd_ccfx,
    product_arr_change_ccfx,
    current_arr_lcu,
    prior_arr_lcu,
    product_arr_change_lcu
  )
select evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  current_product_family,
  prior_product_family,
  current_end_customer,
  prior_end_customer,
  baseline_currency,
  product_bridge,
  pathways,
  sum(current_arr_usd_ccfx) AS current_arr_usd_ccfx,
  sum(prior_arr_usd_ccfx) AS prior_arr_usd_ccfx,
  sum(product_arr_change_ccfx) AS product_arr_change_ccfx,
  sum(current_arr_lcu) AS current_arr_lcu,
  sum(prior_arr_lcu) AS prior_arr_lcu,
  sum(product_arr_change_lcu) AS product_arr_change_lcu
FROM sandbox.temp_arr_table
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