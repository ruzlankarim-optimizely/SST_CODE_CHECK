create or replace function ryzlan.sp_populate_sst_product_bridge_product_group_half(var_period text, run_acquire_customers int) returns void language plpgsql as $$ BEGIN
DELETE from ryzlan.sst_product_bridge
where evaluation_period = var_period;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--SST product Bridge
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists prior_period_customer_arr_tmp;
create temp table prior_period_customer_arr_tmp as
SELECT snapshot_date,
  a.mcid as master_customer_id,
  case
    when coalesce(0, 0) = 1 then acquire_product_group
    else updated_product_group
  end as product_family,
  a.base_currency as baseline_currency,
  max(coalesce(a.end_name, a.parent_name)) as end_customer,
  sum(arr) AS arr_usd_ccfx,
  sum(baseline_arr_local_currency) AS arr_lcu
FROM ryzlan.sst_adhoc a
WHERE 1 = 1
  AND snapshot_date = (
    SELECT prior_period
    from ufdm_grey.periods
    WHERE evaluation_period = var_period
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
  case
    when coalesce(0, 0) = 1 then acquire_product_group
    else updated_product_group
  end as product_family,
  a.base_currency as baseline_currency,
  max(coalesce(a.end_name, a.parent_name)) as end_customer,
  sum(arr) AS arr_usd_ccfx,
  sum(baseline_arr_local_currency) AS arr_lcu
FROM ryzlan.sst_adhoc a
WHERE 1 = 1
  AND snapshot_date = (
    SELECT current_period
    from ufdm_grey.periods
    WHERE evaluation_period = var_period
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
  round(
    (coalesce(cla.prior_arr_usd_ccfx::numeric, 0)),
    2
  ) as prior_arr_usd_ccfx,
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
  AND per.evaluation_period = var_period;
--#############################################
--Price Ramps
--#############################################
drop table if exists temp_product_bridge_price_ramps;
create temp table temp_product_bridge_price_ramps as with cte as (
  select mcid,
    snapshot_date,
    c."Product Group" as updated_product_group,
    sum(Price_Ramp) as PriceRamp_Value,
    sum(Price_Ramp_lcu) as PriceRamp_Value_lcu
  from ryzlan.Price_Ramps a
    join ufdm_grey.periods b on a.snapshot_date = b.current_period
    join ufdm_grey.product_hierarchy_mappings c on a.sku = c."Product Code"
    and c."Included in ARR" = 'Y'
  where b.evaluation_period = var_period
  group by c_name,
    mcid,
    snapshot_date,
    c."Product Group"
)
select pr.evaluation_period,
  pr.prior_period,
  pr.current_period,
  pr.mcid,
  pr.prior_arr_usd_ccfx as prior_period_product_arr_usd_ccfx,
  pr.current_arr_usd_ccfx as current_period_product_arr_usd_ccfx,
  pr.product_arr_change_ccfx,
  pr.product_bridge,
  pr.product_arr_change_lcu,
  pr.prior_arr_lcu,
  cte.PriceRamp_Value,
  cte.PriceRamp_Value_lcu,
  cte.snapshot_date,
  pr.current_product_family
from arr_product_bridge_tmp pr
  inner join cte on pr.mcid = cte.mcid
  and pr.current_period = cte.snapshot_date
  and pr.current_product_family = cte.updated_product_group
where pr.product_bridge = 'Up Sell';
update arr_product_bridge_tmp a
set product_bridge = 'Price Ramp'
from temp_product_bridge_price_ramps b
where a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and coalesce(a.product_arr_change_ccfx::numeric, 0) <= coalesce(b.PriceRamp_Value::numeric, 0)
  and a.product_bridge = 'Up Sell';
drop table if exists temp_Price_Ramp_split;
create temp table temp_Price_Ramp_split as
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.current_product_family,
  a.prior_product_family,
  a.current_end_customer,
  a.prior_end_customer,
  a.baseline_currency,
  a.prior_arr_usd_ccfx as prior_arr_usd_ccfx,
  a.current_arr_usd_ccfx - b.PriceRamp_Value as current_arr_usd_ccfx,
  a.product_arr_change_ccfx - b.PriceRamp_Value as product_arr_change_ccfx,
  a.prior_arr_lcu as prior_arr_lcu,
  a.current_arr_lcu - b.PriceRamp_Value_lcu as current_arr_lcu,
  a.product_arr_change_lcu - b.PriceRamp_Value_lcu as product_arr_change_lcu,
  a.product_bridge
from arr_product_bridge_tmp a
  join temp_product_bridge_price_ramps b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.product_bridge = b.product_bridge
  and a.current_product_family = b.current_product_family
where coalesce(a.product_arr_change_ccfx::numeric, 0) > coalesce(b.PriceRamp_Value::numeric, 0)
union all
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.current_product_family,
  a.prior_product_family,
  a.current_end_customer,
  a.prior_end_customer,
  a.baseline_currency,
  '0'::numeric as prior_arr_usd_ccfx,
  b.PriceRamp_Value as current_arr_usd_ccfx,
  b.PriceRamp_Value as product_arr_change_ccfx,
  '0'::numeric as prior_arr_lcu,
  b.PriceRamp_Value_lcu as current_arr_lcu,
  b.PriceRamp_Value_lcu as product_arr_change_lcu,
  'Price Ramp' as product_bridge
from arr_product_bridge_tmp a
  join temp_product_bridge_price_ramps b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.product_bridge = b.product_bridge
  and a.current_product_family = b.current_product_family
where coalesce(a.product_arr_change_ccfx::numeric, 0) > coalesce(b.PriceRamp_Value::numeric, 0)
order by mcid;
delete from arr_product_bridge_tmp a using temp_product_bridge_price_ramps b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and coalesce(a.product_arr_change_ccfx::numeric, 0) > coalesce(b.PriceRamp_Value::numeric, 0)
  and a.product_bridge = 'Up Sell';
insert into arr_product_bridge_tmp (
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
  current_arr_usd_ccfx,
  prior_arr_usd_ccfx,
  product_arr_change_ccfx,
  current_arr_lcu,
  prior_arr_lcu,
  product_arr_change_lcu,
  product_bridge
from temp_Price_Ramp_split;
--######################################################################
--Downgrade
--######################################################################
update arr_product_bridge_tmp
set product_bridge = 'Downgrade'
where product_bridge = 'Partial Churn';
--Cross-sell
WITH PG_F_C AS (
  SELECT mcid,
    COUNT(distinct product_bridge) as product_family_count
  FROM arr_product_bridge_tmp
  WHERE current_arr_usd_ccfx > 0
    and evaluation_period = var_period
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
  and t.evaluation_period = var_period;
-------
--Downsell
WITH PG_F_C AS (
  SELECT mcid,
    COUNT(distinct product_bridge) as product_family_count
  FROM arr_product_bridge_tmp
  WHERE prior_arr_usd_ccfx > 0
    and evaluation_period = var_period
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
  and t.evaluation_period = var_period;
--#############################################
--CPI
--#############################################
--RAISE NOTICE 'Running Price Increase update on sst product bridge...';
--Price Increase updates
update arr_product_bridge_tmp
set product_bridge = 'Price Uplift'
where product_bridge = 'Up Sell'
  and prior_arr_usd_ccfx > 0
  and (
    (product_arr_change_ccfx / prior_arr_usd_ccfx) * 100
  )::numeric <= case
    when evaluation_period < '2023-01-01' then 5.5
    else 10.5
  end
  and evaluation_period = var_period;
-- ############################################
-- CHURN MIGRATION 
-- ############################################
-- arr_product_bridge_tmp
-- take churn migration classifiers code for particular time frame
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
INSERT INTO ryzlan.sst_product_bridge(
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


END;
$$;

