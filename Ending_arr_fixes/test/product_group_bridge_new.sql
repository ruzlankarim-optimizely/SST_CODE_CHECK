create or replace function ryzlan.sp_populate_sst_product_bridge_product_group_reversal_fix(var_period text) returns void language plpgsql as $$ BEGIN
DELETE from ryzlan.sst_product_group_bridge_reversal_fix
where evaluation_period = var_period;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--SST product Bridge
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists prior_period_customer_arr_tmp;
create temp table prior_period_customer_arr_tmp as
SELECT snapshot_date,
  a.mcid as master_customer_id,
  case
    when coalesce(0, 0) = 1 then null
    else updated_product_group
  end as product_family,
  a.base_currency as baseline_currency,
  max(coalesce(a.end_name, a.parent_name)) as end_customer,
  sum(arr) AS arr_usd_ccfx,
  sum(baseline_arr_local_currency) AS arr_lcu
FROM ryzlan.sst_ending_arr_tester_final a
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
    when coalesce(0, 0) = 1 then null
    else updated_product_group
  end as product_family,
  a.base_currency as baseline_currency,
  max(coalesce(a.end_name, a.parent_name)) as end_customer,
  sum(arr) AS arr_usd_ccfx,
  sum(baseline_arr_local_currency) AS arr_lcu
FROM ryzlan.sst_ending_arr_tester_final a
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
drop table if exists prior_period_customer_arr;
drop table if exists current_period_customer_arr;
drop table if exists customer_level_arr_tmp;
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
RAISE NOTICE 'Running Price Increase update on sst product bridge...';
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
--#############################################
--Product SWAPS
--#############################################
Drop table if exists temp_product_swap;
create temp table temp_product_swap as with cte as (
  select *,
    case
      when product_bridge in ('New', 'Up Sell', 'Cross-sell', 'Price Uplift') then 'Positive'
      when product_bridge in ('Downsell', 'Downgrade', 'Churn') then 'Negative'
    end as Each_Pair
  from arr_product_bridge_tmp a
    inner join (
      select distinct mcid as x_mcid,
        evaluation_period as x_evaluation_period,
        count(mcid)
      from arr_product_bridge_tmp
      where evaluation_period = var_period
        and product_bridge != 'Flat'
      group by mcid,
        evaluation_period
      having count(mcid) = 2
    ) x on a.mcid = x.x_mcid
    and x.x_evaluation_period = a.evaluation_period
  where a.evaluation_period = var_period
) -- SELECT mcid,evaluation_period,COUNT(DISTINCT Each_Pair),count(*) FROM cte GROUP BY mcid,cte.evaluation_period having count(DISTINCT Each_Pair) = 2
select cte.*
from cte
  inner join (
    SELECT mcid,
      evaluation_period
    FROM cte
    GROUP BY mcid,
      cte.evaluation_period
    HAVING COUNT(DISTINCT Each_Pair) = 2
  ) z on cte.evaluation_period = z.evaluation_period
  and cte.mcid = z.mcid
where cte.evaluation_period = var_period
  and coalesce(Each_Pair, '') <> ''
  and product_bridge != 'Flat';
------
Drop table if exists temp_updates;
create temp table temp_updates as WITH cte AS (
  SELECT mcid,
    evaluation_period,
    MAX(
      CASE
        WHEN product_arr_change_ccfx > 0 THEN product_arr_change_ccfx
      END
    ) AS positive_max,
    MAX(
      CASE
        WHEN product_arr_change_ccfx < 0 THEN product_arr_change_ccfx
      END
    ) AS negative_max,
    SUM(
      CASE
        WHEN product_arr_change_ccfx > 0 THEN product_arr_change_ccfx
        ELSE 0
      END
    ) + SUM(
      CASE
        WHEN product_arr_change_ccfx < 0 THEN product_arr_change_ccfx
        ELSE 0
      END
    ) AS Diff
  FROM temp_product_swap
  GROUP BY mcid,
    evaluation_period
)
SELECT cte.*,
  CASE
    WHEN diff between -1 and 1 then 'Neutral'
    when Diff > 1 THEN 'Positive'
    WHEN Diff < -1 THEN 'Negative'
  END AS Type,
  b.product_bridge
FROM cte
  INNER JOIN temp_product_swap b ON cte.mcid = b.mcid
  and cte.evaluation_period = b.evaluation_period --AND temp1.Each_Pair = CASE WHEN diff between -1 and 1 then 'Neutral' when Diff > 1 THEN 'Positive' WHEN Diff < -1 THEN 'Negative'  END
WHERE 1 = 1
  and Diff between -1 and 1
  AND CTE.evaluation_period = var_period
  and product_bridge != 'Flat';
--updating same records
update arr_product_bridge_tmp a
set product_bridge = concat(a.product_bridge, '_swap')
from temp_updates b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.product_bridge = b.product_bridge;
------
Drop table if exists temp_split;
create temp table temp_split as WITH cte AS (
  SELECT mcid,
    evaluation_period,
    MAX(
      CASE
        WHEN product_arr_change_ccfx > 0 THEN product_arr_change_ccfx
      END
    ) AS positive_max,
    MAX(
      CASE
        WHEN product_arr_change_ccfx < 0 THEN product_arr_change_ccfx
      END
    ) AS negative_max,
    SUM(
      CASE
        WHEN product_arr_change_ccfx > 0 THEN product_arr_change_ccfx
        ELSE 0
      END
    ) + SUM(
      CASE
        WHEN product_arr_change_ccfx < 0 THEN product_arr_change_ccfx
        ELSE 0
      END
    ) AS Diff,
    --lcu
    MAX(
      CASE
        WHEN product_arr_change_lcu > 0 THEN product_arr_change_lcu
      END
    ) AS positive_max_lcu,
    MAX(
      CASE
        WHEN product_arr_change_lcu < 0 THEN product_arr_change_lcu
      END
    ) AS negative_max_lcu,
    SUM(
      CASE
        WHEN product_arr_change_lcu > 0 THEN product_arr_change_lcu
        ELSE 0
      END
    ) + SUM(
      CASE
        WHEN product_arr_change_lcu < 0 THEN product_arr_change_lcu
        ELSE 0
      END
    ) AS Diff_lcu
  FROM temp_product_swap
  GROUP BY mcid,
    evaluation_period
)
SELECT cte.*,
  --temp1.evaluation_period,
  CASE
    WHEN diff between -1 and 1 then 'Neutral'
    when Diff > 1 THEN 'Positive'
    WHEN Diff < -1 THEN 'Negative'
  END AS Type,
  b.product_bridge
FROM cte
  INNER JOIN temp_product_swap b ON cte.mcid = b.mcid
  and cte.evaluation_period = b.evaluation_period
  AND b.Each_Pair = CASE
    when Diff > 1 THEN 'Positive'
    WHEN Diff < -1 THEN 'Negative'
  END
WHERE Diff not between -1 and 1
  and product_bridge != 'Flat';
drop table if exists temp_product_swap_split;
create temp table temp_product_swap_split as
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_end_customer,
  a.prior_end_customer,
  a.mcid,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.current_product_family as current_product_group,
  a.prior_product_family as prior_product_group,
  a.baseline_currency as currency_code,
  case
    when a.product_bridge = b.product_bridge
    and b.diff < -1 then a.prior_arr_usd_ccfx + b.diff
    else a.prior_arr_usd_ccfx
  end as prior_period_product_arr_usd_ccfx,
  case
    when a.product_bridge = b.product_bridge
    and b.diff > 1 then a.current_arr_usd_ccfx - b.Diff
    else current_arr_usd_ccfx
  end as current_period_product_arr_usd_ccfx,
  case
    when a.product_bridge = b.product_bridge then a.product_arr_change_ccfx - b.diff
    else product_arr_change_ccfx
  end as product_arr_change_ccfx --lcu
,
  case
    when a.product_bridge = b.product_bridge
    and b.diff < -1 then a.prior_arr_lcu + b.Diff_lcu
    else a.prior_arr_lcu
  end as prior_period_product_arr_lcu,
  case
    when a.product_bridge = b.product_bridge
    and b.diff > 1 then a.current_arr_lcu - b.Diff_lcu
    else current_arr_lcu
  end as current_period_product_arr_lcu,
  case
    when a.product_bridge = b.product_bridge then a.product_arr_change_lcu - b.Diff_lcu
    else product_arr_change_lcu
  end as product_arr_change_lcu,
  concat(a.product_bridge, '_swap') as product_bridge --,a.winback_period_days,a.wip_flag,a.price_increase_amount,a.subsidiary_entity_name,a.churn_period,a.customer_bridge
from arr_product_bridge_tmp a
  join temp_split b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period --and a.product_bridge = b.product_bridge
WHERE a.product_bridge != 'Flat'
union all
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_end_customer,
  a.prior_end_customer,
  a.mcid,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.current_product_family as current_product_group,
  a.prior_product_family as prior_product_group,
  a.baseline_currency as currency_code,
  case
    when a.product_bridge = b.product_bridge
    and b.diff > 1 then 0::numeric
    else -1 * b.diff
  end as prior_period_product_arr_usd_ccfx,
  case
    when a.product_bridge = b.product_bridge
    and b.diff < -1 then 0::numeric
    else b.Diff
  end as current_period_product_arr_usd_ccfx,
  b.diff as product_arr_change_ccfx --lcu
,
  case
    when a.product_bridge = b.product_bridge
    and b.diff > 1 then 0::numeric
    else -1 * b.Diff_lcu
  end as prior_period_product_arr_lcu,
  case
    when a.product_bridge = b.product_bridge
    and b.diff < -1 then 0::numeric
    else b.Diff_lcu
  end as current_period_product_arr_lcu,
  b.Diff_lcu as product_arr_change_lcu,
  b.product_bridge as product_bridge --,a.winback_period_days,a.wip_flag,a.price_increase_amount,a.subsidiary_entity_name,a.churn_period,a.customer_bridge
from arr_product_bridge_tmp a
  join temp_split b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
where 1 = 1
  and a.evaluation_period = var_period
  and b.type = CASE
    when product_arr_change_ccfx > 1 THEN 'Positive'
    WHEN product_arr_change_ccfx < -1 THEN 'Negative'
  END
  and a.product_bridge != 'Flat';
delete from arr_product_bridge_tmp a using temp_split b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period --and a.product_bridge = b.product_bridge
  and a.product_bridge != 'Flat'
  and a.evaluation_period = var_period;
insert into arr_product_bridge_tmp (
    evaluation_period,
    prior_period,
    current_period,
    current_end_customer,
    prior_end_customer,
    mcid,
    current_master_customer_id,
    prior_master_customer_id,
    current_product_family,
    prior_product_family,
    baseline_currency,
    prior_arr_usd_ccfx,
    current_arr_usd_ccfx,
    product_arr_change_ccfx,
    prior_arr_lcu,
    current_arr_lcu,
    product_arr_change_lcu,
    product_bridge
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
  product_bridge
from temp_product_swap_split
where product_bridge != 'Flat';
Drop table if exists temp_product_swap;
Drop table if exists temp_updates;
Drop table if exists temp_split;
drop table if exists temp_product_swap_split;
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
        case
          when coalesce(0, 0) = 1 then null
          else updated_product_group
        end as updated_product_group,
        sb.base_currency,
        sum(arr) as arr,
        sum(baseline_arr_local_currency) as baseline_arr_local_currency
      from ryzlan.sst_ending_arr_tester_final sb
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
      ) <= 90 then 'Winback'
      else 'Winback'
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
INSERT INTO ryzlan.sst_product_group_bridge_reversal_fix (
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
    when b.mcid is not null
    and a.product_bridge in ('New', 'Cross-sell') then case
      when b.arr_at_new > b.arr_at_churn then b.arr_at_churn
      else b.arr_at_new
    end --round(b.arr_at_churn::numeric,2)
    else round(a.current_arr_usd_ccfx::numeric, 2)
  end as current_period_customer_arr_usd_ccfx,
  case
    when b.mcid is not null
    and a.product_bridge in ('New', 'Cross-sell') then case
      when b.arr_at_new > b.arr_at_churn then b.arr_at_churn
      else b.arr_at_new
    end
    else a.product_arr_change_ccfx
  end as product_arr_change_ccfx,
  ------------------------lcu----------------------------
  round(a.prior_arr_lcu::numeric, 2) AS prior_period_product_arr_lcu,
  case
    when b.mcid is not null
    and a.product_bridge in ('New', 'Cross-sell') then case
      when b.arr_lcu_at_new > b.arr_lcu_at_churn then b.arr_lcu_at_churn
      else b.arr_lcu_at_new
    end --round(b.arr_lcu_at_churn::numeric,2)
    else round(a.current_arr_lcu::numeric, 2)
  end as current_period_product_arr_lcu,
  case
    when b.mcid is not null
    and a.product_bridge in ('New', 'Cross-sell') then case
      when b.arr_lcu_at_new > b.arr_lcu_at_churn then b.arr_lcu_at_churn
      else b.arr_lcu_at_new
    end
    else a.product_arr_change_lcu
  end as product_arr_change_lcu,
  case
    when b.mcid is not null
    and a.product_bridge in ('New', 'Cross-sell') then case
      when b.days_diff <= 90 then 'Winback'
      else 'Winback'
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
where b.arr_at_new > b.arr_at_churn
  and a.product_bridge in ('New', 'Cross-sell');
drop table if exists arr_new_products_tmp;
drop table if exists arr_churned_products_tmp;
RAISE NOTICE 'Running customer bridge update on sst product bridge...';
--update customer bridge and subsidiary entity
--     update ryzlan.sst_product_group_bridge_reversal_fix a
--     set customer_bridge = b.customer_bridge
--     from sandbox_pd.sst_customer_bridge b
--     where 1=1
--       and a.evaluation_period = b.evaluation_period
--       and a.mcid =b.mcid
--       and a.evaluation_period = var_period
--     ;
RAISE NOTICE 'Running subsidiary entity name insert on sst product bridge...';
drop table if exists sub_entity_tmp;
create temp table sub_entity_tmp as --update subsidiary_entity_name
with mcid_list as (
  select distinct mcid as master_customer_id
  from arr_product_bridge_tmp
  where evaluation_period = var_period
),
total_arr as (
  select a.mcid as mcid,
    a.snapshot_date,
    a.subsidiary_entity_name,
    sum(a.arr) as total_arr
  from ryzlan.sst_ending_arr_tester_final a
    join mcid_list b on a.mcid = b.master_customer_id
    and a.snapshot_date in (
      SELECT prior_period
      from ufdm_grey.periods
      WHERE evaluation_period = var_period
      union
      SELECT current_period
      from ufdm_grey.periods
      WHERE evaluation_period = var_period
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
RAISE NOTICE 'Running sub entity update on sst product bridge...';
create index nci_sub_entity_tmp_mcid on sub_entity_tmp(mcid);
update ryzlan.sst_product_group_bridge_reversal_fix a
set subsidiary_entity_name = b.subsidiary_entity_name
from sub_entity_tmp b
where a.mcid = b.mcid
  and a.evaluation_period = var_period;
drop table if exists sub_entity_tmp;
drop table if exists arr_product_bridge_tmp;
--###########################################
--WINBACK Downgrade
--###########################################
RAISE NOTICE 'Running WINBACK Downgrade update on sst product bridge group 1...';
drop table if exists temp_win_downgrade_upsell;
create temp table temp_win_downgrade_upsell as with temp1 as (
  select a.mcid,
    a.product_bridge,
    a.evaluation_period as evaluation_period_at_upsell,
    a.current_period as snapshot_date_at_upsell,
    a.product_arr_change_ccfx as Upsell_crosssell_arr,
    a.product_arr_change_lcu as Upsell_crosssell_arr_lcu,
    a.current_product_group
  from ryzlan.sst_product_group_bridge_reversal_fix a
  where 1 = 1
    and a.product_bridge in ('Cross-sell', 'Up Sell')
    and a.evaluation_period = var_period
),
temp2 as (
  select a.mcid,
    a.product_bridge,
    a.evaluation_period_at_upsell,
    a.snapshot_date_at_upsell,
    b.current_period as snapshot_date_Downgrade,
    a.Upsell_crosssell_arr,
    a.Upsell_crosssell_arr_lcu,
    b.product_arr_change_ccfx as Downgrade_downsell_arr,
    b.product_arr_change_ccfx as Downgrade_downsell_arr_lcu,
    b.evaluation_period as Downgrade_evaluation_period,
    b.product_bridge as Downgrade_bridge,
    a.current_product_group,
    row_number() over (
      partition by a.mcid,
      a.evaluation_period_at_upsell,
      a.current_product_group,
      a.product_bridge
      order by b.current_period desc,
        a.snapshot_date_at_upsell
    ) as rnk
  from ryzlan.sst_product_group_bridge_reversal_fix b
    join temp1 a on a.mcid = b.mcid
    and a.current_product_group = b.current_product_group
  where 1 = 1
    and b.product_bridge in ('Downgrade', 'Downsell')
    and b.current_period < (
      select current_period
      from ufdm_grey.periods
      where evaluation_period = var_period
    )
)
select *
from temp2;
RAISE NOTICE 'Running WINBACK Downgrade update on sst product group bridge 1.1 ...';
drop table if exists temp_windowngrade_final;
create temporary table temp_windowngrade_final as with temp1 as (
  select *,
    row_number() over (
      partition by mcid,
      Downgrade_evaluation_period,
      current_product_group,
      product_bridge
      order by snapshot_date_at_upsell
    ) as rnk2
  from temp_win_downgrade_upsell
  where rnk = 1
    and snapshot_date_at_upsell::date - snapshot_date_Downgrade::date < 186
),
temp2 as (
  select *
  from temp1
  where rnk2 = 1
)
select a.mcid,
  a.evaluation_period,
  a.product_bridge,
  b.Upsell_crosssell_arr,
  b.Downgrade_downsell_arr,
  b.Upsell_crosssell_arr_lcu,
  b.Downgrade_downsell_arr_lcu --, case when b.Upsell_crosssell_arr > abs(b.Downgrade_downsell_arr) then 1 else 0 end as Split_record
,
  b.Downgrade_evaluation_period,
  b.Downgrade_bridge,
  a.current_product_group
from ryzlan.sst_product_group_bridge_reversal_fix a,
  temp2 b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period_at_upsell
  and a.current_product_group = b.current_product_group
  and a.product_bridge in ('Cross-sell', 'Up Sell')
  and a.product_bridge = b.product_bridge
  and a.evaluation_period = var_period;
--update when total cross/upsell is less than equal to downgrade/downsell
drop table if exists temp_windowngrade_final_curated;
create temporary table temp_windowngrade_final_curated as with cross_upsell_total as (
  select a.mcid,
    a.evaluation_period,
    a.current_product_group,
    sum(
      coalesce(b.Upsell_crosssell_arr, 0) + coalesce(c.Upsell_crosssell_arr, 0)
    ) as cross_upsell_total,
    sum(coalesce(c.Upsell_crosssell_arr, 0)) as Upsell_arr,
    sum(coalesce(b.Upsell_crosssell_arr, 0)) as Crossell_arr --lcu
,
    sum(
      coalesce(b.Upsell_crosssell_arr_lcu, 0) + coalesce(c.Upsell_crosssell_arr_lcu, 0)
    ) as cross_upsell_total_lcu,
    sum(coalesce(c.Upsell_crosssell_arr_lcu, 0)) as Upsell_arr_lcu,
    sum(coalesce(b.Upsell_crosssell_arr_lcu, 0)) as Crossell_arr_lcu,
    sum(
      case
        when b.mcid is not null
        and c.mcid is not null then 1
        else 0
      end
    ) as cross_upsell_both_exists
  from (
      select distinct mcid,
        evaluation_period,
        current_product_group
      from temp_windowngrade_final
    ) a
    left join temp_windowngrade_final b on a.mcid = b.mcid
    and a.evaluation_period = b.evaluation_period
    and a.current_product_group = b.current_product_group
    and b.product_bridge = 'Cross-sell'
    left join temp_windowngrade_final c on a.mcid = c.mcid
    and a.evaluation_period = c.evaluation_period
    and a.current_product_group = c.current_product_group
    and c.product_bridge = 'Up Sell'
  group by a.mcid,
    a.evaluation_period,
    a.current_product_group
),
downgrade_downsell_total as (
  select a.mcid,
    b.evaluation_period,
    a.evaluation_period as Downgrade_evaluation_period,
    a.product_bridge,
    b.current_product_group,
    abs(sum(product_arr_change_ccfx)) as downgrade_downsell_total,
    sum(
      case
        when a.product_bridge = 'Downgrade' then abs(a.product_arr_change_ccfx)
        else 0
      end
    ) as Downgrade_arr,
    sum(
      case
        when a.product_bridge = 'Downsell' then abs(a.product_arr_change_ccfx)
        else 0
      end
    ) as Downsell_arr --lcu
,
    abs(sum(product_arr_change_lcu)) as downgrade_downsell_total_lcu,
    sum(
      case
        when a.product_bridge = 'Downgrade' then abs(product_arr_change_lcu)
        else 0
      end
    ) as Downgrade_arr_lcu,
    sum(
      case
        when a.product_bridge = 'Downsell' then abs(product_arr_change_lcu)
        else 0
      end
    ) as Downsell_arr_lcu,
    case
      when count(distinct a.product_bridge) > 1 then 1
      else 0
    end as Downgrade_Downsell_both_exists
  from ryzlan.sst_product_group_bridge_reversal_fix a
    join (
      select distinct mcid,
        Downgrade_evaluation_period as Downgrade_evaluation_period,
        evaluation_period,
        current_product_group
      from temp_windowngrade_final
    ) b on a.evaluation_period = b.Downgrade_evaluation_period
    and a.mcid = b.mcid
    and coalesce(a.current_product_group, a.prior_product_group) = b.current_product_group
  where 1 = 1
    and a.product_bridge in ('Downgrade', 'Downsell')
  group by a.mcid,
    b.evaluation_period,
    a.evaluation_period,
    a.product_bridge,
    b.current_product_group
),
temp_new_arr_split as (
  select a.mcid,
    a.evaluation_period,
    a.current_product_group,
    b.downgrade_evaluation_period,
    a.upsell_arr,
    a.Crossell_arr,
    b.downgrade_arr,
    b.Downsell_arr,
    a.cross_upsell_total,
    b.downgrade_downsell_total,
    a.upsell_arr_lcu,
    a.Crossell_arr_lcu,
    b.downgrade_arr_lcu,
    b.Downsell_arr_lcu,
    a.cross_upsell_total_lcu,
    b.downgrade_downsell_total_lcu,
    case
      --if only cross sell or upsell exists then
      when a.cross_upsell_both_exists = 0
      and b.Downgrade_Downsell_both_exists = 0
      and a.Upsell_arr > b.downgrade_downsell_total then a.Upsell_arr - b.downgrade_downsell_total
      when a.cross_upsell_both_exists = 0
      and b.Downgrade_Downsell_both_exists = 0
      and a.Upsell_arr <= b.downgrade_downsell_total then 0
      when a.cross_upsell_both_exists = 1
      and b.Downgrade_Downsell_both_exists in (0, 1)
      and a.cross_upsell_total <= b.downgrade_downsell_total then 0
      when a.cross_upsell_both_exists = 1
      and b.Downgrade_Downsell_both_exists = 0
      and a.cross_upsell_total > b.downgrade_downsell_total then case
        when a.Upsell_arr > 0
        and b.Downgrade_arr > 0
        and a.Upsell_arr <= b.downgrade_downsell_total then 0
        when a.Upsell_arr > 0
        and b.Downgrade_arr > 0
        and a.Upsell_arr > b.downgrade_downsell_total then a.Upsell_arr - b.downgrade_downsell_total
        when a.Upsell_arr > 0
        and b.Downsell_arr > 0
        and a.Crossell_arr >= b.downgrade_downsell_total then a.Upsell_arr
        when a.Upsell_arr > 0
        and b.Downsell_arr > 0
        and a.Crossell_arr < b.downgrade_downsell_total then a.Upsell_arr - (b.downgrade_downsell_total - a.Crossell_arr)
      end
      else 0
    end as upsell_arr_new,
    case
      when a.cross_upsell_both_exists = 0
      and b.Downgrade_Downsell_both_exists = 0
      and a.Crossell_arr > b.downgrade_downsell_total then a.Crossell_arr - b.downgrade_downsell_total
      when a.cross_upsell_both_exists = 0
      and b.Downgrade_Downsell_both_exists = 0
      and a.Crossell_arr <= b.downgrade_downsell_total then 0
      when a.cross_upsell_both_exists = 1
      and b.Downgrade_Downsell_both_exists = 0
      and a.cross_upsell_total <= b.downgrade_downsell_total then 0
      when a.cross_upsell_both_exists = 1
      and b.Downgrade_Downsell_both_exists = 0
      and a.cross_upsell_total > b.downgrade_downsell_total then case
        when a.Crossell_arr > 0
        and b.Downsell_arr > 0
        and a.Crossell_arr <= b.downgrade_downsell_total then 0
        when a.Crossell_arr > 0
        and b.Downsell_arr > 0
        and a.Crossell_arr > b.downgrade_downsell_total then a.Crossell_arr - b.downgrade_downsell_total
        when a.Crossell_arr > 0
        and b.Downgrade_arr > 0
        and a.Crossell_arr >= b.downgrade_downsell_total then a.Crossell_arr
        when a.Crossell_arr > 0
        and b.Downgrade_arr > 0
        and a.Crossell_arr < b.downgrade_downsell_total then a.Crossell_arr - (b.downgrade_downsell_total - a.Upsell_arr)
      end
      else 0
    end as crosssell_arr_new --#######################  lcu  #######----------------------------
,
    case
      --if only cross sell or upsell exists then
      when a.cross_upsell_both_exists = 0
      and b.Downgrade_Downsell_both_exists = 0
      and a.Upsell_arr_lcu > b.downgrade_downsell_total_lcu then a.Upsell_arr_lcu - b.downgrade_downsell_total_lcu
      when a.cross_upsell_both_exists = 0
      and b.Downgrade_Downsell_both_exists = 0
      and a.Upsell_arr_lcu <= b.downgrade_downsell_total_lcu then 0
      when a.cross_upsell_both_exists = 1
      and b.Downgrade_Downsell_both_exists in (0, 1)
      and a.cross_upsell_total_lcu <= b.downgrade_downsell_total_lcu then 0
      when a.cross_upsell_both_exists = 1
      and b.Downgrade_Downsell_both_exists = 0
      and a.cross_upsell_total_lcu > b.downgrade_downsell_total_lcu then case
        when a.Upsell_arr_lcu > 0
        and b.Downgrade_arr_lcu > 0
        and a.Upsell_arr_lcu <= b.downgrade_downsell_total_lcu then 0
        when a.Upsell_arr_lcu > 0
        and b.Downgrade_arr_lcu > 0
        and a.Upsell_arr_lcu > b.downgrade_downsell_total_lcu then a.Upsell_arr_lcu - b.downgrade_downsell_total_lcu
        when a.Upsell_arr_lcu > 0
        and b.Downsell_arr_lcu > 0
        and a.Crossell_arr_lcu >= b.downgrade_downsell_total_lcu then a.Upsell_arr_lcu
        when a.Upsell_arr_lcu > 0
        and b.Downsell_arr_lcu > 0
        and a.Crossell_arr_lcu < b.downgrade_downsell_total_lcu then a.Upsell_arr_lcu - (
          b.downgrade_downsell_total_lcu - a.Crossell_arr_lcu
        )
      end
      else 0
    end as upsell_arr_new_lcu,
    case
      when a.cross_upsell_both_exists = 0
      and b.Downgrade_Downsell_both_exists = 0
      and a.Crossell_arr_lcu > b.downgrade_downsell_total_lcu then a.Crossell_arr_lcu - b.downgrade_downsell_total_lcu
      when a.cross_upsell_both_exists = 0
      and b.Downgrade_Downsell_both_exists = 0
      and a.Crossell_arr_lcu <= b.downgrade_downsell_total_lcu then 0
      when a.cross_upsell_both_exists = 1
      and b.Downgrade_Downsell_both_exists = 0
      and a.cross_upsell_total_lcu <= b.downgrade_downsell_total_lcu then 0
      when a.cross_upsell_both_exists = 1
      and b.Downgrade_Downsell_both_exists = 0
      and a.cross_upsell_total_lcu > b.downgrade_downsell_total_lcu then case
        when a.Crossell_arr_lcu > 0
        and b.Downsell_arr_lcu > 0
        and a.Crossell_arr_lcu <= b.downgrade_downsell_total_lcu then 0
        when a.Crossell_arr_lcu > 0
        and b.Downsell_arr_lcu > 0
        and a.Crossell_arr_lcu > b.downgrade_downsell_total_lcu then a.Crossell_arr_lcu - b.downgrade_downsell_total_lcu
        when a.Crossell_arr_lcu > 0
        and b.Downgrade_arr_lcu > 0
        and a.Crossell_arr_lcu >= b.downgrade_downsell_total_lcu then a.Crossell_arr_lcu
        when a.Crossell_arr_lcu > 0
        and b.Downgrade_arr_lcu > 0
        and a.Crossell_arr_lcu < b.downgrade_downsell_total_lcu then a.Crossell_arr_lcu - (
          b.downgrade_downsell_total_lcu - a.Upsell_arr_lcu
        )
      end
      else 0
    end as crosssell_arr_new_lcu,
    cross_upsell_both_exists,
    Downgrade_Downsell_both_exists
  from cross_upsell_total a
    join downgrade_downsell_total b on a.mcid = b.mcid
    and a.evaluation_period = b.evaluation_period
    and a.current_product_group = b.current_product_group
)
select *,
  case
    when a.cross_upsell_total < a.downgrade_downsell_total then a.cross_upsell_total
    else a.downgrade_downsell_total
  end as winback_arr_new,
  case
    when a.cross_upsell_total_lcu < a.downgrade_downsell_total_lcu then a.cross_upsell_total_lcu
    else a.downgrade_downsell_total_lcu
  end as winback_arr_new_lcu,
  case
    when a.cross_upsell_total <= a.downgrade_downsell_total then 0
    else 1
  end as split_record,
  case
    when a.Downgrade_arr > 0 then 'Downgrade'
    when a.Downsell_arr > 0 then 'Downsell'
    else ''
  end as Downgrade_Downsell_bridge
from temp_new_arr_split a
order by cross_upsell_both_exists;
RAISE NOTICE 'Running WINBACK Downgrade update on sst product group bridge 3...';
drop table if exists temp_windowngrade_split;
create temp table temp_windowngrade_split as
select a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_end_customer,
  a.prior_end_customer,
  a.mcid,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.current_product_group,
  a.prior_product_group,
  a.currency_code,
  a.prior_period_product_arr_usd_ccfx,
  a.prior_period_product_arr_usd_ccfx + b.upsell_arr_new as current_period_product_arr_usd_ccfx,
  b.upsell_arr_new as product_arr_change_ccfx ---lcu
,
  a.prior_period_product_arr_lcu,
  a.prior_period_product_arr_lcu + b.upsell_arr_new_lcu as current_period_product_arr_lcu,
  b.upsell_arr_new_lcu as product_arr_change_lcu,
  a.product_bridge,
  a.winback_period_days,
  a.wip_flag,
  a.price_increase_amount,
  a.subsidiary_entity_name,
  a.churn_period,
  a.customer_bridge
from ryzlan.sst_product_group_bridge_reversal_fix a
  join temp_windowngrade_final_curated b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.current_product_group = b.current_product_group
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.product_bridge in ('Up Sell')
  and b.upsell_arr_new > 0
union all
select a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_end_customer,
  a.prior_end_customer,
  a.mcid,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.current_product_group,
  a.prior_product_group,
  a.currency_code,
  a.prior_period_product_arr_usd_ccfx,
  a.prior_period_product_arr_usd_ccfx + b.crosssell_arr_new as current_period_product_arr_usd_ccfx,
  b.crosssell_arr_new as product_arr_change_ccfx ---lcu
,
  a.prior_period_product_arr_lcu,
  a.prior_period_product_arr_lcu + b.crosssell_arr_new_lcu as current_period_product_arr_lcu,
  b.crosssell_arr_new_lcu as product_arr_change_lcu,
  a.product_bridge,
  a.winback_period_days,
  a.wip_flag,
  a.price_increase_amount,
  a.subsidiary_entity_name,
  a.churn_period,
  a.customer_bridge
from ryzlan.sst_product_group_bridge_reversal_fix a
  join temp_windowngrade_final_curated b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.current_product_group = b.current_product_group
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.product_bridge in ('Cross-sell')
  and b.crosssell_arr_new > 0
union all
select a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_end_customer,
  a.prior_end_customer,
  a.mcid,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.current_product_group,
  a.prior_product_group,
  a.currency_code,
  0 as prior_period_product_arr_usd_ccfx,
  b.winback_arr_new as current_period_product_arr_usd_ccfx,
  b.winback_arr_new as product_arr_change_ccfx,
  0 as prior_period_product_arr_lcu,
  a.product_arr_change_lcu as current_period_product_arr_lcu,
  a.product_arr_change_lcu as product_arr_change_lcu,
  concat('Win back ', b.Downgrade_Downsell_bridge) as product_bridge,
  a.winback_period_days,
  a.wip_flag,
  a.price_increase_amount,
  a.subsidiary_entity_name,
  a.churn_period,
  a.customer_bridge
from ryzlan.sst_product_group_bridge_reversal_fix a
  join temp_windowngrade_final_curated b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.current_product_group = b.current_product_group
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.product_bridge in ('Up Sell')
union all
select a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_end_customer,
  a.prior_end_customer,
  a.mcid,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.current_product_group,
  a.prior_product_group,
  a.currency_code,
  0 as prior_period_product_arr_usd_ccfx,
  b.winback_arr_new as current_period_product_arr_usd_ccfx,
  b.winback_arr_new as product_arr_change_ccfx,
  0 as prior_period_product_arr_lcu,
  a.product_arr_change_lcu as current_period_product_arr_lcu,
  a.product_arr_change_lcu as product_arr_change_lcu,
  concat('Win back ', b.Downgrade_Downsell_bridge) as product_bridge,
  a.winback_period_days,
  a.wip_flag,
  a.price_increase_amount,
  a.subsidiary_entity_name,
  a.churn_period,
  a.customer_bridge
from ryzlan.sst_product_group_bridge_reversal_fix a
  join temp_windowngrade_final_curated b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.current_product_group = b.current_product_group
  left join ryzlan.sst_product_group_bridge_reversal_fix c on c.mcid = a.mcid
  and c.evaluation_period = a.evaluation_period
  and c.current_product_group = a.current_product_group
  and c.product_bridge = 'Up Sell'
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.product_bridge in ('Cross-sell')
  and c.mcid is null
order by mcid;
/*
 select distinct
 a.evaluation_period, a.prior_period, a.current_period, a.current_end_customer, a.prior_end_customer, a.mcid, a.current_master_customer_id, a.prior_master_customer_id, a.current_product_group, a.prior_product_group
 , a.currency_code
 ,case when a.product_bridge = 'Churn' then a.prior_period_product_arr_usd_ccfx - c.product_arr_change_ccfx
 else a.prior_period_product_arr_usd_ccfx end as prior_period_product_arr_usd_ccfx
 ,case when a.product_bridge = 'Churn' then 0 else a.current_period_product_arr_usd_ccfx + c.product_arr_change_ccfx end as current_period_product_arr_usd_ccfx
 ,a.product_arr_change_ccfx + c.product_arr_change_ccfx as product_arr_change_ccfx
 ---lcu
 ,case when a.product_bridge = 'Churn' then a.prior_period_product_arr_lcu - c.product_arr_change_lcu
 else a.prior_period_product_arr_lcu end as prior_period_product_arr_lcu
 ,case when a.product_bridge = 'Churn' then 0 else a.current_period_product_arr_lcu + c.product_arr_change_lcu end as current_period_product_arr_lcu
 ,a.product_arr_change_lcu + c.product_arr_change_lcu as product_arr_change_lcu
 , a.product_bridge, a.winback_period_days, a.wip_flag, a.price_increase_amount, a.subsidiary_entity_name, a.churn_period, a.customer_bridge
 from ryzlan.sst_product_group_bridge_reversal_fix a
 join temp_windowngrade_final b on a.mcid = b.mcid and a.evaluation_period = b.evaluation_period and a.current_product_group = b.current_product_group and a.product_bridge = b.product_bridge
 join ryzlan.sst_product_group_bridge_reversal_fix c on c.mcid = b.mcid and c.evaluation_period = b.Downgrade_evaluation_period and c.current_product_group = b.current_product_group
 and c.product_bridge = b.Downgrade_bridge
 where b.Split_record = 1
 and a.evaluation_period = var_period
 and a.product_bridge in ('Cross-sell','Up Sell')
 union all
 select
 a.evaluation_period, a.prior_period, a.current_period, a.current_end_customer, a.prior_end_customer, a.mcid, a.current_master_customer_id, a.prior_master_customer_id, a.current_product_group, a.prior_product_group
 , a.currency_code
 , c.current_period_product_arr_usd_ccfx as prior_period_product_arr_usd_ccfx
 , c.prior_period_product_arr_usd_ccfx as current_period_product_arr_usd_ccfx
 , -c.product_arr_change_ccfx as product_arr_change_ccfx
 , c.current_period_product_arr_lcu as prior_period_product_arr_lcu
 , c.prior_period_product_arr_lcu as current_period_product_arr_lcu
 , -c.product_arr_change_lcu as product_arr_change_lcu
 , concat('Win back ',b.Downgrade_bridge) as product_bridge, a.winback_period_days, a.wip_flag, a.price_increase_amount, a.subsidiary_entity_name, a.churn_period, a.customer_bridge
 from ryzlan.sst_product_group_bridge_reversal_fix a
 join temp_windowngrade_final b on a.mcid = b.mcid and a.evaluation_period = b.evaluation_period and a.current_product_group = b.current_product_group and a.product_bridge = b.product_bridge
 join ryzlan.sst_product_group_bridge_reversal_fix c on c.mcid = b.mcid and c.evaluation_period = b.Downgrade_evaluation_period and c.current_product_group = b.current_product_group
 and c.product_bridge = b.Downgrade_bridge
 where b.Split_record = 1
 and a.evaluation_period = var_period
 and a.product_bridge in ('Cross-sell','Up Sell')
 order by mcid
 ;
 */
RAISE NOTICE 'Running WINBACK Downgrade update on sst product group bridge 2...';
update ryzlan.sst_product_group_bridge_reversal_fix a
set product_bridge = concat('Win back ', b.Downgrade_Downsell_bridge)
from temp_windowngrade_final_curated b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.current_product_group = b.current_product_group
  and a.evaluation_period = var_period
  and a.product_bridge in ('Cross-sell', 'Up Sell') --and a.product_bridge = b.product_bridge
  and b.Split_record = 0;
RAISE NOTICE 'Running WINBACK Downgrade update on sst product group bridge 4...';
delete from ryzlan.sst_product_group_bridge_reversal_fix a using temp_windowngrade_final_curated b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and b.Split_record = 1
  and a.evaluation_period = var_period
  and a.current_product_group = b.current_product_group
  and a.product_bridge in ('Cross-sell', 'Up Sell') --and a.product_bridge = b.product_bridge
;
insert into ryzlan.sst_product_group_bridge_reversal_fix (
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
  customer_bridge
from temp_windowngrade_split;
drop table if exists temp_win_downgrade_upsell;
drop table if exists temp_windowngrade_final;
drop table if exists temp_windowngrade_final_curated;
drop table if exists temp_windowngrade_split;
--###########################################
--CPI Reversal
--###########################################
RAISE NOTICE 'Running CPI Reversal update on sst product group bridge...';
drop table if exists temp_CPI_Rev_pg;
create temp table temp_CPI_Rev_pg as with temp1 as (
  select a.mcid,
    a.product_bridge,
    a.evaluation_period as evaluation_period_at_Downgrade_Churn,
    p.current_period as snapshot_date_at_Downgrade_Churn,
    product_arr_change_ccfx as current_arr_at_Downgrade_Churn,
    coalesce(a.current_product_group, a.prior_product_group) as product_group_at_Downgrade_Churn
  from ryzlan.sst_product_group_bridge_reversal_fix a
    join ufdm_grey.periods p on a.evaluation_period = p.evaluation_period
  where 1 = 1
    and a.evaluation_period = var_period
    and a.product_bridge in ('Downgrade', 'Churn', 'Downsell')
),
temp2 as (
  select a.mcid,
    a.product_bridge,
    a.evaluation_period_at_Downgrade_Churn,
    a.snapshot_date_at_Downgrade_Churn,
    b.current_period as snapshot_date_CPI,
    a.current_arr_at_Downgrade_Churn,
    b.product_arr_change_ccfx as CPI_arr,
    b.product_bridge as CPI_bridge,
    b.evaluation_period as CPI_evaluation_period,
    a.product_group_at_Downgrade_Churn,
    b.current_product_group as product_group_at_CPI,
    row_number() over (
      partition by b.mcid,
      a.evaluation_period_at_Downgrade_Churn,
      current_product_group
      order by b.current_period desc
    ) as rnk
  from ryzlan.sst_product_group_bridge_reversal_fix b
    join temp1 a on a.mcid = b.mcid
    and a.product_group_at_Downgrade_Churn = b.current_product_group
  where 1 = 1
    and b.product_bridge = 'Price Uplift'
    and b.current_period < (
      select current_period
      from ufdm_grey.periods
      where evaluation_period = var_period
    )
)
select *
from temp2;
RAISE NOTICE 'Running CPI Reversal update on sst product group bridge 1.1 ...';
drop table if exists temp_CPI_Reversal;
create temp table temp_CPI_Reversal as with temp1 as (
  select *,
    row_number() over (
      partition by mcid,
      evaluation_period_at_Downgrade_Churn,
      product_group_at_CPI
      order by snapshot_date_CPI
    ) as rnk2
  from temp_CPI_Rev_pg
  where rnk = 1
    and snapshot_date_at_Downgrade_Churn::date - snapshot_date_CPI::date < 186
),
temp2 as (
  select *
  from temp1
  where rnk2 = 1
)
select a.mcid,
  a.evaluation_period,
  b.current_arr_at_Downgrade_Churn,
  (
    b.CPI_arr - abs(coalesce(c.product_arr_change_ccfx, 0))
  ) as CPI_arr,
  a.customer_bridge,
  case
    when - b.current_arr_at_Downgrade_Churn > (
      b.CPI_arr - abs(coalesce(c.product_arr_change_ccfx, 0))
    ) then 1
    else 0
  end as Split_record,
  b.CPI_evaluation_period,
  b.CPI_bridge,
  a.current_product_group,
  snapshot_date_at_Downgrade_Churn,
  abs(coalesce(c.product_arr_change_ccfx, 0)) as cpi_reversal_arr,
  abs(coalesce(c.product_arr_change_lcu, 0)) as cpi_reversal_lcu,
  coalesce(c.current_period_product_arr_usd_ccfx, 0) as prior_period_arr_usd_ccfx_CPIR,
  coalesce(c.prior_period_product_arr_usd_ccfx, 0) as current_period_arr_usd_ccfx_CPIR,
  coalesce(c.current_period_product_arr_lcu, 0) as prior_period_arr_lcu_CPIR,
  coalesce(c.prior_period_product_arr_lcu, 0) as current_period_lcu_CPIR
from ryzlan.sst_product_group_bridge_reversal_fix a
  join temp2 b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period_at_Downgrade_Churn
  and coalesce (a.current_product_group, a.prior_product_group) = b.product_group_at_CPI
  left join (
    select suba.*,
      subb.current_period as snapshot_Date
    from ryzlan.sst_product_group_bridge_reversal_fix suba
      join ufdm_grey.periods subb on suba.evaluation_period = subb.evaluation_period
  ) c on a.mcid = c.mcid
  and coalesce (a.current_product_group, a.prior_product_group) = c.current_product_group
  and c.customer_bridge = 'Price Uplift Reversal'
  and (c.snapshot_Date) between b.snapshot_date_CPI and b.snapshot_date_at_Downgrade_Churn
where 1 = 1
  and a.product_bridge in ('Downgrade', 'Churn', 'Downsell')
  and a.evaluation_period = var_period
  and (
    c.mcid is null
    or (
      c.mcid is not null
      and abs(coalesce(c.product_arr_change_ccfx, 0)) < abs(b.CPI_arr)
    )
  );
update ryzlan.sst_product_group_bridge_reversal_fix a
set product_bridge = 'Price Uplift Reversal'
from temp_CPI_Reversal b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and coalesce(a.current_product_group, a.prior_product_group) = b.current_product_group
  and a.evaluation_period = var_period
  and b.Split_record = 0
  and a.product_bridge in ('Downgrade', 'Churn', 'Downsell');
drop table if exists temp_cpireversal_split;
create temp table temp_cpireversal_split as
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_end_customer,
  a.prior_end_customer,
  a.mcid,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.current_product_group,
  a.prior_product_group,
  a.currency_code,
  case
    when a.product_bridge = 'Churn' then a.prior_period_product_arr_usd_ccfx - (c.product_arr_change_ccfx - b.cpi_reversal_arr)
    else a.prior_period_product_arr_usd_ccfx
  end as prior_period_product_arr_usd_ccfx,
  case
    when a.product_bridge = 'Churn' then 0
    else a.current_period_product_arr_usd_ccfx + (c.product_arr_change_ccfx - b.cpi_reversal_arr)
  end as current_period_product_arr_usd_ccfx,
  a.product_arr_change_ccfx + (c.product_arr_change_ccfx - b.cpi_reversal_arr) as product_arr_change_ccfx ---lcu
,
  case
    when a.product_bridge = 'Churn' then a.prior_period_product_arr_lcu - (c.product_arr_change_lcu - b.cpi_reversal_lcu)
    else a.prior_period_product_arr_lcu
  end as prior_period_product_arr_lcu,
  case
    when a.product_bridge = 'Churn' then 0
    else a.current_period_product_arr_lcu + (c.product_arr_change_lcu - b.cpi_reversal_lcu)
  end as current_period_product_arr_lcu,
  a.product_arr_change_lcu + (c.product_arr_change_lcu - b.cpi_reversal_lcu) as product_arr_change_lcu,
  a.product_bridge,
  a.winback_period_days,
  a.wip_flag,
  a.price_increase_amount,
  a.subsidiary_entity_name,
  a.churn_period,
  a.customer_bridge
from ryzlan.sst_product_group_bridge_reversal_fix a
  join temp_CPI_Reversal b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and coalesce(a.current_product_group, a.prior_product_group) = b.current_product_group
  join ryzlan.sst_product_group_bridge_reversal_fix c on c.mcid = b.mcid
  and c.evaluation_period = b.CPI_evaluation_period
  and coalesce(c.current_product_group, c.prior_product_group) = b.current_product_group
  and c.product_bridge = 'Price Uplift'
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.product_bridge in ('Downgrade', 'Churn', 'Downsell')
union all
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_end_customer,
  a.prior_end_customer,
  a.mcid,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.current_product_group,
  a.prior_product_group,
  a.currency_code,
  c.current_period_product_arr_usd_ccfx - b.current_period_arr_usd_ccfx_CPIR as prior_period_product_arr_usd_ccfx,
  c.prior_period_product_arr_usd_ccfx - b.prior_period_arr_usd_ccfx_CPIR as current_period_product_arr_usd_ccfx,
  - (c.product_arr_change_ccfx - b.cpi_reversal_arr) as product_arr_change_ccfx,
  c.current_period_product_arr_lcu - b.current_period_lcu_CPIR as prior_period_product_arr_lcu,
  c.prior_period_product_arr_lcu - b.prior_period_arr_lcu_CPIR as current_period_product_arr_lcu,
  - (c.product_arr_change_lcu - b.cpi_reversal_lcu) as product_arr_change_lcu,
  'Price Uplift Reversal' as product_bridge,
  a.winback_period_days,
  a.wip_flag,
  a.price_increase_amount,
  a.subsidiary_entity_name,
  a.churn_period,
  a.customer_bridge
from ryzlan.sst_product_group_bridge_reversal_fix a
  join temp_CPI_Reversal b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and coalesce(a.current_product_group, a.prior_product_group) = b.current_product_group
  join ryzlan.sst_product_group_bridge_reversal_fix c on c.mcid = b.mcid
  and c.evaluation_period = b.CPI_evaluation_period
  and coalesce(c.current_product_group, c.prior_product_group) = b.current_product_group
  and c.product_bridge = 'Price Uplift'
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.product_bridge in ('Downgrade', 'Churn', 'Downsell')
order by mcid;
delete from ryzlan.sst_product_group_bridge_reversal_fix a using temp_CPI_Reversal b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and coalesce(a.current_product_group, a.prior_product_group) = b.current_product_group
  and b.Split_record = 1
  and a.evaluation_period = var_period
  and a.product_bridge in ('Downgrade', 'Churn', 'Downsell');
insert into ryzlan.sst_product_group_bridge_reversal_fix (
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
  customer_bridge
from temp_cpireversal_split;
drop table if exists temp_CPI_Reversal;
drop table if exists temp_cpireversal_final;
drop table if exists temp_cpireversal_split;
drop table if exists temp_CPI_Rev_pg;
--###########################################
--Upsell Reversal
--###########################################
RAISE NOTICE 'Running Upsell Reversal update on sst_product_bridge_product_group...';
drop table if exists temp_downgrade_upsell;
create temp table temp_downgrade_upsell as with temp1 as (
  select a.mcid,
    a.product_bridge as product_bridge_downgrade,
    a.evaluation_period as evaluation_period_at_Downgrade,
    b.current_period as snapshot_date_at_Downgrade,
    a.product_arr_change_ccfx as Downgrade_arr,
    a.current_product_group as product_group_downgrade
  from ryzlan.sst_product_group_bridge_reversal_fix a
    join ufdm_grey.periods b on a.evaluation_period = b.evaluation_period
  where 1 = 1
    and a.product_bridge in ('Downgrade', 'Downsell', 'churn')
    and a.evaluation_period = var_period
),
temp2 as (
  select a.mcid,
    a.product_bridge_downgrade as product_bridge_downgrade,
    a.evaluation_period_at_Downgrade,
    a.snapshot_date_at_Downgrade,
    b.current_period as snapshot_date_upsell,
    a.Downgrade_arr,
    b.product_arr_change_ccfx as upsell_arr,
    b.evaluation_period as upsell_evaluation_period,
    b.product_bridge as upsell_bridge,
    a.product_group_downgrade,
    b.current_product_group as product_group_upsell,
    row_number() over (
      partition by a.mcid,
      a.evaluation_period_at_Downgrade,
      b.current_product_group
      order by b.current_period desc,
        a.snapshot_date_at_Downgrade
    ) as rnk
  from ryzlan.sst_product_group_bridge_reversal_fix b
    join temp1 a on a.mcid = b.mcid
    and a.product_group_downgrade = b.current_product_group
  where 1 = 1
    and b.product_bridge in ('Cross-sell', 'Up Sell')
    and b.current_period < (
      select current_period
      from ufdm_grey.periods
      where evaluation_period = var_period
    )
)
select *
from temp2;
drop table if exists temp_upselldowngrade_final;
create temporary table temp_upselldowngrade_final as with temp1 as (
  select *,
    row_number() over (
      partition by mcid,
      upsell_evaluation_period,
      product_group_downgrade
      order by snapshot_date_at_Downgrade
    ) as rnk2
  from temp_downgrade_upsell
  where rnk = 1
    and snapshot_date_at_Downgrade::date - snapshot_date_upsell::date < 186
),
temp2 as (
  select *
  from temp1
  where rnk2 = 1
)
select a.mcid,
  a.evaluation_period,
  b.Upsell_arr,
  b.Downgrade_arr,
  a.product_bridge,
  case
    when b.Upsell_arr < abs(b.Downgrade_arr) then 1
    else 0
  end as Split_record,
  b.upsell_evaluation_period,
  b.upsell_bridge,
  a.current_product_group as product_group_downgrade
from ryzlan.sst_product_group_bridge_reversal_fix a,
  temp2 b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period_at_Downgrade
  and a.current_product_group = b.product_group_downgrade
  and a.product_bridge in ('Downgrade', 'Downsell', 'churn')
  and a.evaluation_period = var_period;
update ryzlan.sst_product_group_bridge_reversal_fix a
set product_bridge = concat(b.upsell_bridge, ' Reversal')
from temp_upselldowngrade_final b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.current_product_group = b.product_group_downgrade
  and a.evaluation_period = var_period
  and a.product_bridge in ('Downgrade', 'Downsell', 'churn')
  and Split_record = 0;
drop table if exists temp_upselldowngrade_split;
create temp table temp_upselldowngrade_split as
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_end_customer,
  a.prior_end_customer,
  a.mcid,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.current_product_group,
  a.prior_product_group,
  a.currency_code,
  case
    when a.product_bridge = 'Churn' then a.prior_period_product_arr_usd_ccfx - c.product_arr_change_ccfx
    else a.prior_period_product_arr_usd_ccfx
  end as prior_period_product_arr_usd_ccfx,
  case
    when a.product_bridge = 'Churn' then 0
    else a.current_period_product_arr_usd_ccfx + c.product_arr_change_ccfx
  end as current_period_product_arr_usd_ccfx,
  a.product_arr_change_ccfx + c.product_arr_change_ccfx as product_arr_change_ccfx ---lcu
,
  case
    when a.product_bridge = 'Churn' then a.prior_period_product_arr_lcu - c.product_arr_change_lcu
    else a.prior_period_product_arr_lcu
  end as prior_period_product_arr_lcu,
  case
    when a.product_bridge = 'Churn' then 0
    else a.current_period_product_arr_lcu + c.product_arr_change_lcu
  end as current_period_product_arr_lcu,
  a.product_arr_change_lcu + c.product_arr_change_lcu as product_arr_change_lcu,
  a.product_bridge,
  a.winback_period_days,
  a.wip_flag,
  a.price_increase_amount,
  a.subsidiary_entity_name,
  a.churn_period,
  a.customer_bridge
from ryzlan.sst_product_group_bridge_reversal_fix a
  join temp_upselldowngrade_final b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.current_product_group = b.product_group_downgrade
  join ryzlan.sst_product_group_bridge_reversal_fix c on c.mcid = b.mcid
  and c.evaluation_period = b.upsell_evaluation_period
  and c.product_bridge = b.upsell_bridge
  and b.product_group_downgrade = c.current_product_group
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.product_bridge in ('Downgrade', 'Downsell', 'churn')
union all
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_end_customer,
  a.prior_end_customer,
  a.mcid,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.current_product_group,
  a.prior_product_group,
  a.currency_code,
  c.current_period_product_arr_usd_ccfx as prior_period_product_arr_usd_ccfx,
  c.prior_period_product_arr_usd_ccfx as current_period_product_arr_usd_ccfx,
  - c.product_arr_change_ccfx as product_arr_change_ccfx,
  c.current_period_product_arr_lcu as prior_period_product_arr_lcu,
  c.prior_period_product_arr_lcu as current_period_product_arr_lcu,
  - c.product_arr_change_lcu as product_arr_change_lcu,
  concat(b.upsell_bridge, ' Reversal') as product_bridge,
  a.winback_period_days,
  a.wip_flag,
  a.price_increase_amount,
  a.subsidiary_entity_name,
  a.churn_period,
  a.customer_bridge
from ryzlan.sst_product_group_bridge_reversal_fix a
  join temp_upselldowngrade_final b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.current_product_group = b.product_group_downgrade
  join ryzlan.sst_product_group_bridge_reversal_fix c on c.mcid = b.mcid
  and c.evaluation_period = b.upsell_evaluation_period
  and c.product_bridge = b.upsell_bridge
  and b.product_group_downgrade = c.current_product_group
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.product_bridge in ('Downgrade', 'Downsell', 'churn')
order by mcid;
delete from ryzlan.sst_product_group_bridge_reversal_fix a using temp_upselldowngrade_final b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.current_product_group = b.product_group_downgrade
  and b.Split_record = 1
  and a.evaluation_period = var_period
  and a.product_bridge in ('Downgrade', 'Downsell', 'churn');
insert into ryzlan.sst_product_group_bridge_reversal_fix (
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
  customer_bridge
from temp_upselldowngrade_split;
drop table if exists temp_downgrade_upsell;
drop table if exists temp_upselldowngrade_final;
drop table if exists temp_upselldowngrade_split;
--#############################################
--Price Ramps
--#############################################
RAISE NOTICE 'Running Price Ramps on sst product bridge...';
drop table if exists temp_product_bridge_price_ramps;
create temp table temp_product_bridge_price_ramps as with cte as (
    select mcid,
        snapshot_date,
        b.evaluation_period,
        c."Product Group" as updated_product_group,
        sum(Price_Ramp) as PriceRamp_Value,
        sum(Price_Ramp_lcu) as PriceRamp_Value_lcu
    from sandbox_pd.Price_Ramps a
        join ufdm_grey.periods b on a.snapshot_date = b.current_period
        join ufdm_grey.product_hierarchy_mappings c on a.sku = c."Product Code"
        and c."Included in ARR" = 'Y'
    where b.evaluation_period = var_period
    group by c_name,
        mcid,
        snapshot_date,
        c."Product Group",
        b.evaluation_period
)
select pr.evaluation_period,
    pr.prior_period,
    pr.current_period,
    pr.mcid,
    pr.prior_period_product_arr_usd_ccfx,
    pr.current_period_product_arr_usd_ccfx,
    pr.product_arr_change_ccfx,
    pr.product_bridge,
    pr.product_arr_change_lcu,
    pr.prior_period_product_arr_lcu,
    cte.PriceRamp_Value,
    cte.PriceRamp_Value_lcu,
    cte.snapshot_date,
    pr.current_product_group
from ryzlan.sst_product_group_bridge_reversal_fix pr
    inner join cte on pr.mcid = cte.mcid
    and pr.evaluation_period = cte.evaluation_period
    and pr.current_product_group = cte.updated_product_group
where pr.product_bridge = 'Up Sell'
    and pr.evaluation_period = var_period;
update ryzlan.sst_product_group_bridge_reversal_fix a
set product_bridge = 'Price Ramp'
from temp_product_bridge_price_ramps b
where a.mcid = b.mcid
    and a.evaluation_period = b.evaluation_period
    and coalesce(a.product_arr_change_ccfx::numeric, 0) <= coalesce(b.PriceRamp_Value::numeric, 0)
    and a.product_bridge = 'Up Sell'
    and a.evaluation_period = var_period
    and a.current_product_group = b.current_product_group;
drop table if exists temp_Price_Ramp_split;
create temp table temp_Price_Ramp_split as
select distinct a.evaluation_period,
    a.prior_period,
    a.current_period,
    a.current_master_customer_id,
    a.prior_master_customer_id,
    a.mcid,
    a.current_product_group,
    a.prior_product_group,
    a.current_end_customer,
    a.prior_end_customer,
    a.currency_code,
    a.prior_period_product_arr_usd_ccfx as prior_arr_usd_ccfx,
    a.current_period_product_arr_usd_ccfx - b.PriceRamp_Value as current_arr_usd_ccfx,
    a.product_arr_change_ccfx - b.PriceRamp_Value as product_arr_change_ccfx,
    a.prior_period_product_arr_lcu as prior_arr_lcu,
    a.current_period_product_arr_lcu - b.PriceRamp_Value_lcu as current_arr_lcu,
    a.product_arr_change_lcu - b.PriceRamp_Value_lcu as product_arr_change_lcu,
    a.product_bridge
from ryzlan.sst_product_group_bridge_reversal_fix a
    join temp_product_bridge_price_ramps b on a.mcid = b.mcid
    and a.evaluation_period = b.evaluation_period
    and a.product_bridge = b.product_bridge
    and a.current_product_group = b.current_product_group
where coalesce(a.product_arr_change_ccfx::numeric, 0) > coalesce(b.PriceRamp_Value::numeric, 0)
    and a.evaluation_period = var_period
union all
select distinct a.evaluation_period,
    a.prior_period,
    a.current_period,
    a.current_master_customer_id,
    a.prior_master_customer_id,
    a.mcid,
    a.current_product_group,
    a.prior_product_group,
    a.current_end_customer,
    a.prior_end_customer,
    a.currency_code,
    '0'::numeric as prior_arr_usd_ccfx,
    b.PriceRamp_Value as current_arr_usd_ccfx,
    b.PriceRamp_Value as product_arr_change_ccfx,
    '0'::numeric as prior_arr_lcu,
    b.PriceRamp_Value_lcu as current_arr_lcu,
    b.PriceRamp_Value_lcu as product_arr_change_lcu,
    'Price Ramp' as product_bridge
from ryzlan.sst_product_group_bridge_reversal_fix a
    join temp_product_bridge_price_ramps b on a.mcid = b.mcid
    and a.evaluation_period = b.evaluation_period
    and a.product_bridge = b.product_bridge
    and a.current_product_group = b.current_product_group
where coalesce(a.product_arr_change_ccfx::numeric, 0) > coalesce(b.PriceRamp_Value::numeric, 0)
    and a.evaluation_period = var_period
order by mcid;
delete from ryzlan.sst_product_group_bridge_reversal_fix a using temp_product_bridge_price_ramps b
where 1 = 1
    and a.mcid = b.mcid
    and a.evaluation_period = b.evaluation_period
    and coalesce(a.product_arr_change_ccfx::numeric, 0) > coalesce(b.PriceRamp_Value::numeric, 0)
    and a.product_bridge = 'Up Sell'
    and a.evaluation_period = var_period
    and a.current_product_group = b.current_product_group;
insert into ryzlan.sst_product_group_bridge_reversal_fix (
        evaluation_period,
        prior_period,
        current_period,
        current_master_customer_id,
        prior_master_customer_id,
        mcid,
        current_product_group,
        prior_product_group,
        current_end_customer,
        prior_end_customer,
        currency_code,
        current_period_product_arr_usd_ccfx,
        prior_period_product_arr_usd_ccfx,
        product_arr_change_ccfx,
        current_period_product_arr_lcu,
        prior_period_product_arr_lcu,
        product_arr_change_lcu,
        product_bridge
    )
select evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    current_product_group,
    prior_product_group,
    current_end_customer,
    prior_end_customer,
    currency_code,
    current_arr_usd_ccfx,
    prior_arr_usd_ccfx,
    product_arr_change_ccfx,
    current_arr_lcu,
    prior_arr_lcu,
    product_arr_change_lcu,
    product_bridge
from temp_Price_Ramp_split;
drop table if exists temp_product_bridge_price_ramps;
drop table if exists temp_Price_Ramp_split;
RAISE NOTICE 'Running rounding errors update on sst product bridge group...';
--rounding errors updates
update ryzlan.sst_product_group_bridge_reversal_fix
set product_bridge = 'Rounding'
where product_bridge = 'Flat'
  and coalesce(product_arr_change_ccfx, 0) <> 0
  and evaluation_period = var_period;
END;
$$;