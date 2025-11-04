create or replace function ryzlan.sp_populate_sst_customer_bridge(var_period text) returns void language plpgsql as $$ BEGIN RAISE NOTICE 'Running sst_customer_bridge for %...',
  var_period;
DELETE from ryzlan.sst_customer_bridge
where evaluation_period = var_period;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--SST customer Bridge
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
RAISE NOTICE 'Running sst_customer_bridge for %',
var_period;
drop table if exists prior_period_customer_arr;
create temp table prior_period_customer_arr as
SELECT snapshot_date,
  a.mcid as master_customer_id,
  a.base_currency as baseline_currency,
  sum(arr) AS arr_usd_ccfx,
  SUM(baseline_arr_local_currency) as arr_lcu
FROM ryzlan.sst_adhoc a
WHERE 1 = 1
  AND snapshot_date = (
    SELECT prior_period
    from ufdm_grey.periods
    WHERE evaluation_period = var_period
  )
  and a.overage_flag ilike '%N%'
GROUP BY 1,
  2,
  3;
drop table if exists current_period_customer_arr;
create temp table current_period_customer_arr as
SELECT snapshot_date,
  a.mcid as master_customer_id,
  a.base_currency as baseline_currency,
  sum(arr) AS arr_usd_ccfx,
  SUM(baseline_arr_local_currency) as arr_lcu
FROM ryzlan.sst_adhoc a
WHERE 1 = 1
  AND snapshot_date = (
    SELECT current_period
    from ufdm_grey.periods
    WHERE evaluation_period = var_period
  )
  and a.overage_flag ilike '%N%'
GROUP BY 1,
  2,
  3;
drop table if exists customer_level_arr;
create temp table customer_level_arr as
SELECT c1.master_customer_id AS current_cust_id,
  c2.master_customer_id AS prior_cust_id,
  c2.snapshot_date AS prior_period,
  c1.snapshot_date AS current_period,
  c1.baseline_currency as current_baseline_currency,
  c2.baseline_currency as prior_baseline_currency,
  coalesce(c1.arr_usd_ccfx, 0) AS current_arr_usd_ccfx,
  coalesce(c2.arr_usd_ccfx, 0) AS prior_arr_usd_ccfx,
  coalesce(c1.arr_lcu, 0) AS current_arr_lcu,
  coalesce(c2.arr_lcu, 0) AS prior_arr_lcu,
  COALESCE(c1.baseline_currency, c2.baseline_currency) AS baseline_currency
FROM current_period_customer_arr c1
  FULL OUTER JOIN prior_period_customer_arr c2 ON c1.master_customer_id = c2.master_customer_id
  and c1.baseline_currency = c2.baseline_currency;
drop table if exists account;
create temp table account as
SELECT coalesce(a.dynamics_id_c, a.sf_guid_c) as master_customer_id,
  a.name,
  row_number() over (
    partition by coalesce(a.dynamics_id_c, a.sf_guid_c)
  ) as rn
FROM opti_salesforce.account a
WHERE a.is_deleted IS DISTINCT
FROM TRUE;
------------------------------------------
-- Evaluate bridge categories
-- New
-- Upsell
-- Flat
-- Churn
-- Partial Churn
--------------------------------------------------
drop table if exists arr_bridge_tmp;
create temp table arr_bridge_tmp as with arr_bridge AS (
  SELECT per.evaluation_period,
    cla.prior_period,
    cla.current_period,
    cla.current_cust_id as current_master_customer_id,
    cla.prior_cust_id as prior_master_customer_id,
    coalesce(current_cust_id, prior_cust_id) as mcid,
    a.name,
    cla.baseline_currency,
    null as subsidiary_entity_name,
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
    ) AS customer_arr_change_ccfx,
    round((coalesce(cla.current_arr_lcu::numeric, 0)), 2) as current_arr_lcu,
    round((coalesce(cla.prior_arr_lcu::numeric, 0)), 2) as prior_arr_lcu,
    round(
      (
        coalesce(cla.current_arr_lcu::numeric, 0) - coalesce(cla.prior_arr_lcu::numeric, 0)
      ),
      2
    ) AS customer_arr_change_lcu,
    CASE
      WHEN (
        coalesce (cla.prior_arr_usd_ccfx, 0) = 0
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
    END AS customer_bridge
  FROM customer_level_arr cla
    LEFT JOIN account a on a.master_customer_id = cla.current_cust_id
    and a.rn = 1
    CROSS JOIN ufdm_grey.periods per
  WHERE 1 = 1
    AND per.evaluation_period = var_period
  ORDER BY coalesce(a.name)
)
select *
from arr_bridge;
--#############################################
--Price Ramps
--#############################################
drop table if exists temp_customer_bridge_price_ramps;
create temp table temp_customer_bridge_price_ramps as with cte as (
  select mcid,
    snapshot_date,
    sum(Price_Ramp) as PriceRamp_Value,
    sum(Price_Ramp_lcu) as PriceRamp_Value_lcu
  from sandbox_pd.Price_Ramps a
    join ufdm_grey.periods b on a.snapshot_date = b.current_period --where b.evaluation_period = var_period
  group by c_name,
    mcid,
    snapshot_date
)
select pr.evaluation_period,
  pr.prior_period,
  pr.current_period,
  pr.mcid,
  pr.prior_arr_usd_ccfx as prior_period_customer_arr_usd_ccfx,
  pr.current_arr_usd_ccfx as current_period_customer_arr_usd_ccfx,
  pr.customer_arr_change_ccfx,
  pr.customer_bridge,
  pr.customer_arr_change_lcu,
  pr.prior_arr_lcu,
  cte.PriceRamp_Value,
  cte.PriceRamp_Value_lcu,
  cte.snapshot_date
from arr_bridge_tmp pr
  inner join cte on pr.mcid = cte.mcid
  and pr.current_period = cte.snapshot_date
where pr.customer_bridge = 'Up Sell';
update arr_bridge_tmp a
set customer_bridge = 'Price Ramp'
from temp_customer_bridge_price_ramps b
where a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and coalesce(a.customer_arr_change_ccfx::numeric, 0) <= coalesce(b.PriceRamp_Value::numeric, 0)
  and a.customer_bridge = 'Up Sell';
drop table if exists temp_Price_Ramp_split;
create temp table temp_Price_Ramp_split as
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  a.prior_arr_usd_ccfx as prior_period_customer_arr_usd_ccfx,
  a.current_arr_usd_ccfx - b.PriceRamp_Value as current_period_customer_arr_usd_ccfx,
  a.customer_arr_change_ccfx - b.PriceRamp_Value as customer_arr_change_ccfx,
  a.prior_arr_lcu as prior_period_customer_arr_lcu,
  a.current_arr_lcu - b.PriceRamp_Value_lcu as current_period_customer_lcu,
  a.customer_arr_change_lcu - b.PriceRamp_Value_lcu as customer_arr_change_lcu,
  a.customer_bridge
from arr_bridge_tmp a
  join temp_customer_bridge_price_ramps b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.customer_bridge = b.customer_bridge
where coalesce(a.customer_arr_change_ccfx::numeric, 0) > coalesce(b.PriceRamp_Value::numeric, 0)
union all
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  '0'::numeric as prior_period_customer_arr_usd_ccfx,
  b.PriceRamp_Value as current_period_customer_arr_usd_ccfx,
  b.PriceRamp_Value as customer_arr_change_ccfx,
  '0'::numeric as prior_period_customer_arr_lcu,
  b.PriceRamp_Value_lcu as current_period_customer_lcu,
  b.PriceRamp_Value_lcu as customer_arr_change_lcu,
  'Price Ramp' as customer_bridge
from arr_bridge_tmp a
  join temp_customer_bridge_price_ramps b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.customer_bridge = b.customer_bridge
where coalesce(a.customer_arr_change_ccfx::numeric, 0) > coalesce(b.PriceRamp_Value::numeric, 0)
order by mcid;
delete from arr_bridge_tmp a using temp_customer_bridge_price_ramps b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and coalesce(a.customer_arr_change_ccfx::numeric, 0) > coalesce(b.PriceRamp_Value::numeric, 0)
  and a.customer_bridge = 'Up Sell';
insert into arr_bridge_tmp (
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    name,
    baseline_currency,
    subsidiary_entity_name,
    prior_arr_usd_ccfx,
    current_arr_usd_ccfx,
    customer_arr_change_ccfx,
    prior_arr_lcu,
    current_arr_lcu,
    customer_arr_change_lcu,
    customer_bridge
  )
select evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  name,
  baseline_currency,
  subsidiary_entity_name,
  prior_period_customer_arr_usd_ccfx,
  current_period_customer_arr_usd_ccfx,
  customer_arr_change_ccfx,
  prior_period_customer_arr_lcu,
  current_period_customer_lcu,
  customer_arr_change_lcu,
  customer_bridge
from temp_Price_Ramp_split;
--#############################################
--Downgrade
--#############################################
update arr_bridge_tmp
set customer_bridge = 'Downgrade'
where customer_bridge = 'Partial Churn';
--###########################################
--DOWNSELL
--###########################################
--RAISE NOTICE 'Running downsell update on sst_customer_bridge...';
drop table if exists temp_pb_downsell;
create temp table temp_pb_downsell as
select mcid,
  product_bridge,
  currency_code,
  evaluation_period,
  sum(product_arr_change_ccfx) as product_arr_change_ccfx,
  sum(product_arr_change_lcu) as product_arr_change_lcu
from sandbox.sst_product_group_bridge
where 1 = 1
  and product_bridge in ('Downsell', 'Downsell - migration')
  and evaluation_period = var_period
group by 1,
  2,
  3,
  4;
drop table if exists temp_pb_downsell_final;
create temp table temp_pb_downsell_final as with temp as (
  SELECT a.*,
    b.product_bridge,
    b.product_arr_change_ccfx,
    b.product_arr_change_lcu,
    count(a.mcid) over(
      PARTITION BY a.mcid,
      a.evaluation_period,
      a.baseline_currency,
      a.customer_bridge
    ) AS count_pb_records,
    count(b.mcid) over(
      PARTITION BY b.mcid,
      b.evaluation_period,
      b.currency_code,
      b.product_bridge
    ) AS count_pb_bridges,
    sum(product_arr_change_ccfx) over(
      PARTITION BY a.mcid,
      a.evaluation_period,
      a.baseline_currency,
      a.customer_bridge
    ) AS total_pb_amount_ccfx,
    sum(product_arr_change_lcu) over(
      PARTITION BY a.mcid,
      a.evaluation_period,
      a.baseline_currency,
      a.customer_bridge
    ) AS total_pb_amount_lcu
  from arr_bridge_tmp a
    LEFT join temp_pb_downsell b on a.mcid = b.mcid
    AND b.currency_code = a.baseline_currency
  where a.customer_bridge not in ('Flat', 'Rounding')
    and a.evaluation_period = b.evaluation_period
    and customer_arr_change_ccfx < 0 --    AND a.mcid = 'd74620b9-768f-dd11-a26e-0018717a8c82'
),
classifier AS (
  select *,
    CASE
      WHEN abs(round(total_pb_amount_ccfx)) = abs(round(customer_arr_change_ccfx)) THEN 'EQUAL'
      ELSE CASE
        WHEN abs(round(total_pb_amount_ccfx)) < abs(round(customer_arr_change_ccfx)) THEN 'LESS'
        ELSE CASE
          WHEN abs(round(total_pb_amount_ccfx)) > abs(round(customer_arr_change_ccfx)) THEN CASE
            WHEN count_pb_records > count_pb_bridges
            OR product_bridge ILIKE ('%migration%') THEN 'GREATER WITH MIGRATION'
            ELSE CASE
              WHEN product_bridge NOT ILIKE ('%migration%') THEN 'GREATER WITHOUT MIGRATION'
              ELSE NULL
            END
          END
        END
      END
    END AS flag
  from TEMP
)
SELECT a.*,
  CASE
    WHEN abs(round(total_pb_amount_ccfx)) < abs(round(customer_arr_change_ccfx)) THEN round(
      (customer_arr_change_ccfx - total_pb_amount_ccfx) / count_pb_records
    )
    ELSE NULL
  END AS leftover_ccfx,
  CASE
    WHEN abs(round(total_pb_amount_lcu)) < abs(round(customer_arr_change_lcu)) THEN round(
      customer_arr_change_lcu - total_pb_amount_lcu / count_pb_records
    )
    ELSE NULL
  END AS leftover_lcu
FROM classifier AS a;
-- Handle Equal delete previous record add new record using product group bridge
DELETE FROM arr_bridge_tmp AS a USING temp_pb_downsell_final AS b
WHERE a.evaluation_period = b.evaluation_period
  AND a.mcid = b.mcid
  AND a.baseline_currency = b.baseline_currency
  AND a.customer_bridge = b.customer_bridge
  AND b.flag IN ('EQUAL', 'LESS');
insert into arr_bridge_tmp (
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    name,
    baseline_currency,
    subsidiary_entity_name,
    prior_arr_usd_ccfx,
    current_arr_usd_ccfx,
    customer_arr_change_ccfx,
    prior_arr_lcu,
    current_arr_lcu,
    customer_arr_change_lcu,
    customer_bridge --, winback_period_days, wip_flag
  )
select evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  name,
  baseline_currency,
  subsidiary_entity_name,
  prior_arr_usd_ccfx * abs(
    product_arr_change_ccfx / customer_arr_change_ccfx
  ) AS prior_arr_usd_ccfx,
  current_arr_usd_ccfx * abs(
    product_arr_change_ccfx / customer_arr_change_ccfx
  ) AS current_arr_usd_ccfx,
  product_arr_change_ccfx AS customer_arr_change_ccfx,
  --customer_arr_change_ccfx,
  prior_arr_lcu * abs(
    product_arr_change_lcu / customer_arr_change_lcu
  ) AS prior_arr_lcu,
  current_arr_lcu * abs(
    product_arr_change_lcu / customer_arr_change_lcu
  ) AS current_arr_lcu,
  product_arr_change_lcu AS customer_arr_change_lcu,
  --  customer_arr_change_lcu,
  product_bridge AS customer_bridge --customer_bridge --, winback_period_days, wip_flag
from temp_pb_downsell_final AS b
WHERE b.flag IN ('EQUAL', 'LESS');
insert into arr_bridge_tmp (
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    name,
    baseline_currency,
    subsidiary_entity_name,
    prior_arr_usd_ccfx,
    current_arr_usd_ccfx,
    customer_arr_change_ccfx,
    prior_arr_lcu,
    current_arr_lcu,
    customer_arr_change_lcu,
    customer_bridge --, winback_period_days, wip_flag
  )
SELECT evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  name,
  baseline_currency,
  subsidiary_entity_name,
  sum(prior_arr_usd_ccfx) AS prior_arr_usd_ccfx,
  sum(current_arr_usd_ccfx) AS current_arr_usd_ccfx,
  sum(customer_arr_change_ccfx) AS customer_arr_change_ccfx,
  sum(prior_arr_lcu) AS prior_arr_lcu,
  sum(current_arr_lcu) AS current_arr_lcu,
  sum(customer_arr_change_lcu) AS customer_arr_change_lcu,
  customer_bridge
FROM (
    select evaluation_period,
      prior_period,
      current_period,
      current_master_customer_id,
      prior_master_customer_id,
      mcid,
      name,
      baseline_currency,
      subsidiary_entity_name,
      prior_arr_usd_ccfx * abs(leftover_ccfx / customer_arr_change_ccfx) AS prior_arr_usd_ccfx,
      current_arr_usd_ccfx * abs(leftover_ccfx / customer_arr_change_ccfx) AS current_arr_usd_ccfx,
      leftover_ccfx AS customer_arr_change_ccfx,
      --customer_arr_change_ccfx,
      prior_arr_lcu * abs(leftover_lcu / customer_arr_change_lcu) AS prior_arr_lcu,
      current_arr_lcu * abs(leftover_lcu / customer_arr_change_lcu) AS current_arr_lcu,
      leftover_lcu AS customer_arr_change_lcu,
      --  customer_arr_change_lcu,
      customer_bridge --, winback_period_days, wip_flag
    from temp_pb_downsell_final AS b
    WHERE b.flag IN ('LESS')
  ) AS a
GROUP BY 1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  16;
UPDATE arr_bridge_tmp AS a
SET customer_bridge = CASE
    WHEN b.flag = 'GREATER WITH MIGRATION' THEN 'Downsell - migration'
    ELSE 'Downsell'
  END
FROM temp_pb_downsell_final AS b
WHERE a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.customer_bridge = b.customer_bridge
  AND b.flag IN (
    'GREATER WITH MIGRATION',
    'GREATER WITHOUT MIGRATION'
  );
--############################
--cross sell
--############################
--RAISE NOTICE 'Running crossell update on sst_customer_bridge...';
drop table if exists temp_pb_crosssell;
create temp table temp_pb_crosssell as
select mcid,
  product_bridge,
  currency_code,
  evaluation_period,
  sum(product_arr_change_ccfx) as product_arr_change_ccfx,
  sum(product_arr_change_lcu) as product_arr_change_lcu
from sandbox.sst_product_group_bridge
where 1 = 1
  and product_bridge IN ('Cross-sell', 'Cross-sell - migration')
  and evaluation_period = var_period
group by 1,
  2,
  3,
  4;
drop table if exists temp_pb_crosssell_final;
create temp table temp_pb_crosssell_final as with temp as (
  SELECT a.*,
    b.product_bridge,
    b.product_arr_change_ccfx,
    b.product_arr_change_lcu,
    count(a.mcid) over(
      PARTITION BY a.mcid,
      a.evaluation_period,
      a.baseline_currency,
      a.customer_bridge
    ) AS count_pb_records,
    count(b.mcid) over(
      PARTITION BY b.mcid,
      b.evaluation_period,
      b.currency_code,
      b.product_bridge
    ) AS count_pb_bridges,
    sum(product_arr_change_ccfx) over(
      PARTITION BY a.mcid,
      a.evaluation_period,
      a.baseline_currency,
      a.customer_bridge
    ) AS total_pb_amount_ccfx,
    sum(product_arr_change_lcu) over(
      PARTITION BY a.mcid,
      a.evaluation_period,
      a.baseline_currency,
      a.customer_bridge
    ) AS total_pb_amount_lcu
  from arr_bridge_tmp a
    LEFT join temp_pb_crosssell b on a.mcid = b.mcid
    AND b.currency_code = a.baseline_currency
  where a.customer_bridge not in ('Flat', 'Rounding')
    and a.evaluation_period = b.evaluation_period
    and customer_arr_change_ccfx > 0 --    
    --    AND a.mcid = 'b26166bb-17ca-12e3-c980-affe0b81894d'
),
classifier AS (
  select *,
    CASE
      WHEN abs(round(total_pb_amount_ccfx)) = abs(round(customer_arr_change_ccfx)) THEN 'EQUAL'
      ELSE CASE
        WHEN abs(round(total_pb_amount_ccfx)) < abs(round(customer_arr_change_ccfx)) THEN 'LESS'
        ELSE CASE
          WHEN abs(round(total_pb_amount_ccfx)) > abs(round(customer_arr_change_ccfx)) THEN CASE
            WHEN count_pb_records > count_pb_bridges
            OR product_bridge ILIKE ('%migration%') THEN 'GREATER WITH MIGRATION'
            ELSE CASE
              WHEN product_bridge NOT ILIKE ('%migration%') THEN 'GREATER WITHOUT MIGRATION'
              ELSE NULL
            END
          END
        END
      END
    END AS flag
  from TEMP
)
SELECT a.*,
  CASE
    WHEN abs(round(total_pb_amount_ccfx)) < abs(round(customer_arr_change_ccfx)) THEN round(
      (customer_arr_change_ccfx - total_pb_amount_ccfx) / count_pb_records
    )
    ELSE NULL
  END AS leftover_ccfx,
  CASE
    WHEN abs(round(total_pb_amount_lcu)) < abs(round(customer_arr_change_lcu)) THEN round(
      customer_arr_change_lcu - total_pb_amount_lcu / count_pb_records
    )
    ELSE NULL
  END AS leftover_lcu
FROM classifier AS a;
DELETE FROM arr_bridge_tmp AS a USING temp_pb_crosssell_final AS b
WHERE a.evaluation_period = b.evaluation_period
  AND a.mcid = b.mcid
  AND a.baseline_currency = b.baseline_currency
  AND a.customer_bridge = b.customer_bridge
  AND b.flag IN ('EQUAL', 'LESS');
insert into arr_bridge_tmp (
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    name,
    baseline_currency,
    subsidiary_entity_name,
    prior_arr_usd_ccfx,
    current_arr_usd_ccfx,
    customer_arr_change_ccfx,
    prior_arr_lcu,
    current_arr_lcu,
    customer_arr_change_lcu,
    customer_bridge --, winback_period_days, wip_flag
  )
select evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  name,
  baseline_currency,
  subsidiary_entity_name,
  prior_arr_usd_ccfx * abs(
    product_arr_change_ccfx / customer_arr_change_ccfx
  ) AS prior_arr_usd_ccfx,
  current_arr_usd_ccfx * abs(
    product_arr_change_ccfx / customer_arr_change_ccfx
  ) AS current_arr_usd_ccfx,
  product_arr_change_ccfx AS customer_arr_change_ccfx,
  --customer_arr_change_ccfx,
  prior_arr_lcu * abs(
    product_arr_change_lcu / customer_arr_change_lcu
  ) AS prior_arr_lcu,
  current_arr_lcu * abs(
    product_arr_change_lcu / customer_arr_change_lcu
  ) AS current_arr_lcu,
  product_arr_change_lcu AS customer_arr_change_lcu,
  --  customer_arr_change_lcu,
  product_bridge AS customer_bridge --customer_bridge --, winback_period_days, wip_flag
from temp_pb_crosssell_final AS b
WHERE b.flag IN ('EQUAL', 'LESS');
insert into arr_bridge_tmp (
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    name,
    baseline_currency,
    subsidiary_entity_name,
    prior_arr_usd_ccfx,
    current_arr_usd_ccfx,
    customer_arr_change_ccfx,
    prior_arr_lcu,
    current_arr_lcu,
    customer_arr_change_lcu,
    customer_bridge --, winback_period_days, wip_flag
  )
SELECT evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  name,
  baseline_currency,
  subsidiary_entity_name,
  sum(prior_arr_usd_ccfx) AS prior_arr_usd_ccfx,
  sum(current_arr_usd_ccfx) AS current_arr_usd_ccfx,
  sum(customer_arr_change_ccfx) AS customer_arr_change_ccfx,
  sum(prior_arr_lcu) AS prior_arr_lcu,
  sum(current_arr_lcu) AS current_arr_lcu,
  sum(customer_arr_change_lcu) AS customer_arr_change_lcu,
  customer_bridge
FROM (
    select evaluation_period,
      prior_period,
      current_period,
      current_master_customer_id,
      prior_master_customer_id,
      mcid,
      name,
      baseline_currency,
      subsidiary_entity_name,
      prior_arr_usd_ccfx * abs(leftover_ccfx / customer_arr_change_ccfx) AS prior_arr_usd_ccfx,
      current_arr_usd_ccfx * abs(leftover_ccfx / customer_arr_change_ccfx) AS current_arr_usd_ccfx,
      leftover_ccfx AS customer_arr_change_ccfx,
      --customer_arr_change_ccfx,
      prior_arr_lcu * abs(leftover_lcu / customer_arr_change_lcu) AS prior_arr_lcu,
      current_arr_lcu * abs(leftover_lcu / customer_arr_change_lcu) AS current_arr_lcu,
      leftover_lcu AS customer_arr_change_lcu,
      --  customer_arr_change_lcu,
      customer_bridge --, winback_period_days, wip_flag ('Cross-sell', 'Cross-sell - migration')
    from temp_pb_crosssell_final AS b
    WHERE b.flag IN ('LESS')
  ) AS a
GROUP BY 1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  16;
UPDATE arr_bridge_tmp AS a
SET customer_bridge = CASE
    WHEN b.flag = 'GREATER WITH MIGRATION' THEN 'Cross-sell - migration'
    ELSE 'Cross-sell'
  END
FROM temp_pb_crosssell_final AS b
WHERE a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.customer_bridge = b.customer_bridge
  AND b.flag IN (
    'GREATER WITH MIGRATION',
    'GREATER WITHOUT MIGRATION'
  );
--#############################################
--CPI
--#############################################
RAISE NOTICE 'Running Price increase update on sst customer bridge...';
--Price Increase updates
update arr_bridge_tmp
set customer_bridge = 'Price Uplift'
where customer_bridge = 'Up Sell'
  and (
    (customer_arr_change_ccfx / prior_arr_usd_ccfx) * 100
  )::numeric(10, 2) < case
    when evaluation_period < '2023-01-01' then 5.5
    else 10.5
  end
  and prior_arr_usd_ccfx > 0
  and evaluation_period = var_period;
--#############################################
--WIP/WINBACK
--#############################################
drop table if exists arr_new_products_tmp;
create temp table arr_new_products_tmp AS
select distinct a.mcid as mcid,
  a.current_period as snapshot_date,
  a.current_arr_usd_ccfx as arr_at_new,
  a.current_arr_lcu as arr_lcu_at_new,
  baseline_currency
from arr_bridge_tmp a
where customer_bridge in ('New');
--get most recent postivie arr for above new product which should have been churned
drop table if exists arr_churned_products_tmp;
create temp table arr_churned_products_tmp AS with temp as (
  select b.snapshot_date,
    b.mcid --, b.product_family
,
    a.baseline_currency,
    a.snapshot_date as snapshot_date_at_new,
    sum(b.arr) as arr_at_churn,
    sum(b.baseline_arr_local_currency) as arr_lcu_at_churn,
    max(a.arr_at_new) as arr_at_new,
    max(a.arr_lcu_at_new) as arr_lcu_at_new,
    row_number() over (
      partition by b.mcid
      order by b.snapshot_date desc
    ) as rnk
  from arr_new_products_tmp a
    join ryzlan.sst_adhoc b on a.mcid = b.mcid
    and a.baseline_currency = b.base_currency
  where b.snapshot_date < a.snapshot_date
    and b.overage_flag ilike '%N%'
    and b.arr > 0
  group by 1,
    2,
    3,
    4
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
  end as customer_bridge_new,
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
INSERT INTO ryzlan.sst_customer_bridge (
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    Prior_master_customer_id,
    "name",
    prior_period_customer_arr_usd_ccfx,
    current_period_customer_arr_usd_ccfx,
    customer_arr_change_ccfx,
    prior_period_customer_arr_lcu,
    current_period_customer_lcu,
    customer_arr_change_lcu,
    customer_bridge,
    subsidiary_entity_name,
    mcid,
    baseline_currency,
    Winback_period_days,
    Wip_Flag
  )
SELECT a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.Prior_master_customer_id,
  a."name",
  ---usd ccfx ----
  round(a.prior_arr_usd_ccfx::numeric, 2) AS prior_period_customer_arr_usd_ccfx,
  case
    when b.mcid is not null then case
      when b.arr_at_new > b.arr_at_churn then b.arr_at_churn
      else b.arr_at_new
    end
    else round(a.current_arr_usd_ccfx::numeric, 2)
  end as current_period_customer_arr_usd_ccfx,
  case
    when b.mcid is not null then case
      when b.arr_at_new > b.arr_at_churn then b.arr_at_churn
      else b.arr_at_new
    end
    else a.customer_arr_change_ccfx
  end as customer_arr_change_ccfx,
  ------------------------lcu----------------------------
  round(a.prior_arr_lcu::numeric, 2) AS prior_period_customer_arr_lcu,
  case
    when b.mcid is not null then case
      when b.arr_lcu_at_new > b.arr_lcu_at_churn then b.arr_lcu_at_churn
      else b.arr_lcu_at_new
    end
    else round(a.current_arr_lcu::numeric, 2)
  end as current_period_customer_arr_lcu,
  case
    when b.mcid is not null then case
      when b.arr_lcu_at_new > b.arr_lcu_at_churn then b.arr_lcu_at_churn
      else b.arr_lcu_at_new
    end
    else a.customer_arr_change_lcu
  end as customer_arr_change_lcu,
  case
    when b.mcid is not null then case
      when b.days_diff <= 90 then 'Winback ST'
      else 'Winback LT'
    end
    else a.customer_bridge
  end as customer_bridge,
  a.subsidiary_entity_name,
  a.mcid,
  a.baseline_currency,
  b.days_diff as Winback_period_days,
  case
    when b.days_diff <= 90 then 'Y'
    else 'N'
  end as Wip_Flag
FROM arr_bridge_tmp a
  left join arr_churned_products_tmp b on a.current_master_customer_id = b.mcid
  and a.baseline_currency = b.baseline_currency
  and a.current_period = b.snapshot_date_at_new
union all
SELECT a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.Prior_master_customer_id,
  a."name",
  round(a.prior_arr_usd_ccfx::numeric, 2) AS prior_period_customer_arr_usd_ccfx,
  b.arr_diff as current_period_customer_arr_usd_ccfx,
  b.arr_diff as customer_arr_change_ccfx,
  round(a.prior_arr_lcu::numeric, 2) AS prior_period_customer_arr_lcu,
  b.arr_lcu_diff as current_period_customer_arr_lcu,
  b.arr_lcu_diff,
  'Up Sell' as customer_bridge,
  a.subsidiary_entity_name,
  a.mcid,
  a.baseline_currency,
  null,
  null
FROM arr_bridge_tmp a
  join arr_churned_products_tmp b on a.current_master_customer_id = b.mcid
  and a.baseline_currency = b.baseline_currency
  and a.current_period = b.snapshot_date_at_new
where b.arr_at_new > b.arr_at_churn;
RAISE NOTICE 'Running subsidiary entity name insert on sst customer bridge...';
drop table if exists sub_entity_tmp;
create temp table sub_entity_tmp as --update subsidiary_entity_name
with mcid_list as (
  select distinct mcid as master_customer_id
  from arr_bridge_tmp
  where evaluation_period = var_period
),
total_arr as (
  select a.mcid as mcid,
    a.snapshot_date,
    a.subsidiary_entity_name,
    sum(a.arr) as total_arr
  from ryzlan.sst_adhoc a
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
RAISE NOTICE 'Running sub entity update on sst customer bridge...';
create index nci_sub_entity_tmp_mcid on sub_entity_tmp(mcid);
update ryzlan.sst_customer_bridge a
set subsidiary_entity_name = b.subsidiary_entity_name
from sub_entity_tmp b
where a.mcid = b.mcid
  and a.evaluation_period = var_period;
--###########################################
--WINBACK Downgrade
--###########################################
RAISE NOTICE 'Running WINBACK Downgrade update on sst customer bridge 1...';
drop table if exists temp_win_downgrade_upsell;
create temp table temp_win_downgrade_upsell as with temp1 as (
  select a.mcid,
    a.customer_bridge,
    a.evaluation_period as evaluation_period_at_upsell,
    a.current_period as snapshot_date_at_upsell,
    a.customer_arr_change_ccfx as Upsell_crosssell_arr,
    a.customer_arr_change_lcu as Upsell_crosssell_arr_lcu
  from ryzlan.sst_customer_bridge a
  where 1 = 1
    and a.customer_bridge in ('Cross-sell', 'Up Sell')
    and a.evaluation_period = var_period
),
temp2 as (
  select a.mcid,
    a.customer_bridge,
    a.evaluation_period_at_upsell,
    a.snapshot_date_at_upsell,
    b.current_period as snapshot_date_Downgrade,
    a.Upsell_crosssell_arr,
    a.Upsell_crosssell_arr_lcu,
    b.customer_arr_change_ccfx as Downgrade_downsell_arr,
    b.customer_arr_change_lcu as Downgrade_downsell_arr_lcu,
    b.evaluation_period as Downgrade_evaluation_period,
    b.customer_bridge as Downgrade_bridge,
    row_number() over (
      partition by a.mcid,
      a.evaluation_period_at_upsell,
      a.customer_bridge
      order by b.current_period desc,
        a.snapshot_date_at_upsell
    ) as rnk
  from ryzlan.sst_customer_bridge b
    join temp1 a on a.mcid = b.mcid
  where 1 = 1
    and b.customer_bridge in ('Downgrade', 'Downsell')
    and b.current_period < (
      select current_period
      from ufdm_grey.periods
      where evaluation_period = var_period
    )
)
select *
from temp2;
RAISE NOTICE 'Running WINBACK Downgrade update on sst customer bridge 1.1 ...';
drop table if exists temp_windowngrade_final;
create temporary table temp_windowngrade_final as with temp1 as (
  select *,
    row_number() over (
      partition by mcid,
      Downgrade_evaluation_period,
      customer_bridge
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
  a.customer_bridge,
  b.Upsell_crosssell_arr,
  b.Downgrade_downsell_arr,
  b.Upsell_crosssell_arr_lcu,
  b.Downgrade_downsell_arr_lcu,
  b.Downgrade_evaluation_period,
  b.Downgrade_bridge
from ryzlan.sst_customer_bridge a,
  temp2 b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period_at_upsell
  and a.customer_bridge = b.customer_bridge
  and a.customer_bridge in ('Cross-sell', 'Up Sell')
  and a.evaluation_period = var_period;
--update when total cross/upsell is less than equal to downgrade/downsell
drop table if exists temp_windowngrade_final_curated;
create temporary table temp_windowngrade_final_curated as with cross_upsell_total as (
  select a.mcid,
    a.evaluation_period,
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
        evaluation_period
      from temp_windowngrade_final
    ) a
    left join temp_windowngrade_final b on a.mcid = b.mcid
    and a.evaluation_period = b.evaluation_period
    and b.customer_bridge = 'Cross-sell'
    left join temp_windowngrade_final c on a.mcid = c.mcid
    and a.evaluation_period = c.evaluation_period
    and c.customer_bridge = 'Up Sell'
  group by a.mcid,
    a.evaluation_period
),
downgrade_downsell_total as (
  select a.mcid,
    b.evaluation_period,
    a.evaluation_period as Downgrade_evaluation_period --, a.customer_bridge
,
    abs(sum(customer_arr_change_ccfx)) as downgrade_downsell_total,
    sum(
      case
        when a.customer_bridge = 'Downgrade' then abs(customer_arr_change_ccfx)
        else 0
      end
    ) as Downgrade_arr,
    sum(
      case
        when a.customer_bridge = 'Downsell' then abs(customer_arr_change_ccfx)
        else 0
      end
    ) as Downsell_arr --lcu
,
    abs(sum(customer_arr_change_lcu)) as downgrade_downsell_total_lcu,
    sum(
      case
        when a.customer_bridge = 'Downgrade' then abs(customer_arr_change_lcu)
        else 0
      end
    ) as Downgrade_arr_lcu,
    sum(
      case
        when a.customer_bridge = 'Downsell' then abs(customer_arr_change_lcu)
        else 0
      end
    ) as Downsell_arr_lcu,
    case
      when count(distinct a.customer_bridge) > 1 then 1
      else 0
    end as Downgrade_Downsell_both_exists
  from ryzlan.sst_customer_bridge a
    join (
      select distinct mcid,
        Downgrade_evaluation_period as Downgrade_evaluation_period,
        evaluation_period
      from temp_windowngrade_final
    ) b on a.evaluation_period = b.Downgrade_evaluation_period
    and a.mcid = b.mcid
  where 1 = 1
    and a.customer_bridge in ('Downgrade', 'Downsell')
  group by a.mcid,
    b.evaluation_period,
    a.evaluation_period --, a.customer_bridge
),
temp_new_arr_split as (
  select a.mcid,
    a.evaluation_period,
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
      when a.cross_upsell_both_exists = 0
      and b.Downgrade_Downsell_both_exists = 1
      and a.cross_upsell_total <= b.downgrade_downsell_total then 0
      when a.cross_upsell_both_exists = 0
      and b.Downgrade_Downsell_both_exists = 1
      and a.cross_upsell_total > b.downgrade_downsell_total then case
        when a.Upsell_arr > 0 then a.cross_upsell_total - b.downgrade_downsell_total
        else 0
      end --if cross sell and upsell both exists
      when a.cross_upsell_both_exists = 1
      and b.Downgrade_Downsell_both_exists = 0
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
      end ---new scenario where both exists
      when a.cross_upsell_both_exists = 1
      and b.Downgrade_Downsell_both_exists = 1
      and a.cross_upsell_total <= b.downgrade_downsell_total then 0
      when a.cross_upsell_both_exists = 1
      and b.Downgrade_Downsell_both_exists = 1
      and a.cross_upsell_total > b.downgrade_downsell_total then case
        when a.Upsell_arr <= b.Downgrade_arr then 0
        when a.Upsell_arr > b.Downgrade_arr
        and a.Crossell_arr <= b.Downsell_arr then (a.Upsell_arr - b.Downgrade_arr) - (b.Downsell_arr - a.Crossell_arr)
        when a.Upsell_arr > b.Downgrade_arr
        and a.Crossell_arr > b.Downsell_arr then (a.Upsell_arr - b.Downgrade_arr)
        else 0
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
      when a.cross_upsell_both_exists = 0
      and b.Downgrade_Downsell_both_exists = 1
      and a.cross_upsell_total <= b.downgrade_downsell_total then 0
      when a.cross_upsell_both_exists = 0
      and b.Downgrade_Downsell_both_exists = 1
      and a.cross_upsell_total > b.downgrade_downsell_total then case
        when a.Crossell_arr > 0 then a.cross_upsell_total - b.downgrade_downsell_total
        else 0
      end
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
        and a.Upsell_arr >= b.downgrade_downsell_total then a.Crossell_arr
        when a.Crossell_arr > 0
        and b.Downgrade_arr > 0
        and a.Upsell_arr < b.downgrade_downsell_total then a.Crossell_arr - (b.downgrade_downsell_total - a.Upsell_arr)
      end ---new scenario where both exists
      when a.cross_upsell_both_exists = 1
      and b.Downgrade_Downsell_both_exists = 1
      and a.cross_upsell_total <= b.downgrade_downsell_total then 0
      when a.cross_upsell_both_exists = 1
      and b.Downgrade_Downsell_both_exists = 1
      and a.cross_upsell_total > b.downgrade_downsell_total then case
        when a.Crossell_arr <= b.Downsell_arr then 0
        when a.Crossell_arr > b.Downsell_arr
        and a.Upsell_arr <= b.Downgrade_arr then (a.Crossell_arr - b.Downsell_arr) - (b.Downgrade_arr - a.Upsell_arr)
        when a.Crossell_arr > b.Downsell_arr
        and a.Upsell_arr > b.Downgrade_arr then (a.Crossell_arr - b.Downsell_arr)
        else 0
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
      when a.cross_upsell_both_exists = 0
      and b.Downgrade_Downsell_both_exists = 1
      and a.cross_upsell_total_lcu <= b.downgrade_downsell_total_lcu then 0
      when a.cross_upsell_both_exists = 0
      and b.Downgrade_Downsell_both_exists = 1
      and a.cross_upsell_total_lcu > b.downgrade_downsell_total_lcu then case
        when a.Upsell_arr_lcu > 0 then a.cross_upsell_total_lcu - b.downgrade_downsell_total_lcu
        else 0
      end
      when a.cross_upsell_both_exists = 1
      and b.Downgrade_Downsell_both_exists = 0
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
      end ---new scenario where both exists
      when a.cross_upsell_both_exists = 1
      and b.Downgrade_Downsell_both_exists = 1
      and a.cross_upsell_total_lcu <= b.downgrade_downsell_total_lcu then 0
      when a.cross_upsell_both_exists = 1
      and b.Downgrade_Downsell_both_exists = 1
      and a.cross_upsell_total_lcu > b.downgrade_downsell_total_lcu then case
        when a.Upsell_arr_lcu <= b.Downgrade_arr_lcu then 0
        when a.Upsell_arr_lcu > b.Downgrade_arr_lcu
        and a.Crossell_arr_lcu <= b.Downsell_arr_lcu then (a.Upsell_arr_lcu - b.Downgrade_arr_lcu) - (b.Downsell_arr_lcu - a.Crossell_arr_lcu)
        when a.Upsell_arr_lcu > b.Downgrade_arr_lcu
        and a.Crossell_arr_lcu > b.Downsell_arr_lcu then (a.Upsell_arr_lcu - b.Downgrade_arr_lcu)
        else 0
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
      when a.cross_upsell_both_exists = 0
      and b.Downgrade_Downsell_both_exists = 1
      and a.cross_upsell_total_lcu <= b.downgrade_downsell_total_lcu then 0
      when a.cross_upsell_both_exists = 0
      and b.Downgrade_Downsell_both_exists = 1
      and a.cross_upsell_total_lcu > b.downgrade_downsell_total_lcu then case
        when a.Crossell_arr_lcu > 0 then a.cross_upsell_total_lcu - b.downgrade_downsell_total_lcu
        else 0
      end
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
        and a.Upsell_arr_lcu >= b.downgrade_downsell_total_lcu then a.Crossell_arr_lcu
        when a.Crossell_arr_lcu > 0
        and b.Downgrade_arr_lcu > 0
        and a.Upsell_arr_lcu < b.downgrade_downsell_total_lcu then a.Crossell_arr_lcu - (
          b.downgrade_downsell_total_lcu - a.Upsell_arr_lcu
        )
      end ---new scenario where both exists
      when a.cross_upsell_both_exists = 1
      and b.Downgrade_Downsell_both_exists = 1
      and a.cross_upsell_total_lcu <= b.downgrade_downsell_total_lcu then 0
      when a.cross_upsell_both_exists = 1
      and b.Downgrade_Downsell_both_exists = 1
      and a.cross_upsell_total_lcu > b.downgrade_downsell_total_lcu then case
        when a.Crossell_arr_lcu <= b.Downsell_arr_lcu then 0
        when a.Crossell_arr_lcu > b.Downsell_arr_lcu
        and a.Upsell_arr_lcu <= b.Downgrade_arr_lcu then (a.Crossell_arr_lcu - b.Downsell_arr_lcu) - (b.Downgrade_arr_lcu - a.Upsell_arr_lcu)
        when a.Crossell_arr_lcu > b.Downsell_arr_lcu
        and a.Upsell_arr_lcu > b.Downgrade_arr_lcu then (a.Crossell_arr_lcu - b.Downsell_arr_lcu)
        else 0
      end
      else 0
    end as crosssell_arr_new_lcu,
    cross_upsell_both_exists,
    Downgrade_Downsell_both_exists
  from cross_upsell_total a
    join downgrade_downsell_total b on a.mcid = b.mcid
    and a.evaluation_period = b.evaluation_period
)
select *,
  case
    when a.Downgrade_Downsell_both_exists = 0
    and downgrade_arr > 0 then case
      when a.cross_upsell_total < a.downgrade_downsell_total then a.cross_upsell_total
      else a.downgrade_downsell_total
    end
    when a.Downgrade_Downsell_both_exists = 1
    and a.cross_upsell_both_exists = 0 then case
      when a.Crossell_arr > 0
      and a.Crossell_arr <= a.Downsell_arr then 0
      when a.Crossell_arr > 0
      and a.Crossell_arr > a.Downsell_arr then case
        when a.Crossell_arr < a.downgrade_downsell_total then a.Crossell_arr - a.Downsell_arr
        else a.Downgrade_arr
      end
      when a.Upsell_arr > 0
      and a.Upsell_arr <= a.Downgrade_arr then a.Upsell_arr
      when a.Upsell_arr > 0
      and a.Upsell_arr > a.Downgrade_arr then a.Downgrade_arr
    end
    when a.Downgrade_Downsell_both_exists = 1
    and a.cross_upsell_both_exists = 1 then case
      when a.cross_upsell_total > a.downgrade_downsell_total then a.Downgrade_arr
      else case
        when a.Upsell_arr > a.Downgrade_arr then a.Downgrade_arr
        else a.Upsell_arr
      end
    end
    else 0
  end as winback_downgrade_arr_new,
  case
    when a.Downgrade_Downsell_both_exists = 0
    and Downsell_arr > 0 then case
      when a.cross_upsell_total < a.downgrade_downsell_total then a.cross_upsell_total
      else a.downgrade_downsell_total
    end
    when a.Downgrade_Downsell_both_exists = 1
    and a.cross_upsell_both_exists = 0 then case
      when a.Upsell_arr > 0
      and a.Upsell_arr <= a.Downgrade_arr then 0
      when a.Upsell_arr > 0
      and a.Upsell_arr > a.Downgrade_arr then case
        when a.Upsell_arr < a.downgrade_downsell_total then a.Upsell_arr - a.Downgrade_arr
        else a.Downsell_arr
      end
      when a.Crossell_arr > 0
      and a.Crossell_arr <= a.Downsell_arr then a.Crossell_arr
      when a.Crossell_arr > 0
      and a.Crossell_arr > a.Downsell_arr then a.Downsell_arr
    end
    when a.Downgrade_Downsell_both_exists = 1
    and a.cross_upsell_both_exists = 1 then case
      when a.cross_upsell_total > a.downgrade_downsell_total then a.Downsell_arr
      else case
        when a.Crossell_arr > a.Downsell_arr then a.Downsell_arr
        else a.Crossell_arr
      end
    end
    else 0
  end as winback_downsell_arr_new --#######################lcu #######################--
,
  case
    when a.Downgrade_Downsell_both_exists = 0
    and downgrade_arr > 0 then case
      when a.cross_upsell_total_lcu < a.downgrade_downsell_total_lcu then a.cross_upsell_total_lcu
      else a.downgrade_downsell_total_lcu
    end
    when a.Downgrade_Downsell_both_exists = 1
    and a.cross_upsell_both_exists = 0 then case
      when a.Crossell_arr_lcu > 0
      and a.Crossell_arr_lcu <= a.Downsell_arr_lcu then 0
      when a.Crossell_arr_lcu > 0
      and a.Crossell_arr_lcu > a.Downsell_arr_lcu then case
        when a.Crossell_arr_lcu < a.downgrade_downsell_total_lcu then a.Crossell_arr_lcu - a.Downsell_arr_lcu
        else a.Downgrade_arr_lcu
      end
      when a.Upsell_arr_lcu > 0
      and a.Upsell_arr_lcu <= a.Downgrade_arr_lcu then a.Upsell_arr_lcu
      when a.Upsell_arr_lcu > 0
      and a.Upsell_arr_lcu > a.Downgrade_arr_lcu then a.Downgrade_arr_lcu
    end
    when a.Downgrade_Downsell_both_exists = 1
    and a.cross_upsell_both_exists = 1 then case
      when a.cross_upsell_total_lcu > a.downgrade_downsell_total_lcu then a.Downgrade_arr_lcu
      else case
        when a.Upsell_arr_lcu > a.Downgrade_arr_lcu then a.Downgrade_arr_lcu
        else a.Upsell_arr_lcu
      end
    end
    else 0
  end as winback_downgrade_arr_new_lcu,
  case
    when a.Downgrade_Downsell_both_exists = 0
    and Downsell_arr > 0 then case
      when a.cross_upsell_total_lcu < a.downgrade_downsell_total_lcu then a.cross_upsell_total_lcu
      else a.downgrade_downsell_total_lcu
    end
    when a.Downgrade_Downsell_both_exists = 1
    and a.cross_upsell_both_exists = 0 then case
      when a.Upsell_arr_lcu > 0
      and a.Upsell_arr_lcu <= a.Downgrade_arr_lcu then 0
      when a.Upsell_arr_lcu > 0
      and a.Upsell_arr_lcu > a.Downgrade_arr_lcu then case
        when a.Upsell_arr_lcu < a.downgrade_downsell_total_lcu then a.Upsell_arr_lcu - a.Downgrade_arr_lcu
        else a.Downsell_arr_lcu
      end
      when a.Crossell_arr_lcu > 0
      and a.Crossell_arr_lcu <= a.Downsell_arr_lcu then a.Crossell_arr_lcu
      when a.Crossell_arr_lcu > 0
      and a.Crossell_arr_lcu > a.Downsell_arr_lcu then a.Downsell_arr_lcu
    end
    when a.Downgrade_Downsell_both_exists = 1
    and a.cross_upsell_both_exists = 1 then case
      when a.cross_upsell_total_lcu > a.downgrade_downsell_total_lcu then a.Downsell_arr_lcu
      else case
        when a.Crossell_arr_lcu > a.Downsell_arr_lcu then a.Downsell_arr_lcu
        else a.Crossell_arr_lcu
      end
    end
    else 0
  end as winback_downsell_arr_new_lcu,
  1 as split_record
from temp_new_arr_split a
order by cross_upsell_both_exists;
RAISE NOTICE 'Running WINBACK Downgrade update on sst customer bridge 2...';
drop table if exists temp_windowngrade_split;
create temp table temp_windowngrade_split as
select a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  a.prior_period_customer_arr_usd_ccfx,
  a.prior_period_customer_arr_usd_ccfx + b.crosssell_arr_new as current_period_customer_arr_usd_ccfx,
  b.crosssell_arr_new as customer_arr_change_ccfx,
  a.prior_period_customer_arr_lcu,
  a.prior_period_customer_arr_lcu + b.crosssell_arr_new_lcu as current_period_customer_lcu,
  b.crosssell_arr_new_lcu as customer_arr_change_lcu,
  a.customer_bridge,
  a.winback_period_days,
  a.wip_flag
from ryzlan.sst_customer_bridge a
  join temp_windowngrade_final_curated b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.customer_bridge in ('Cross-sell')
  and b.crosssell_arr_new > 0
union all
select a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  a.prior_period_customer_arr_usd_ccfx,
  a.prior_period_customer_arr_usd_ccfx + b.upsell_arr_new as current_period_customer_arr_usd_ccfx,
  b.upsell_arr_new as customer_arr_change_ccfx,
  a.prior_period_customer_arr_lcu,
  a.prior_period_customer_arr_lcu + b.upsell_arr_new_lcu as current_period_customer_lcu,
  b.upsell_arr_new_lcu as customer_arr_change_lcu,
  a.customer_bridge,
  a.winback_period_days,
  a.wip_flag
from ryzlan.sst_customer_bridge a
  join temp_windowngrade_final_curated b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.customer_bridge in ('Up Sell')
  and b.upsell_arr_new > 0
union all
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  0 as prior_period_customer_arr_usd_ccfx,
  b.winback_downgrade_arr_new as current_period_customer_arr_usd_ccfx,
  b.winback_downgrade_arr_new as customer_arr_change_ccfx,
  0 as prior_period_customer_arr_lcu,
  b.winback_downgrade_arr_new_lcu as current_period_customer_lcu,
  b.winback_downgrade_arr_new_lcu as customer_arr_change_lcu,
  'Win back Downgrade' as customer_bridge,
  null as winback_period_days,
  null as wip_flag --select b.*
from ryzlan.sst_customer_bridge a
  join temp_windowngrade_final_curated b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
where b.Split_record = 1
  and a.evaluation_period = var_period --and a.customer_bridge in ('Up Sell')
  and b.winback_downgrade_arr_new > 0
  and a.customer_bridge <> 'Flat'
union all
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  0 as prior_period_customer_arr_usd_ccfx,
  b.winback_downsell_arr_new as current_period_customer_arr_usd_ccfx,
  b.winback_downsell_arr_new as customer_arr_change_ccfx,
  0 as prior_period_customer_arr_lcu,
  b.winback_downsell_arr_new_lcu as current_period_customer_lcu,
  b.winback_downsell_arr_new_lcu as customer_arr_change_lcu,
  'Win back Downsell' as customer_bridge,
  null as winback_period_days,
  null as wip_flag --select b.*
from ryzlan.sst_customer_bridge a
  join temp_windowngrade_final_curated b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period --left join ryzlan.sst_customer_bridge c on c.mcid = a.mcid and c.evaluation_period = a.evaluation_period and c.customer_bridge = 'Up Sell'
where b.Split_record = 1
  and a.evaluation_period = var_period --and a.customer_bridge in ('Cross-sell')
  --and c.mcid is null
  and b.winback_downsell_arr_new > 0
  and a.customer_bridge <> 'Flat'
order by mcid;
RAISE NOTICE 'Running WINBACK Downgrade update on sst customer bridge 4...';
delete from ryzlan.sst_customer_bridge a using temp_windowngrade_final_curated b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and b.Split_record = 1
  and a.evaluation_period = var_period -- and a.customer_bridge = b.customer_bridge
  and a.customer_bridge in ('Cross-sell', 'Up Sell');
insert into ryzlan.sst_customer_bridge (
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    name,
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
    wip_flag
  )
select evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  name,
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
  wip_flag
from temp_windowngrade_split;
--###########################################
--CPI Reversal
--###########################################
RAISE NOTICE 'Running CPI Reversal update on sst customer bridge...';
drop table if exists temp_CPI_Reversal;
create table temp_CPI_Reversal as with temp1 as (
  select a.mcid,
    a.customer_bridge,
    a.evaluation_period as evaluation_period_at_Downgrade_Churn,
    p.current_period as snapshot_date_at_Downgrade_Churn,
    customer_arr_change_ccfx as current_arr
  from ryzlan.sst_customer_bridge a
    join ufdm_grey.periods p on a.evaluation_period = p.evaluation_period
  where 1 = 1
    and a.evaluation_period = var_period
    and a.customer_bridge in (
      'Downgrade',
      'Churn',
      'Downsell',
      'Churn Migration'
    ) --and a.mcid = '1ce5a898-1eaa-db11-8952-0018717a8c82'
),
temp2 as (
  select a.mcid,
    a.customer_bridge,
    a.evaluation_period_at_Downgrade_Churn,
    a.snapshot_date_at_Downgrade_Churn,
    b.current_period as snapshot_date_CPI,
    a.current_arr,
    b.customer_arr_change_ccfx as CPI_arr,
    b.customer_bridge as CPI_bridge,
    b.evaluation_period as CPI_evaluation_period,
    row_number() over (
      partition by a.mcid,
      a.evaluation_period_at_Downgrade_Churn
      order by b.current_period desc,
        a.snapshot_date_at_Downgrade_Churn
    ) as rnk
  from ryzlan.sst_customer_bridge b
    join temp1 a on a.mcid = b.mcid
  where 1 = 1
    and b.customer_bridge = 'Price Uplift'
    and b.current_period < (
      select current_period
      from ufdm_grey.periods
      where evaluation_period = var_period
    )
)
select *
from temp2;
if (
  (
    select count(*)
    from temp_CPI_Reversal
  ) > 0
) then drop table if exists temp_cpireversal_final;
create temporary table temp_cpireversal_final as with temp1 as (
  select *,
    row_number() over (
      partition by mcid,
      evaluation_period_at_Downgrade_Churn
      order by snapshot_date_CPI
    ) as rnk2
  from temp_CPI_Reversal
  where rnk = 1
    and snapshot_date_at_Downgrade_Churn::date - snapshot_date_CPI::date < 186
),
temp2 as (
  select *
  from temp1
  where rnk2 = 1
)
select distinct a.mcid,
  a.evaluation_period,
  b.current_arr,
  (
    b.CPI_arr - abs(coalesce(c.customer_arr_change_ccfx, 0))
  ) as CPI_arr,
  a.customer_bridge,
  case
    when - b.current_arr > (
      b.CPI_arr - abs(coalesce(c.customer_arr_change_ccfx, 0))
    ) then 1
    else 0
  end as Split_record,
  b.CPI_evaluation_period,
  b.CPI_bridge,
  b.snapshot_date_CPI,
  snapshot_date_at_Downgrade_Churn,
  abs(coalesce(c.customer_arr_change_ccfx, 0)) as cpi_reversal_arr,
  abs(coalesce(c.customer_arr_change_lcu, 0)) as cpi_reversal_lcu,
  coalesce(c.current_period_customer_arr_usd_ccfx, 0) as prior_period_customer_arr_usd_ccfx_CPIR,
  coalesce(c.prior_period_customer_arr_usd_ccfx, 0) as current_period_customer_arr_usd_ccfx_CPIR,
  coalesce(c.current_period_customer_lcu, 0) as prior_period_customer_arr_lcu_CPIR,
  coalesce(c.prior_period_customer_arr_lcu, 0) as current_period_customer_lcu_CPIR
from ryzlan.sst_customer_bridge a
  join temp2 b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period_at_Downgrade_Churn
  left join (
    select suba.*,
      subb.current_period as snapshot_Date
    from ryzlan.sst_customer_bridge suba
      join ufdm_grey.periods subb on suba.evaluation_period = subb.evaluation_period
  ) c on a.mcid = c.mcid
  and c.customer_bridge = 'Price Uplift Reversal'
  and (c.snapshot_Date) between b.snapshot_date_CPI and b.snapshot_date_at_Downgrade_Churn
where 1 = 1
  and a.customer_bridge in ('Downgrade', 'Churn', 'Downsell')
  and a.evaluation_period = var_period
  and (
    c.mcid is null
    or (
      c.mcid is not null
      and abs(coalesce(c.customer_arr_change_ccfx, 0)) < abs(b.CPI_arr)
    )
  );
RAISE NOTICE 'Running cpi Reversal update on sst customer bridge 1...';
--update when total cross/upsell is less than equal to downgrade/downsell
drop table if exists temp_cpi_reversal_final_curated;
create temporary table temp_cpi_reversal_final_curated as with cpi_total as (
  select a.mcid,
    b.evaluation_period,
    a.evaluation_period as CPI_evaluation_period,
    abs(sum(customer_arr_change_ccfx)) as CPI_total,
    abs(sum(customer_arr_change_ccfx)) as CPI_arr,
    abs(sum(customer_arr_change_lcu)) as CPI_arr_lcu,
    abs(sum(customer_arr_change_lcu)) as CPI_total_lcu
  from ryzlan.sst_customer_bridge a
    join (
      select distinct mcid,
        cpi_evaluation_period,
        evaluation_period
      from temp_cpireversal_final
    ) b on a.evaluation_period = b.cpi_evaluation_period
    and a.mcid = b.mcid
  where 1 = 1
    and a.customer_bridge in ('Price Uplift')
  group by a.mcid,
    b.evaluation_period,
    a.evaluation_period
),
downgrade_downsell_churn_total as (
  select a.mcid,
    b.evaluation_period,
    a.evaluation_period as evaluation_period_downgrade_downsell_churn,
    abs(sum(customer_arr_change_ccfx)) as downgrade_downsell_churn_total,
    sum(
      case
        when a.customer_bridge = 'Downgrade' then abs(customer_arr_change_ccfx)
        else 0
      end
    ) as Downgrade_arr,
    sum(
      case
        when a.customer_bridge = 'Downsell' then abs(customer_arr_change_ccfx)
        else 0
      end
    ) as Downsell_arr,
    sum(
      case
        when a.customer_bridge = 'Churn' then abs(customer_arr_change_ccfx)
        else 0
      end
    ) as Churn_arr --lcu
,
    abs(sum(customer_arr_change_lcu)) as downgrade_downsell_churn_total_lcu,
    sum(
      case
        when a.customer_bridge = 'Downgrade' then abs(customer_arr_change_lcu)
        else 0
      end
    ) as Downgrade_arr_lcu,
    sum(
      case
        when a.customer_bridge = 'Downsell' then abs(customer_arr_change_lcu)
        else 0
      end
    ) as Downsell_arr_lcu,
    sum(
      case
        when a.customer_bridge = 'Churn' then abs(customer_arr_change_lcu)
        else 0
      end
    ) as Churn_arr_lcu,
    case
      when count(distinct a.customer_bridge) > 1 then 1
      else 0
    end as Downgrade_Downsell_churn_both_exists
  from ryzlan.sst_customer_bridge a
    join (
      select distinct mcid,
        evaluation_period
      from temp_cpireversal_final
    ) b on a.evaluation_period = b.evaluation_period
    and a.mcid = b.mcid
  where 1 = 1
    and a.customer_bridge in ('Downgrade', 'Downsell', 'Churn')
  group by a.mcid,
    b.evaluation_period,
    a.evaluation_period
),
temp_new_arr_split as (
  select a.mcid,
    a.evaluation_period,
    a.evaluation_period_downgrade_downsell_churn,
    b.CPI_arr,
    b.CPI_arr_lcu,
    a.Churn_arr,
    a.Churn_arr_lcu,
    a.Downgrade_arr,
    a.Downgrade_arr_lcu,
    a.Downsell_arr,
    a.Downsell_arr_lcu,
    b.CPI_total,
    b.CPI_total_lcu,
    a.downgrade_downsell_churn_total,
    a.downgrade_downsell_churn_total_lcu --, 0 as Downgrade_arr_new,0 as Downgrade_arr_new_lcu,0 as Downsell_arr_new,0 as Downsell_arr_new_lcu,0 as Churn_arr_new,0 as Churn_arr_new_lcu
,
    case
      when a.Downgrade_Downsell_churn_both_exists = 0 then case
        when a.Downgrade_arr > 0
        and a.downgrade_downsell_churn_total > b.CPI_total then a.downgrade_downsell_churn_total - b.CPI_total
        else 0
      end
      when a.Downgrade_Downsell_churn_both_exists = 1 then case
        when a.downgrade_downsell_churn_total <= b.CPI_total then 0
        else case
          when a.Downgrade_arr > 0
          and a.Downgrade_arr <= b.CPI_total then 0
          else a.Downgrade_arr - b.CPI_total
        end
      end
    end as Downgrade_arr_new,
    case
      when a.Downgrade_Downsell_churn_both_exists = 0 then case
        when a.Downsell_arr > 0
        and a.downgrade_downsell_churn_total > b.CPI_total then a.downgrade_downsell_churn_total - b.CPI_total
        else 0
      end
      when a.Downgrade_Downsell_churn_both_exists = 1 then case
        when a.downgrade_downsell_churn_total <= b.CPI_total then 0
        else case
          when a.Downsell_arr > 0
          and a.Downgrade_arr >= b.CPI_total then a.Downsell_arr
          else a.Downsell_arr - (b.CPI_total - a.Downgrade_arr)
        end
      end
    end as Downsell_arr_new,
    case
      when a.Churn_arr > 0
      and a.downgrade_downsell_churn_total > b.CPI_total then a.downgrade_downsell_churn_total - b.CPI_total
      else 0
    end as Churn_arr_new --#######################  lcu  #######----------------------------
,
    case
      when a.Downgrade_Downsell_churn_both_exists = 0 then case
        when a.Downgrade_arr_lcu > 0
        and a.downgrade_downsell_churn_total_lcu > b.CPI_total_lcu then a.downgrade_downsell_churn_total_lcu - b.CPI_total_lcu
        else 0
      end
      when a.Downgrade_Downsell_churn_both_exists = 1 then case
        when a.downgrade_downsell_churn_total_lcu <= b.CPI_total_lcu then 0
        else case
          when a.Downgrade_arr_lcu > 0
          and a.Downgrade_arr_lcu <= b.CPI_total_lcu then 0
          else a.Downgrade_arr_lcu - b.CPI_total_lcu
        end
      end
    end as Downgrade_arr_new_lcu,
    case
      when a.Downgrade_Downsell_churn_both_exists = 0 then case
        when a.Downsell_arr_lcu > 0
        and a.downgrade_downsell_churn_total_lcu > b.CPI_total_lcu then a.downgrade_downsell_churn_total_lcu - b.CPI_total_lcu
        else 0
      end
      when a.Downgrade_Downsell_churn_both_exists = 1 then case
        when a.downgrade_downsell_churn_total_lcu <= b.CPI_total_lcu then 0
        else case
          when a.Downsell_arr_lcu > 0
          and a.Downgrade_arr_lcu >= b.CPI_total_lcu then a.Downsell_arr_lcu
          else a.Downsell_arr_lcu - (b.CPI_total_lcu - a.Downgrade_arr_lcu)
        end
      end
    end as Downsell_arr_new_lcu,
    case
      when a.Churn_arr_lcu > 0
      and a.downgrade_downsell_churn_total_lcu > b.CPI_total_lcu then a.downgrade_downsell_churn_total_lcu - b.CPI_total_lcu
      else 0
    end as Churn_arr_new_lcu,
    Downgrade_Downsell_churn_both_exists
  from downgrade_downsell_churn_total a
    join CPI_total b on a.mcid = b.mcid
    and a.evaluation_period = b.evaluation_period
)
select *,
  case
    when a.downgrade_downsell_churn_total <= a.CPI_total then a.downgrade_downsell_churn_total
    else a.CPI_total
  end as cpi_reversal_arr_new --#######################lcu #######################--
,
  case
    when a.downgrade_downsell_churn_total_lcu <= a.CPI_total_lcu then a.downgrade_downsell_churn_total_lcu
    else a.CPI_total_lcu
  end as cpi_reversal_arr_new_lcu,
  1 as split_record
from temp_new_arr_split a --order by cross_upsell_both_exists
;
drop table if exists temp_cpireversal_split;
create temp table temp_cpireversal_split as
select a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  0 as prior_period_customer_arr_usd_ccfx,
  0 as current_period_customer_arr_usd_ccfx,
  - Downgrade_arr_new as customer_arr_change_ccfx ---lcu
,
  0 as prior_period_customer_arr_lcu,
  0 as current_period_customer_lcu,
  - Downgrade_arr_new_lcu as customer_arr_change_lcu,
  a.customer_bridge
from ryzlan.sst_customer_bridge a
  join temp_cpi_reversal_final_curated b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.customer_bridge in ('Downgrade')
  and b.Downgrade_arr_new > 0
union all
select a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  0 as prior_period_customer_arr_usd_ccfx,
  0 as current_period_customer_arr_usd_ccfx,
  - b.Downsell_arr_new as customer_arr_change_ccfx ---lcu
,
  0 as prior_period_customer_arr_lcu,
  0 as current_period_customer_lcu,
  - b.Downsell_arr_new_lcu as customer_arr_change_lcu,
  a.customer_bridge
from ryzlan.sst_customer_bridge a
  join temp_cpi_reversal_final_curated b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.customer_bridge in ('Downsell')
  and b.Downsell_arr_new > 0
union all
select a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  0 as prior_period_customer_arr_usd_ccfx,
  0 as current_period_customer_arr_usd_ccfx,
  - b.Churn_arr_new as customer_arr_change_ccfx ---lcu
,
  0 as prior_period_customer_arr_lcu,
  0 as current_period_customer_lcu,
  - b.Churn_arr_new_lcu as customer_arr_change_lcu,
  a.customer_bridge
from ryzlan.sst_customer_bridge a
  join temp_cpi_reversal_final_curated b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.customer_bridge in ('Churn')
  and b.Churn_arr_new > 0
union all
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  0 as prior_period_customer_arr_usd_ccfx,
  cpi_reversal_arr_new as current_period_customer_arr_usd_ccfx,
  - cpi_reversal_arr_new as customer_arr_change_ccfx ---lcu
,
  0 as prior_period_customer_arr_lcu,
  cpi_reversal_arr_new_lcu as current_period_customer_lcu,
  - cpi_reversal_arr_new_lcu as customer_arr_change_lcu,
  'Price Uplift Reversal' as customer_bridge
from ryzlan.sst_customer_bridge a
  join temp_cpi_reversal_final_curated b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
where b.Split_record = 1
  and a.evaluation_period = var_period
  and b.cpi_reversal_arr_new > 0
  and a.customer_bridge <> 'Flat'
order by mcid;
delete from ryzlan.sst_customer_bridge a using temp_cpi_reversal_final_curated b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and b.Split_record = 1
  and a.evaluation_period = var_period
  and a.customer_bridge in ('Downgrade', 'Churn', 'Downsell');
insert into ryzlan.sst_customer_bridge (
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    name,
    baseline_currency,
    subsidiary_entity_name,
    prior_period_customer_arr_usd_ccfx,
    current_period_customer_arr_usd_ccfx,
    customer_arr_change_ccfx,
    prior_period_customer_arr_lcu,
    current_period_customer_lcu,
    customer_arr_change_lcu,
    customer_bridge
  )
select evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  name,
  baseline_currency,
  subsidiary_entity_name,
  prior_period_customer_arr_usd_ccfx,
  current_period_customer_arr_usd_ccfx,
  customer_arr_change_ccfx,
  prior_period_customer_arr_lcu,
  current_period_customer_lcu,
  customer_arr_change_lcu,
  customer_bridge
from temp_cpireversal_split;
end if;
--###########################################
--Upsell Reversal
--###########################################
RAISE NOTICE 'Running Upsell Reversal update on sst customer bridge...';
drop table if exists temp_cross_upsell_reversal;
create temp table temp_cross_upsell_reversal as with temp1 as (
  select a.mcid,
    a.customer_bridge,
    a.evaluation_period as evaluation_period_at_Downgrade_Downsell,
    b.current_period as snapshot_date_at_Downgrade_Downsell,
    a.customer_arr_change_ccfx as Downgrade_Downsell_arr,
    a.customer_arr_change_lcu as Downgrade_Downsell_arr_lcu
  from ryzlan.sst_customer_bridge a
    join ufdm_grey.periods b on a.evaluation_period = b.evaluation_period
  where 1 = 1 --and a.customer_bridge in ('Downgrade','Downsell')
    and a.customer_bridge in ('Downgrade', 'Downsell', 'Churn')
    and a.evaluation_period = var_period
),
temp2 as (
  select a.mcid,
    a.customer_bridge,
    a.evaluation_period_at_Downgrade_Downsell,
    a.snapshot_date_at_Downgrade_Downsell,
    b.current_period as snapshot_date_cross_upsell,
    a.Downgrade_Downsell_arr,
    a.Downgrade_Downsell_arr_lcu,
    b.customer_arr_change_ccfx as Upsell_crosssell_arr,
    b.customer_arr_change_lcu as Upsell_crosssell_arr_lcu,
    b.evaluation_period as cross_upsell_evaluation_period,
    b.customer_bridge as cross_upsell_bridge,
    row_number() over (
      partition by a.mcid,
      a.evaluation_period_at_Downgrade_Downsell,
      a.customer_bridge
      order by b.current_period desc,
        a.snapshot_date_at_Downgrade_Downsell
    ) as rnk
  from ryzlan.sst_customer_bridge b
    join temp1 a on a.mcid = b.mcid
  where 1 = 1
    and b.customer_bridge in ('Cross-sell', 'Up Sell')
    and b.current_period < (
      select current_period
      from ufdm_grey.periods
      where evaluation_period = var_period
    )
)
select *
from temp2;
if (
  (
    select count(*)
    from temp_cross_upsell_reversal
  ) > 0
) then drop table if exists temp_cross_upsell_reversal_final;
create temporary table temp_cross_upsell_reversal_final as with temp1 as (
  select *,
    row_number() over (
      partition by mcid,
      cross_upsell_evaluation_period,
      customer_bridge
      order by snapshot_date_at_Downgrade_Downsell
    ) as rnk2
  from temp_cross_upsell_reversal
  where rnk = 1
    and snapshot_date_at_Downgrade_Downsell::date - snapshot_date_cross_upsell::date < 186
),
temp2 as (
  select *
  from temp1
  where rnk2 = 1
)
select a.mcid,
  a.evaluation_period,
  b.Upsell_crosssell_arr,
  b.Downgrade_Downsell_arr,
  a.customer_bridge,
  b.cross_upsell_evaluation_period,
  b.cross_upsell_bridge,
  b.evaluation_period_at_Downgrade_Downsell,
  b.Upsell_crosssell_arr_lcu,
  b.Downgrade_Downsell_arr_lcu
from ryzlan.sst_customer_bridge a,
  temp2 b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period_at_Downgrade_Downsell
  and a.customer_bridge = b.customer_bridge --and a.customer_bridge in ('Downgrade','Downsell')
  and a.customer_bridge in ('Downgrade', 'Downsell', 'Churn')
  and a.evaluation_period = var_period;
RAISE NOTICE 'Running Upsell Reversal update on sst customer bridge 1...';
--update when total cross/upsell is less than equal to downgrade/downsell
drop table if exists temp_cross_upsell_reversal_final_curated;
create temporary table temp_cross_upsell_reversal_final_curated as with cross_upsell_total as (
  select a.mcid,
    b.evaluation_period,
    a.evaluation_period as cross_upsell_evaluation_period --, a.customer_bridge
,
    abs(sum(customer_arr_change_ccfx)) as cross_upsell_total,
    sum(
      case
        when a.customer_bridge = 'Cross-sell' then abs(customer_arr_change_ccfx)
        else 0
      end
    ) as Crossell_arr,
    sum(
      case
        when a.customer_bridge = 'Up Sell' then abs(customer_arr_change_ccfx)
        else 0
      end
    ) as Upsell_arr --lcu
,
    abs(sum(customer_arr_change_lcu)) as cross_upsell_total_lcu,
    sum(
      case
        when a.customer_bridge = 'Cross-sell' then abs(customer_arr_change_lcu)
        else 0
      end
    ) as Crossell_arr_lcu,
    sum(
      case
        when a.customer_bridge = 'Up Sell' then abs(customer_arr_change_lcu)
        else 0
      end
    ) as Upsell_arr_lcu,
    case
      when count(distinct a.customer_bridge) > 1 then 1
      else 0
    end as cross_upsell_both_exists
  from ryzlan.sst_customer_bridge a
    join (
      select distinct mcid,
        cross_upsell_evaluation_period,
        evaluation_period
      from temp_cross_upsell_reversal_final
    ) b on a.evaluation_period = b.cross_upsell_evaluation_period
    and a.mcid = b.mcid
  where 1 = 1
    and a.customer_bridge in ('Cross-sell', 'Up Sell')
  group by a.mcid,
    b.evaluation_period,
    a.evaluation_period --, a.customer_bridge
),
downgrade_downsell_total as (
  select a.mcid,
    b.evaluation_period,
    a.evaluation_period as Downgrade_evaluation_period --, a.customer_bridge
,
    abs(sum(customer_arr_change_ccfx)) as downgrade_downsell_total,
    sum(
      case
        when a.customer_bridge = 'Downgrade' then abs(customer_arr_change_ccfx)
        else 0
      end
    ) as Downgrade_arr,
    sum(
      case
        when a.customer_bridge = 'Downsell' then abs(customer_arr_change_ccfx)
        else 0
      end
    ) as Downsell_arr,
    sum(
      case
        when a.customer_bridge = 'Churn' then abs(customer_arr_change_ccfx)
        else 0
      end
    ) as Churn_arr --lcu
,
    abs(sum(customer_arr_change_lcu)) as downgrade_downsell_total_lcu,
    sum(
      case
        when a.customer_bridge = 'Downgrade' then abs(customer_arr_change_lcu)
        else 0
      end
    ) as Downgrade_arr_lcu,
    sum(
      case
        when a.customer_bridge = 'Downsell' then abs(customer_arr_change_lcu)
        else 0
      end
    ) as Downsell_arr_lcu,
    sum(
      case
        when a.customer_bridge = 'Churn' then abs(customer_arr_change_lcu)
        else 0
      end
    ) as Churn_arr_lcu,
    case
      when count(distinct a.customer_bridge) > 1 then 1
      else 0
    end as Downgrade_Downsell_both_exists,
    sum(
      case
        when a.customer_bridge = 'Churn' then 1
        else 0
      end
    ) as Churn_exists
  from ryzlan.sst_customer_bridge a
    join (
      select distinct mcid,
        evaluation_period_at_Downgrade_Downsell,
        evaluation_period
      from temp_cross_upsell_reversal_final
    ) b on a.evaluation_period = b.evaluation_period_at_Downgrade_Downsell
    and a.mcid = b.mcid
  where 1 = 1
    and a.customer_bridge in ('Downgrade', 'Downsell') --and a.customer_bridge in ('Downgrade','Downsell','Churn')
  group by a.mcid,
    b.evaluation_period,
    a.evaluation_period --, a.customer_bridge
),
temp_new_arr_split as (
  select a.mcid,
    a.evaluation_period,
    a.downgrade_evaluation_period,
    b.upsell_arr,
    b.Crossell_arr,
    a.downgrade_arr,
    a.Downsell_arr,
    b.cross_upsell_total,
    a.downgrade_downsell_total,
    b.upsell_arr_lcu,
    b.Crossell_arr_lcu,
    a.downgrade_arr_lcu,
    a.Downsell_arr_lcu,
    b.cross_upsell_total_lcu,
    a.downgrade_downsell_total_lcu,
    case
      --if only cross sell or upsell exists then
      when b.cross_upsell_both_exists = 0
      and a.Downgrade_Downsell_both_exists = 0
      and a.downgrade_arr > b.cross_upsell_total then a.downgrade_arr - b.cross_upsell_total
      when b.cross_upsell_both_exists = 0
      and a.Downgrade_Downsell_both_exists = 0
      and a.downgrade_arr <= b.cross_upsell_total then 0
      when b.cross_upsell_both_exists = 1
      and a.Downgrade_Downsell_both_exists = 0
      and a.downgrade_downsell_total <= b.cross_upsell_total then 0
      when b.cross_upsell_both_exists = 1
      and a.Downgrade_Downsell_both_exists = 0
      and a.downgrade_downsell_total > b.cross_upsell_total then case
        when a.downgrade_arr > 0 then a.downgrade_downsell_total - b.cross_upsell_total
        else 0
      end --if cross sell and upsell both exists
      when b.cross_upsell_both_exists = 0
      and a.Downgrade_Downsell_both_exists = 1
      and a.downgrade_downsell_total <= b.cross_upsell_total then 0
      when b.cross_upsell_both_exists = 0
      and a.Downgrade_Downsell_both_exists = 1
      and a.downgrade_downsell_total > b.cross_upsell_total then case
        when a.downgrade_arr > 0
        and b.Upsell_arr > 0
        and a.downgrade_arr <= b.cross_upsell_total then 0
        when a.downgrade_arr > 0
        and b.Upsell_arr > 0
        and a.downgrade_arr > b.cross_upsell_total then a.downgrade_arr - b.cross_upsell_total
        when a.downgrade_arr > 0
        and b.Crossell_arr > 0
        and a.Downsell_arr >= b.cross_upsell_total then a.downgrade_arr
        when a.downgrade_arr > 0
        and b.Crossell_arr > 0
        and a.Downsell_arr < b.cross_upsell_total then a.downgrade_arr - (b.cross_upsell_total - a.Downsell_arr)
      end ---new scenario where both exists
      when b.cross_upsell_both_exists = 1
      and a.Downgrade_Downsell_both_exists = 1
      and a.downgrade_downsell_total <= b.cross_upsell_total then 0
      when b.cross_upsell_both_exists = 1
      and a.Downgrade_Downsell_both_exists = 1
      and a.downgrade_downsell_total > b.cross_upsell_total then case
        when a.Downgrade_arr <= b.Upsell_arr then 0
        when a.Downgrade_arr > b.Upsell_arr
        and a.Downsell_arr <= b.Crossell_arr then (a.Downgrade_arr - b.Upsell_arr) - (b.Crossell_arr - a.Downsell_arr)
        when a.Downgrade_arr > b.Upsell_arr
        and a.Downsell_arr > b.Crossell_arr then (a.Downgrade_arr - b.Upsell_arr)
        else 0
      end
      else 0
    end as downgrade_arr_new,
    case
      when b.cross_upsell_both_exists = 0
      and a.Downgrade_Downsell_both_exists = 0
      and a.Downsell_arr > b.cross_upsell_total then a.Downsell_arr - b.cross_upsell_total
      when b.cross_upsell_both_exists = 0
      and a.Downgrade_Downsell_both_exists = 0
      and a.Downsell_arr <= b.cross_upsell_total then 0
      when b.cross_upsell_both_exists = 1
      and a.Downgrade_Downsell_both_exists = 0
      and a.downgrade_downsell_total <= b.cross_upsell_total then 0
      when b.cross_upsell_both_exists = 1
      and a.Downgrade_Downsell_both_exists = 0
      and a.downgrade_downsell_total > b.cross_upsell_total then case
        when a.Downsell_arr > 0 then a.downgrade_downsell_total - b.cross_upsell_total
        else 0
      end --if cross sell and upsell both exists
      when b.cross_upsell_both_exists = 0
      and a.Downgrade_Downsell_both_exists = 1
      and a.downgrade_downsell_total <= b.cross_upsell_total then 0
      when b.cross_upsell_both_exists = 0
      and a.Downgrade_Downsell_both_exists = 1
      and a.downgrade_downsell_total > b.cross_upsell_total then case
        when a.Downsell_arr > 0
        and b.Crossell_arr > 0
        and a.Downsell_arr <= b.cross_upsell_total then 0
        when a.Downsell_arr > 0
        and b.Crossell_arr > 0
        and a.Downsell_arr > b.cross_upsell_total then a.Downsell_arr - b.cross_upsell_total
        when a.Downsell_arr > 0
        and b.Upsell_arr > 0
        and a.Downgrade_arr >= b.cross_upsell_total then a.Downsell_arr
        when a.Downsell_arr > 0
        and b.Upsell_arr > 0
        and a.Downgrade_arr < b.cross_upsell_total then a.Downsell_arr - (b.cross_upsell_total - a.Downgrade_arr)
      end ---new scenario where both exists
      when b.cross_upsell_both_exists = 1
      and a.Downgrade_Downsell_both_exists = 1
      and a.downgrade_downsell_total <= b.cross_upsell_total then 0
      when b.cross_upsell_both_exists = 1
      and a.Downgrade_Downsell_both_exists = 1
      and a.downgrade_downsell_total > b.cross_upsell_total then case
        when a.Downsell_arr <= b.Crossell_arr then 0
        when a.Downsell_arr > b.Crossell_arr
        and a.Downgrade_arr <= b.Upsell_arr then (a.Downsell_arr - b.Crossell_arr) - (b.Upsell_arr - a.Downgrade_arr)
        when a.Downsell_arr > b.Crossell_arr
        and a.Downgrade_arr > b.Upsell_arr then (a.Downsell_arr - b.Crossell_arr)
        else 0
      end
      else 0
    end as downsell_arr_new --#######################  lcu  #######----------------------------
,
    case
      --if only cross sell or upsell exists then
      when b.cross_upsell_both_exists = 0
      and a.Downgrade_Downsell_both_exists = 0
      and a.downgrade_arr_lcu > b.cross_upsell_total_lcu then a.downgrade_arr_lcu - b.cross_upsell_total_lcu
      when b.cross_upsell_both_exists = 0
      and a.Downgrade_Downsell_both_exists = 0
      and a.downgrade_arr_lcu <= b.cross_upsell_total_lcu then 0
      when b.cross_upsell_both_exists = 1
      and a.Downgrade_Downsell_both_exists = 0
      and a.downgrade_downsell_total_lcu <= b.cross_upsell_total_lcu then 0
      when b.cross_upsell_both_exists = 1
      and a.Downgrade_Downsell_both_exists = 0
      and a.downgrade_downsell_total_lcu > b.cross_upsell_total_lcu then case
        when a.downgrade_arr_lcu > 0 then a.downgrade_downsell_total_lcu - b.cross_upsell_total_lcu
        else 0
      end --if cross sell and upsell both exists
      when b.cross_upsell_both_exists = 0
      and a.Downgrade_Downsell_both_exists = 1
      and a.downgrade_downsell_total_lcu <= b.cross_upsell_total_lcu then 0
      when b.cross_upsell_both_exists = 0
      and a.Downgrade_Downsell_both_exists = 1
      and a.downgrade_downsell_total_lcu > b.cross_upsell_total_lcu then case
        when a.downgrade_arr_lcu > 0
        and b.Upsell_arr_lcu > 0
        and a.downgrade_arr_lcu <= b.cross_upsell_total_lcu then 0
        when a.downgrade_arr_lcu > 0
        and b.Upsell_arr_lcu > 0
        and a.downgrade_arr_lcu > b.cross_upsell_total_lcu then a.downgrade_arr_lcu - b.cross_upsell_total_lcu
        when a.downgrade_arr_lcu > 0
        and b.Crossell_arr_lcu > 0
        and a.Downsell_arr_lcu >= b.cross_upsell_total_lcu then a.downgrade_arr_lcu
        when a.downgrade_arr_lcu > 0
        and b.Crossell_arr_lcu > 0
        and a.Downsell_arr_lcu < b.cross_upsell_total_lcu then a.downgrade_arr_lcu - (b.cross_upsell_total_lcu - a.Downsell_arr_lcu)
      end ---new scenario where both exists
      when b.cross_upsell_both_exists = 1
      and a.Downgrade_Downsell_both_exists = 1
      and a.downgrade_downsell_total_lcu <= b.cross_upsell_total_lcu then 0
      when b.cross_upsell_both_exists = 1
      and a.Downgrade_Downsell_both_exists = 1
      and a.downgrade_downsell_total_lcu > b.cross_upsell_total_lcu then case
        when a.Downgrade_arr_lcu <= b.Upsell_arr_lcu then 0
        when a.Downgrade_arr_lcu > b.Upsell_arr_lcu
        and a.Downsell_arr_lcu <= b.Crossell_arr_lcu then (a.Downgrade_arr_lcu - b.Upsell_arr_lcu) - (b.Crossell_arr_lcu - a.Downsell_arr_lcu)
        when a.Downgrade_arr_lcu > b.Upsell_arr_lcu
        and a.Downsell_arr_lcu > b.Crossell_arr_lcu then (a.Downgrade_arr_lcu - b.Upsell_arr_lcu)
        else 0
      end
      else 0
    end as downgrade_arr_new_lcu,
    case
      when b.cross_upsell_both_exists = 0
      and a.Downgrade_Downsell_both_exists = 0
      and a.Downsell_arr_lcu > b.cross_upsell_total_lcu then a.Downsell_arr_lcu - b.cross_upsell_total_lcu
      when b.cross_upsell_both_exists = 0
      and a.Downgrade_Downsell_both_exists = 0
      and a.Downsell_arr_lcu <= b.cross_upsell_total_lcu then 0
      when b.cross_upsell_both_exists = 1
      and a.Downgrade_Downsell_both_exists = 0
      and a.downgrade_downsell_total_lcu <= b.cross_upsell_total_lcu then 0
      when b.cross_upsell_both_exists = 1
      and a.Downgrade_Downsell_both_exists = 0
      and a.downgrade_downsell_total_lcu > b.cross_upsell_total_lcu then case
        when a.Downsell_arr_lcu > 0 then a.downgrade_downsell_total_lcu - b.cross_upsell_total_lcu
        else 0
      end --if cross sell and upsell both exists
      when b.cross_upsell_both_exists = 0
      and a.Downgrade_Downsell_both_exists = 1
      and a.downgrade_downsell_total_lcu <= b.cross_upsell_total_lcu then 0
      when b.cross_upsell_both_exists = 0
      and a.Downgrade_Downsell_both_exists = 1
      and a.downgrade_downsell_total_lcu > b.cross_upsell_total_lcu then case
        when a.Downsell_arr_lcu > 0
        and b.Crossell_arr_lcu > 0
        and a.Downsell_arr_lcu <= b.cross_upsell_total_lcu then 0
        when a.Downsell_arr_lcu > 0
        and b.Crossell_arr_lcu > 0
        and a.Downsell_arr_lcu > b.cross_upsell_total_lcu then a.Downsell_arr_lcu - b.cross_upsell_total_lcu
        when a.Downsell_arr_lcu > 0
        and b.Upsell_arr_lcu > 0
        and a.Downgrade_arr_lcu >= b.cross_upsell_total_lcu then a.Downsell_arr_lcu
        when a.Downsell_arr_lcu > 0
        and b.Upsell_arr_lcu > 0
        and a.Downgrade_arr_lcu < b.cross_upsell_total_lcu then a.Downsell_arr_lcu - (b.cross_upsell_total_lcu - a.Downgrade_arr_lcu)
      end ---new scenario where both exists
      when b.cross_upsell_both_exists = 1
      and a.Downgrade_Downsell_both_exists = 1
      and a.downgrade_downsell_total_lcu <= b.cross_upsell_total_lcu then 0
      when b.cross_upsell_both_exists = 1
      and a.Downgrade_Downsell_both_exists = 1
      and a.downgrade_downsell_total_lcu > b.cross_upsell_total_lcu then case
        when a.Downsell_arr_lcu <= b.Crossell_arr_lcu then 0
        when a.Downsell_arr_lcu > b.Crossell_arr_lcu
        and a.Downgrade_arr_lcu <= b.Upsell_arr_lcu then (a.Downsell_arr_lcu - b.Crossell_arr_lcu) - (b.Upsell_arr_lcu - a.Downgrade_arr_lcu)
        when a.Downsell_arr_lcu > b.Crossell_arr_lcu
        and a.Downgrade_arr_lcu > b.Upsell_arr_lcu then (a.Downsell_arr_lcu - b.Crossell_arr_lcu)
        else 0
      end
      else 0
    end as downsell_arr_new_lcu,
    cross_upsell_both_exists,
    Downgrade_Downsell_both_exists
  from downgrade_downsell_total a
    join cross_upsell_total b on a.mcid = b.mcid
    and a.evaluation_period = b.evaluation_period
)
select *,
  case
    when a.cross_upsell_both_exists = 0
    and a.Downgrade_Downsell_both_exists = 0 then case
      when a.Upsell_arr > 0
      and a.downgrade_downsell_total >= a.cross_upsell_total then a.cross_upsell_total
      when a.Upsell_arr > 0
      and a.downgrade_downsell_total < a.cross_upsell_total then a.downgrade_downsell_total
      else 0
    end
    when a.cross_upsell_both_exists = 0
    and a.Downgrade_Downsell_both_exists = 1 then case
      when a.Upsell_arr > 0
      and a.downgrade_downsell_total < a.cross_upsell_total then a.downgrade_downsell_total
      when a.Upsell_arr > 0
      and a.downgrade_downsell_total >= a.cross_upsell_total then a.cross_upsell_total
      else 0
    end
    when a.cross_upsell_both_exists = 1
    and a.Downgrade_Downsell_both_exists = 0 then case
      when a.Downsell_arr > 0
      and a.Downsell_arr <= a.Crossell_arr then 0
      when a.Downsell_arr > 0
      and a.Downsell_arr > a.Crossell_arr then case
        when a.Downsell_arr < a.downgrade_downsell_total then a.Downsell_arr - a.Crossell_arr
        else a.Upsell_arr
      end
      when a.Downgrade_arr > 0
      and a.Downgrade_arr <= a.Upsell_arr then a.Downgrade_arr
      when a.Downgrade_arr > 0
      and a.Downgrade_arr > a.Upsell_arr then a.Upsell_arr
    end
    when a.cross_upsell_both_exists = 1
    and a.Downgrade_Downsell_both_exists = 1 then case
      when a.downgrade_downsell_total > a.cross_upsell_total then a.Upsell_arr
      else case
        when a.Downgrade_arr > a.Upsell_arr then a.Upsell_arr
        else a.Downgrade_arr
      end
    end
    else 0
  end as upsell_reversal_arr_new,
  case
    when a.cross_upsell_both_exists = 0
    and a.Downgrade_Downsell_both_exists = 0 then case
      when a.Crossell_arr > 0
      and a.downgrade_downsell_total >= a.cross_upsell_total then a.cross_upsell_total
      when a.Crossell_arr > 0
      and a.downgrade_downsell_total < a.cross_upsell_total then a.downgrade_downsell_total
      else 0
    end
    when a.cross_upsell_both_exists = 0
    and a.Downgrade_Downsell_both_exists = 1 then case
      when a.Crossell_arr > 0
      and a.downgrade_downsell_total < a.cross_upsell_total then a.downgrade_downsell_total
      when a.Crossell_arr > 0
      and a.downgrade_downsell_total >= a.cross_upsell_total then a.cross_upsell_total
      else 0
    end
    when a.cross_upsell_both_exists = 1
    and a.Downgrade_Downsell_both_exists = 0 then case
      when a.Downgrade_arr > 0
      and a.Downgrade_arr <= a.Upsell_arr then 0
      when a.Downgrade_arr > 0
      and a.Downgrade_arr > a.Upsell_arr then case
        when a.Downgrade_arr < a.cross_upsell_total then a.Downgrade_arr - a.Upsell_arr
        else a.Crossell_arr
      end
      when a.Downsell_arr > 0
      and a.Downsell_arr <= a.Crossell_arr then a.Downsell_arr
      when a.Downsell_arr > 0
      and a.Downsell_arr > a.Crossell_arr then a.Crossell_arr
    end
    when a.cross_upsell_both_exists = 1
    and a.Downgrade_Downsell_both_exists = 1 then case
      when a.downgrade_downsell_total > a.cross_upsell_total then a.Crossell_arr
      else case
        when a.Downsell_arr > a.Crossell_arr then a.Crossell_arr
        else a.Downsell_arr
      end
    end
    else 0
  end as crosssell_reversal_arr_new --#######################lcu #######################--
,
  case
    when a.cross_upsell_both_exists = 0
    and a.Downgrade_Downsell_both_exists = 0 then case
      when a.Upsell_arr_lcu > 0
      and a.downgrade_downsell_total_lcu >= a.cross_upsell_total_lcu then a.cross_upsell_total_lcu
      when a.Upsell_arr_lcu > 0
      and a.downgrade_downsell_total_lcu < a.cross_upsell_total_lcu then a.downgrade_downsell_total_lcu
      else 0
    end
    when a.cross_upsell_both_exists = 0
    and a.Downgrade_Downsell_both_exists = 1 then case
      when a.Upsell_arr_lcu > 0
      and a.downgrade_downsell_total_lcu < a.cross_upsell_total_lcu then a.downgrade_downsell_total_lcu
      when a.Upsell_arr_lcu > 0
      and a.downgrade_downsell_total_lcu >= a.cross_upsell_total_lcu then a.cross_upsell_total_lcu
      else 0
    end
    when a.cross_upsell_both_exists = 1
    and a.Downgrade_Downsell_both_exists = 0 then case
      when a.Downsell_arr_lcu > 0
      and a.Downsell_arr_lcu <= a.Crossell_arr_lcu then 0
      when a.Downsell_arr_lcu > 0
      and a.Downsell_arr_lcu > a.Crossell_arr_lcu then case
        when a.Downsell_arr_lcu < a.downgrade_downsell_total_lcu then a.Downsell_arr_lcu - a.Crossell_arr_lcu
        else a.Upsell_arr_lcu
      end
      when a.Downgrade_arr_lcu > 0
      and a.Downgrade_arr_lcu <= a.Upsell_arr_lcu then a.Downgrade_arr_lcu
      when a.Downgrade_arr_lcu > 0
      and a.Downgrade_arr_lcu > a.Upsell_arr_lcu then a.Upsell_arr_lcu
    end
    when a.cross_upsell_both_exists = 1
    and a.Downgrade_Downsell_both_exists = 1 then case
      when a.downgrade_downsell_total_lcu > a.cross_upsell_total_lcu then a.Upsell_arr_lcu
      else case
        when a.Downgrade_arr_lcu > a.Upsell_arr_lcu then a.Upsell_arr_lcu
        else a.Downgrade_arr_lcu
      end
    end
    else 0
  end as upsell_reversal_arr_new_lcu,
  case
    when a.cross_upsell_both_exists = 0
    and a.Downgrade_Downsell_both_exists = 0 then case
      when a.Crossell_arr_lcu > 0
      and a.downgrade_downsell_total_lcu >= a.cross_upsell_total_lcu then a.cross_upsell_total_lcu
      when a.Crossell_arr_lcu > 0
      and a.downgrade_downsell_total_lcu < a.cross_upsell_total_lcu then a.downgrade_downsell_total_lcu
      else 0
    end
    when a.cross_upsell_both_exists = 0
    and a.Downgrade_Downsell_both_exists = 1 then case
      when a.Crossell_arr_lcu > 0
      and a.downgrade_downsell_total_lcu < a.cross_upsell_total_lcu then a.downgrade_downsell_total_lcu
      when a.Crossell_arr_lcu > 0
      and a.downgrade_downsell_total_lcu >= a.cross_upsell_total_lcu then a.cross_upsell_total_lcu
      else 0
    end
    when a.cross_upsell_both_exists = 1
    and a.Downgrade_Downsell_both_exists = 0 then case
      when a.Downgrade_arr_lcu > 0
      and a.Downgrade_arr_lcu <= a.Upsell_arr_lcu then 0
      when a.Downgrade_arr_lcu > 0
      and a.Downgrade_arr_lcu > a.Upsell_arr_lcu then case
        when a.Downgrade_arr_lcu < a.cross_upsell_total_lcu then a.Downgrade_arr_lcu - a.Upsell_arr_lcu
        else a.Crossell_arr_lcu
      end
      when a.Downsell_arr_lcu > 0
      and a.Downsell_arr_lcu <= a.Crossell_arr_lcu then a.Downsell_arr_lcu
      when a.Downsell_arr_lcu > 0
      and a.Downsell_arr_lcu > a.Crossell_arr_lcu then a.Crossell_arr_lcu
    end
    when a.cross_upsell_both_exists = 1
    and a.Downgrade_Downsell_both_exists = 1 then case
      when a.downgrade_downsell_total_lcu > a.cross_upsell_total_lcu then a.Crossell_arr_lcu
      else case
        when a.Downsell_arr_lcu > a.Crossell_arr_lcu then a.Crossell_arr_lcu
        else a.Downsell_arr_lcu
      end
    end
    else 0
  end as crosssell_reversal_arr_new_lcu,
  1 as split_record,
  null::numeric as Churn_arr,
  null::numeric as Churn_arr_lcu,
  null::numeric as Churn_arr_new,
  null::numeric as Churn_arr_lcu_new
from temp_new_arr_split a
order by cross_upsell_both_exists;
--get churn also into upsell reversal
RAISE NOTICE 'Running cross/Upsell reversal update on sst customer bridge 1.2...';
drop table if exists temp_cross_upsell_reversal_final_curated_churn;
create temporary table temp_cross_upsell_reversal_final_curated_churn as with cross_upsell_total as (
  select a.mcid,
    b.evaluation_period,
    a.evaluation_period as cross_upsell_evaluation_period,
    abs(sum(customer_arr_change_ccfx)) as cross_upsell_total,
    sum(
      case
        when a.customer_bridge = 'Cross-sell' then abs(customer_arr_change_ccfx)
        else 0
      end
    ) as Crossell_arr,
    sum(
      case
        when a.customer_bridge = 'Up Sell' then abs(customer_arr_change_ccfx)
        else 0
      end
    ) as Upsell_arr --lcu
,
    abs(sum(customer_arr_change_lcu)) as cross_upsell_total_lcu,
    sum(
      case
        when a.customer_bridge = 'Cross-sell' then abs(customer_arr_change_lcu)
        else 0
      end
    ) as Crossell_arr_lcu,
    sum(
      case
        when a.customer_bridge = 'Up Sell' then abs(customer_arr_change_lcu)
        else 0
      end
    ) as Upsell_arr_lcu,
    case
      when count(distinct a.customer_bridge) > 1 then 1
      else 0
    end as cross_upsell_both_exists
  from ryzlan.sst_customer_bridge a
    join (
      select distinct mcid,
        cross_upsell_evaluation_period,
        evaluation_period
      from temp_cross_upsell_reversal_final
    ) b on a.evaluation_period = b.cross_upsell_evaluation_period
    and a.mcid = b.mcid
  where 1 = 1
    and a.customer_bridge in ('Cross-sell', 'Up Sell')
  group by a.mcid,
    b.evaluation_period,
    a.evaluation_period
),
Churn_total as (
  select a.mcid,
    b.evaluation_period,
    a.evaluation_period as Downgrade_evaluation_period,
    abs(sum(customer_arr_change_ccfx)) as Churn_total,
    sum(
      case
        when a.customer_bridge = 'Churn' then abs(customer_arr_change_ccfx)
        else 0
      end
    ) as Churn_arr --lcu
,
    abs(sum(customer_arr_change_lcu)) as Churn_total_lcu,
    sum(
      case
        when a.customer_bridge = 'Churn' then abs(customer_arr_change_lcu)
        else 0
      end
    ) as Churn_arr_lcu,
    sum(
      case
        when a.customer_bridge = 'Churn' then 1
        else 0
      end
    ) as Churn_exists
  from ryzlan.sst_customer_bridge a
    join (
      select distinct mcid,
        evaluation_period_at_Downgrade_Downsell,
        evaluation_period
      from temp_cross_upsell_reversal_final
    ) b on a.evaluation_period = b.evaluation_period_at_Downgrade_Downsell
    and a.mcid = b.mcid
  where 1 = 1
    and a.customer_bridge in ('Churn')
  group by a.mcid,
    b.evaluation_period,
    a.evaluation_period
),
temp_new_arr_split_churn as (
  select a.mcid,
    a.evaluation_period,
    a.downgrade_evaluation_period,
    b.upsell_arr,
    b.Crossell_arr,
    null::numeric as downgrade_arr,
    null::numeric as Downsell_arr,
    b.cross_upsell_total,
    null::numeric as downgrade_downsell_total,
    b.upsell_arr_lcu,
    b.Crossell_arr_lcu,
    null::numeric as downgrade_arr_lcu,
    null::numeric as Downsell_arr_lcu,
    b.cross_upsell_total_lcu,
    null::numeric as downgrade_downsell_total_lcu,
    null::numeric as downgrade_arr_new,
    null::numeric as downsell_arr_new,
    null::numeric as downgrade_arr_new_lcu,
    null::numeric as downsell_arr_new_lcu,
    b.cross_upsell_both_exists,
    null::numeric as Downgrade_Downsell_both_exists,
    1 as split_record,
    a.Churn_arr as Churn_arr,
    a.Churn_arr_lcu as Churn_arr_lcu,
    case
      when a.Churn_arr < b.cross_upsell_total then 0
      else a.Churn_arr - b.cross_upsell_total
    end as Churn_arr_new,
    case
      when a.Churn_arr_lcu < b.cross_upsell_total_lcu then 0
      else a.Churn_arr_lcu - b.cross_upsell_total_lcu
    end as Churn_arr_lcu_new,
    case
      when b.cross_upsell_both_exists = 0
      and b.Upsell_arr > 0 then case
        when b.upsell_arr >= a.Churn_arr then a.Churn_arr
        else b.Upsell_arr
      end
      when b.cross_upsell_both_exists = 1
      and b.cross_upsell_total <= a.Churn_arr then b.upsell_arr
      when b.cross_upsell_both_exists = 1
      and b.cross_upsell_total > a.Churn_arr then case
        when b.upsell_arr >= a.Churn_arr then a.Churn_arr
        else b.Upsell_arr
      end
    end as upsell_reversal_arr_new,
    case
      when b.cross_upsell_both_exists = 0
      and b.Crossell_arr > 0 then case
        when b.Crossell_arr >= a.Churn_arr then a.Churn_arr
        else b.Crossell_arr
      end
      when b.cross_upsell_both_exists = 1
      and b.cross_upsell_total <= a.Churn_arr then b.Crossell_arr
      when b.cross_upsell_both_exists = 1
      and b.cross_upsell_total > a.Churn_arr then case
        when b.Crossell_arr - (a.Churn_arr - b.upsell_arr) > 0 then b.Crossell_arr - (a.Churn_arr - b.upsell_arr)
        else 0
      end
    end as crosssell_reversal_arr_new,
    case
      when b.cross_upsell_both_exists = 0
      and b.Upsell_arr_lcu > 0 then case
        when b.upsell_arr_lcu >= a.Churn_arr_lcu then a.Churn_arr_lcu
        else b.Upsell_arr_lcu
      end
      when b.cross_upsell_both_exists = 1
      and b.cross_upsell_total <= a.Churn_arr_lcu then b.upsell_arr_lcu
      when b.cross_upsell_both_exists = 1
      and b.cross_upsell_total > a.Churn_arr_lcu then case
        when b.upsell_arr_lcu >= a.Churn_arr_lcu then a.Churn_arr_lcu
        else b.Upsell_arr_lcu
      end
    end as upsell_reversal_arr_new_lcu,
    case
      when b.cross_upsell_both_exists = 0
      and b.Crossell_arr_lcu > 0 then case
        when b.Crossell_arr_lcu >= a.Churn_arr_lcu then a.Churn_arr_lcu
        else b.Crossell_arr_lcu
      end
      when b.cross_upsell_both_exists = 1
      and b.cross_upsell_total_lcu <= a.Churn_arr_lcu then b.Crossell_arr_lcu
      when b.cross_upsell_both_exists = 1
      and b.cross_upsell_total_lcu > a.Churn_arr_lcu then case
        when b.Crossell_arr_lcu - (a.Churn_arr_lcu - b.upsell_arr_lcu) > 0 then b.Crossell_arr_lcu - (a.Churn_arr_lcu - b.upsell_arr_lcu)
        else 0
      end
    end as crosssell_reversal_arr_new_lcu
  from Churn_total a
    join cross_upsell_total b on a.mcid = b.mcid
    and a.evaluation_period = b.evaluation_period
    left join temp_cross_upsell_reversal_final_curated c on a.mcid = c.mcid
    and a.evaluation_period = c.evaluation_period
),
temp_final as (
  select a.mcid,
    a.evaluation_period,
    a.downgrade_evaluation_period,
    a.upsell_arr,
    a.crossell_arr,
    a.downgrade_arr,
    a.downsell_arr,
    a.cross_upsell_total,
    a.downgrade_downsell_total,
    a.upsell_arr_lcu,
    a.crossell_arr_lcu,
    a.downgrade_arr_lcu,
    a.downsell_arr_lcu,
    a.cross_upsell_total_lcu,
    a.downgrade_downsell_total_lcu,
    a.downgrade_arr_new,
    a.downsell_arr_new,
    a.downgrade_arr_new_lcu,
    a.downsell_arr_new_lcu,
    a.cross_upsell_both_exists,
    a.downgrade_downsell_both_exists,
    a.split_record,
    a.churn_arr,
    a.churn_arr_lcu,
    a.churn_arr_new,
    a.churn_arr_lcu_new,
    a.upsell_reversal_arr_new,
    a.crosssell_reversal_arr_new,
    a.upsell_reversal_arr_new_lcu,
    a.crosssell_reversal_arr_new_lcu
  from temp_new_arr_split_churn a
    left join temp_cross_upsell_reversal_final_curated b on a.mcid = b.mcid
    and a.evaluation_period = b.evaluation_period
  where b.mcid is null
  union all
  select a.mcid,
    a.evaluation_period,
    a.downgrade_evaluation_period,
    a.upsell_arr,
    a.crossell_arr,
    a.downgrade_arr,
    a.downsell_arr,
    a.cross_upsell_total,
    a.downgrade_downsell_total,
    a.upsell_arr_lcu,
    a.crossell_arr_lcu,
    a.downgrade_arr_lcu,
    a.downsell_arr_lcu,
    a.cross_upsell_total_lcu,
    a.downgrade_downsell_total_lcu,
    a.downgrade_arr_new,
    a.downsell_arr_new,
    a.downgrade_arr_new_lcu,
    a.downsell_arr_new_lcu,
    a.cross_upsell_both_exists,
    a.downgrade_downsell_both_exists,
    a.split_record,
    a.churn_arr,
    a.churn_arr_lcu,
    a.churn_arr_new,
    a.churn_arr_lcu_new,
    a.upsell_reversal_arr_new,
    a.crosssell_reversal_arr_new,
    a.upsell_reversal_arr_new_lcu,
    a.crosssell_reversal_arr_new_lcu
  from temp_cross_upsell_reversal_final_curated a
    left join temp_new_arr_split_churn b on a.mcid = b.mcid
    and a.evaluation_period = b.evaluation_period
)
select *
from temp_final
where 1 = 1 --and mcid = 'c05cfedf-513e-b046-e5ca-39359ef4d5f4'
;
RAISE NOTICE 'Running cross/Upsell reversal update on sst customer bridge 2...';
drop table if exists temp_cross_upsell_reversal_split;
create temp table temp_cross_upsell_reversal_split as
select a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  a.prior_period_customer_arr_usd_ccfx,
  a.prior_period_customer_arr_usd_ccfx - b.downsell_arr_new as current_period_customer_arr_usd_ccfx,
  - b.downsell_arr_new as customer_arr_change_ccfx,
  a.prior_period_customer_arr_lcu,
  a.prior_period_customer_arr_lcu - b.downsell_arr_new_lcu as current_period_customer_lcu,
  - b.downsell_arr_new_lcu as customer_arr_change_lcu,
  a.customer_bridge,
  a.winback_period_days,
  a.wip_flag
from ryzlan.sst_customer_bridge a
  join temp_cross_upsell_reversal_final_curated_churn b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.customer_bridge in ('Downsell')
  and b.downsell_arr_new > 0
union all
select a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  a.prior_period_customer_arr_usd_ccfx,
  a.prior_period_customer_arr_usd_ccfx - b.downgrade_arr_new as current_period_customer_arr_usd_ccfx,
  - b.downgrade_arr_new as customer_arr_change_ccfx,
  a.prior_period_customer_arr_lcu,
  a.prior_period_customer_arr_lcu - b.downgrade_arr_new_lcu as current_period_customer_lcu,
  - b.downgrade_arr_new_lcu as customer_arr_change_lcu,
  a.customer_bridge,
  a.winback_period_days,
  a.wip_flag
from ryzlan.sst_customer_bridge a
  join temp_cross_upsell_reversal_final_curated_churn b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.customer_bridge in ('Downgrade')
  and b.downgrade_arr_new > 0
union all
select a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  a.prior_period_customer_arr_usd_ccfx,
  a.prior_period_customer_arr_usd_ccfx - b.Churn_arr_new as current_period_customer_arr_usd_ccfx,
  - b.Churn_arr_new as customer_arr_change_ccfx,
  a.prior_period_customer_arr_lcu,
  a.prior_period_customer_arr_lcu - b.churn_arr_lcu_new as current_period_customer_lcu,
  - b.churn_arr_lcu_new as customer_arr_change_lcu,
  a.customer_bridge,
  a.winback_period_days,
  a.wip_flag
from ryzlan.sst_customer_bridge a
  join temp_cross_upsell_reversal_final_curated_churn b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.customer_bridge in ('Churn')
  and b.churn_arr_new > 0
union all
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  0 as prior_period_customer_arr_usd_ccfx,
  b.upsell_reversal_arr_new as current_period_customer_arr_usd_ccfx,
  - b.upsell_reversal_arr_new as customer_arr_change_ccfx,
  0 as prior_period_customer_arr_lcu,
  b.upsell_reversal_arr_new_lcu as current_period_customer_lcu,
  - b.upsell_reversal_arr_new_lcu as customer_arr_change_lcu,
  'Up Sell Reversal' as customer_bridge,
  null as winback_period_days,
  null as wip_flag --select b.*
from ryzlan.sst_customer_bridge a
  join temp_cross_upsell_reversal_final_curated_churn b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
where b.Split_record = 1
  and a.evaluation_period = var_period
  and b.upsell_reversal_arr_new > 0
  and a.customer_bridge <> 'Flat'
union all
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  0 as prior_period_customer_arr_usd_ccfx,
  b.crosssell_reversal_arr_new as current_period_customer_arr_usd_ccfx,
  - b.crosssell_reversal_arr_new as customer_arr_change_ccfx,
  0 as prior_period_customer_arr_lcu,
  b.crosssell_reversal_arr_new_lcu as current_period_customer_lcu,
  - b.crosssell_reversal_arr_new_lcu as customer_arr_change_lcu,
  'Cross-sell Reversal' as customer_bridge,
  null as winback_period_days,
  null as wip_flag --select b.*
from ryzlan.sst_customer_bridge a
  join temp_cross_upsell_reversal_final_curated_churn b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
where b.Split_record = 1
  and a.evaluation_period = var_period
  and b.crosssell_reversal_arr_new > 0
  and a.customer_bridge <> 'Flat'
order by mcid;
RAISE NOTICE 'Running cross/Upsell reversal update on sst customer bridge 4...';
delete from ryzlan.sst_customer_bridge a using temp_cross_upsell_reversal_final_curated_churn b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and b.Split_record = 1
  and a.evaluation_period = var_period -- and a.customer_bridge = b.customer_bridge
  and a.customer_bridge in ('Downgrade', 'Downsell', 'Churn');
insert into ryzlan.sst_customer_bridge (
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    name,
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
    wip_flag
  )
select evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  name,
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
  wip_flag
from temp_cross_upsell_reversal_split;
end if;
RAISE NOTICE 'Running rounding errors update on sst customer bridge...';
--rounding errors updates
update ryzlan.sst_customer_bridge
set customer_bridge = 'Rounding'
where customer_bridge = 'Flat'
  and coalesce(customer_arr_change_ccfx, 0) <> 0
  and evaluation_period = var_period;
-- drop all temp tables
drop table if exists prior_period_customer_arr;
drop table if exists current_period_customer_arr;
drop table if exists customer_level_arr;
drop table if exists account;
drop table if exists arr_bridge_tmp;
drop table if exists temp_cross_sell_data;
drop table if exists temp_pb_crosssell;
drop table if exists temp_pb_crosssell_final;
drop table if exists temp_cb_crosssell_split;
drop table if exists temp_downsell_data;
drop table if exists temp_pb_downsell;
drop table if exists temp_pb_downsell_final;
drop table if exists temp_cb_downsell_split;
drop table if exists temp_customer_bridge_price_ramps;
drop table if exists temp_Price_Ramp_split;
drop table if exists arr_new_products_tmp;
drop table if exists arr_churned_products_tmp;
drop table if exists sub_entity_tmp;
drop table if exists temp_win_downgrade_upsell;
drop table if exists temp_windowngrade_final;
drop table if exists temp_windowngrade_final_curated;
drop table if exists temp_windowngrade_split;
drop table if exists temp_CPI_Reversal;
drop table if exists temp_cpireversal_final;
drop table if exists temp_cpireversal_split;
drop table if exists temp_downgrade_upsell;
drop table if exists temp_upselldowngrade_final;
drop table if exists temp_upselldowngrade_split;
End;
$$;