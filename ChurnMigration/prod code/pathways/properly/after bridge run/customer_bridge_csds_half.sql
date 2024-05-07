create or replace function ryzlan.sp_populate_sst_customer_bridge(var_period text) returns void language plpgsql as $$ BEGIN RAISE NOTICE 'Running sst_customer_bridge for %...',
  var_period;
DELETE from ryzlan.sst_customer_bridge_csds_half
where evaluation_period = var_period;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--SST customer Bridge
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
  and product_bridge in (
    'Downsell',
    'Downsell - migration',
    'Downgrade',
    'Downgrade - migration'
  )
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
    CASE
      WHEN product_bridge ilike('%migration') THEN TRUE
      ELSE FALSE
    END AS migration_flag,
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
    and customer_arr_change_ccfx <= 0 --    
    -- AND a.mcid = '3008e8bf-2fe5-e411-9afb-0050568d2da8'
),
classifier AS (
  select *,
    CASE
      WHEN abs(round(total_pb_amount_ccfx)) = abs(round(customer_arr_change_ccfx)) THEN 'EQUAL'
      ELSE CASE
        WHEN abs(round(total_pb_amount_ccfx)) < abs(round(customer_arr_change_ccfx)) THEN 'LESS'
        ELSE CASE
          WHEN abs(round(total_pb_amount_ccfx)) > abs(round(customer_arr_change_ccfx)) THEN 'GREATER'
          ELSE NULL
        END
      END
    END AS flag
  from TEMP
)
SELECT a.*,
  CASE
    WHEN flag = 'LESS' THEN round(
      (customer_arr_change_ccfx - total_pb_amount_ccfx) / count_pb_records
    )
    ELSE NULL
  END AS leftover_ccfx,
  CASE
    WHEN flag = 'LESS' THEN round(
      customer_arr_change_lcu - total_pb_amount_lcu / count_pb_records
    )
    ELSE NULL
  END AS leftover_lcu,
  CASE
    WHEN flag = 'GREATER' THEN round(
      customer_arr_change_ccfx * (product_arr_change_ccfx / total_pb_amount_ccfx)
    )
  END AS greater_amount_ccfx,
  CASE
    WHEN flag = 'GREATER' THEN round(
      customer_arr_change_lcu * (product_arr_change_lcu / total_pb_amount_lcu)
    )
  END AS greater_amount_lcu
FROM classifier AS a;
-- Handle Equal delete previous record add new record using product group bridge
DELETE FROM arr_bridge_tmp AS a USING temp_pb_downsell_final AS b
WHERE a.evaluation_period = b.evaluation_period
  AND a.mcid = b.mcid
  AND a.baseline_currency = b.baseline_currency
  AND a.customer_bridge = b.customer_bridge
  AND b.flag IS NOT NULL;
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
    product_arr_change_ccfx / case
      when customer_arr_change_ccfx = 0 then 1
      else customer_arr_change_ccfx
    end
  ) AS prior_arr_usd_ccfx,
  current_arr_usd_ccfx * abs(
    product_arr_change_ccfx / case
      when customer_arr_change_ccfx = 0 then 1
      else customer_arr_change_ccfx
    end
  ) AS current_arr_usd_ccfx,
  product_arr_change_ccfx AS customer_arr_change_ccfx,
  --customer_arr_change_ccfx,
  prior_arr_lcu * abs(
    product_arr_change_lcu / case
      when customer_arr_change_lcu = 0 then 1
      else customer_arr_change_lcu
    end
  ) AS prior_arr_lcu,
  current_arr_lcu * abs(
    product_arr_change_lcu / case
      when customer_arr_change_lcu = 0 then 1
      else customer_arr_change_lcu
    end
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
      prior_arr_usd_ccfx * abs(
        leftover_ccfx / case
          when customer_arr_change_ccfx = 0 then 1
          else customer_arr_change_ccfx
        end
      ) AS prior_arr_usd_ccfx,
      current_arr_usd_ccfx * abs(
        leftover_ccfx / case
          when customer_arr_change_ccfx = 0 then 1
          else customer_arr_change_ccfx
        end
      ) AS current_arr_usd_ccfx,
      leftover_ccfx AS customer_arr_change_ccfx,
      --customer_arr_change_ccfx,
      prior_arr_lcu * abs(
        leftover_lcu / case
          when customer_arr_change_lcu = 0 then 1
          else customer_arr_change_lcu
        end
      ) AS prior_arr_lcu,
      current_arr_lcu * abs(
        leftover_lcu / case
          when customer_arr_change_lcu = 0 then 1
          else customer_arr_change_lcu
        end
      ) AS current_arr_lcu,
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
    greater_amount_ccfx / case
      when customer_arr_change_ccfx = 0 then 1
      else customer_arr_change_ccfx
    end
  ) AS prior_arr_usd_ccfx,
  current_arr_usd_ccfx * abs(
    greater_amount_ccfx / case
      when customer_arr_change_ccfx = 0 then 1
      else customer_arr_change_ccfx
    end
  ) AS current_arr_usd_ccfx,
  greater_amount_ccfx AS customer_arr_change_ccfx,
  --customer_arr_change_ccfx,
  prior_arr_lcu * abs(
    greater_amount_lcu / case
      when customer_arr_change_lcu = 0 then 1
      else customer_arr_change_lcu
    end
  ) AS prior_arr_lcu,
  current_arr_lcu * abs(
    greater_amount_lcu / case
      when customer_arr_change_lcu = 0 then 1
      else customer_arr_change_lcu
    end
  ) AS current_arr_lcu,
  greater_amount_lcu AS customer_arr_change_lcu,
  --  customer_arr_change_lcu,
  product_bridge AS customer_bridge --customer_bridge --, winback_period_days, wip_flag
from temp_pb_downsell_final AS b
WHERE b.flag IN ('GREATER');
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
  and product_bridge IN (
    'Cross-sell',
    'Cross-sell - migration',
    'Up Sell - migration',
    'Up Sell'
  )
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
    CASE
      WHEN product_bridge ilike('%migration') THEN TRUE
      ELSE FALSE
    END AS migration_flag,
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
    and customer_arr_change_ccfx > 0
    and customer_bridge not in ('New') --    
    -- AND a.mcid = '3008e8bf-2fe5-e411-9afb-0050568d2da8'
),
classifier AS (
  select *,
    CASE
      WHEN abs(round(total_pb_amount_ccfx)) = abs(round(customer_arr_change_ccfx)) THEN 'EQUAL'
      ELSE CASE
        WHEN abs(round(total_pb_amount_ccfx)) < abs(round(customer_arr_change_ccfx)) THEN 'LESS'
        ELSE CASE
          WHEN abs(round(total_pb_amount_ccfx)) > abs(round(customer_arr_change_ccfx)) THEN 'GREATER'
          ELSE NULL
        END
      END
    END AS flag
  from TEMP
)
SELECT a.*,
  CASE
    WHEN flag = 'LESS' THEN round(
      (customer_arr_change_ccfx - total_pb_amount_ccfx) / count_pb_records
    )
    ELSE NULL
  END AS leftover_ccfx,
  CASE
    WHEN flag = 'LESS' THEN round(
      customer_arr_change_lcu - total_pb_amount_lcu / count_pb_records
    )
    ELSE NULL
  END AS leftover_lcu,
  CASE
    WHEN flag = 'GREATER' THEN round(
      customer_arr_change_ccfx * (product_arr_change_ccfx / total_pb_amount_ccfx)
    )
  END AS greater_amount_ccfx,
  CASE
    WHEN flag = 'GREATER' THEN round(
      customer_arr_change_lcu * (product_arr_change_lcu / total_pb_amount_lcu)
    )
  END AS greater_amount_lcu
FROM classifier AS a;
-- Handle Equal delete previous record add new record using product group bridge
DELETE FROM arr_bridge_tmp AS a USING temp_pb_crosssell_final AS b
WHERE a.evaluation_period = b.evaluation_period
  AND a.mcid = b.mcid
  AND a.baseline_currency = b.baseline_currency
  AND a.customer_bridge = b.customer_bridge
  AND b.flag IS NOT NULL;
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
    product_arr_change_ccfx / case
      when customer_arr_change_ccfx = 0 then 1
      else customer_arr_change_ccfx
    end
  ) AS prior_arr_usd_ccfx,
  current_arr_usd_ccfx * abs(
    product_arr_change_ccfx / case
      when customer_arr_change_ccfx = 0 then 1
      else customer_arr_change_ccfx
    end
  ) AS current_arr_usd_ccfx,
  product_arr_change_ccfx AS customer_arr_change_ccfx,
  --customer_arr_change_ccfx,
  prior_arr_lcu * abs(
    product_arr_change_lcu / case
      when customer_arr_change_lcu = 0 then 1
      else customer_arr_change_lcu
    end
  ) AS prior_arr_lcu,
  current_arr_lcu * abs(
    product_arr_change_lcu / case
      when customer_arr_change_lcu = 0 then 1
      else customer_arr_change_lcu
    end
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
      prior_arr_usd_ccfx * abs(
        leftover_ccfx / case
          when customer_arr_change_ccfx = 0 then 1
          else customer_arr_change_ccfx
        end
      ) AS prior_arr_usd_ccfx,
      current_arr_usd_ccfx * abs(
        leftover_ccfx / case
          when customer_arr_change_ccfx = 0 then 1
          else customer_arr_change_ccfx
        end
      ) AS current_arr_usd_ccfx,
      leftover_ccfx AS customer_arr_change_ccfx,
      --customer_arr_change_ccfx,
      prior_arr_lcu * abs(
        leftover_lcu / case
          when customer_arr_change_lcu = 0 then 1
          else customer_arr_change_lcu
        end
      ) AS prior_arr_lcu,
      current_arr_lcu * abs(
        leftover_lcu / case
          when customer_arr_change_lcu = 0 then 1
          else customer_arr_change_lcu
        end
      ) AS current_arr_lcu,
      leftover_lcu AS customer_arr_change_lcu,
      --  customer_arr_change_lcu,
      customer_bridge --, winback_period_days, wip_flag
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
    greater_amount_ccfx / case
      when customer_arr_change_ccfx = 0 then 1
      else customer_arr_change_ccfx
    end
  ) AS prior_arr_usd_ccfx,
  current_arr_usd_ccfx * abs(
    greater_amount_ccfx / case
      when customer_arr_change_ccfx = 0 then 1
      else customer_arr_change_ccfx
    end
  ) AS current_arr_usd_ccfx,
  greater_amount_ccfx AS customer_arr_change_ccfx,
  --customer_arr_change_ccfx,
  prior_arr_lcu * abs(
    greater_amount_lcu / case
      when customer_arr_change_lcu = 0 then 1
      else customer_arr_change_lcu
    end
  ) AS prior_arr_lcu,
  current_arr_lcu * abs(
    greater_amount_lcu / case
      when customer_arr_change_lcu = 0 then 1
      else customer_arr_change_lcu
    end
  ) AS current_arr_lcu,
  greater_amount_lcu AS customer_arr_change_lcu,
  --  customer_arr_change_lcu,
  product_bridge AS customer_bridge --customer_bridge --, winback_period_days, wip_flag
from temp_pb_crosssell_final AS b
WHERE b.flag IN ('GREATER');
insert into ryzlan.sst_customer_bridge_csds_half(
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
  prior_arr_usd_ccfx,
  current_arr_usd_ccfx,
  customer_arr_change_ccfx,
  prior_arr_lcu,
  current_arr_lcu,
  customer_arr_change_lcu,
  customer_bridge
from arr_bridge_tmp;
End;
$$;