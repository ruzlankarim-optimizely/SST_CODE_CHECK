-- New script in dw-prod-rds-master.cr9dekxonyuj.us-east-1.rds.amaz.
-- Date: Apr 25, 2024
-- Time: 3:07:22 PM
CREATE OR replace FUNCTION sandbox_pd.sp_populate_sst_customer_bridge(var_period text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'Running sst_customer_bridge for %...', var_period;

DELETE
FROM sandbox_pd.sst_customer_bridge
WHERE evaluation_period = var_period;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--SST customer Bridge
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS prior_period_customer_arr;

CREATE temp TABLE prior_period_customer_arr AS
SELECT snapshot_date,
  a.mcid AS master_customer_id,
  a.base_currency AS baseline_currency,
  sum(arr) AS arr_usd_ccfx,
  SUM(baseline_arr_local_currency) AS arr_lcu
FROM sandbox_pd.sst_adhoc a
WHERE 1 = 1
AND snapshot_date = (
    SELECT prior_period
FROM ufdm_grey.periods
WHERE evaluation_period = var_period
  )
AND a.overage_flag ilike '%N%'
GROUP BY 1, 2, 3;

DROP TABLE IF EXISTS current_period_customer_arr;

CREATE temp TABLE current_period_customer_arr AS
SELECT snapshot_date,
  a.mcid AS master_customer_id,
  a.base_currency AS baseline_currency,
  sum(arr) AS arr_usd_ccfx,
  SUM(baseline_arr_local_currency) AS arr_lcu
FROM sandbox_pd.sst_adhoc a
WHERE 1 = 1
AND snapshot_date = (
    SELECT current_period
FROM ufdm_grey.periods
WHERE evaluation_period = var_period
  )
AND a.overage_flag ilike '%N%'
GROUP BY 1, 2, 3;

DROP TABLE IF EXISTS customer_level_arr;

CREATE temp TABLE customer_level_arr AS
SELECT c1.master_customer_id AS current_cust_id,
  c2.master_customer_id AS prior_cust_id,
  c2.snapshot_date AS prior_period,
  c1.snapshot_date AS current_period,
  c1.baseline_currency AS current_baseline_currency,
  c2.baseline_currency AS prior_baseline_currency,
  COALESCE(c1.arr_usd_ccfx, 0) AS current_arr_usd_ccfx,
  COALESCE(c2.arr_usd_ccfx, 0) AS prior_arr_usd_ccfx,
  COALESCE(c1.arr_lcu, 0) AS current_arr_lcu,
  COALESCE(c2.arr_lcu, 0) AS prior_arr_lcu,
  COALESCE(c1.baseline_currency, c2.baseline_currency) AS baseline_currency
FROM current_period_customer_arr c1
FULL OUTER JOIN prior_period_customer_arr c2 ON
c1.master_customer_id = c2.master_customer_id
AND c1.baseline_currency = c2.baseline_currency;

DROP TABLE IF EXISTS account;

CREATE temp TABLE account AS
SELECT COALESCE(a.dynamics_id_c, a.sf_guid_c) AS master_customer_id,
  a.name,
  ROW_NUMBER() OVER (
    PARTITION BY COALESCE(a.dynamics_id_c, a.sf_guid_c)
  ) AS rn
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
DROP TABLE IF EXISTS arr_bridge_tmp;

CREATE temp TABLE arr_bridge_tmp AS WITH arr_bridge AS (
  SELECT per.evaluation_period,
  cla.prior_period,
  cla.current_period,
  cla.current_cust_id AS current_master_customer_id,
  cla.prior_cust_id AS prior_master_customer_id,
  COALESCE(current_cust_id, prior_cust_id) AS mcid,
  a.name,
  cla.baseline_currency,
  NULL AS subsidiary_entity_name,
  round(
    (COALESCE(cla.current_arr_usd_ccfx::NUMERIC, 0)),
    2
  ) AS current_arr_usd_ccfx,
  round(
    (COALESCE(cla.prior_arr_usd_ccfx::NUMERIC, 0)),
    2
  ) AS prior_arr_usd_ccfx,
  round(
    (
      COALESCE(cla.current_arr_usd_ccfx::NUMERIC, 0) - COALESCE(cla.prior_arr_usd_ccfx::NUMERIC, 0)
    ),
    2
  ) AS customer_arr_change_ccfx,
  round((COALESCE(cla.current_arr_lcu::NUMERIC, 0)), 2) AS current_arr_lcu,
  round((COALESCE(cla.prior_arr_lcu::NUMERIC, 0)), 2) AS prior_arr_lcu,
  round(
    (
      COALESCE(cla.current_arr_lcu::NUMERIC, 0) - COALESCE(cla.prior_arr_lcu::NUMERIC, 0)
    ),
    2
  ) AS customer_arr_change_lcu,
  CASE
    WHEN (
      COALESCE (cla.prior_arr_usd_ccfx, 0) = 0
      AND cla.current_arr_usd_ccfx > 0
    ) THEN 'New'
    WHEN cla.current_arr_usd_ccfx - cla.prior_arr_usd_ccfx BETWEEN -1 AND 1 THEN 'Flat'
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
  LEFT JOIN account a ON a.master_customer_id = cla.current_cust_id
  AND a.rn = 1
  CROSS JOIN ufdm_grey.periods per
WHERE 1 = 1
  AND per.evaluation_period = var_period
ORDER BY COALESCE(a.name)
)
SELECT *
FROM arr_bridge;
--#############################################
--Price Ramps
--#############################################
DROP TABLE IF EXISTS temp_customer_bridge_price_ramps;

CREATE temp TABLE temp_customer_bridge_price_ramps AS WITH cte AS (
  SELECT mcid,
  snapshot_date,
  sum(Price_Ramp) AS PriceRamp_Value,
  sum(Price_Ramp_lcu) AS PriceRamp_Value_lcu
FROM sandbox_pd.Price_Ramps a
  JOIN ufdm_grey.periods b ON a.snapshot_date = b.current_period --where b.evaluation_period = var_period
GROUP BY c_name,
  mcid,
  snapshot_date
)
SELECT pr.evaluation_period,
  pr.prior_period,
  pr.current_period,
  pr.mcid,
  pr.prior_arr_usd_ccfx AS prior_period_customer_arr_usd_ccfx,
  pr.current_arr_usd_ccfx AS current_period_customer_arr_usd_ccfx,
  pr.customer_arr_change_ccfx,
  pr.customer_bridge,
  pr.customer_arr_change_lcu,
  pr.prior_arr_lcu,
  cte.PriceRamp_Value,
  cte.PriceRamp_Value_lcu,
  cte.snapshot_date
FROM arr_bridge_tmp pr
  INNER JOIN cte ON pr.mcid = cte.mcid
  AND pr.current_period = cte.snapshot_date
WHERE pr.customer_bridge = 'Up Sell';

UPDATE arr_bridge_tmp a
SET customer_bridge = 'Price Ramp'
FROM temp_customer_bridge_price_ramps b
WHERE a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND COALESCE(a.customer_arr_change_ccfx::NUMERIC, 0) <= COALESCE(b.PriceRamp_Value::NUMERIC, 0)
  AND a.customer_bridge = 'Up Sell';

DROP TABLE IF EXISTS temp_Price_Ramp_split;

CREATE temp TABLE temp_Price_Ramp_split AS
SELECT DISTINCT a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  a.prior_arr_usd_ccfx AS prior_period_customer_arr_usd_ccfx,
  a.current_arr_usd_ccfx - b.PriceRamp_Value AS current_period_customer_arr_usd_ccfx,
  a.customer_arr_change_ccfx - b.PriceRamp_Value AS customer_arr_change_ccfx,
  a.prior_arr_lcu AS prior_period_customer_arr_lcu,
  a.current_arr_lcu - b.PriceRamp_Value_lcu AS current_period_customer_lcu,
  a.customer_arr_change_lcu - b.PriceRamp_Value_lcu AS customer_arr_change_lcu,
  a.customer_bridge
FROM arr_bridge_tmp a
JOIN temp_customer_bridge_price_ramps b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
AND a.customer_bridge = b.customer_bridge
WHERE COALESCE(a.customer_arr_change_ccfx::NUMERIC, 0) > COALESCE(b.PriceRamp_Value::NUMERIC, 0)
UNION ALL
SELECT DISTINCT a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  '0'::NUMERIC AS prior_period_customer_arr_usd_ccfx,
  b.PriceRamp_Value AS current_period_customer_arr_usd_ccfx,
  b.PriceRamp_Value AS customer_arr_change_ccfx,
  '0'::NUMERIC AS prior_period_customer_arr_lcu,
  b.PriceRamp_Value_lcu AS current_period_customer_lcu,
  b.PriceRamp_Value_lcu AS customer_arr_change_lcu,
  'Price Ramp' AS customer_bridge
FROM arr_bridge_tmp a
JOIN temp_customer_bridge_price_ramps b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
AND a.customer_bridge = b.customer_bridge
WHERE COALESCE(a.customer_arr_change_ccfx::NUMERIC, 0) > COALESCE(b.PriceRamp_Value::NUMERIC, 0)
ORDER BY mcid;

DELETE
FROM arr_bridge_tmp a
    USING temp_customer_bridge_price_ramps b
WHERE 1 = 1
AND a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
AND COALESCE(a.customer_arr_change_ccfx::NUMERIC, 0) > COALESCE(b.PriceRamp_Value::NUMERIC, 0)
AND a.customer_bridge = 'Up Sell';

INSERT INTO arr_bridge_tmp (
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
SELECT evaluation_period,
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
FROM temp_Price_Ramp_split;
--#############################################
--Downgrade
--#############################################
UPDATE arr_bridge_tmp
SET
customer_bridge = 'Downgrade'
WHERE customer_bridge = 'Partial Churn';
--###########################################
--DOWNSELL
--###########################################
RAISE NOTICE 'Running downsell update on sst_customer_bridge...';

DROP TABLE IF EXISTS temp_pb_downsell;

CREATE temp TABLE temp_pb_downsell AS
SELECT mcid,
  product_bridge,
  evaluation_period,
  sum(product_arr_change_ccfx) AS product_arr_change_ccfx,
  sum(product_arr_change_lcu) AS product_arr_change_lcu
FROM sandbox_pd.sst_product_bridge_product_group
WHERE 1 = 1
AND product_bridge IN ('Downsell')
AND evaluation_period = var_period
GROUP BY mcid, product_bridge, evaluation_period;

DROP TABLE IF EXISTS temp_pb_downsell_final;

CREATE temp TABLE temp_pb_downsell_final AS WITH temp AS (
  SELECT a.customer_arr_change_ccfx,
  a.customer_bridge,
  b.*,
  ROW_NUMBER() OVER (
    PARTITION BY a.mcid
    ORDER BY a.customer_arr_change_ccfx
  ) AS rnk
FROM arr_bridge_tmp a
JOIN temp_pb_downsell b ON
a.mcid = b.mcid
WHERE a.customer_bridge NOT IN ('Flat', 'Rounding')
    AND a.evaluation_period = var_period
    AND customer_arr_change_ccfx < 0
)
SELECT *, CASE
    WHEN abs(customer_arr_change_ccfx) <= abs(product_arr_change_ccfx) THEN 0
ELSE 1
END AS split_record
FROM temp
WHERE rnk = 1;

UPDATE arr_bridge_tmp a
SET
customer_bridge = 'Downsell'
FROM temp_pb_downsell_final b
WHERE a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
AND a.customer_bridge = b.customer_bridge
AND b.split_record = 0
AND a.evaluation_period = var_period;

DROP TABLE IF EXISTS temp_cb_downsell_split;

CREATE temp TABLE temp_cb_downsell_split AS
SELECT DISTINCT a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  a.prior_arr_usd_ccfx AS prior_period_customer_arr_usd_ccfx,
  a.current_arr_usd_ccfx - b.product_arr_change_ccfx AS current_period_customer_arr_usd_ccfx,
  a.customer_arr_change_ccfx - b.product_arr_change_ccfx AS customer_arr_change_ccfx,
  a.prior_arr_lcu AS prior_period_customer_arr_lcu,
  a.current_arr_lcu - b.product_arr_change_lcu AS current_period_customer_lcu,
  a.customer_arr_change_lcu - b.product_arr_change_lcu AS customer_arr_change_lcu,
  a.customer_bridge --,a.winback_period_days,a.wip_flag
FROM arr_bridge_tmp a
JOIN temp_pb_downsell_final b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
AND a.customer_bridge = b.customer_bridge
WHERE b.Split_record = 1
AND a.evaluation_period = var_period
UNION ALL
SELECT DISTINCT a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  '0'::NUMERIC AS prior_period_customer_arr_usd_ccfx,
  b.product_arr_change_ccfx AS current_period_customer_arr_usd_ccfx,
  b.product_arr_change_ccfx AS customer_arr_change_ccfx,
  '0'::NUMERIC AS prior_period_customer_arr_lcu,
  b.product_arr_change_lcu AS current_period_customer_lcu,
  b.product_arr_change_lcu AS customer_arr_change_lcu,
  'Downsell' AS customer_bridge --,a.winback_period_days,a.wip_flag
FROM arr_bridge_tmp a
JOIN temp_pb_downsell_final b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
AND a.customer_bridge = b.customer_bridge
WHERE b.Split_record = 1
AND a.evaluation_period = var_period
ORDER BY mcid;

DELETE
FROM arr_bridge_tmp a
    USING temp_pb_downsell_final b
WHERE 1 = 1
AND a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
AND b.Split_record = 1
AND a.evaluation_period = var_period
AND a.customer_bridge = b.customer_bridge;

INSERT INTO arr_bridge_tmp (
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
  prior_period_customer_arr_usd_ccfx,
  current_period_customer_arr_usd_ccfx,
  customer_arr_change_ccfx,
  prior_period_customer_arr_lcu,
  current_period_customer_lcu,
  customer_arr_change_lcu,
  customer_bridge --, winback_period_days, wip_flag
FROM temp_cb_downsell_split;
--############################
--cross sell
--############################
RAISE NOTICE 'Running crossell update on sst_customer_bridge...';

DROP TABLE IF EXISTS temp_pb_crosssell;

CREATE temp TABLE temp_pb_crosssell AS
SELECT mcid,
  product_bridge,
  evaluation_period,
  sum(product_arr_change_ccfx) AS product_arr_change_ccfx,
  sum(product_arr_change_lcu) AS product_arr_change_lcu
FROM sandbox_pd.sst_product_bridge_product_group
WHERE 1 = 1
AND product_bridge IN ('Cross-sell')
AND evaluation_period = var_period
GROUP BY mcid, product_bridge, evaluation_period;

DROP TABLE IF EXISTS temp_pb_crosssell_final;

CREATE temp TABLE temp_pb_crosssell_final AS WITH temp AS (
  SELECT a.customer_arr_change_ccfx, a.customer_bridge, b.*, ROW_NUMBER() OVER (
      PARTITION BY a.mcid
ORDER BY a.customer_arr_change_ccfx DESC
    ) AS rnk
FROM arr_bridge_tmp a
JOIN temp_pb_crosssell b ON
a.mcid = b.mcid
WHERE a.customer_bridge NOT IN ('Flat', 'Rounding')
    AND a.evaluation_period = var_period
    AND customer_arr_change_ccfx > 0
)
SELECT *, CASE
    WHEN abs(customer_arr_change_ccfx) <= abs(product_arr_change_ccfx) THEN 0
ELSE 1
END AS split_record
FROM temp
WHERE rnk = 1;

UPDATE arr_bridge_tmp a
SET
customer_bridge = 'Cross-sell'
FROM temp_pb_crosssell_final b
WHERE a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
AND a.customer_bridge = b.customer_bridge
AND b.split_record = 0
AND a.evaluation_period = var_period;

DROP TABLE IF EXISTS temp_cb_crosssell_split;

CREATE temp TABLE temp_cb_crosssell_split AS
SELECT DISTINCT a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  a.prior_arr_usd_ccfx AS prior_period_customer_arr_usd_ccfx,
  a.current_arr_usd_ccfx - b.product_arr_change_ccfx AS current_period_customer_arr_usd_ccfx,
  a.customer_arr_change_ccfx - b.product_arr_change_ccfx AS customer_arr_change_ccfx,
  a.prior_arr_lcu AS prior_period_customer_arr_lcu,
  a.current_arr_lcu - b.product_arr_change_lcu AS current_period_customer_lcu,
  a.customer_arr_change_lcu - b.product_arr_change_lcu AS customer_arr_change_lcu,
  a.customer_bridge --,a.winback_period_days,a.wip_flag
FROM arr_bridge_tmp a
JOIN temp_pb_crosssell_final b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
AND a.customer_bridge = b.customer_bridge
WHERE b.Split_record = 1
AND a.evaluation_period = var_period
UNION ALL
SELECT DISTINCT a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  '0'::NUMERIC AS prior_period_customer_arr_usd_ccfx,
  b.product_arr_change_ccfx AS current_period_customer_arr_usd_ccfx,
  b.product_arr_change_ccfx AS customer_arr_change_ccfx,
  '0'::NUMERIC AS prior_period_customer_arr_lcu,
  b.product_arr_change_lcu AS current_period_customer_lcu,
  b.product_arr_change_lcu AS customer_arr_change_lcu,
  'Cross-sell' AS customer_bridge --,a.winback_period_days,a.wip_flag
FROM arr_bridge_tmp a
JOIN temp_pb_crosssell_final b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
AND a.customer_bridge = b.customer_bridge
WHERE b.Split_record = 1
AND a.evaluation_period = var_period
ORDER BY mcid;

DELETE
FROM arr_bridge_tmp a
    USING temp_pb_crosssell_final b
WHERE 1 = 1
AND a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
AND b.Split_record = 1
AND a.evaluation_period = var_period
AND a.customer_bridge = b.customer_bridge;

INSERT INTO arr_bridge_tmp (
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
  prior_period_customer_arr_usd_ccfx,
  current_period_customer_arr_usd_ccfx,
  customer_arr_change_ccfx,
  prior_period_customer_arr_lcu,
  current_period_customer_lcu,
  customer_arr_change_lcu,
  customer_bridge --, winback_period_days, wip_flag
FROM temp_cb_crosssell_split;

RAISE NOTICE 'Running Price increase update on sst customer bridge...';
--Price Increase updates
UPDATE arr_bridge_tmp
SET
customer_bridge = 'CPI'
WHERE customer_bridge = 'Up Sell'
AND (
    (
      customer_arr_change_ccfx / prior_period_customer_arr_usd_ccfx
    ) * 100
  )::NUMERIC(10, 2) < CASE
    WHEN evaluation_period < '2023-01-01' THEN 5.5
ELSE 10.5
END
AND prior_period_customer_arr_usd_ccfx > 0
AND evaluation_period = var_period;
-- check these tables 
INSERT INTO sandbox_pd.sst_customer_bridge
SELECT *
FROM arr_bridge_tmp
--#############################################
--WIP/WINBACK
--#############################################
  DROP TABLE IF EXISTS arr_new_products_tmp;

CREATE temp TABLE arr_new_products_tmp AS
SELECT DISTINCT a.mcid AS mcid, a.current_period AS snapshot_date, a.current_arr_usd_ccfx AS arr_at_new, a.current_arr_lcu AS arr_lcu_at_new, baseline_currency
FROM arr_bridge_tmp a
WHERE customer_bridge IN ('New');
--get most recent postivie arr for above new product which should have been churned
DROP TABLE IF EXISTS arr_churned_products_tmp;

CREATE temp TABLE arr_churned_products_tmp AS WITH temp AS (
  SELECT b.snapshot_date, b.mcid
--, b.product_family
, a.baseline_currency, a.snapshot_date AS snapshot_date_at_new, sum(b.arr) AS arr_at_churn, sum(b.baseline_arr_local_currency) AS arr_lcu_at_churn, max(a.arr_at_new) AS arr_at_new, max(a.arr_lcu_at_new) AS arr_lcu_at_new, ROW_NUMBER() OVER (
      PARTITION BY b.mcid
ORDER BY b.snapshot_date DESC
    ) AS rnk
FROM arr_new_products_tmp a
JOIN sandbox_pd.sst_adhoc b ON
a.mcid = b.mcid
AND a.baseline_currency = b.base_currency
WHERE b.snapshot_date < a.snapshot_date
AND b.overage_flag ilike '%N%'
AND b.arr > 0
GROUP BY 1, 2, 3, 4
)
SELECT *,(
    DATE_PART('year', snapshot_date_at_new::date) - DATE_PART('year', snapshot_date::date)
  ) * 12 + (
    DATE_PART('month', snapshot_date_at_new::date) - DATE_PART('month', snapshot_date::date)
  ) AS months_diff, CASE
    WHEN arr_at_new > arr_at_churn THEN 'Upsell'
ELSE CASE
      WHEN EXTRACT(
        DAY
FROM snapshot_date_at_new::timestamp - (snapshot_date + INTERVAL '1 month')::date
      ) <= 90 THEN 'Winback ST'
ELSE 'Winback LT'
END
END AS customer_bridge_new, arr_at_new - arr_at_churn AS arr_diff, arr_lcu_at_new - arr_lcu_at_churn AS arr_lcu_diff, EXTRACT(
    DAY
FROM snapshot_date_at_new::timestamp - (snapshot_date + INTERVAL '1 month')::date
  ) AS days_diff, snapshot_date AS churn_period
FROM temp
WHERE rnk = 1
AND EXTRACT(
    DAY
FROM snapshot_date_at_new::timestamp - (snapshot_date + INTERVAL '1 month')::date
  ) < 186;

INSERT INTO sandbox_pd.sst_customer_bridge (
    evaluation_period, prior_period, current_period, current_master_customer_id, Prior_master_customer_id, "name", prior_period_customer_arr_usd_ccfx, current_period_customer_arr_usd_ccfx, customer_arr_change_ccfx, prior_period_customer_arr_lcu, current_period_customer_lcu, customer_arr_change_lcu, customer_bridge, subsidiary_entity_name, mcid, baseline_currency, Winback_period_days, Wip_Flag
  )
SELECT a.evaluation_period, a.prior_period, a.current_period, a.current_master_customer_id, a.Prior_master_customer_id, a."name",
---usd ccfx ----
round(a.prior_arr_usd_ccfx::NUMERIC, 2) AS prior_period_customer_arr_usd_ccfx, CASE
    WHEN b.mcid IS NOT NULL THEN CASE
      WHEN b.arr_at_new > b.arr_at_churn THEN b.arr_at_churn
ELSE b.arr_at_new
END
ELSE round(a.current_arr_usd_ccfx::NUMERIC, 2)
END AS current_period_customer_arr_usd_ccfx, CASE
    WHEN b.mcid IS NOT NULL THEN CASE
      WHEN b.arr_at_new > b.arr_at_churn THEN b.arr_at_churn
ELSE b.arr_at_new
END
ELSE a.customer_arr_change_ccfx
END AS customer_arr_change_ccfx,
------------------------lcu----------------------------
round(a.prior_arr_lcu::NUMERIC, 2) AS prior_period_customer_arr_lcu, CASE
    WHEN b.mcid IS NOT NULL THEN CASE
      WHEN b.arr_lcu_at_new > b.arr_lcu_at_churn THEN b.arr_lcu_at_churn
ELSE b.arr_lcu_at_new
END
ELSE round(a.current_arr_lcu::NUMERIC, 2)
END AS current_period_customer_arr_lcu, CASE
    WHEN b.mcid IS NOT NULL THEN CASE
      WHEN b.arr_lcu_at_new > b.arr_lcu_at_churn THEN b.arr_lcu_at_churn
ELSE b.arr_lcu_at_new
END
ELSE a.customer_arr_change_lcu
END AS customer_arr_change_lcu, CASE
    WHEN b.mcid IS NOT NULL THEN CASE
      WHEN b.days_diff <= 90 THEN 'Winback ST'
ELSE 'Winback LT'
END
ELSE a.customer_bridge
END AS customer_bridge, a.subsidiary_entity_name, a.mcid, a.baseline_currency, b.days_diff AS Winback_period_days, CASE
    WHEN b.days_diff <= 90 THEN 'Y'
ELSE 'N'
END AS Wip_Flag
FROM arr_bridge_tmp a
LEFT JOIN arr_churned_products_tmp b ON
a.current_master_customer_id = b.mcid
AND a.baseline_currency = b.baseline_currency
AND a.current_period = b.snapshot_date_at_new
UNION ALL
SELECT a.evaluation_period, a.prior_period, a.current_period, a.current_master_customer_id, a.Prior_master_customer_id, a."name", round(a.prior_arr_usd_ccfx::NUMERIC, 2) AS prior_period_customer_arr_usd_ccfx, b.arr_diff AS current_period_customer_arr_usd_ccfx, b.arr_diff AS customer_arr_change_ccfx, round(a.prior_arr_lcu::NUMERIC, 2) AS prior_period_customer_arr_lcu, b.arr_lcu_diff AS current_period_customer_arr_lcu, b.arr_lcu_diff, 'Up Sell' AS customer_bridge, a.subsidiary_entity_name, a.mcid, a.baseline_currency, NULL, NULL
FROM arr_bridge_tmp a
JOIN arr_churned_products_tmp b ON
a.current_master_customer_id = b.mcid
AND a.baseline_currency = b.baseline_currency
AND a.current_period = b.snapshot_date_at_new
WHERE b.arr_at_new > b.arr_at_churn;

RAISE NOTICE 'Running subsidiary entity name insert on sst customer bridge...';

DROP TABLE IF EXISTS sub_entity_tmp;

CREATE temp TABLE sub_entity_tmp AS --update subsidiary_entity_name
WITH mcid_list AS (
  SELECT DISTINCT mcid AS master_customer_id
  FROM arr_bridge_tmp
  WHERE evaluation_period = var_period
),
total_arr AS (
  SELECT a.mcid AS mcid,
    a.snapshot_date,
    a.subsidiary_entity_name,
    sum(a.arr) AS total_arr
  FROM sandbox_pd.sst_adhoc a
    JOIN mcid_list b ON a.mcid = b.master_customer_id
    AND a.snapshot_date IN (
      SELECT prior_period
      FROM ufdm_grey.periods
      WHERE evaluation_period = var_period
      UNION
      SELECT current_period
      FROM ufdm_grey.periods
      WHERE evaluation_period = var_period
    )
  GROUP BY a.mcid,
    a.snapshot_date,
    a.subsidiary_entity_name
),
sub_entity AS (
  SELECT *,
    ROW_NUMBER () OVER (
      PARTITION BY mcid
      ORDER BY total_arr DESC
    ) AS rnk
  FROM total_arr
)
SELECT *
FROM sub_entity
WHERE rnk = 1;

RAISE NOTICE 'Running sub entity update on sst customer bridge...';

CREATE INDEX nci_sub_entity_tmp_mcid ON
sub_entity_tmp(mcid);

UPDATE sandbox_pd.sst_customer_bridge a
SET
subsidiary_entity_name = b.subsidiary_entity_name
FROM sub_entity_tmp b
WHERE a.mcid = b.mcid
AND a.evaluation_period = var_period;
--#############################################
--CPI
--#############################################
RAISE NOTICE 'Running Price increase update on sst customer bridge...';
--Price Increase updates
UPDATE sandbox_pd.sst_customer_bridge
SET
customer_bridge = 'CPI'
WHERE customer_bridge = 'Up Sell'
AND (
    (
      customer_arr_change_ccfx / prior_period_customer_arr_usd_ccfx
    ) * 100
  )::NUMERIC(10, 2) < CASE
    WHEN evaluation_period < '2023-01-01' THEN 5.5
ELSE 10.5
END
AND prior_period_customer_arr_usd_ccfx > 0
AND evaluation_period = var_period;
--###########################################
--WINBACK Downgrade
--###########################################
RAISE NOTICE 'Running WINBACK Downgrade update on sst customer bridge 1...';

DROP TABLE IF EXISTS temp_win_downgrade_upsell;

CREATE temp TABLE temp_win_downgrade_upsell AS WITH temp1 AS (
  SELECT a.mcid,
    a.customer_bridge,
    a.evaluation_period AS evaluation_period_at_upsell,
    a.current_period AS snapshot_date_at_upsell,
    a.customer_arr_change_ccfx AS Upsell_crosssell_arr,
    a.customer_arr_change_lcu AS Upsell_crosssell_arr_lcu
  FROM sandbox_pd.sst_customer_bridge a
  WHERE 1 = 1
    AND a.customer_bridge IN ('Cross-sell', 'Up Sell')
    AND a.evaluation_period = var_period
),
temp2 AS (
  SELECT a.mcid,
    a.customer_bridge,
    a.evaluation_period_at_upsell,
    a.snapshot_date_at_upsell,
    b.current_period AS snapshot_date_Downgrade,
    a.Upsell_crosssell_arr,
    a.Upsell_crosssell_arr_lcu,
    b.customer_arr_change_ccfx AS Downgrade_downsell_arr,
    b.customer_arr_change_lcu AS Downgrade_downsell_arr_lcu,
    b.evaluation_period AS Downgrade_evaluation_period,
    b.customer_bridge AS Downgrade_bridge,
    ROW_NUMBER() OVER (
      PARTITION BY a.mcid,
      a.evaluation_period_at_upsell,
      a.customer_bridge
      ORDER BY b.current_period DESC,
        a.snapshot_date_at_upsell
    ) AS rnk
  FROM sandbox_pd.sst_customer_bridge b
    JOIN temp1 a ON a.mcid = b.mcid
  WHERE 1 = 1
    AND b.customer_bridge IN ('Downgrade', 'Downsell')
    AND b.current_period < (
      SELECT current_period
      FROM ufdm_grey.periods
      WHERE evaluation_period = var_period
    )
)
SELECT *
FROM temp2;

RAISE NOTICE 'Running WINBACK Downgrade update on sst customer bridge 1.1 ...';

DROP TABLE IF EXISTS temp_windowngrade_final;

CREATE TEMPORARY TABLE temp_windowngrade_final AS WITH temp1 AS (
  SELECT *, ROW_NUMBER() OVER (
      PARTITION BY mcid, Downgrade_evaluation_period, customer_bridge
ORDER BY snapshot_date_at_upsell
    ) AS rnk2
FROM temp_win_downgrade_upsell
WHERE rnk = 1
AND snapshot_date_at_upsell::date - snapshot_date_Downgrade::date < 186
), temp2 AS (
  SELECT *
FROM temp1
WHERE rnk2 = 1
)
SELECT a.mcid,
  a.evaluation_period,
  a.customer_bridge,
  b.Upsell_crosssell_arr,
  b.Downgrade_downsell_arr,
  b.Upsell_crosssell_arr_lcu,
  b.Downgrade_downsell_arr_lcu,
  b.Downgrade_evaluation_period,
  b.Downgrade_bridge
FROM sandbox_pd.sst_customer_bridge a,
  temp2 b
WHERE 1 = 1
  AND a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period_at_upsell
  AND a.customer_bridge = b.customer_bridge
  AND a.customer_bridge IN ('Cross-sell', 'Up Sell')
  AND a.evaluation_period = var_period;
--update when total cross/upsell is less than equal to downgrade/downsell
DROP TABLE IF EXISTS temp_windowngrade_final_curated;

CREATE TEMPORARY TABLE temp_windowngrade_final_curated AS WITH cross_upsell_total AS (
  SELECT a.mcid, a.evaluation_period, sum(
      COALESCE(b.Upsell_crosssell_arr, 0) + COALESCE(c.Upsell_crosssell_arr, 0)
    ) AS cross_upsell_total, sum(COALESCE(c.Upsell_crosssell_arr, 0)) AS Upsell_arr, sum(COALESCE(b.Upsell_crosssell_arr, 0)) AS Crossell_arr
--lcu
, sum(
      COALESCE(b.Upsell_crosssell_arr_lcu, 0) + COALESCE(c.Upsell_crosssell_arr_lcu, 0)
    ) AS cross_upsell_total_lcu, sum(COALESCE(c.Upsell_crosssell_arr_lcu, 0)) AS Upsell_arr_lcu, sum(COALESCE(b.Upsell_crosssell_arr_lcu, 0)) AS Crossell_arr_lcu, sum(
      CASE
        WHEN b.mcid IS NOT NULL
        AND c.mcid IS NOT NULL THEN 1
        ELSE 0
      END
    ) AS cross_upsell_both_exists
FROM(
      SELECT DISTINCT mcid, evaluation_period
FROM temp_windowngrade_final
    ) a
LEFT JOIN temp_windowngrade_final b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
AND b.customer_bridge = 'Cross-sell'
LEFT JOIN temp_windowngrade_final c ON
a.mcid = c.mcid
AND a.evaluation_period = c.evaluation_period
AND c.customer_bridge = 'Up Sell'
GROUP BY a.mcid, a.evaluation_period
), downgrade_downsell_total AS (
  SELECT a.mcid, b.evaluation_period, a.evaluation_period AS Downgrade_evaluation_period
--, a.customer_bridge
, abs(sum(customer_arr_change_ccfx)) AS downgrade_downsell_total, sum(
      CASE
        WHEN a.customer_bridge = 'Downgrade' THEN abs(customer_arr_change_ccfx)
        ELSE 0
      END
    ) AS Downgrade_arr, sum(
      CASE
        WHEN a.customer_bridge = 'Downsell' THEN abs(customer_arr_change_ccfx)
        ELSE 0
      END
    ) AS Downsell_arr
--lcu
, abs(sum(customer_arr_change_lcu)) AS downgrade_downsell_total_lcu, sum(
      CASE
        WHEN a.customer_bridge = 'Downgrade' THEN abs(customer_arr_change_lcu)
        ELSE 0
      END
    ) AS Downgrade_arr_lcu, sum(
      CASE
        WHEN a.customer_bridge = 'Downsell' THEN abs(customer_arr_change_lcu)
        ELSE 0
      END
    ) AS Downsell_arr_lcu, CASE
      WHEN count(DISTINCT a.customer_bridge) > 1 THEN 1
ELSE 0
END AS Downgrade_Downsell_both_exists
FROM sandbox_pd.sst_customer_bridge a
JOIN (
      SELECT DISTINCT mcid, Downgrade_evaluation_period AS Downgrade_evaluation_period, evaluation_period
FROM temp_windowngrade_final
    ) b ON
a.evaluation_period = b.Downgrade_evaluation_period
AND a.mcid = b.mcid
WHERE 1 = 1
AND a.customer_bridge IN ('Downgrade', 'Downsell')
GROUP BY a.mcid, b.evaluation_period, a.evaluation_period
--, a.customer_bridge
), temp_new_arr_split AS (
  SELECT a.mcid, a.evaluation_period, b.downgrade_evaluation_period, a.upsell_arr, a.Crossell_arr, b.downgrade_arr, b.Downsell_arr, a.cross_upsell_total, b.downgrade_downsell_total, a.upsell_arr_lcu, a.Crossell_arr_lcu, b.downgrade_arr_lcu, b.Downsell_arr_lcu, a.cross_upsell_total_lcu, b.downgrade_downsell_total_lcu, CASE
--if only cross sell or upsell exists then
      WHEN a.cross_upsell_both_exists = 0
    AND b.Downgrade_Downsell_both_exists = 0
    AND a.Upsell_arr > b.downgrade_downsell_total THEN a.Upsell_arr - b.downgrade_downsell_total
    WHEN a.cross_upsell_both_exists = 0
    AND b.Downgrade_Downsell_both_exists = 0
    AND a.Upsell_arr <= b.downgrade_downsell_total THEN 0
    WHEN a.cross_upsell_both_exists = 0
    AND b.Downgrade_Downsell_both_exists = 1
    AND a.cross_upsell_total <= b.downgrade_downsell_total THEN 0
    WHEN a.cross_upsell_both_exists = 0
    AND b.Downgrade_Downsell_both_exists = 1
    AND a.cross_upsell_total > b.downgrade_downsell_total THEN CASE
        WHEN a.Upsell_arr > 0 THEN a.cross_upsell_total - b.downgrade_downsell_total
    ELSE 0
END
--if cross sell and upsell both exists
WHEN a.cross_upsell_both_exists = 1
AND b.Downgrade_Downsell_both_exists = 0
AND a.cross_upsell_total <= b.downgrade_downsell_total THEN 0
WHEN a.cross_upsell_both_exists = 1
AND b.Downgrade_Downsell_both_exists = 0
AND a.cross_upsell_total > b.downgrade_downsell_total THEN CASE
        WHEN a.Upsell_arr > 0
AND b.Downgrade_arr > 0
AND a.Upsell_arr <= b.downgrade_downsell_total THEN 0
WHEN a.Upsell_arr > 0
AND b.Downgrade_arr > 0
AND a.Upsell_arr > b.downgrade_downsell_total THEN a.Upsell_arr - b.downgrade_downsell_total
WHEN a.Upsell_arr > 0
AND b.Downsell_arr > 0
AND a.Crossell_arr >= b.downgrade_downsell_total THEN a.Upsell_arr
WHEN a.Upsell_arr > 0
AND b.Downsell_arr > 0
AND a.Crossell_arr < b.downgrade_downsell_total THEN a.Upsell_arr - (b.downgrade_downsell_total - a.Crossell_arr)
END
---new scenario where both exists
WHEN a.cross_upsell_both_exists = 1
AND b.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_total <= b.downgrade_downsell_total THEN 0
WHEN a.cross_upsell_both_exists = 1
AND b.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_total > b.downgrade_downsell_total THEN CASE
        WHEN a.Upsell_arr <= b.Downgrade_arr THEN 0
WHEN a.Upsell_arr > b.Downgrade_arr
AND a.Crossell_arr <= b.Downsell_arr THEN (a.Upsell_arr - b.Downgrade_arr) - (b.Downsell_arr - a.Crossell_arr)
WHEN a.Upsell_arr > b.Downgrade_arr
    AND a.Crossell_arr > b.Downsell_arr THEN (a.Upsell_arr - b.Downgrade_arr)
    ELSE 0
END
ELSE 0
END AS upsell_arr_new, CASE
      WHEN a.cross_upsell_both_exists = 0
AND b.Downgrade_Downsell_both_exists = 0
AND a.Crossell_arr > b.downgrade_downsell_total THEN a.Crossell_arr - b.downgrade_downsell_total
WHEN a.cross_upsell_both_exists = 0
AND b.Downgrade_Downsell_both_exists = 0
AND a.Crossell_arr <= b.downgrade_downsell_total THEN 0
WHEN a.cross_upsell_both_exists = 0
AND b.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_total <= b.downgrade_downsell_total THEN 0
WHEN a.cross_upsell_both_exists = 0
AND b.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_total > b.downgrade_downsell_total THEN CASE
        WHEN a.Crossell_arr > 0 THEN a.cross_upsell_total - b.downgrade_downsell_total
ELSE 0
END
WHEN a.cross_upsell_both_exists = 1
AND b.Downgrade_Downsell_both_exists = 0
AND a.cross_upsell_total <= b.downgrade_downsell_total THEN 0
WHEN a.cross_upsell_both_exists = 1
AND b.Downgrade_Downsell_both_exists = 0
AND a.cross_upsell_total > b.downgrade_downsell_total THEN CASE
        WHEN a.Crossell_arr > 0
AND b.Downsell_arr > 0
AND a.Crossell_arr <= b.downgrade_downsell_total THEN 0
WHEN a.Crossell_arr > 0
AND b.Downsell_arr > 0
AND a.Crossell_arr > b.downgrade_downsell_total THEN a.Crossell_arr - b.downgrade_downsell_total
WHEN a.Crossell_arr > 0
AND b.Downgrade_arr > 0
AND a.Upsell_arr >= b.downgrade_downsell_total THEN a.Crossell_arr
WHEN a.Crossell_arr > 0
AND b.Downgrade_arr > 0
AND a.Upsell_arr < b.downgrade_downsell_total THEN a.Crossell_arr - (b.downgrade_downsell_total - a.Upsell_arr)
END
---new scenario where both exists
WHEN a.cross_upsell_both_exists = 1
AND b.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_total <= b.downgrade_downsell_total THEN 0
WHEN a.cross_upsell_both_exists = 1
AND b.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_total > b.downgrade_downsell_total THEN CASE
        WHEN a.Crossell_arr <= b.Downsell_arr THEN 0
WHEN a.Crossell_arr > b.Downsell_arr
AND a.Upsell_arr <= b.Downgrade_arr THEN (a.Crossell_arr - b.Downsell_arr) - (b.Downgrade_arr - a.Upsell_arr)
WHEN a.Crossell_arr > b.Downsell_arr
AND a.Upsell_arr > b.Downgrade_arr THEN (a.Crossell_arr - b.Downsell_arr)
ELSE 0
END
ELSE 0
END AS crosssell_arr_new
--#######################  lcu  #######----------------------------
, CASE
--if only cross sell or upsell exists then
      WHEN a.cross_upsell_both_exists = 0
AND b.Downgrade_Downsell_both_exists = 0
AND a.Upsell_arr_lcu > b.downgrade_downsell_total_lcu THEN a.Upsell_arr_lcu - b.downgrade_downsell_total_lcu
WHEN a.cross_upsell_both_exists = 0
AND b.Downgrade_Downsell_both_exists = 0
AND a.Upsell_arr_lcu <= b.downgrade_downsell_total_lcu THEN 0
WHEN a.cross_upsell_both_exists = 0
AND b.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_total_lcu <= b.downgrade_downsell_total_lcu THEN 0
WHEN a.cross_upsell_both_exists = 0
AND b.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_total_lcu > b.downgrade_downsell_total_lcu THEN CASE
        WHEN a.Upsell_arr_lcu > 0 THEN a.cross_upsell_total_lcu - b.downgrade_downsell_total_lcu
ELSE 0
END
WHEN a.cross_upsell_both_exists = 1
AND b.Downgrade_Downsell_both_exists = 0
AND a.cross_upsell_total_lcu <= b.downgrade_downsell_total_lcu THEN 0
WHEN a.cross_upsell_both_exists = 1
AND b.Downgrade_Downsell_both_exists = 0
AND a.cross_upsell_total_lcu > b.downgrade_downsell_total_lcu THEN CASE
        WHEN a.Upsell_arr_lcu > 0
AND b.Downgrade_arr_lcu > 0
AND a.Upsell_arr_lcu <= b.downgrade_downsell_total_lcu THEN 0
WHEN a.Upsell_arr_lcu > 0
AND b.Downgrade_arr_lcu > 0
AND a.Upsell_arr_lcu > b.downgrade_downsell_total_lcu THEN a.Upsell_arr_lcu - b.downgrade_downsell_total_lcu
WHEN a.Upsell_arr_lcu > 0
AND b.Downsell_arr_lcu > 0
AND a.Crossell_arr_lcu >= b.downgrade_downsell_total_lcu THEN a.Upsell_arr_lcu
WHEN a.Upsell_arr_lcu > 0
AND b.Downsell_arr_lcu > 0
AND a.Crossell_arr_lcu < b.downgrade_downsell_total_lcu THEN a.Upsell_arr_lcu - (
          b.downgrade_downsell_total_lcu - a.Crossell_arr_lcu
        )
END
---new scenario where both exists
WHEN a.cross_upsell_both_exists = 1
AND b.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_total_lcu <= b.downgrade_downsell_total_lcu THEN 0
WHEN a.cross_upsell_both_exists = 1
AND b.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_total_lcu > b.downgrade_downsell_total_lcu THEN CASE
        WHEN a.Upsell_arr_lcu <= b.Downgrade_arr_lcu THEN 0
WHEN a.Upsell_arr_lcu > b.Downgrade_arr_lcu
AND a.Crossell_arr_lcu <= b.Downsell_arr_lcu THEN (a.Upsell_arr_lcu - b.Downgrade_arr_lcu) - (b.Downsell_arr_lcu - a.Crossell_arr_lcu)
WHEN a.Upsell_arr_lcu > b.Downgrade_arr_lcu
AND a.Crossell_arr_lcu > b.Downsell_arr_lcu THEN (a.Upsell_arr_lcu - b.Downgrade_arr_lcu)
ELSE 0
END
ELSE 0
END AS upsell_arr_new_lcu, CASE
      WHEN a.cross_upsell_both_exists = 0
AND b.Downgrade_Downsell_both_exists = 0
AND a.Crossell_arr_lcu > b.downgrade_downsell_total_lcu THEN a.Crossell_arr_lcu - b.downgrade_downsell_total_lcu
WHEN a.cross_upsell_both_exists = 0
AND b.Downgrade_Downsell_both_exists = 0
AND a.Crossell_arr_lcu <= b.downgrade_downsell_total_lcu THEN 0
WHEN a.cross_upsell_both_exists = 0
AND b.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_total_lcu <= b.downgrade_downsell_total_lcu THEN 0
WHEN a.cross_upsell_both_exists = 0
AND b.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_total_lcu > b.downgrade_downsell_total_lcu THEN CASE
        WHEN a.Crossell_arr_lcu > 0 THEN a.cross_upsell_total_lcu - b.downgrade_downsell_total_lcu
ELSE 0
END
WHEN a.cross_upsell_both_exists = 1
AND b.Downgrade_Downsell_both_exists = 0
AND a.cross_upsell_total_lcu <= b.downgrade_downsell_total_lcu THEN 0
WHEN a.cross_upsell_both_exists = 1
AND b.Downgrade_Downsell_both_exists = 0
AND a.cross_upsell_total_lcu > b.downgrade_downsell_total_lcu THEN CASE
        WHEN a.Crossell_arr_lcu > 0
AND b.Downsell_arr_lcu > 0
AND a.Crossell_arr_lcu <= b.downgrade_downsell_total_lcu THEN 0
WHEN a.Crossell_arr_lcu > 0
AND b.Downsell_arr_lcu > 0
AND a.Crossell_arr_lcu > b.downgrade_downsell_total_lcu THEN a.Crossell_arr_lcu - b.downgrade_downsell_total_lcu
WHEN a.Crossell_arr_lcu > 0
AND b.Downgrade_arr_lcu > 0
AND a.Upsell_arr_lcu >= b.downgrade_downsell_total_lcu THEN a.Crossell_arr_lcu
WHEN a.Crossell_arr_lcu > 0
AND b.Downgrade_arr_lcu > 0
AND a.Upsell_arr_lcu < b.downgrade_downsell_total_lcu THEN a.Crossell_arr_lcu - (
          b.downgrade_downsell_total_lcu - a.Upsell_arr_lcu
        )
END
---new scenario where both exists
WHEN a.cross_upsell_both_exists = 1
AND b.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_total_lcu <= b.downgrade_downsell_total_lcu THEN 0
WHEN a.cross_upsell_both_exists = 1
AND b.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_total_lcu > b.downgrade_downsell_total_lcu THEN CASE
        WHEN a.Crossell_arr_lcu <= b.Downsell_arr_lcu THEN 0
WHEN a.Crossell_arr_lcu > b.Downsell_arr_lcu
AND a.Upsell_arr_lcu <= b.Downgrade_arr_lcu THEN (a.Crossell_arr_lcu - b.Downsell_arr_lcu) - (b.Downgrade_arr_lcu - a.Upsell_arr_lcu)
WHEN a.Crossell_arr_lcu > b.Downsell_arr_lcu
AND a.Upsell_arr_lcu > b.Downgrade_arr_lcu THEN (a.Crossell_arr_lcu - b.Downsell_arr_lcu)
ELSE 0
END
ELSE 0
END AS crosssell_arr_new_lcu, cross_upsell_both_exists, Downgrade_Downsell_both_exists
FROM cross_upsell_total a
JOIN downgrade_downsell_total b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
)
SELECT *, CASE
    WHEN a.Downgrade_Downsell_both_exists = 0
AND downgrade_arr > 0 THEN CASE
      WHEN a.cross_upsell_total < a.downgrade_downsell_total THEN a.cross_upsell_total
ELSE a.downgrade_downsell_total
END
WHEN a.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_both_exists = 0 THEN CASE
      WHEN a.Crossell_arr > 0
AND a.Crossell_arr <= a.Downsell_arr THEN 0
WHEN a.Crossell_arr > 0
AND a.Crossell_arr > a.Downsell_arr THEN CASE
        WHEN a.Crossell_arr < a.downgrade_downsell_total THEN a.Crossell_arr - a.Downsell_arr
ELSE a.Downgrade_arr
END
WHEN a.Upsell_arr > 0
AND a.Upsell_arr <= a.Downgrade_arr THEN a.Upsell_arr
WHEN a.Upsell_arr > 0
AND a.Upsell_arr > a.Downgrade_arr THEN a.Downgrade_arr
END
WHEN a.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_both_exists = 1 THEN CASE
      WHEN a.cross_upsell_total > a.downgrade_downsell_total THEN a.Downgrade_arr
ELSE CASE
        WHEN a.Upsell_arr > a.Downgrade_arr THEN a.Downgrade_arr
ELSE a.Upsell_arr
END
END
ELSE 0
END AS winback_downgrade_arr_new, CASE
    WHEN a.Downgrade_Downsell_both_exists = 0
AND Downsell_arr > 0 THEN CASE
      WHEN a.cross_upsell_total < a.downgrade_downsell_total THEN a.cross_upsell_total
ELSE a.downgrade_downsell_total
END
WHEN a.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_both_exists = 0 THEN CASE
      WHEN a.Upsell_arr > 0
AND a.Upsell_arr <= a.Downgrade_arr THEN 0
WHEN a.Upsell_arr > 0
AND a.Upsell_arr > a.Downgrade_arr THEN CASE
        WHEN a.Upsell_arr < a.downgrade_downsell_total THEN a.Upsell_arr - a.Downgrade_arr
ELSE a.Downsell_arr
END
WHEN a.Crossell_arr > 0
AND a.Crossell_arr <= a.Downsell_arr THEN a.Crossell_arr
WHEN a.Crossell_arr > 0
AND a.Crossell_arr > a.Downsell_arr THEN a.Downsell_arr
END
WHEN a.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_both_exists = 1 THEN CASE
      WHEN a.cross_upsell_total > a.downgrade_downsell_total THEN a.Downsell_arr
ELSE CASE
        WHEN a.Crossell_arr > a.Downsell_arr THEN a.Downsell_arr
ELSE a.Crossell_arr
END
END
ELSE 0
END AS winback_downsell_arr_new
--#######################lcu #######################--
, CASE
    WHEN a.Downgrade_Downsell_both_exists = 0
AND downgrade_arr > 0 THEN CASE
      WHEN a.cross_upsell_total_lcu < a.downgrade_downsell_total_lcu THEN a.cross_upsell_total_lcu
ELSE a.downgrade_downsell_total_lcu
END
WHEN a.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_both_exists = 0 THEN CASE
      WHEN a.Crossell_arr_lcu > 0
AND a.Crossell_arr_lcu <= a.Downsell_arr_lcu THEN 0
WHEN a.Crossell_arr_lcu > 0
AND a.Crossell_arr_lcu > a.Downsell_arr_lcu THEN CASE
        WHEN a.Crossell_arr_lcu < a.downgrade_downsell_total_lcu THEN a.Crossell_arr_lcu - a.Downsell_arr_lcu
ELSE a.Downgrade_arr_lcu
END
WHEN a.Upsell_arr_lcu > 0
AND a.Upsell_arr_lcu <= a.Downgrade_arr_lcu THEN a.Upsell_arr_lcu
WHEN a.Upsell_arr_lcu > 0
AND a.Upsell_arr_lcu > a.Downgrade_arr_lcu THEN a.Downgrade_arr_lcu
END
WHEN a.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_both_exists = 1 THEN CASE
      WHEN a.cross_upsell_total_lcu > a.downgrade_downsell_total_lcu THEN a.Downgrade_arr_lcu
ELSE CASE
        WHEN a.Upsell_arr_lcu > a.Downgrade_arr_lcu THEN a.Downgrade_arr_lcu
ELSE a.Upsell_arr_lcu
END
END
ELSE 0
END AS winback_downgrade_arr_new_lcu, CASE
    WHEN a.Downgrade_Downsell_both_exists = 0
AND Downsell_arr > 0 THEN CASE
      WHEN a.cross_upsell_total_lcu < a.downgrade_downsell_total_lcu THEN a.cross_upsell_total_lcu
ELSE a.downgrade_downsell_total_lcu
END
WHEN a.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_both_exists = 0 THEN CASE
      WHEN a.Upsell_arr_lcu > 0
AND a.Upsell_arr_lcu <= a.Downgrade_arr_lcu THEN 0
WHEN a.Upsell_arr_lcu > 0
AND a.Upsell_arr_lcu > a.Downgrade_arr_lcu THEN CASE
        WHEN a.Upsell_arr_lcu < a.downgrade_downsell_total_lcu THEN a.Upsell_arr_lcu - a.Downgrade_arr_lcu
ELSE a.Downsell_arr_lcu
END
WHEN a.Crossell_arr_lcu > 0
AND a.Crossell_arr_lcu <= a.Downsell_arr_lcu THEN a.Crossell_arr_lcu
WHEN a.Crossell_arr_lcu > 0
AND a.Crossell_arr_lcu > a.Downsell_arr_lcu THEN a.Downsell_arr_lcu
END
WHEN a.Downgrade_Downsell_both_exists = 1
AND a.cross_upsell_both_exists = 1 THEN CASE
      WHEN a.cross_upsell_total_lcu > a.downgrade_downsell_total_lcu THEN a.Downsell_arr_lcu
ELSE CASE
        WHEN a.Crossell_arr_lcu > a.Downsell_arr_lcu THEN a.Downsell_arr_lcu
ELSE a.Crossell_arr_lcu
END
END
ELSE 0
END AS winback_downsell_arr_new_lcu, 1 AS split_record
FROM temp_new_arr_split a
ORDER BY cross_upsell_both_exists;

RAISE NOTICE 'Running WINBACK Downgrade update on sst customer bridge 2...';

DROP TABLE IF EXISTS temp_windowngrade_split;

CREATE temp TABLE temp_windowngrade_split AS
SELECT a.evaluation_period, a.prior_period, a.current_period, a.current_master_customer_id, a.prior_master_customer_id, a.mcid, a.name, a.baseline_currency, a.subsidiary_entity_name, a.prior_period_customer_arr_usd_ccfx, a.prior_period_customer_arr_usd_ccfx + b.crosssell_arr_new AS current_period_customer_arr_usd_ccfx, b.crosssell_arr_new AS customer_arr_change_ccfx, a.prior_period_customer_arr_lcu, a.prior_period_customer_arr_lcu + b.crosssell_arr_new_lcu AS current_period_customer_lcu, b.crosssell_arr_new_lcu AS customer_arr_change_lcu, a.customer_bridge, a.winback_period_days, a.wip_flag
FROM sandbox_pd.sst_customer_bridge a
JOIN temp_windowngrade_final_curated b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
WHERE b.Split_record = 1
AND a.evaluation_period = var_period
AND a.customer_bridge IN ('Cross-sell')
AND b.crosssell_arr_new > 0
UNION ALL
SELECT a.evaluation_period, a.prior_period, a.current_period, a.current_master_customer_id, a.prior_master_customer_id, a.mcid, a.name, a.baseline_currency, a.subsidiary_entity_name, a.prior_period_customer_arr_usd_ccfx, a.prior_period_customer_arr_usd_ccfx + b.upsell_arr_new AS current_period_customer_arr_usd_ccfx, b.upsell_arr_new AS customer_arr_change_ccfx, a.prior_period_customer_arr_lcu, a.prior_period_customer_arr_lcu + b.upsell_arr_new_lcu AS current_period_customer_lcu, b.upsell_arr_new_lcu AS customer_arr_change_lcu, a.customer_bridge, a.winback_period_days, a.wip_flag
FROM sandbox_pd.sst_customer_bridge a
JOIN temp_windowngrade_final_curated b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
WHERE b.Split_record = 1
AND a.evaluation_period = var_period
AND a.customer_bridge IN ('Up Sell')
AND b.upsell_arr_new > 0
UNION ALL
SELECT DISTINCT a.evaluation_period, a.prior_period, a.current_period, a.current_master_customer_id, a.prior_master_customer_id, a.mcid, a.name, a.baseline_currency, a.subsidiary_entity_name, 0 AS prior_period_customer_arr_usd_ccfx, b.winback_downgrade_arr_new AS current_period_customer_arr_usd_ccfx, b.winback_downgrade_arr_new AS customer_arr_change_ccfx, 0 AS prior_period_customer_arr_lcu, b.winback_downgrade_arr_new_lcu AS current_period_customer_lcu, b.winback_downgrade_arr_new_lcu AS customer_arr_change_lcu, 'Win back Downgrade' AS customer_bridge, NULL AS winback_period_days, NULL AS wip_flag
--select b.*
FROM sandbox_pd.sst_customer_bridge a
JOIN temp_windowngrade_final_curated b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
WHERE b.Split_record = 1
AND a.evaluation_period = var_period
--and a.customer_bridge in ('Up Sell')
AND b.winback_downgrade_arr_new > 0
AND a.customer_bridge <> 'Flat'
UNION ALL
SELECT DISTINCT a.evaluation_period, a.prior_period, a.current_period, a.current_master_customer_id, a.prior_master_customer_id, a.mcid, a.name, a.baseline_currency, a.subsidiary_entity_name, 0 AS prior_period_customer_arr_usd_ccfx, b.winback_downsell_arr_new AS current_period_customer_arr_usd_ccfx, b.winback_downsell_arr_new AS customer_arr_change_ccfx, 0 AS prior_period_customer_arr_lcu, b.winback_downsell_arr_new_lcu AS current_period_customer_lcu, b.winback_downsell_arr_new_lcu AS customer_arr_change_lcu, 'Win back Downsell' AS customer_bridge, NULL AS winback_period_days, NULL AS wip_flag
--select b.*
FROM sandbox_pd.sst_customer_bridge a
JOIN temp_windowngrade_final_curated b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
--left join sandbox_pd.sst_customer_bridge c on c.mcid = a.mcid and c.evaluation_period = a.evaluation_period and c.customer_bridge = 'Up Sell'
WHERE b.Split_record = 1
AND a.evaluation_period = var_period
--and a.customer_bridge in ('Cross-sell')
--and c.mcid is null
AND b.winback_downsell_arr_new > 0
AND a.customer_bridge <> 'Flat'
ORDER BY mcid;

RAISE NOTICE 'Running WINBACK Downgrade update on sst customer bridge 4...';

DELETE
FROM sandbox_pd.sst_customer_bridge a
    USING temp_windowngrade_final_curated b
WHERE 1 = 1
AND a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
AND b.Split_record = 1
AND a.evaluation_period = var_period
-- and a.customer_bridge = b.customer_bridge
AND a.customer_bridge IN ('Cross-sell', 'Up Sell');

INSERT INTO sandbox_pd.sst_customer_bridge (
    evaluation_period, prior_period, current_period, current_master_customer_id, prior_master_customer_id, mcid, name, baseline_currency, subsidiary_entity_name, prior_period_customer_arr_usd_ccfx, current_period_customer_arr_usd_ccfx, customer_arr_change_ccfx, prior_period_customer_arr_lcu, current_period_customer_lcu, customer_arr_change_lcu, customer_bridge, winback_period_days, wip_flag
  )
SELECT evaluation_period, prior_period, current_period, current_master_customer_id, prior_master_customer_id, mcid, name, baseline_currency, subsidiary_entity_name, prior_period_customer_arr_usd_ccfx, current_period_customer_arr_usd_ccfx, customer_arr_change_ccfx, prior_period_customer_arr_lcu, current_period_customer_lcu, customer_arr_change_lcu, customer_bridge, winback_period_days, wip_flag
FROM temp_windowngrade_split;
--###########################################
--CPI Reversal
--###########################################
RAISE NOTICE 'Running CPI Reversal update on sst customer bridge...';

DROP TABLE IF EXISTS temp_CPI_Reversal;

CREATE TABLE temp_CPI_Reversal AS WITH temp1 AS (
  SELECT a.mcid, a.customer_bridge, a.evaluation_period AS evaluation_period_at_Downgrade_Churn, p.current_period AS snapshot_date_at_Downgrade_Churn, customer_arr_change_ccfx AS current_arr
FROM sandbox_pd.sst_customer_bridge a
JOIN ufdm_grey.periods p ON
a.evaluation_period = p.evaluation_period
WHERE 1 = 1
AND a.evaluation_period = var_period
AND a.customer_bridge IN ('Downgrade', 'Churn', 'Downsell')
--and a.mcid = '1ce5a898-1eaa-db11-8952-0018717a8c82'
), temp2 AS (
  SELECT a.mcid, a.customer_bridge, a.evaluation_period_at_Downgrade_Churn, a.snapshot_date_at_Downgrade_Churn, b.current_period AS snapshot_date_CPI, a.current_arr, b.customer_arr_change_ccfx AS CPI_arr, b.customer_bridge AS CPI_bridge, b.evaluation_period AS CPI_evaluation_period, ROW_NUMBER() OVER (
      PARTITION BY a.mcid, a.evaluation_period_at_Downgrade_Churn
ORDER BY b.current_period DESC, a.snapshot_date_at_Downgrade_Churn
    ) AS rnk
FROM sandbox_pd.sst_customer_bridge b
JOIN temp1 a ON
a.mcid = b.mcid
WHERE 1 = 1
AND b.customer_bridge = 'CPI'
AND b.current_period < (
      SELECT current_period
FROM ufdm_grey.periods
WHERE evaluation_period = var_period
    )
)
SELECT *
FROM temp2;

IF (
  (
    SELECT count(*)
FROM temp_CPI_Reversal
  ) > 0
) THEN DROP TABLE IF EXISTS temp_cpireversal_final;

CREATE TEMPORARY TABLE temp_cpireversal_final AS WITH temp1 AS (
  SELECT *, ROW_NUMBER() OVER (
      PARTITION BY mcid, evaluation_period_at_Downgrade_Churn
ORDER BY snapshot_date_CPI
    ) AS rnk2
FROM temp_CPI_Reversal
WHERE rnk = 1
AND snapshot_date_at_Downgrade_Churn::date - snapshot_date_CPI::date < 186
), temp2 AS (
  SELECT *
FROM temp1
WHERE rnk2 = 1
)
SELECT DISTINCT a.mcid, a.evaluation_period, b.current_arr,(
    b.CPI_arr - abs(COALESCE(c.customer_arr_change_ccfx, 0))
  ) AS CPI_arr, a.customer_bridge, CASE
    WHEN - b.current_arr > (
      b.CPI_arr - abs(COALESCE(c.customer_arr_change_ccfx, 0))
    ) THEN 1
ELSE 0
END AS Split_record, b.CPI_evaluation_period, b.CPI_bridge, b.snapshot_date_CPI, snapshot_date_at_Downgrade_Churn, abs(COALESCE(c.customer_arr_change_ccfx, 0)) AS cpi_reversal_arr, abs(COALESCE(c.customer_arr_change_lcu, 0)) AS cpi_reversal_lcu, COALESCE(c.current_period_customer_arr_usd_ccfx, 0) AS prior_period_customer_arr_usd_ccfx_CPIR, COALESCE(c.prior_period_customer_arr_usd_ccfx, 0) AS current_period_customer_arr_usd_ccfx_CPIR, COALESCE(c.current_period_customer_lcu, 0) AS prior_period_customer_arr_lcu_CPIR, COALESCE(c.prior_period_customer_arr_lcu, 0) AS current_period_customer_lcu_CPIR
FROM sandbox_pd.sst_customer_bridge a
JOIN temp2 b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period_at_Downgrade_Churn
LEFT JOIN (
    SELECT suba.*, subb.current_period AS snapshot_Date
FROM sandbox_pd.sst_customer_bridge suba
JOIN ufdm_grey.periods subb ON
suba.evaluation_period = subb.evaluation_period
  ) c ON
a.mcid = c.mcid
AND c.customer_bridge = 'CPI Reversal'
AND (c.snapshot_Date) BETWEEN b.snapshot_date_CPI AND b.snapshot_date_at_Downgrade_Churn
WHERE 1 = 1
AND a.customer_bridge IN ('Downgrade', 'Churn', 'Downsell')
AND a.evaluation_period = var_period
AND (
    c.mcid IS NULL
OR (
      c.mcid IS NOT NULL
    AND abs(COALESCE(c.customer_arr_change_ccfx, 0)) < abs(b.CPI_arr)
    )
  );

RAISE NOTICE 'Running cpi Reversal update on sst customer bridge 1...';
--update when total cross/upsell is less than equal to downgrade/downsell
DROP TABLE IF EXISTS temp_cpi_reversal_final_curated;

CREATE TEMPORARY TABLE temp_cpi_reversal_final_curated AS WITH cpi_total AS (
  SELECT a.mcid, b.evaluation_period, a.evaluation_period AS CPI_evaluation_period, abs(sum(customer_arr_change_ccfx)) AS CPI_total, abs(sum(customer_arr_change_ccfx)) AS CPI_arr, abs(sum(customer_arr_change_lcu)) AS CPI_arr_lcu, abs(sum(customer_arr_change_lcu)) AS CPI_total_lcu
FROM sandbox_pd.sst_customer_bridge a
JOIN (
      SELECT DISTINCT mcid, cpi_evaluation_period, evaluation_period
FROM temp_cpireversal_final
    ) b ON
a.evaluation_period = b.cpi_evaluation_period
AND a.mcid = b.mcid
WHERE 1 = 1
AND a.customer_bridge IN ('CPI')
GROUP BY a.mcid, b.evaluation_period, a.evaluation_period
), downgrade_downsell_churn_total AS (
  SELECT a.mcid, b.evaluation_period, a.evaluation_period AS evaluation_period_downgrade_downsell_churn, abs(sum(customer_arr_change_ccfx)) AS downgrade_downsell_churn_total, sum(
      CASE
        WHEN a.customer_bridge = 'Downgrade' THEN abs(customer_arr_change_ccfx)
        ELSE 0
      END
    ) AS Downgrade_arr, sum(
      CASE
        WHEN a.customer_bridge = 'Downsell' THEN abs(customer_arr_change_ccfx)
        ELSE 0
      END
    ) AS Downsell_arr, sum(
      CASE
        WHEN a.customer_bridge = 'Churn' THEN abs(customer_arr_change_ccfx)
        ELSE 0
      END
    ) AS Churn_arr
--lcu
, abs(sum(customer_arr_change_lcu)) AS downgrade_downsell_churn_total_lcu, sum(
      CASE
        WHEN a.customer_bridge = 'Downgrade' THEN abs(customer_arr_change_lcu)
        ELSE 0
      END
    ) AS Downgrade_arr_lcu, sum(
      CASE
        WHEN a.customer_bridge = 'Downsell' THEN abs(customer_arr_change_lcu)
        ELSE 0
      END
    ) AS Downsell_arr_lcu, sum(
      CASE
        WHEN a.customer_bridge = 'Churn' THEN abs(customer_arr_change_lcu)
        ELSE 0
      END
    ) AS Churn_arr_lcu, CASE
      WHEN count(DISTINCT a.customer_bridge) > 1 THEN 1
ELSE 0
END AS Downgrade_Downsell_churn_both_exists
FROM sandbox_pd.sst_customer_bridge a
JOIN (
      SELECT DISTINCT mcid, evaluation_period
FROM temp_cpireversal_final
    ) b ON
a.evaluation_period = b.evaluation_period
AND a.mcid = b.mcid
WHERE 1 = 1
AND a.customer_bridge IN ('Downgrade', 'Downsell', 'Churn')
GROUP BY a.mcid, b.evaluation_period, a.evaluation_period
), temp_new_arr_split AS (
  SELECT a.mcid, a.evaluation_period, a.evaluation_period_downgrade_downsell_churn, b.CPI_arr, b.CPI_arr_lcu, a.Churn_arr, a.Churn_arr_lcu, a.Downgrade_arr, a.Downgrade_arr_lcu, a.Downsell_arr, a.Downsell_arr_lcu, b.CPI_total, b.CPI_total_lcu, a.downgrade_downsell_churn_total, a.downgrade_downsell_churn_total_lcu
--, 0 as Downgrade_arr_new,0 as Downgrade_arr_new_lcu,0 as Downsell_arr_new,0 as Downsell_arr_new_lcu,0 as Churn_arr_new,0 as Churn_arr_new_lcu
, CASE
      WHEN a.Downgrade_Downsell_churn_both_exists = 0 THEN CASE
        WHEN a.Downgrade_arr > 0
    AND a.downgrade_downsell_churn_total > b.CPI_total THEN a.downgrade_downsell_churn_total - b.CPI_total
    ELSE 0
END
WHEN a.Downgrade_Downsell_churn_both_exists = 1 THEN CASE
        WHEN a.downgrade_downsell_churn_total <= b.CPI_total THEN 0
ELSE CASE
          WHEN a.Downgrade_arr > 0
AND a.Downgrade_arr <= b.CPI_total THEN 0
ELSE a.Downgrade_arr - b.CPI_total
END
END
END AS Downgrade_arr_new, CASE
      WHEN a.Downgrade_Downsell_churn_both_exists = 0 THEN CASE
        WHEN a.Downsell_arr > 0
AND a.downgrade_downsell_churn_total > b.CPI_total THEN a.downgrade_downsell_churn_total - b.CPI_total
ELSE 0
END
WHEN a.Downgrade_Downsell_churn_both_exists = 1 THEN CASE
        WHEN a.downgrade_downsell_churn_total <= b.CPI_total THEN 0
ELSE CASE
          WHEN a.Downsell_arr > 0
AND a.Downgrade_arr >= b.CPI_total THEN a.Downsell_arr
ELSE a.Downsell_arr - (b.CPI_total - a.Downgrade_arr)
END
END
END AS Downsell_arr_new, CASE
      WHEN a.Churn_arr > 0
AND a.downgrade_downsell_churn_total > b.CPI_total THEN a.downgrade_downsell_churn_total - b.CPI_total
ELSE 0
END AS Churn_arr_new
--#######################  lcu  #######----------------------------
, CASE
      WHEN a.Downgrade_Downsell_churn_both_exists = 0 THEN CASE
        WHEN a.Downgrade_arr_lcu > 0
AND a.downgrade_downsell_churn_total_lcu > b.CPI_total_lcu THEN a.downgrade_downsell_churn_total_lcu - b.CPI_total_lcu
ELSE 0
END
WHEN a.Downgrade_Downsell_churn_both_exists = 1 THEN CASE
        WHEN a.downgrade_downsell_churn_total_lcu <= b.CPI_total_lcu THEN 0
ELSE CASE
          WHEN a.Downgrade_arr_lcu > 0
AND a.Downgrade_arr_lcu <= b.CPI_total_lcu THEN 0
ELSE a.Downgrade_arr_lcu - b.CPI_total_lcu
END
END
END AS Downgrade_arr_new_lcu, CASE
      WHEN a.Downgrade_Downsell_churn_both_exists = 0 THEN CASE
        WHEN a.Downsell_arr_lcu > 0
AND a.downgrade_downsell_churn_total_lcu > b.CPI_total_lcu THEN a.downgrade_downsell_churn_total_lcu - b.CPI_total_lcu
ELSE 0
END
WHEN a.Downgrade_Downsell_churn_both_exists = 1 THEN CASE
        WHEN a.downgrade_downsell_churn_total_lcu <= b.CPI_total_lcu THEN 0
ELSE CASE
          WHEN a.Downsell_arr_lcu > 0
AND a.Downgrade_arr_lcu >= b.CPI_total_lcu THEN a.Downsell_arr_lcu
ELSE a.Downsell_arr_lcu - (b.CPI_total_lcu - a.Downgrade_arr_lcu)
END
END
END AS Downsell_arr_new_lcu, CASE
      WHEN a.Churn_arr_lcu > 0
AND a.downgrade_downsell_churn_total_lcu > b.CPI_total_lcu THEN a.downgrade_downsell_churn_total_lcu - b.CPI_total_lcu
ELSE 0
END AS Churn_arr_new_lcu, Downgrade_Downsell_churn_both_exists
FROM downgrade_downsell_churn_total a
JOIN CPI_total b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
)
SELECT *, CASE
    WHEN a.downgrade_downsell_churn_total <= a.CPI_total THEN a.downgrade_downsell_churn_total
ELSE a.CPI_total
END AS cpi_reversal_arr_new
--#######################lcu #######################--
, CASE
    WHEN a.downgrade_downsell_churn_total_lcu <= a.CPI_total_lcu THEN a.downgrade_downsell_churn_total_lcu
ELSE a.CPI_total_lcu
END AS cpi_reversal_arr_new_lcu, 1 AS split_record
FROM temp_new_arr_split a
--order by cross_upsell_both_exists
;

DROP TABLE IF EXISTS temp_cpireversal_split;

CREATE temp TABLE temp_cpireversal_split AS
SELECT a.evaluation_period, a.prior_period, a.current_period, a.current_master_customer_id, a.prior_master_customer_id, a.mcid, a.name, a.baseline_currency, a.subsidiary_entity_name, 0 AS prior_period_customer_arr_usd_ccfx, 0 AS current_period_customer_arr_usd_ccfx,- Downgrade_arr_new AS customer_arr_change_ccfx
---lcu
, 0 AS prior_period_customer_arr_lcu, 0 AS current_period_customer_lcu,- Downgrade_arr_new_lcu AS customer_arr_change_lcu, a.customer_bridge
FROM sandbox_pd.sst_customer_bridge a
JOIN temp_cpi_reversal_final_curated b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
WHERE b.Split_record = 1
AND a.evaluation_period = var_period
AND a.customer_bridge IN ('Downgrade')
AND b.Downgrade_arr_new > 0
UNION ALL
SELECT a.evaluation_period, a.prior_period, a.current_period, a.current_master_customer_id, a.prior_master_customer_id, a.mcid, a.name, a.baseline_currency, a.subsidiary_entity_name, 0 AS prior_period_customer_arr_usd_ccfx, 0 AS current_period_customer_arr_usd_ccfx,- b.Downsell_arr_new AS customer_arr_change_ccfx
---lcu
, 0 AS prior_period_customer_arr_lcu, 0 AS current_period_customer_lcu,- b.Downsell_arr_new_lcu AS customer_arr_change_lcu, a.customer_bridge
FROM sandbox_pd.sst_customer_bridge a
JOIN temp_cpi_reversal_final_curated b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
WHERE b.Split_record = 1
AND a.evaluation_period = var_period
AND a.customer_bridge IN ('Downsell')
AND b.Downsell_arr_new > 0
UNION ALL
SELECT a.evaluation_period, a.prior_period, a.current_period, a.current_master_customer_id, a.prior_master_customer_id, a.mcid, a.name, a.baseline_currency, a.subsidiary_entity_name, 0 AS prior_period_customer_arr_usd_ccfx, 0 AS current_period_customer_arr_usd_ccfx,- b.Churn_arr_new AS customer_arr_change_ccfx
---lcu
, 0 AS prior_period_customer_arr_lcu, 0 AS current_period_customer_lcu,- b.Churn_arr_new_lcu AS customer_arr_change_lcu, a.customer_bridge
FROM sandbox_pd.sst_customer_bridge a
JOIN temp_cpi_reversal_final_curated b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
WHERE b.Split_record = 1
AND a.evaluation_period = var_period
AND a.customer_bridge IN ('Churn')
AND b.Churn_arr_new > 0
UNION ALL
SELECT DISTINCT a.evaluation_period, a.prior_period, a.current_period, a.current_master_customer_id, a.prior_master_customer_id, a.mcid, a.name, a.baseline_currency, a.subsidiary_entity_name, 0 AS prior_period_customer_arr_usd_ccfx, cpi_reversal_arr_new AS current_period_customer_arr_usd_ccfx,- cpi_reversal_arr_new AS customer_arr_change_ccfx
---lcu
, 0 AS prior_period_customer_arr_lcu, cpi_reversal_arr_new_lcu AS current_period_customer_lcu,- cpi_reversal_arr_new_lcu AS customer_arr_change_lcu, 'CPI Reversal' AS customer_bridge
FROM sandbox_pd.sst_customer_bridge a
JOIN temp_cpi_reversal_final_curated b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
WHERE b.Split_record = 1
AND a.evaluation_period = var_period
AND b.cpi_reversal_arr_new > 0
AND a.customer_bridge <> 'Flat'
ORDER BY mcid;

DELETE
FROM sandbox_pd.sst_customer_bridge a
    USING temp_cpi_reversal_final_curated b
WHERE 1 = 1
AND a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
AND b.Split_record = 1
AND a.evaluation_period = var_period
AND a.customer_bridge IN ('Downgrade', 'Churn', 'Downsell');

INSERT INTO sandbox_pd.sst_customer_bridge (
    evaluation_period, prior_period, current_period, current_master_customer_id, prior_master_customer_id, mcid, name, baseline_currency, subsidiary_entity_name, prior_period_customer_arr_usd_ccfx, current_period_customer_arr_usd_ccfx, customer_arr_change_ccfx, prior_period_customer_arr_lcu, current_period_customer_lcu, customer_arr_change_lcu, customer_bridge
  )
SELECT evaluation_period, prior_period, current_period, current_master_customer_id, prior_master_customer_id, mcid, name, baseline_currency, subsidiary_entity_name, prior_period_customer_arr_usd_ccfx, current_period_customer_arr_usd_ccfx, customer_arr_change_ccfx, prior_period_customer_arr_lcu, current_period_customer_lcu, customer_arr_change_lcu, customer_bridge
FROM temp_cpireversal_split;
END IF;

/*
 update sandbox_pd.sst_customer_bridge a
 set customer_bridge = 'CPI Reversal'
 from temp_cpireversal_final b
 where 1=1
 and a.mcid = b.mcid
 and a.evaluation_period = b.evaluation_period
 and a.evaluation_period = var_period
 and b.Split_record = 0
 and a.customer_bridge in ('Downgrade','Churn', 'Downsell')
 ;
 
 drop table if exists temp_cpireversal_split;
 
 create temp table temp_cpireversal_split as
 select distinct a.evaluation_period,a.prior_period,a.current_period,a.current_master_customer_id,a.prior_master_customer_id,a.mcid,a.name
 ,a.baseline_currency,a.subsidiary_entity_name
 ,case when a.customer_bridge = 'Churn' then a.prior_period_customer_arr_usd_ccfx - (c.customer_arr_change_ccfx - b.cpi_reversal_arr)
 else a.prior_period_customer_arr_usd_ccfx end as prior_period_customer_arr_usd_ccfx
 ,case when a.customer_bridge = 'Churn' then 0 else a.current_period_customer_arr_usd_ccfx + (c.customer_arr_change_ccfx - b.cpi_reversal_arr) end as current_period_customer_arr_usd_ccfx
 ,a.customer_arr_change_ccfx + (c.customer_arr_change_ccfx - b.cpi_reversal_arr) as customer_arr_change_ccfx
 ---lcu
 ,case when a.customer_bridge = 'Churn' then a.prior_period_customer_arr_lcu - (c.customer_arr_change_lcu - b.cpi_reversal_lcu)
 else a.prior_period_customer_arr_lcu end as prior_period_customer_arr_lcu
 ,case when a.customer_bridge = 'Churn' then 0 else a.current_period_customer_lcu + (c.customer_arr_change_lcu - b.cpi_reversal_lcu) end as current_period_customer_lcu
 ,a.customer_arr_change_lcu + (c.customer_arr_change_lcu - b.cpi_reversal_lcu) as customer_arr_change_lcu
 ,a.customer_bridge,a.winback_period_days,a.wip_flag
 from sandbox_pd.sst_customer_bridge a
 join temp_cpireversal_final b on a.mcid = b.mcid and a.evaluation_period = b.evaluation_period
 join sandbox_pd.sst_customer_bridge c on c.mcid = b.mcid and c.evaluation_period = b.CPI_evaluation_period and c.customer_bridge = 'CPI'
 where b.Split_record = 1
 and a.evaluation_period = var_period
 and a.customer_bridge in ('Downgrade','Churn', 'Downsell')
 union all
 select distinct a.evaluation_period,a.prior_period,a.current_period,a.current_master_customer_id,a.prior_master_customer_id,a.mcid,a.name
 ,a.baseline_currency,a.subsidiary_entity_name
 ,c.current_period_customer_arr_usd_ccfx - current_period_customer_arr_usd_ccfx_CPIR as prior_period_customer_arr_usd_ccfx
 ,c.prior_period_customer_arr_usd_ccfx - prior_period_customer_arr_usd_ccfx_CPIR as current_period_customer_arr_usd_ccfx
 ,-(c.customer_arr_change_ccfx - b.cpi_reversal_arr)
 ,c.current_period_customer_lcu - current_period_customer_lcu_CPIR as prior_period_customer_arr_lcu
 ,c.prior_period_customer_arr_lcu - prior_period_customer_arr_lcu_CPIR as current_period_customer_lcu
 ,-(c.customer_arr_change_lcu - b.cpi_reversal_lcu)
 ,'CPI Reversal' as customer_bridge,a.winback_period_days,a.wip_flag
 from sandbox_pd.sst_customer_bridge a
 join temp_cpireversal_final b on a.mcid = b.mcid and a.evaluation_period = b.evaluation_period
 join sandbox_pd.sst_customer_bridge c on c.mcid = b.mcid and c.evaluation_period = b.CPI_evaluation_period and c.customer_bridge = 'CPI'
 where b.Split_record = 1
 and a.evaluation_period = var_period
 and a.customer_bridge in ('Downgrade','Churn', 'Downsell')
 order by mcid
 ;
 
 delete from sandbox_pd.sst_customer_bridge a
 using temp_cpireversal_final b
 where 1=1
 and a.mcid = b.mcid
 and a.evaluation_period = b.evaluation_period
 and b.Split_record = 1
 and a.evaluation_period = var_period
 and a.customer_bridge in ('Downgrade','Churn', 'Downsell')
 ;
 
 insert into sandbox_pd.sst_customer_bridge
 (  evaluation_period, prior_period, current_period, current_master_customer_id, prior_master_customer_id, mcid, name, baseline_currency, subsidiary_entity_name
 , prior_period_customer_arr_usd_ccfx, current_period_customer_arr_usd_ccfx, customer_arr_change_ccfx, prior_period_customer_arr_lcu, current_period_customer_lcu
 , customer_arr_change_lcu, customer_bridge, winback_period_days, wip_flag
 )
 select  evaluation_period, prior_period, current_period, current_master_customer_id, prior_master_customer_id, mcid, name, baseline_currency, subsidiary_entity_name
 , prior_period_customer_arr_usd_ccfx, current_period_customer_arr_usd_ccfx, customer_arr_change_ccfx, prior_period_customer_arr_lcu, current_period_customer_lcu
 , customer_arr_change_lcu, customer_bridge, winback_period_days, wip_flag
 from temp_cpireversal_split
 ;
 */
--###########################################
--Upsell Reversal
--###########################################
RAISE NOTICE 'Running Upsell Reversal update on sst customer bridge...';

DROP TABLE IF EXISTS temp_cross_upsell_reversal;

CREATE temp TABLE temp_cross_upsell_reversal AS WITH temp1 AS (
  SELECT a.mcid, a.customer_bridge, a.evaluation_period AS evaluation_period_at_Downgrade_Downsell, a.current_period AS snapshot_date_at_Downgrade_Downsell, a.customer_arr_change_ccfx AS Downgrade_Downsell_arr, a.customer_arr_change_lcu AS Downgrade_Downsell_arr_lcu
FROM sandbox_pd.sst_customer_bridge a
WHERE 1 = 1
AND a.customer_bridge IN ('Downgrade', 'Downsell')
    AND a.evaluation_period = var_period
), temp2 AS (
  SELECT a.mcid, a.customer_bridge, a.evaluation_period_at_Downgrade_Downsell, a.snapshot_date_at_Downgrade_Downsell, b.current_period AS snapshot_date_cross_upsell, a.Downgrade_Downsell_arr, a.Downgrade_Downsell_arr_lcu, b.customer_arr_change_ccfx AS Upsell_crosssell_arr, b.customer_arr_change_lcu AS Upsell_crosssell_arr_lcu, b.evaluation_period AS cross_upsell_evaluation_period, b.customer_bridge AS cross_upsell_bridge, ROW_NUMBER() OVER (
      PARTITION BY a.mcid, a.evaluation_period_at_Downgrade_Downsell, a.customer_bridge
ORDER BY b.current_period DESC, a.snapshot_date_at_Downgrade_Downsell
    ) AS rnk
FROM sandbox_pd.sst_customer_bridge b
JOIN temp1 a ON
a.mcid = b.mcid
WHERE 1 = 1
AND b.customer_bridge IN ('Cross-sell', 'Up Sell')
    AND b.current_period < (
      SELECT current_period
FROM ufdm_grey.periods
WHERE evaluation_period = var_period
    )
)
SELECT *
FROM temp2;

IF (
  (
    SELECT count(*)
FROM temp_cross_upsell_reversal
  ) > 0
) THEN DROP TABLE IF EXISTS temp_cross_upsell_reversal_final;

CREATE TEMPORARY TABLE temp_cross_upsell_reversal_final AS WITH temp1 AS (
  SELECT *, ROW_NUMBER() OVER (
      PARTITION BY mcid, cross_upsell_evaluation_period, customer_bridge
ORDER BY snapshot_date_at_Downgrade_Downsell
    ) AS rnk2
FROM temp_cross_upsell_reversal
WHERE rnk = 1
AND snapshot_date_at_Downgrade_Downsell::date - snapshot_date_cross_upsell::date < 186
), temp2 AS (
  SELECT *
FROM temp1
WHERE rnk2 = 1
)
SELECT a.mcid, a.evaluation_period, b.Upsell_crosssell_arr, b.Downgrade_Downsell_arr, a.customer_bridge, b.cross_upsell_evaluation_period, b.cross_upsell_bridge, b.evaluation_period_at_Downgrade_Downsell, b.Upsell_crosssell_arr_lcu, b.Downgrade_Downsell_arr_lcu
FROM sandbox_pd.sst_customer_bridge a, temp2 b
WHERE 1 = 1
AND a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period_at_Downgrade_Downsell
AND a.customer_bridge = b.customer_bridge
AND a.customer_bridge IN ('Downgrade', 'Downsell')
AND a.evaluation_period = var_period;

RAISE NOTICE 'Running Upsell Reversal update on sst customer bridge 1...';
--update when total cross/upsell is less than equal to downgrade/downsell
DROP TABLE IF EXISTS temp_cross_upsell_reversal_final_curated;

CREATE TEMPORARY TABLE temp_cross_upsell_reversal_final_curated AS WITH cross_upsell_total AS (
  SELECT a.mcid, b.evaluation_period, a.evaluation_period AS cross_upsell_evaluation_period
--, a.customer_bridge
, abs(sum(customer_arr_change_ccfx)) AS cross_upsell_total, sum(
      CASE
        WHEN a.customer_bridge = 'Cross-sell' THEN abs(customer_arr_change_ccfx)
        ELSE 0
      END
    ) AS Crossell_arr, sum(
      CASE
        WHEN a.customer_bridge = 'Up Sell' THEN abs(customer_arr_change_ccfx)
        ELSE 0
      END
    ) AS Upsell_arr
--lcu
, abs(sum(customer_arr_change_lcu)) AS cross_upsell_total_lcu, sum(
      CASE
        WHEN a.customer_bridge = 'Cross-sell' THEN abs(customer_arr_change_lcu)
        ELSE 0
      END
    ) AS Crossell_arr_lcu, sum(
      CASE
        WHEN a.customer_bridge = 'Up Sell' THEN abs(customer_arr_change_lcu)
        ELSE 0
      END
    ) AS Upsell_arr_lcu, CASE
      WHEN count(DISTINCT a.customer_bridge) > 1 THEN 1
ELSE 0
END AS cross_upsell_both_exists
FROM sandbox_pd.sst_customer_bridge a
JOIN (
      SELECT DISTINCT mcid, cross_upsell_evaluation_period, evaluation_period
FROM temp_cross_upsell_reversal_final
    ) b ON
a.evaluation_period = b.cross_upsell_evaluation_period
AND a.mcid = b.mcid
WHERE 1 = 1
AND a.customer_bridge IN ('Cross-sell', 'Up Sell')
GROUP BY a.mcid, b.evaluation_period, a.evaluation_period
--, a.customer_bridge
), downgrade_downsell_total AS (
  SELECT a.mcid, b.evaluation_period, a.evaluation_period AS Downgrade_evaluation_period
--, a.customer_bridge
, abs(sum(customer_arr_change_ccfx)) AS downgrade_downsell_total, sum(
      CASE
        WHEN a.customer_bridge = 'Downgrade' THEN abs(customer_arr_change_ccfx)
        ELSE 0
      END
    ) AS Downgrade_arr, sum(
      CASE
        WHEN a.customer_bridge = 'Downsell' THEN abs(customer_arr_change_ccfx)
        ELSE 0
      END
    ) AS Downsell_arr
--lcu
, abs(sum(customer_arr_change_lcu)) AS downgrade_downsell_total_lcu, sum(
      CASE
        WHEN a.customer_bridge = 'Downgrade' THEN abs(customer_arr_change_lcu)
        ELSE 0
      END
    ) AS Downgrade_arr_lcu, sum(
      CASE
        WHEN a.customer_bridge = 'Downsell' THEN abs(customer_arr_change_lcu)
        ELSE 0
      END
    ) AS Downsell_arr_lcu, CASE
      WHEN count(DISTINCT a.customer_bridge) > 1 THEN 1
ELSE 0
END AS Downgrade_Downsell_both_exists
FROM sandbox_pd.sst_customer_bridge a
JOIN (
      SELECT DISTINCT mcid, evaluation_period_at_Downgrade_Downsell, evaluation_period
FROM temp_cross_upsell_reversal_final
    ) b ON
a.evaluation_period = b.evaluation_period_at_Downgrade_Downsell
AND a.mcid = b.mcid
WHERE 1 = 1
AND a.customer_bridge IN ('Downgrade', 'Downsell')
GROUP BY a.mcid, b.evaluation_period, a.evaluation_period
--, a.customer_bridge
), temp_new_arr_split AS (
  SELECT a.mcid, a.evaluation_period, a.downgrade_evaluation_period, b.upsell_arr, b.Crossell_arr, a.downgrade_arr, a.Downsell_arr, b.cross_upsell_total, a.downgrade_downsell_total, b.upsell_arr_lcu, b.Crossell_arr_lcu, a.downgrade_arr_lcu, a.Downsell_arr_lcu, b.cross_upsell_total_lcu, a.downgrade_downsell_total_lcu, CASE
--if only cross sell or upsell exists then
      WHEN b.cross_upsell_both_exists = 0
    AND a.Downgrade_Downsell_both_exists = 0
    AND a.downgrade_arr > b.cross_upsell_total THEN a.downgrade_arr - b.cross_upsell_total
    WHEN b.cross_upsell_both_exists = 0
    AND a.Downgrade_Downsell_both_exists = 0
    AND a.downgrade_arr <= b.cross_upsell_total THEN 0
    WHEN b.cross_upsell_both_exists = 1
    AND a.Downgrade_Downsell_both_exists = 0
    AND a.downgrade_downsell_total <= b.cross_upsell_total THEN 0
    WHEN b.cross_upsell_both_exists = 1
    AND a.Downgrade_Downsell_both_exists = 0
    AND a.downgrade_downsell_total > b.cross_upsell_total THEN CASE
        WHEN a.downgrade_arr > 0 THEN a.downgrade_downsell_total - b.cross_upsell_total
    ELSE 0
END
--if cross sell and upsell both exists
WHEN b.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 1
AND a.downgrade_downsell_total <= b.cross_upsell_total THEN 0
WHEN b.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 1
AND a.downgrade_downsell_total > b.cross_upsell_total THEN CASE
        WHEN a.downgrade_arr > 0
AND b.Upsell_arr > 0
AND a.downgrade_arr <= b.cross_upsell_total THEN 0
WHEN a.downgrade_arr > 0
AND b.Upsell_arr > 0
AND a.downgrade_arr > b.cross_upsell_total THEN a.downgrade_arr - b.cross_upsell_total
WHEN a.downgrade_arr > 0
AND b.Crossell_arr > 0
AND a.Downsell_arr >= b.cross_upsell_total THEN a.downgrade_arr
WHEN a.downgrade_arr > 0
AND b.Crossell_arr > 0
AND a.Downsell_arr < b.cross_upsell_total THEN a.downgrade_arr - (b.cross_upsell_total - a.Downsell_arr)
END
---new scenario where both exists
WHEN b.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 1
AND a.downgrade_downsell_total <= b.cross_upsell_total THEN 0
WHEN b.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 1
AND a.downgrade_downsell_total > b.cross_upsell_total THEN CASE
        WHEN a.Downgrade_arr <= b.Upsell_arr THEN 0
WHEN a.Downgrade_arr > b.Upsell_arr
AND a.Downsell_arr <= b.Crossell_arr THEN (a.Downgrade_arr - b.Upsell_arr) - (b.Crossell_arr - a.Downsell_arr)
WHEN a.Downgrade_arr > b.Upsell_arr
    AND a.Downsell_arr > b.Crossell_arr THEN (a.Downgrade_arr - b.Upsell_arr)
    ELSE 0
END
ELSE 0
END AS downgrade_arr_new, CASE
      WHEN b.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 0
AND a.Downsell_arr > b.cross_upsell_total THEN a.Downsell_arr - b.cross_upsell_total
WHEN b.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 0
AND a.Downsell_arr <= b.cross_upsell_total THEN 0
WHEN b.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 0
AND a.downgrade_downsell_total <= b.cross_upsell_total THEN 0
WHEN b.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 0
AND a.downgrade_downsell_total > b.cross_upsell_total THEN CASE
        WHEN a.Downsell_arr > 0 THEN a.downgrade_downsell_total - b.cross_upsell_total
ELSE 0
END
--if cross sell and upsell both exists
WHEN b.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 1
AND a.downgrade_downsell_total <= b.cross_upsell_total THEN 0
WHEN b.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 1
AND a.downgrade_downsell_total > b.cross_upsell_total THEN CASE
        WHEN a.Downsell_arr > 0
AND b.Crossell_arr > 0
AND a.Downsell_arr <= b.cross_upsell_total THEN 0
WHEN a.Downsell_arr > 0
AND b.Crossell_arr > 0
AND a.Downsell_arr > b.cross_upsell_total THEN a.Downsell_arr - b.cross_upsell_total
WHEN a.Downsell_arr > 0
AND b.Upsell_arr > 0
AND a.Downgrade_arr >= b.cross_upsell_total THEN a.Downsell_arr
WHEN a.Downsell_arr > 0
AND b.Upsell_arr > 0
AND a.Downgrade_arr < b.cross_upsell_total THEN a.Downsell_arr - (b.cross_upsell_total - a.Downgrade_arr)
END
---new scenario where both exists
WHEN b.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 1
AND a.downgrade_downsell_total <= b.cross_upsell_total THEN 0
WHEN b.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 1
AND a.downgrade_downsell_total > b.cross_upsell_total THEN CASE
        WHEN a.Downsell_arr <= b.Crossell_arr THEN 0
WHEN a.Downsell_arr > b.Crossell_arr
AND a.Downgrade_arr <= b.Upsell_arr THEN (a.Downsell_arr - b.Crossell_arr) - (b.Upsell_arr - a.Downgrade_arr)
WHEN a.Downsell_arr > b.Crossell_arr
AND a.Downgrade_arr > b.Upsell_arr THEN (a.Downsell_arr - b.Crossell_arr)
ELSE 0
END
ELSE 0
END AS downsell_arr_new
--#######################  lcu  #######----------------------------
, CASE
--if only cross sell or upsell exists then
      WHEN b.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 0
AND a.downgrade_arr_lcu > b.cross_upsell_total_lcu THEN a.downgrade_arr_lcu - b.cross_upsell_total_lcu
WHEN b.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 0
AND a.downgrade_arr_lcu <= b.cross_upsell_total_lcu THEN 0
WHEN b.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 0
AND a.downgrade_downsell_total_lcu <= b.cross_upsell_total_lcu THEN 0
WHEN b.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 0
AND a.downgrade_downsell_total_lcu > b.cross_upsell_total_lcu THEN CASE
        WHEN a.downgrade_arr_lcu > 0 THEN a.downgrade_downsell_total_lcu - b.cross_upsell_total_lcu
ELSE 0
END
--if cross sell and upsell both exists
WHEN b.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 1
AND a.downgrade_downsell_total_lcu <= b.cross_upsell_total_lcu THEN 0
WHEN b.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 1
AND a.downgrade_downsell_total_lcu > b.cross_upsell_total_lcu THEN CASE
        WHEN a.downgrade_arr_lcu > 0
AND b.Upsell_arr_lcu > 0
AND a.downgrade_arr_lcu <= b.cross_upsell_total_lcu THEN 0
WHEN a.downgrade_arr_lcu > 0
AND b.Upsell_arr_lcu > 0
AND a.downgrade_arr_lcu > b.cross_upsell_total_lcu THEN a.downgrade_arr_lcu - b.cross_upsell_total_lcu
WHEN a.downgrade_arr_lcu > 0
AND b.Crossell_arr_lcu > 0
AND a.Downsell_arr_lcu >= b.cross_upsell_total_lcu THEN a.downgrade_arr_lcu
WHEN a.downgrade_arr_lcu > 0
AND b.Crossell_arr_lcu > 0
AND a.Downsell_arr_lcu < b.cross_upsell_total_lcu THEN a.downgrade_arr_lcu - (b.cross_upsell_total_lcu - a.Downsell_arr_lcu)
END
---new scenario where both exists
WHEN b.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 1
AND a.downgrade_downsell_total_lcu <= b.cross_upsell_total_lcu THEN 0
WHEN b.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 1
AND a.downgrade_downsell_total_lcu > b.cross_upsell_total_lcu THEN CASE
        WHEN a.Downgrade_arr_lcu <= b.Upsell_arr_lcu THEN 0
WHEN a.Downgrade_arr_lcu > b.Upsell_arr_lcu
AND a.Downsell_arr_lcu <= b.Crossell_arr_lcu THEN (a.Downgrade_arr_lcu - b.Upsell_arr_lcu) - (b.Crossell_arr_lcu - a.Downsell_arr_lcu)
WHEN a.Downgrade_arr_lcu > b.Upsell_arr_lcu
AND a.Downsell_arr_lcu > b.Crossell_arr_lcu THEN (a.Downgrade_arr_lcu - b.Upsell_arr_lcu)
ELSE 0
END
ELSE 0
END AS downgrade_arr_new_lcu, CASE
      WHEN b.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 0
AND a.Downsell_arr_lcu > b.cross_upsell_total_lcu THEN a.Downsell_arr_lcu - b.cross_upsell_total_lcu
WHEN b.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 0
AND a.Downsell_arr_lcu <= b.cross_upsell_total_lcu THEN 0
WHEN b.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 0
AND a.downgrade_downsell_total_lcu <= b.cross_upsell_total_lcu THEN 0
WHEN b.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 0
AND a.downgrade_downsell_total_lcu > b.cross_upsell_total_lcu THEN CASE
        WHEN a.Downsell_arr_lcu > 0 THEN a.downgrade_downsell_total_lcu - b.cross_upsell_total_lcu
ELSE 0
END
--if cross sell and upsell both exists
WHEN b.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 1
AND a.downgrade_downsell_total_lcu <= b.cross_upsell_total_lcu THEN 0
WHEN b.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 1
AND a.downgrade_downsell_total_lcu > b.cross_upsell_total_lcu THEN CASE
        WHEN a.Downsell_arr_lcu > 0
AND b.Crossell_arr_lcu > 0
AND a.Downsell_arr_lcu <= b.cross_upsell_total_lcu THEN 0
WHEN a.Downsell_arr_lcu > 0
AND b.Crossell_arr_lcu > 0
AND a.Downsell_arr_lcu > b.cross_upsell_total_lcu THEN a.Downsell_arr_lcu - b.cross_upsell_total_lcu
WHEN a.Downsell_arr_lcu > 0
AND b.Upsell_arr_lcu > 0
AND a.Downgrade_arr_lcu >= b.cross_upsell_total_lcu THEN a.Downsell_arr_lcu
WHEN a.Downsell_arr_lcu > 0
AND b.Upsell_arr_lcu > 0
AND a.Downgrade_arr_lcu < b.cross_upsell_total_lcu THEN a.Downsell_arr_lcu - (b.cross_upsell_total_lcu - a.Downgrade_arr_lcu)
END
---new scenario where both exists
WHEN b.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 1
AND a.downgrade_downsell_total_lcu <= b.cross_upsell_total_lcu THEN 0
WHEN b.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 1
AND a.downgrade_downsell_total_lcu > b.cross_upsell_total_lcu THEN CASE
        WHEN a.Downsell_arr_lcu <= b.Crossell_arr_lcu THEN 0
WHEN a.Downsell_arr_lcu > b.Crossell_arr_lcu
AND a.Downgrade_arr_lcu <= b.Upsell_arr_lcu THEN (a.Downsell_arr_lcu - b.Crossell_arr_lcu) - (b.Upsell_arr_lcu - a.Downgrade_arr_lcu)
WHEN a.Downsell_arr_lcu > b.Crossell_arr_lcu
AND a.Downgrade_arr_lcu > b.Upsell_arr_lcu THEN (a.Downsell_arr_lcu - b.Crossell_arr_lcu)
ELSE 0
END
ELSE 0
END AS downsell_arr_new_lcu, cross_upsell_both_exists, Downgrade_Downsell_both_exists
FROM downgrade_downsell_total a
JOIN cross_upsell_total b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
)
SELECT *, CASE
    WHEN a.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 0 THEN CASE
      WHEN a.Upsell_arr > 0
AND a.downgrade_downsell_total >= a.cross_upsell_total THEN a.cross_upsell_total
WHEN a.Upsell_arr > 0
AND a.downgrade_downsell_total < a.cross_upsell_total THEN a.downgrade_downsell_total
ELSE 0
END
WHEN a.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 1 THEN CASE
      WHEN a.Upsell_arr > 0
AND a.downgrade_downsell_total < a.cross_upsell_total THEN a.downgrade_downsell_total
WHEN a.Upsell_arr > 0
AND a.downgrade_downsell_total >= a.cross_upsell_total THEN a.cross_upsell_total
ELSE 0
END
WHEN a.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 0 THEN CASE
      WHEN a.Downsell_arr > 0
AND a.Downsell_arr <= a.Crossell_arr THEN 0
WHEN a.Downsell_arr > 0
AND a.Downsell_arr > a.Crossell_arr THEN CASE
        WHEN a.Downsell_arr < a.downgrade_downsell_total THEN a.Downsell_arr - a.Crossell_arr
ELSE a.Upsell_arr
END
WHEN a.Downgrade_arr > 0
AND a.Downgrade_arr <= a.Upsell_arr THEN a.Downgrade_arr
WHEN a.Downgrade_arr > 0
AND a.Downgrade_arr > a.Upsell_arr THEN a.Upsell_arr
END
WHEN a.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 1 THEN CASE
      WHEN a.downgrade_downsell_total > a.cross_upsell_total THEN a.Upsell_arr
ELSE CASE
        WHEN a.Downgrade_arr > a.Upsell_arr THEN a.Upsell_arr
ELSE a.Downgrade_arr
END
END
ELSE 0
END AS upsell_reversal_arr_new, CASE
    WHEN a.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 0 THEN CASE
      WHEN a.Crossell_arr > 0
AND a.downgrade_downsell_total >= a.cross_upsell_total THEN a.cross_upsell_total
WHEN a.Crossell_arr > 0
AND a.downgrade_downsell_total < a.cross_upsell_total THEN a.downgrade_downsell_total
ELSE 0
END
WHEN a.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 1 THEN CASE
      WHEN a.Crossell_arr > 0
AND a.downgrade_downsell_total < a.cross_upsell_total THEN a.downgrade_downsell_total
WHEN a.Crossell_arr > 0
AND a.downgrade_downsell_total >= a.cross_upsell_total THEN a.cross_upsell_total
ELSE 0
END
WHEN a.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 0 THEN CASE
      WHEN a.Downgrade_arr > 0
AND a.Downgrade_arr <= a.Upsell_arr THEN 0
WHEN a.Downgrade_arr > 0
AND a.Downgrade_arr > a.Upsell_arr THEN CASE
        WHEN a.Downgrade_arr < a.cross_upsell_total THEN a.Downgrade_arr - a.Upsell_arr
ELSE a.Crossell_arr
END
WHEN a.Downsell_arr > 0
AND a.Downsell_arr <= a.Crossell_arr THEN a.Downsell_arr
WHEN a.Downsell_arr > 0
AND a.Downsell_arr > a.Crossell_arr THEN a.Crossell_arr
END
WHEN a.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 1 THEN CASE
      WHEN a.downgrade_downsell_total > a.cross_upsell_total THEN a.Crossell_arr
ELSE CASE
        WHEN a.Downsell_arr > a.Crossell_arr THEN a.Crossell_arr
ELSE a.Downsell_arr
END
END
ELSE 0
END AS crosssell_reversal_arr_new
--#######################lcu #######################--
, CASE
    WHEN a.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 0 THEN CASE
      WHEN a.Upsell_arr_lcu > 0
AND a.downgrade_downsell_total_lcu >= a.cross_upsell_total_lcu THEN a.cross_upsell_total_lcu
WHEN a.Upsell_arr_lcu > 0
AND a.downgrade_downsell_total_lcu < a.cross_upsell_total_lcu THEN a.downgrade_downsell_total_lcu
ELSE 0
END
WHEN a.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 1 THEN CASE
      WHEN a.Upsell_arr_lcu > 0
AND a.downgrade_downsell_total_lcu < a.cross_upsell_total_lcu THEN a.downgrade_downsell_total_lcu
WHEN a.Upsell_arr_lcu > 0
AND a.downgrade_downsell_total_lcu >= a.cross_upsell_total_lcu THEN a.cross_upsell_total_lcu
ELSE 0
END
WHEN a.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 0 THEN CASE
      WHEN a.Downsell_arr_lcu > 0
AND a.Downsell_arr_lcu <= a.Crossell_arr_lcu THEN 0
WHEN a.Downsell_arr_lcu > 0
AND a.Downsell_arr_lcu > a.Crossell_arr_lcu THEN CASE
        WHEN a.Downsell_arr_lcu < a.downgrade_downsell_total_lcu THEN a.Downsell_arr_lcu - a.Crossell_arr_lcu
ELSE a.Upsell_arr_lcu
END
WHEN a.Downgrade_arr_lcu > 0
AND a.Downgrade_arr_lcu <= a.Upsell_arr_lcu THEN a.Downgrade_arr_lcu
WHEN a.Downgrade_arr_lcu > 0
AND a.Downgrade_arr_lcu > a.Upsell_arr_lcu THEN a.Upsell_arr_lcu
END
WHEN a.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 1 THEN CASE
      WHEN a.downgrade_downsell_total_lcu > a.cross_upsell_total_lcu THEN a.Upsell_arr_lcu
ELSE CASE
        WHEN a.Downgrade_arr_lcu > a.Upsell_arr_lcu THEN a.Upsell_arr_lcu
ELSE a.Downgrade_arr_lcu
END
END
ELSE 0
END AS upsell_reversal_arr_new_lcu, CASE
    WHEN a.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 0 THEN CASE
      WHEN a.Crossell_arr_lcu > 0
AND a.downgrade_downsell_total_lcu >= a.cross_upsell_total_lcu THEN a.cross_upsell_total_lcu
WHEN a.Crossell_arr_lcu > 0
AND a.downgrade_downsell_total_lcu < a.cross_upsell_total_lcu THEN a.downgrade_downsell_total_lcu
ELSE 0
END
WHEN a.cross_upsell_both_exists = 0
AND a.Downgrade_Downsell_both_exists = 1 THEN CASE
      WHEN a.Crossell_arr_lcu > 0
AND a.downgrade_downsell_total_lcu < a.cross_upsell_total_lcu THEN a.downgrade_downsell_total_lcu
WHEN a.Crossell_arr_lcu > 0
AND a.downgrade_downsell_total_lcu >= a.cross_upsell_total_lcu THEN a.cross_upsell_total_lcu
ELSE 0
END
WHEN a.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 0 THEN CASE
      WHEN a.Downgrade_arr_lcu > 0
AND a.Downgrade_arr_lcu <= a.Upsell_arr_lcu THEN 0
WHEN a.Downgrade_arr_lcu > 0
AND a.Downgrade_arr_lcu > a.Upsell_arr_lcu THEN CASE
        WHEN a.Downgrade_arr_lcu < a.cross_upsell_total_lcu THEN a.Downgrade_arr_lcu - a.Upsell_arr_lcu
ELSE a.Crossell_arr_lcu
END
WHEN a.Downsell_arr_lcu > 0
AND a.Downsell_arr_lcu <= a.Crossell_arr_lcu THEN a.Downsell_arr_lcu
WHEN a.Downsell_arr_lcu > 0
AND a.Downsell_arr_lcu > a.Crossell_arr_lcu THEN a.Crossell_arr_lcu
END
WHEN a.cross_upsell_both_exists = 1
AND a.Downgrade_Downsell_both_exists = 1 THEN CASE
      WHEN a.downgrade_downsell_total_lcu > a.cross_upsell_total_lcu THEN a.Crossell_arr_lcu
ELSE CASE
        WHEN a.Downsell_arr_lcu > a.Crossell_arr_lcu THEN a.Crossell_arr_lcu
ELSE a.Downsell_arr_lcu
END
END
ELSE 0
END AS crosssell_reversal_arr_new_lcu, 1 AS split_record
FROM temp_new_arr_split a
ORDER BY cross_upsell_both_exists;

RAISE NOTICE 'Running cross/Upsell reversal update on sst customer bridge 2...';

DROP TABLE IF EXISTS temp_cross_upsell_reversal_split;

CREATE temp TABLE temp_cross_upsell_reversal_split AS
SELECT a.evaluation_period, a.prior_period, a.current_period, a.current_master_customer_id, a.prior_master_customer_id, a.mcid, a.name, a.baseline_currency, a.subsidiary_entity_name, a.prior_period_customer_arr_usd_ccfx, a.prior_period_customer_arr_usd_ccfx - b.downsell_arr_new AS current_period_customer_arr_usd_ccfx,- b.downsell_arr_new AS customer_arr_change_ccfx, a.prior_period_customer_arr_lcu, a.prior_period_customer_arr_lcu - b.downsell_arr_new_lcu AS current_period_customer_lcu,- b.downsell_arr_new_lcu AS customer_arr_change_lcu, a.customer_bridge, a.winback_period_days, a.wip_flag
FROM sandbox_pd.sst_customer_bridge a
JOIN temp_cross_upsell_reversal_final_curated b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
WHERE b.Split_record = 1
AND a.evaluation_period = var_period
AND a.customer_bridge IN ('Downsell')
AND b.downsell_arr_new > 0
UNION ALL
SELECT a.evaluation_period, a.prior_period, a.current_period, a.current_master_customer_id, a.prior_master_customer_id, a.mcid, a.name, a.baseline_currency, a.subsidiary_entity_name, a.prior_period_customer_arr_usd_ccfx, a.prior_period_customer_arr_usd_ccfx - b.downgrade_arr_new AS current_period_customer_arr_usd_ccfx,- b.downgrade_arr_new AS customer_arr_change_ccfx, a.prior_period_customer_arr_lcu, a.prior_period_customer_arr_lcu - b.downgrade_arr_new_lcu AS current_period_customer_lcu,- b.downgrade_arr_new_lcu AS customer_arr_change_lcu, a.customer_bridge, a.winback_period_days, a.wip_flag
FROM sandbox_pd.sst_customer_bridge a
JOIN temp_cross_upsell_reversal_final_curated b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
WHERE b.Split_record = 1
AND a.evaluation_period = var_period
AND a.customer_bridge IN ('Downgrade')
AND b.downgrade_arr_new > 0
UNION ALL
SELECT DISTINCT a.evaluation_period, a.prior_period, a.current_period, a.current_master_customer_id, a.prior_master_customer_id, a.mcid, a.name, a.baseline_currency, a.subsidiary_entity_name, 0 AS prior_period_customer_arr_usd_ccfx, b.upsell_reversal_arr_new AS current_period_customer_arr_usd_ccfx,- b.upsell_reversal_arr_new AS customer_arr_change_ccfx, 0 AS prior_period_customer_arr_lcu, b.upsell_reversal_arr_new_lcu AS current_period_customer_lcu,- b.upsell_reversal_arr_new_lcu AS customer_arr_change_lcu, 'Up Sell Reversal' AS customer_bridge, NULL AS winback_period_days, NULL AS wip_flag
--select b.*
FROM sandbox_pd.sst_customer_bridge a
JOIN temp_cross_upsell_reversal_final_curated b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
WHERE b.Split_record = 1
AND a.evaluation_period = var_period
AND b.upsell_reversal_arr_new > 0
AND a.customer_bridge <> 'Flat'
UNION ALL
SELECT DISTINCT a.evaluation_period, a.prior_period, a.current_period, a.current_master_customer_id, a.prior_master_customer_id, a.mcid, a.name, a.baseline_currency, a.subsidiary_entity_name, 0 AS prior_period_customer_arr_usd_ccfx, b.crosssell_reversal_arr_new AS current_period_customer_arr_usd_ccfx,- b.crosssell_reversal_arr_new AS customer_arr_change_ccfx, 0 AS prior_period_customer_arr_lcu, b.crosssell_reversal_arr_new_lcu AS current_period_customer_lcu,- b.crosssell_reversal_arr_new_lcu AS customer_arr_change_lcu, 'Cross-sell Reversal' AS customer_bridge, NULL AS winback_period_days, NULL AS wip_flag
--select b.*
FROM sandbox_pd.sst_customer_bridge a
JOIN temp_cross_upsell_reversal_final_curated b ON
a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
WHERE b.Split_record = 1
AND a.evaluation_period = var_period
AND b.crosssell_reversal_arr_new > 0
AND a.customer_bridge <> 'Flat'
ORDER BY mcid;

RAISE NOTICE 'Running cross/Upsell reversal update on sst customer bridge 4...';

DELETE
FROM sandbox_pd.sst_customer_bridge a
    USING temp_cross_upsell_reversal_final_curated b
WHERE 1 = 1
AND a.mcid = b.mcid
AND a.evaluation_period = b.evaluation_period
AND b.Split_record = 1
AND a.evaluation_period = var_period
-- and a.customer_bridge = b.customer_bridge
AND a.customer_bridge IN ('Downgrade', 'Downsell');

INSERT INTO sandbox_pd.sst_customer_bridge (
    evaluation_period, prior_period, current_period, current_master_customer_id, prior_master_customer_id, mcid, name, baseline_currency, subsidiary_entity_name, prior_period_customer_arr_usd_ccfx, current_period_customer_arr_usd_ccfx, customer_arr_change_ccfx, prior_period_customer_arr_lcu, current_period_customer_lcu, customer_arr_change_lcu, customer_bridge, winback_period_days, wip_flag
  )
SELECT evaluation_period, prior_period, current_period, current_master_customer_id, prior_master_customer_id, mcid, name, baseline_currency, subsidiary_entity_name, prior_period_customer_arr_usd_ccfx, current_period_customer_arr_usd_ccfx, customer_arr_change_ccfx, prior_period_customer_arr_lcu, current_period_customer_lcu, customer_arr_change_lcu, customer_bridge, winback_period_days, wip_flag
FROM temp_cross_upsell_reversal_split;

/*
 update sandbox_pd.sst_customer_bridge a
 set customer_bridge = concat(b.upsell_bridge ,' Reversal')
 from temp_upselldowngrade_final b
 where 1=1
 and a.mcid = b.mcid
 and a.evaluation_period = b.evaluation_period
 and a.evaluation_period = var_period
 and a.customer_bridge = b.customer_bridge
 and a.customer_bridge in ('Downgrade','Downsell')
 and Split_record = 0
 ;
 
 drop table if exists temp_upselldowngrade_split;
 
 create temp table temp_upselldowngrade_split as
 select distinct a.evaluation_period,a.prior_period,a.current_period,a.current_master_customer_id,a.prior_master_customer_id,a.mcid,a.name
 ,a.baseline_currency,a.subsidiary_entity_name
 ,case when a.customer_bridge = 'Churn' then a.prior_period_customer_arr_usd_ccfx - c.customer_arr_change_ccfx
 else a.prior_period_customer_arr_usd_ccfx end as prior_period_customer_arr_usd_ccfx
 ,case when a.customer_bridge = 'Churn' then 0 else a.current_period_customer_arr_usd_ccfx + c.customer_arr_change_ccfx end as current_period_customer_arr_usd_ccfx
 ,a.customer_arr_change_ccfx + c.customer_arr_change_ccfx as customer_arr_change_ccfx
 ---lcu
 ,case when a.customer_bridge = 'Churn' then a.prior_period_customer_arr_lcu - c.customer_arr_change_lcu
 else a.prior_period_customer_arr_lcu end as prior_period_customer_arr_lcu
 ,case when a.customer_bridge = 'Churn' then 0 else a.current_period_customer_lcu + c.customer_arr_change_lcu end as current_period_customer_lcu
 ,a.customer_arr_change_lcu + c.customer_arr_change_lcu as customer_arr_change_lcu
 ,a.customer_bridge,a.winback_period_days,a.wip_flag
 from sandbox_pd.sst_customer_bridge a
 join temp_upselldowngrade_final b on a.mcid = b.mcid and a.evaluation_period = b.evaluation_period and a.customer_bridge = b.customer_bridge
 join sandbox_pd.sst_customer_bridge c on c.mcid = b.mcid and c.evaluation_period = b.upsell_evaluation_period
 and c.customer_bridge = b.upsell_bridge
 where b.Split_record = 1
 and a.evaluation_period = var_period
 and a.customer_bridge in ('Downgrade','Downsell')
 union all
 select  a.evaluation_period,a.prior_period,a.current_period,a.current_master_customer_id,a.prior_master_customer_id,a.mcid,a.name
 ,a.baseline_currency,a.subsidiary_entity_name,c.current_period_customer_arr_usd_ccfx as prior_period_customer_arr_usd_ccfx,c.prior_period_customer_arr_usd_ccfx as current_period_customer_arr_usd_ccfx
 ,-c.customer_arr_change_ccfx
 ,c.current_period_customer_lcu as prior_period_customer_arr_lcu
 ,c.prior_period_customer_arr_lcu  as current_period_customer_lcu
 ,-c.customer_arr_change_lcu
 ,concat(b.upsell_bridge ,' Reversal') as customer_bridge,a.winback_period_days,a.wip_flag
 from sandbox_pd.sst_customer_bridge a
 join temp_upselldowngrade_final b on a.mcid = b.mcid and a.evaluation_period = b.evaluation_period and a.customer_bridge = b.customer_bridge
 join sandbox_pd.sst_customer_bridge c on c.mcid = b.mcid and c.evaluation_period = b.upsell_evaluation_period
 and c.customer_bridge = b.upsell_bridge
 where b.Split_record = 1
 and a.evaluation_period = var_period
 and a.customer_bridge in ('Downgrade','Downsell')
 order by mcid
 ;
 
 delete from sandbox_pd.sst_customer_bridge a
 using temp_upselldowngrade_final b
 where 1=1
 and a.mcid = b.mcid
 and a.evaluation_period = b.evaluation_period
 and b.Split_record = 1
 and a.evaluation_period = var_period
 and a.customer_bridge = b.customer_bridge
 and a.customer_bridge in ('Downgrade','Downsell')
 ;
 
 insert into sandbox_pd.sst_customer_bridge
 (  evaluation_period, prior_period, current_period, current_master_customer_id, prior_master_customer_id, mcid, name, baseline_currency, subsidiary_entity_name
 , prior_period_customer_arr_usd_ccfx, current_period_customer_arr_usd_ccfx, customer_arr_change_ccfx, prior_period_customer_arr_lcu, current_period_customer_lcu
 , customer_arr_change_lcu, customer_bridge, winback_period_days, wip_flag
 )
 select  evaluation_period, prior_period, current_period, current_master_customer_id, prior_master_customer_id, mcid, name, baseline_currency, subsidiary_entity_name
 , prior_period_customer_arr_usd_ccfx, current_period_customer_arr_usd_ccfx, customer_arr_change_ccfx, prior_period_customer_arr_lcu, current_period_customer_lcu
 , customer_arr_change_lcu, customer_bridge, winback_period_days, wip_flag
 from temp_upselldowngrade_split
 ;
 */
END IF;

RAISE NOTICE 'Running rounding errors update on sst customer bridge...';
--rounding errors updates
UPDATE sandbox_pd.sst_customer_bridge
SET
customer_bridge = 'Rounding'
WHERE customer_bridge = 'Flat'
AND COALESCE(customer_arr_change_ccfx, 0) <> 0
AND evaluation_period = var_period;
-- drop all temp tables
DROP TABLE IF EXISTS prior_period_customer_arr;

DROP TABLE IF EXISTS current_period_customer_arr;

DROP TABLE IF EXISTS customer_level_arr;

DROP TABLE IF EXISTS account;

DROP TABLE IF EXISTS arr_bridge_tmp;

DROP TABLE IF EXISTS temp_cross_sell_data;

DROP TABLE IF EXISTS temp_pb_crosssell;

DROP TABLE IF EXISTS temp_pb_crosssell_final;

DROP TABLE IF EXISTS temp_cb_crosssell_split;

DROP TABLE IF EXISTS temp_downsell_data;

DROP TABLE IF EXISTS temp_pb_downsell;

DROP TABLE IF EXISTS temp_pb_downsell_final;

DROP TABLE IF EXISTS temp_cb_downsell_split;

DROP TABLE IF EXISTS temp_customer_bridge_price_ramps;

DROP TABLE IF EXISTS temp_Price_Ramp_split;

DROP TABLE IF EXISTS arr_new_products_tmp;

DROP TABLE IF EXISTS arr_churned_products_tmp;

DROP TABLE IF EXISTS sub_entity_tmp;

DROP TABLE IF EXISTS temp_win_downgrade_upsell;

DROP TABLE IF EXISTS temp_windowngrade_final;

DROP TABLE IF EXISTS temp_windowngrade_final_curated;

DROP TABLE IF EXISTS temp_windowngrade_split;

DROP TABLE IF EXISTS temp_CPI_Reversal;

DROP TABLE IF EXISTS temp_cpireversal_final;

DROP TABLE IF EXISTS temp_cpireversal_split;

DROP TABLE IF EXISTS temp_downgrade_upsell;

DROP TABLE IF EXISTS temp_upselldowngrade_final;

DROP TABLE IF EXISTS temp_upselldowngrade_split;
END;

$$;