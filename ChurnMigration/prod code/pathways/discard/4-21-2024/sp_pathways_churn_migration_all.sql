CREATE TABLE ryzlan.sst_product_bridge_product_family_pathways_CM_all AS
SELECT *
FROM ryzlan.sst_product_bridge_product_family_pathways_CM
LIMIT 1;
TRUNCATE TABLE ryzlan.sst_product_bridge_product_family_pathways_CM_all;
ALTER TABLE ryzlan.sst_product_bridge_product_family_pathways_CM_all
  RENAME COLUMN current_product_family TO current_product_solution;
ALTER TABLE ryzlan.sst_product_bridge_product_family_pathways_CM_all
  RENAME COLUMN prior_product_family TO prior_product_solution;
CREATE OR REPLACE FUNCTION ryzlan.sp_pathways_churn_migration_all(var_period TEXT) RETURNS void LANGUAGE plpgsql AS $$ BEGIN
DELETE FROM ryzlan.sst_product_bridge_product_family_pathways_CM_all
WHERE evaluation_period = var_period;
DROP TABLE IF EXISTS sst_temp;
CREATE TEMP TABLE sst_temp AS
SELECT *,
  CASE
    WHEN migration_from = 'Y' THEN CASE
      WHEN new_product IN (
        'Everweb',
        'Ektron',
        'Personalized Find',
        'Visitor Intelligence',
        'Search & Navigation - Standalone'
      ) THEN new_product
      WHEN new_line_of_business IN ('Licenses') THEN new_line_of_business
    END
    WHEN migration_to = 'Y' THEN CASE
      WHEN new_product IN (
        'Content Managemen System (CMS)',
        'Content Management System (CMS)',
        'Content Graph',
        'Data Platform (ODP)'
      ) THEN new_product
      WHEN new_line_of_business IN ('Cloud') THEN new_line_of_business
    END
    ELSE 'Usual'
  END AS pathways
FROM ufdm.sst
WHERE snapshot_date = (
    SELECT current_period
    FROM ufdm_grey.periods
    WHERE evaluation_period = var_period
  )
  OR snapshot_date = (
    SELECT prior_period
    FROM ufdm_grey.periods
    WHERE evaluation_period = var_period
  );
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--SST product Bridge
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS prior_period_customer_arr_tmp;
CREATE TEMP TABLE prior_period_customer_arr_tmp AS
SELECT snapshot_date,
  a.mcid AS master_customer_id,
  pathways,
  updated_product_group AS product_group,
  new_product_solution AS product_solution,
  a.base_currency AS baseline_currency,
  max(COALESCE(a.end_name, a.parent_name)) AS end_customer,
  sum(arr) AS arr_usd_ccfx,
  sum(baseline_arr_local_currency) AS arr_lcu
FROM sst_temp a
WHERE 1 = 1
  AND snapshot_date = (
    SELECT prior_period
    FROM ufdm_grey.periods
    WHERE evaluation_period = var_period
  )
  AND a.overage_flag IS DISTINCT
FROM 'Y'
GROUP BY 1,
  2,
  3,
  4,
  5,
  6;
DROP TABLE IF EXISTS current_period_customer_arr_tmp;
CREATE TEMP TABLE current_period_customer_arr_tmp AS
SELECT snapshot_date,
  a.mcid AS master_customer_id,
  pathways,
  updated_product_group AS product_group,
  new_product_solution AS product_solution,
  a.base_currency AS baseline_currency,
  max(COALESCE(a.end_name, a.parent_name)) AS end_customer,
  sum(arr) AS arr_usd_ccfx,
  sum(baseline_arr_local_currency) AS arr_lcu
FROM sst_temp a
WHERE 1 = 1
  AND snapshot_date = (
    SELECT current_period
    FROM ufdm_grey.periods
    WHERE evaluation_period = var_period
  )
  AND a.overage_flag IS DISTINCT
FROM 'Y'
GROUP BY 1,
  2,
  3,
  4,
  5,
  6;
DROP TABLE IF EXISTS customer_level_arr_tmp;
CREATE TEMP TABLE customer_level_arr_tmp AS
SELECT c1.master_customer_id AS current_cust_id,
  c2.master_customer_id AS prior_cust_id,
  c1.end_customer AS current_end_customer,
  c2.end_customer AS prior_end_customer,
  c2.snapshot_date AS prior_period,
  c1.snapshot_date AS current_period,
  c1.baseline_currency AS current_baseline_currency,
  c2.baseline_currency AS prior_baseline_currency,
  COALESCE(c1.baseline_currency, c2.baseline_currency) AS baseline_currency,
  c1.pathways AS current_pathways,
  c2.pathways AS prior_pathways,
  c2.product_group AS prior_product_group,
  c1.product_group AS current_product_group,
  c2.product_solution AS prior_product_solution,
  c1.product_solution AS current_product_solution,
  COALESCE(c1.arr_usd_ccfx, 0) AS current_arr_usd_ccfx,
  COALESCE(c2.arr_usd_ccfx, 0) AS prior_arr_usd_ccfx,
  COALESCE(c1.arr_lcu, 0) AS current_arr_lcu,
  COALESCE(c2.arr_lcu, 0) AS prior_arr_lcu --c3.arr AS prior2_arr, --WIP
FROM current_period_customer_arr_tmp c1
  FULL OUTER JOIN prior_period_customer_arr_tmp c2 ON c1.master_customer_id = c2.master_customer_id
  AND c1.product_solution = c2.product_solution
  AND c1.product_group = c2.product_group
  AND c1.baseline_currency = c2.baseline_currency
  AND c1.pathways = c2.pathways;
DROP TABLE IF EXISTS arr_product_bridge_tmp;
CREATE TEMP TABLE arr_product_bridge_tmp AS
SELECT per.evaluation_period,
  cla.prior_period,
  cla.current_period,
  cla.current_cust_id AS current_master_customer_id,
  cla.prior_cust_id AS prior_master_customer_id,
  COALESCE(cla.current_cust_id, cla.prior_cust_id) AS mcid,
  cla.current_product_group,
  cla.prior_product_group,
  cla.current_product_solution,
  cla.prior_product_solution,
  cla.current_pathways,
  cla.prior_pathways,
  cla.current_end_customer,
  cla.prior_end_customer,
  cla.baseline_currency,
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
  ) AS product_arr_change_ccfx,
  round((COALESCE(cla.current_arr_lcu::NUMERIC, 0)), 2) AS current_arr_lcu,
  round((COALESCE(cla.prior_arr_lcu::NUMERIC, 0)), 2) AS prior_arr_lcu,
  round(
    (
      COALESCE(cla.current_arr_lcu::NUMERIC, 0) - COALESCE(cla.prior_arr_lcu::NUMERIC, 0)
    ),
    2
  ) AS product_arr_change_lcu,
  CASE
    WHEN (
      (
        COALESCE (cla.prior_arr_usd_ccfx, 0) = 0
        OR COALESCE (cla.prior_arr_usd_ccfx, 0) = 0.00
      ) --OR (cla.prior_arr_usd_ccfx = 0 and clp.prior_product_family_agg is null))
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
  END AS product_bridge
FROM customer_level_arr_tmp cla
  CROSS JOIN ufdm_grey.periods per
WHERE 1 = 1
  AND per.evaluation_period = var_period;
--Downgrade
UPDATE arr_product_bridge_tmp
SET product_bridge = 'Downgrade'
WHERE product_bridge = 'Partial Churn';
--Cross-sell
WITH PG_F_C AS (
  SELECT mcid,
    COUNT(DISTINCT product_bridge) AS product_family_count
  FROM arr_product_bridge_tmp
  WHERE current_arr_usd_ccfx > 0
    AND evaluation_period = var_period
  GROUP BY mcid,
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
  AND t.evaluation_period = var_period;
-------
--Downsell
WITH PG_F_C AS (
  SELECT mcid,
    COUNT(DISTINCT product_bridge) AS product_family_count
  FROM arr_product_bridge_tmp
  WHERE prior_arr_usd_ccfx > 0
    AND evaluation_period = var_period
  GROUP BY mcid,
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
  AND t.evaluation_period = var_period;
--#############################################
--WIP/WINBACK
--#############################################
DROP TABLE IF EXISTS arr_new_products_tmp;
CREATE TEMP TABLE arr_new_products_tmp AS
SELECT a.current_master_customer_id AS mcid,
  a.current_product_solution AS product_solution,
  a.current_product_group AS product_group,
  a.current_pathways AS pathways,
  a.current_period AS snapshot_date,
  a.current_arr_usd_ccfx AS arr_at_new,
  a.current_arr_lcu AS arr_lcu_at_new,
  baseline_currency
FROM arr_product_bridge_tmp a
WHERE product_bridge IN ('New', 'Cross-sell');
--get most recent postivie arr for above new product which should have been churned
DROP TABLE IF EXISTS arr_churned_products_tmp;
CREATE TEMP TABLE arr_churned_products_tmp AS WITH TEMP AS (
  SELECT b.snapshot_date,
    b.mcid,
    b.new_product_solution AS product_solution,
    b.updated_product_group AS product_group,
    b.pathways,
    a.baseline_currency,
    a.snapshot_date AS snapshot_date_at_new,
    sum(b.arr) AS arr_at_churn,
    sum(b.baseline_arr_local_currency) AS arr_lcu_at_churn,
    sum(a.arr_at_new) AS arr_at_new,
    sum(a.arr_lcu_at_new) AS arr_lcu_at_new,
    ROW_NUMBER() OVER (
      PARTITION BY b.mcid,
      b.new_product_solution,
      b.updated_product_group,
      b.pathways
      ORDER BY b.snapshot_date DESC
    ) AS rnk
  FROM arr_new_products_tmp a
    JOIN (
      SELECT sb.mcid,
        sb.snapshot_date,
        sb.overage_flag,
        sb.new_product_solution,
        sb.updated_product_group,
        sb.pathways,
        sb.base_currency,
        sum(arr) AS arr,
        sum(baseline_arr_local_currency) AS baseline_arr_local_currency
      FROM sst_temp sb
      GROUP BY 1,
        2,
        3,
        4,
        5,
        6,
        7
    ) b ON a.mcid = b.mcid --and a.product_solution = b.product_solution
    AND a.product_solution = b.new_product_solution
    AND a.product_group = b.updated_product_group
    AND a.pathways = b.pathways
    AND a.baseline_currency = b.base_currency
  WHERE b.snapshot_date < a.snapshot_date
    AND b.overage_flag ILIKE '%N%'
    AND b.arr > 0
  GROUP BY 1,
    2,
    3,
    4,
    5,
    6,
    7
)
SELECT *,
  (
    DATE_PART('year', snapshot_date_at_new::date) - DATE_PART('year', snapshot_date::date)
  ) * 12 + (
    DATE_PART('month', snapshot_date_at_new::date) - DATE_PART('month', snapshot_date::date)
  ) AS months_diff,
  CASE
    WHEN arr_at_new > arr_at_churn THEN 'Upsell'
    ELSE CASE
      WHEN EXTRACT(
        DAY
        FROM snapshot_date_at_new::timestamp - (snapshot_date + INTERVAL '1 month')::date
      ) <= 90 THEN 'Winback ST'
      ELSE 'Winback LT'
    END
  END AS product_bridge_new,
  arr_at_new - arr_at_churn AS arr_diff,
  arr_lcu_at_new - arr_lcu_at_churn AS arr_lcu_diff,
  EXTRACT(
    DAY
    FROM snapshot_date_at_new::timestamp - (snapshot_date + INTERVAL '1 month')::date
  ) AS days_diff,
  snapshot_date AS churn_period
FROM TEMP
WHERE rnk = 1
  AND EXTRACT(
    DAY
    FROM snapshot_date_at_new::timestamp - (snapshot_date + INTERVAL '1 month')::date
  ) < 186;
-- create index if not exists nci_arr_churned_products_tmp_tmp_composite on arr_churned_products_tmp(mcid,product_solution,baseline_currency,snapshot_date_at_new) include(arr_at_new, arr_at_churn);
INSERT INTO ryzlan.sst_product_bridge_product_family_pathways_CM_all (
    evaluation_period,
    prior_period,
    current_period,
    current_end_customer,
    prior_end_customer,
    mcid,
    current_master_customer_id,
    Prior_master_customer_id,
    current_product_solution,
    prior_product_solution,
    current_product_group,
    prior_product_group,
    current_pathways,
    prior_pathways,
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
  a.current_product_solution,
  a.prior_product_solution,
  a.current_product_group,
  a.prior_product_group,
  a.current_pathways,
  a.prior_pathways,
  --         "name",
  round(a.prior_arr_usd_ccfx::NUMERIC, 2) AS prior_period_customer_arr_usd_ccfx,
  --round(a.current_arr_usd_ccfx::numeric,2) AS current_period_customer_arr_usd_ccfx,
  CASE
    WHEN b.mcid IS NOT NULL THEN CASE
      WHEN b.arr_at_new > b.arr_at_churn THEN b.arr_at_churn
      ELSE b.arr_at_new
    END --round(b.arr_at_churn::numeric,2)
    ELSE round(a.current_arr_usd_ccfx::NUMERIC, 2)
  END AS current_period_customer_arr_usd_ccfx,
  CASE
    WHEN b.mcid IS NOT NULL THEN CASE
      WHEN b.arr_at_new > b.arr_at_churn THEN b.arr_at_churn
      ELSE b.arr_at_new
    END
    ELSE a.product_arr_change_ccfx
  END AS product_arr_change_ccfx,
  ------------------------lcu----------------------------
  round(a.prior_arr_lcu::NUMERIC, 2) AS prior_period_product_arr_lcu,
  CASE
    WHEN b.mcid IS NOT NULL THEN CASE
      WHEN b.arr_lcu_at_new > b.arr_lcu_at_churn THEN b.arr_lcu_at_churn
      ELSE b.arr_lcu_at_new
    END --round(b.arr_lcu_at_churn::numeric,2)
    ELSE round(a.current_arr_lcu::NUMERIC, 2)
  END AS current_period_product_arr_lcu,
  CASE
    WHEN b.mcid IS NOT NULL THEN CASE
      WHEN b.arr_lcu_at_new > b.arr_lcu_at_churn THEN b.arr_lcu_at_churn
      ELSE b.arr_lcu_at_new
    END
    ELSE a.product_arr_change_lcu
  END AS product_arr_change_lcu,
  CASE
    WHEN b.mcid IS NOT NULL THEN CASE
      WHEN b.days_diff <= 90 THEN 'Winback ST'
      ELSE 'Winback LT'
    END --b.product_bridge_new --'Winback' --/WIP
    ELSE a.product_bridge
  END AS product_bridge,
  b.days_diff AS Winback_period_days,
  CASE
    WHEN b.days_diff <= 90 THEN 'Y'
    ELSE 'N'
  END AS Wip_Flag,
  NULL::NUMERIC AS price_increase_amount,
  NULL::TEXT AS subsidiary_entity_name,
  b.churn_period,
  a.baseline_currency
FROM arr_product_bridge_tmp a
  LEFT JOIN arr_churned_products_tmp b ON a.current_master_customer_id = b.mcid
  AND a.current_product_solution = b.product_solution
  AND a.current_product_group = b.product_group
  AND a.current_pathways = b.pathways
  AND a.baseline_currency = b.baseline_currency
  AND a.current_period = b.snapshot_date_at_new
UNION ALL
SELECT a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_end_customer,
  a.prior_end_customer,
  a.mcid,
  a.current_master_customer_id,
  a.Prior_master_customer_id,
  a.current_product_solution,
  a.prior_product_solution,
  a.current_product_group,
  a.prior_product_group,
  a.current_pathways,
  a.prior_pathways,
  --         "name",
  round(a.prior_arr_usd_ccfx::NUMERIC, 2) AS prior_period_customer_arr_usd_ccfx,
  b.arr_diff AS current_period_customer_arr_usd_ccfx,
  b.arr_diff,
  round(a.prior_arr_lcu::NUMERIC, 2) AS prior_period_product_arr_lcu,
  b.arr_lcu_diff AS current_period_product_arr_lcu,
  b.arr_lcu_diff,
  'Up Sell' AS product_bridge,
  NULL AS Winback_period_days,
  NULL AS Wip_Flag,
  NULL::NUMERIC AS price_increase_amount,
  NULL::TEXT AS subsidiary_entity_name,
  NULL::date AS churn_period,
  a.baseline_currency
FROM arr_product_bridge_tmp a
  JOIN arr_churned_products_tmp b ON a.current_master_customer_id = b.mcid
  AND a.current_product_solution = b.product_solution
  AND a.current_product_group = b.product_group
  AND a.current_pathways = b.pathways
  AND a.baseline_currency = b.baseline_currency
  AND a.current_period = b.snapshot_date_at_new
WHERE b.arr_at_new > b.arr_at_churn;
RAISE NOTICE 'Running customer bridge update on sst product bridge...';
--UPDATE customer bridge
--AND subsidiary entity
UPDATE ryzlan.sst_product_bridge_product_family_pathways_CM_all a
SET customer_bridge = b.customer_bridge
FROM ufdm.sst_customer_bridge b
WHERE 1 = 1
  AND a.evaluation_period = b.evaluation_period
  AND a.mcid = b.mcid
  AND a.evaluation_period = var_period;
RAISE NOTICE 'Running subsidiary entity name insert on sst product bridge...';
DROP TABLE IF EXISTS sub_entity_tmp;
CREATE TEMP TABLE sub_entity_tmp AS --update subsidiary_entity_name
WITH mcid_list AS (
  SELECT DISTINCT mcid AS master_customer_id
  FROM arr_product_bridge_tmp
  WHERE evaluation_period = var_period
),
total_arr AS (
  SELECT a.mcid AS mcid,
    a.snapshot_date,
    a.subsidiary_entity_name,
    sum(a.arr) AS total_arr
  FROM sst_temp a
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
RAISE NOTICE 'Running sub entity update on sst product bridge...';
CREATE INDEX nci_sub_entity_tmp_mcid ON sub_entity_tmp(mcid);
UPDATE ryzlan.sst_product_bridge_product_family_pathways_CM_all a
SET subsidiary_entity_name = b.subsidiary_entity_name
FROM sub_entity_tmp b
WHERE a.mcid = b.mcid
  AND a.evaluation_period = var_period;
RAISE NOTICE 'Running Price Increase update on sst product bridge...';
--Price Increase updates
UPDATE ryzlan.sst_product_bridge_product_family_pathways_CM_all
SET product_bridge = 'CPI'
WHERE product_bridge = 'Up Sell'
  AND prior_period_product_arr_usd_ccfx > 0
  AND (
    (
      product_arr_change_ccfx / prior_period_product_arr_usd_ccfx
    ) * 100
  )::NUMERIC <= CASE
    WHEN current_period < '2023-01-01' THEN 5.5
    ELSE 10.5
  END
  AND evaluation_period = var_period;
END;
$$ TRUNCATE TABLE ryzlan.sst_product_bridge_product_family_pathways_CM_all;
SELECT ryzlan.sp_pathways_churn_migration_all('2019M01');
SELECT ryzlan.sp_pathways_churn_migration_all('2019M02');
SELECT ryzlan.sp_pathways_churn_migration_all('2019M03');
SELECT ryzlan.sp_pathways_churn_migration_all('2019M04');
SELECT ryzlan.sp_pathways_churn_migration_all('2019M05');
SELECT ryzlan.sp_pathways_churn_migration_all('2019M06');
SELECT ryzlan.sp_pathways_churn_migration_all('2019M07');
SELECT ryzlan.sp_pathways_churn_migration_all('2019M08');
SELECT ryzlan.sp_pathways_churn_migration_all('2019M09');
SELECT ryzlan.sp_pathways_churn_migration_all('2019M10');
SELECT ryzlan.sp_pathways_churn_migration_all('2019M11');
SELECT ryzlan.sp_pathways_churn_migration_all('2019M12');
SELECT ryzlan.sp_pathways_churn_migration_all('2020M01');
SELECT ryzlan.sp_pathways_churn_migration_all('2020M02');
SELECT ryzlan.sp_pathways_churn_migration_all('2020M03');
SELECT ryzlan.sp_pathways_churn_migration_all('2020M04');
SELECT ryzlan.sp_pathways_churn_migration_all('2020M05');
SELECT ryzlan.sp_pathways_churn_migration_all('2020M06');
SELECT ryzlan.sp_pathways_churn_migration_all('2020M07');
SELECT ryzlan.sp_pathways_churn_migration_all('2020M08');
SELECT ryzlan.sp_pathways_churn_migration_all('2020M09');
SELECT ryzlan.sp_pathways_churn_migration_all('2020M10');
SELECT ryzlan.sp_pathways_churn_migration_all('2020M11');
SELECT ryzlan.sp_pathways_churn_migration_all('2020M12');
SELECT ryzlan.sp_pathways_churn_migration_all('2021M01');
SELECT ryzlan.sp_pathways_churn_migration_all('2021M02');
SELECT ryzlan.sp_pathways_churn_migration_all('2021M03');
SELECT ryzlan.sp_pathways_churn_migration_all('2021M04');
SELECT ryzlan.sp_pathways_churn_migration_all('2021M05');
SELECT ryzlan.sp_pathways_churn_migration_all('2021M06');
SELECT ryzlan.sp_pathways_churn_migration_all('2021M07');
SELECT ryzlan.sp_pathways_churn_migration_all('2021M08');
SELECT ryzlan.sp_pathways_churn_migration_all('2021M09');
SELECT ryzlan.sp_pathways_churn_migration_all('2021M10');
SELECT ryzlan.sp_pathways_churn_migration_all('2021M11');
SELECT ryzlan.sp_pathways_churn_migration_all('2021M12');
SELECT ryzlan.sp_pathways_churn_migration_all('2022M01');
SELECT ryzlan.sp_pathways_churn_migration_all('2022M02');
SELECT ryzlan.sp_pathways_churn_migration_all('2022M03');
SELECT ryzlan.sp_pathways_churn_migration_all('2022M04');
SELECT ryzlan.sp_pathways_churn_migration_all('2022M05');
SELECT ryzlan.sp_pathways_churn_migration_all('2022M06');
SELECT ryzlan.sp_pathways_churn_migration_all('2022M07');
SELECT ryzlan.sp_pathways_churn_migration_all('2022M08');
SELECT ryzlan.sp_pathways_churn_migration_all('2022M09');
SELECT ryzlan.sp_pathways_churn_migration_all('2022M10');
SELECT ryzlan.sp_pathways_churn_migration_all('2022M11');
SELECT ryzlan.sp_pathways_churn_migration_all('2022M12');
SELECT ryzlan.sp_pathways_churn_migration_all('2023M01');
SELECT ryzlan.sp_pathways_churn_migration_all('2023M02');
SELECT ryzlan.sp_pathways_churn_migration_all('2023M03');
SELECT ryzlan.sp_pathways_churn_migration_all('2023M04');
SELECT ryzlan.sp_pathways_churn_migration_all('2023M05');
SELECT ryzlan.sp_pathways_churn_migration_all('2023M06');
SELECT ryzlan.sp_pathways_churn_migration_all('2023M07');
SELECT ryzlan.sp_pathways_churn_migration_all('2023M08');
SELECT ryzlan.sp_pathways_churn_migration_all('2023M09');
SELECT ryzlan.sp_pathways_churn_migration_all('2023M10');
SELECT ryzlan.sp_pathways_churn_migration_all('2023M11');
SELECT ryzlan.sp_pathways_churn_migration_all('2023M12');