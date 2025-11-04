
create or replace function ryzlan.sp_populate_sst_product_pathways(var_period text) returns void language plpgsql as $$ BEGIN
DELETE from ryzlan.sst_product_pathways_bridge
where evaluation_period = var_period;

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
FROM ryzlan.sst_adhoc
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


drop table if exists prior_period_customer_arr_tmp;
create temp table prior_period_customer_arr_tmp as
SELECT snapshot_date,
  a.mcid as master_customer_id,
  pathways,
  updated_product_group AS product_group,
  new_product_solution AS product_solution,
  a.base_currency as baseline_currency,
  max(coalesce(a.end_name, a.parent_name)) as end_customer,
  sum(arr) AS arr_usd_ccfx,
  sum(baseline_arr_local_currency) AS arr_lcu
FROM sst_temp a
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
  4,
  5,
  6;
drop table if exists current_period_customer_arr_tmp;
create temp table current_period_customer_arr_tmp as
SELECT snapshot_date,
  a.mcid as master_customer_id,
  pathways,
  updated_product_group AS product_group,
  new_product_solution AS product_solution,
  a.base_currency as baseline_currency,
  max(coalesce(a.end_name, a.parent_name)) as end_customer,
  sum(arr) AS arr_usd_ccfx,
  sum(baseline_arr_local_currency) AS arr_lcu
FROM sst_temp a
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
  4,
  5,
  6;
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
  c1.pathways AS current_pathways,
  c2.pathways AS prior_pathways,
  c2.product_group AS prior_product_group,
  c1.product_group AS current_product_group,
  c2.product_solution AS prior_product_solution,
  c1.product_solution AS current_product_solution,
  coalesce(c1.arr_usd_ccfx, 0) AS current_arr_usd_ccfx,
  coalesce(c2.arr_usd_ccfx, 0) AS prior_arr_usd_ccfx,
  coalesce(c1.arr_lcu, 0) AS current_arr_lcu,
  coalesce(c2.arr_lcu, 0) AS prior_arr_lcu --c3.arr AS prior2_arr, --WIP
FROM current_period_customer_arr_tmp c1
  FULL OUTER JOIN prior_period_customer_arr_tmp c2 
  ON c1.master_customer_id = c2.master_customer_id
  AND c1.product_solution = c2.product_solution
  AND c1.product_group = c2.product_group
  AND c1.pathways = c2.pathways
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
    (coalesce(cla.current_arr_usd_ccfx::numeric, 0)),
    2
  ) as current_arr_usd_ccfx,
  round((coalesce(cla.prior_arr_usd_ccfx::numeric, 0)), 2) as prior_arr_usd_ccfx,
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
  
  Insert Into ryzlan.sst_product_pathways_bridge 
  select * from arr_product_bridge_tmp;


END;
$$


TRUNCATE TABLE ryzlan.sst_product_pathways_bridge;
SELECT ryzlan.sp_populate_sst_product_pathways('2019M01');
SELECT ryzlan.sp_populate_sst_product_pathways('2019M02');
SELECT ryzlan.sp_populate_sst_product_pathways('2019M03');
SELECT ryzlan.sp_populate_sst_product_pathways('2019M04');
SELECT ryzlan.sp_populate_sst_product_pathways('2019M05');
SELECT ryzlan.sp_populate_sst_product_pathways('2019M06');
SELECT ryzlan.sp_populate_sst_product_pathways('2019M07');
SELECT ryzlan.sp_populate_sst_product_pathways('2019M08');
SELECT ryzlan.sp_populate_sst_product_pathways('2019M09');
SELECT ryzlan.sp_populate_sst_product_pathways('2019M10');
SELECT ryzlan.sp_populate_sst_product_pathways('2019M11');
SELECT ryzlan.sp_populate_sst_product_pathways('2019M12');
SELECT ryzlan.sp_populate_sst_product_pathways('2020M01');
SELECT ryzlan.sp_populate_sst_product_pathways('2020M02');
SELECT ryzlan.sp_populate_sst_product_pathways('2020M03');
SELECT ryzlan.sp_populate_sst_product_pathways('2020M04');
SELECT ryzlan.sp_populate_sst_product_pathways('2020M05');
SELECT ryzlan.sp_populate_sst_product_pathways('2020M06');
SELECT ryzlan.sp_populate_sst_product_pathways('2020M07');
SELECT ryzlan.sp_populate_sst_product_pathways('2020M08');
SELECT ryzlan.sp_populate_sst_product_pathways('2020M09');
SELECT ryzlan.sp_populate_sst_product_pathways('2020M10');
SELECT ryzlan.sp_populate_sst_product_pathways('2020M11');
SELECT ryzlan.sp_populate_sst_product_pathways('2020M12');
SELECT ryzlan.sp_populate_sst_product_pathways('2021M01');
SELECT ryzlan.sp_populate_sst_product_pathways('2021M02');
SELECT ryzlan.sp_populate_sst_product_pathways('2021M03');
SELECT ryzlan.sp_populate_sst_product_pathways('2021M04');
SELECT ryzlan.sp_populate_sst_product_pathways('2021M05');
SELECT ryzlan.sp_populate_sst_product_pathways('2021M06');
SELECT ryzlan.sp_populate_sst_product_pathways('2021M07');
SELECT ryzlan.sp_populate_sst_product_pathways('2021M08');
SELECT ryzlan.sp_populate_sst_product_pathways('2021M09');
SELECT ryzlan.sp_populate_sst_product_pathways('2021M10');
SELECT ryzlan.sp_populate_sst_product_pathways('2021M11');
SELECT ryzlan.sp_populate_sst_product_pathways('2021M12');
SELECT ryzlan.sp_populate_sst_product_pathways('2022M01');
SELECT ryzlan.sp_populate_sst_product_pathways('2022M02');
SELECT ryzlan.sp_populate_sst_product_pathways('2022M03');
SELECT ryzlan.sp_populate_sst_product_pathways('2022M04');
SELECT ryzlan.sp_populate_sst_product_pathways('2022M05');
SELECT ryzlan.sp_populate_sst_product_pathways('2022M06');
SELECT ryzlan.sp_populate_sst_product_pathways('2022M07');
SELECT ryzlan.sp_populate_sst_product_pathways('2022M08');
SELECT ryzlan.sp_populate_sst_product_pathways('2022M09');
SELECT ryzlan.sp_populate_sst_product_pathways('2022M10');
SELECT ryzlan.sp_populate_sst_product_pathways('2022M11');
SELECT ryzlan.sp_populate_sst_product_pathways('2022M12');
SELECT ryzlan.sp_populate_sst_product_pathways('2023M01');
SELECT ryzlan.sp_populate_sst_product_pathways('2023M02');
SELECT ryzlan.sp_populate_sst_product_pathways('2023M03');
SELECT ryzlan.sp_populate_sst_product_pathways('2023M04');
SELECT ryzlan.sp_populate_sst_product_pathways('2023M05');
SELECT ryzlan.sp_populate_sst_product_pathways('2023M06');
SELECT ryzlan.sp_populate_sst_product_pathways('2023M07');
SELECT ryzlan.sp_populate_sst_product_pathways('2023M08');
SELECT ryzlan.sp_populate_sst_product_pathways('2023M09');
SELECT ryzlan.sp_populate_sst_product_pathways('2023M10');
SELECT ryzlan.sp_populate_sst_product_pathways('2023M11');
SELECT ryzlan.sp_populate_sst_product_pathways('2023M12');


ALTER TABLE ryzlan.sst_product_pathways_bridge
  RENAME COLUMN prior_arr_usd_ccfx TO prior_period_product_arr_usd_ccfx;
ALTER TABLE ryzlan.sst_product_pathways_bridge
  RENAME COLUMN current_arr_usd_ccfx TO current_period_product_arr_usd_ccfx;
ALTER TABLE ryzlan.sst_product_pathways_bridge
  RENAME COLUMN prior_arr_lcu TO prior_period_product_arr_lcu;
ALTER TABLE ryzlan.sst_product_pathways_bridge
  RENAME COLUMN current_arr_lcu TO current_period_product_arr_lcu;
ALTER TABLE ryzlan.sst_product_pathways_bridge
  RENAME COLUMN baseline_currency TO currency_code;