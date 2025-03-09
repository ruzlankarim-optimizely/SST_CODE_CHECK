create or replace function ryzlan.sp_populate_sst_product_pathway2(var_period text) returns void language plpgsql as $$ BEGIN
DELETE from ryzlan.sst_product_pathways_bridge2
where evaluation_period = var_period;
DROP TABLE IF EXISTS sst_temp;
CREATE TEMP TABLE sst_temp AS
SELECT *,
    CASE
    WHEN migration_from is not null THEN migration_from
    WHEN migration_to is not null THEN migration_to
    ELSE 'USUAL'
  END AS pathways
FROM sandbox_pd.sst_adhoc
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
  FULL OUTER JOIN prior_period_customer_arr_tmp c2 ON c1.master_customer_id = c2.master_customer_id
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
  cla.baseline_currency as currency_code,
  round(
    (coalesce(cla.current_arr_usd_ccfx::numeric, 0)),
    2
  ) as current_period_product_arr_usd_ccfx,
  round(
    (coalesce(cla.prior_arr_usd_ccfx::numeric, 0)),
    2
  ) as prior_period_product_arr_usd_ccfx,
  round(
    (
      coalesce(cla.current_arr_usd_ccfx::numeric, 0) - coalesce(cla.prior_arr_usd_ccfx::numeric, 0)
    ),
    2
  ) AS product_arr_change_ccfx,
  round((coalesce(cla.current_arr_lcu::numeric, 0)), 2) as current_period_product_arr_lcu,
  round((coalesce(cla.prior_arr_lcu::numeric, 0)), 2) as prior_period_product_arr_lcu,
  round(
    (
      coalesce(cla.current_arr_lcu::numeric, 0) - coalesce(cla.prior_arr_lcu::numeric, 0)
    ),
    2
  ) AS product_arr_change_lcu,
  null as product_bridge 
FROM customer_level_arr_tmp cla
  CROSS JOIN ufdm_grey.periods per
WHERE 1 = 1
  AND per.evaluation_period = var_period;

Insert Into ryzlan.sst_product_pathways_bridge2
select *
from arr_product_bridge_tmp;
END;
$$ 