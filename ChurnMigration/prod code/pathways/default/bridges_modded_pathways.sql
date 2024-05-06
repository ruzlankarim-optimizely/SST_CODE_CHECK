--- Product_Group_Solution_CB_modded
-- PRODUCT GROUP BRIDGE MODDED 
DROP TABLE IF EXISTS sandbox.PG_modded_pathways;
CREATE TABLE sandbox.PG_modded_pathways AS
SELECT *
FROM ufdm.sst_product_bridge_product_group;
ALTER TABLE sandbox.PG_modded_pathways DROP COLUMN id;
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
FROM sandbox.churn_migration_main_pathways
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
FROM sandbox.PG_modded_pathways AS a
  JOIN sandbox.churn_migration_test_pg AS b ON a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.currency_code = b.currency_code
  AND a.current_product_group = b.current_product_group
  AND a.prior_product_group = b.prior_product_group
  AND a.product_bridge = b.pg_bridge
WHERE lower("PG Migration: Classification") ILIKE ('%migration')
  AND "PG Leftover: Rolled Up Amount" IS NULL;
DROP TABLE IF EXISTS sandbox.PG_migration_split;
CREATE TABLE sandbox.PG_migration_split AS
SELECT a.*,
  "PG Migration: Rolled Up Amount",
  "PG Leftover: Rolled Up Amount",
  "PG Migration: Rolled Up Amount LCU",
  "PG Leftover: Rolled Up Amount LCU",
  "PG Migration: Classification",
  "PG Leftover: Classification"
FROM sandbox.PG_modded_pathways AS a
  JOIN sandbox.churn_migration_test_pg AS b ON a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.currency_code = b.currency_code
  AND a.current_product_group = b.current_product_group
  AND a.prior_product_group = b.prior_product_group
  AND a.product_bridge = b.pg_bridge --AND round(a.product_arr_change_ccfx)  = round(b.pg_arr_change) 
WHERE "PG Migration: Classification" ILIKE ('%migration')
  AND "PG Leftover: Rolled Up Amount" IS NOT NULL;
DELETE FROM sandbox.PG_modded_pathways AS a USING sandbox.PG_migration_default AS b
WHERE a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.currency_code = b.currency_code
  AND a.current_product_group = b.current_product_group
  AND a.prior_product_group = b.prior_product_group
  AND a.product_bridge = b.product_bridge;
INSERT INTO sandbox.PG_modded_pathways AS a (
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
  prior_period_product_arr_usd_ccfx,
  current_period_product_arr_usd_ccfx,
  product_arr_change_ccfx,
  prior_period_product_arr_lcu,
  current_period_product_arr_lcu,
  product_arr_change_lcu,
  --    product_bridge ,
  COALESCE("PG Migration: Classification", product_bridge),
  winback_period_days,
  wip_flag,
  price_increase_amount,
  subsidiary_entity_name,
  churn_period,
  customer_bridge
FROM sandbox.PG_migration_default AS b
WHERE mcid = b.mcid
  AND evaluation_period = b.evaluation_period
  AND currency_code = b.currency_code
  AND current_product_group = b.current_product_group
  AND prior_product_group = b.prior_product_group
  AND product_bridge = b.product_bridge;
DELETE FROM sandbox.PG_modded_pathways AS a USING sandbox.PG_migration_split AS b
WHERE a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.currency_code = b.currency_code
  AND a.current_product_group = b.current_product_group
  AND a.prior_product_group = b.prior_product_group
  AND a.product_bridge = b.product_bridge;
INSERT INTO sandbox.PG_modded_pathways AS a (
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
  --    product_bridge ,
  COALESCE("PG Migration: Classification", product_bridge),
  winback_period_days,
  wip_flag,
  price_increase_amount,
  subsidiary_entity_name,
  churn_period,
  customer_bridge
FROM sandbox.PG_migration_split AS b
WHERE mcid = b.mcid
  AND evaluation_period = b.evaluation_period
  AND currency_code = b.currency_code
  AND current_product_group = b.current_product_group
  AND prior_product_group = b.prior_product_group
  AND product_bridge = b.product_bridge;
INSERT INTO sandbox.PG_modded_pathways AS a (
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
    "PG Leftover: Rolled Up Amount" / product_arr_change_ccfx
  ),
  current_period_product_arr_usd_ccfx * abs(
    "PG Leftover: Rolled Up Amount" / product_arr_change_ccfx
  ),
  --  product_arr_change_ccfx ,
  -- change this to default once and then to migrated value
  "PG Leftover: Rolled Up Amount",
  prior_period_product_arr_lcu * abs(
    "PG Leftover: Rolled Up Amount LCU" / product_arr_change_lcu
  ),
  current_period_product_arr_lcu * abs(
    "PG Leftover: Rolled Up Amount LCU" / product_arr_change_lcu
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
  customer_bridge
FROM sandbox.PG_migration_split AS b
WHERE mcid = b.mcid
  AND evaluation_period = b.evaluation_period
  AND currency_code = b.currency_code
  AND current_product_group = b.current_product_group
  AND prior_product_group = b.prior_product_group
  AND product_bridge = b.product_bridge;
----- PRODUCT SOLUTION MODDED 
DROP TABLE IF EXISTS sandbox.PS_modded_pathways;
CREATE TABLE sandbox.PS_modded_pathways AS
SELECT *
FROM ufdm.sst_product_bridge_product_solution;
ALTER TABLE sandbox.PS_modded_pathways DROP COLUMN id;
DROP TABLE IF EXISTS sandbox.churn_migration_test_ps;
CREATE TABLE sandbox.churn_migration_test_ps AS
SELECT DISTINCT mcid,
  evaluation_period,
  currency_code,
  current_product_solution,
  prior_product_solution,
  "PS Migration: Rolled Up Amount",
  "PS Leftover: Rolled Up Amount",
  "PS Migration: Rolled Up Amount LCU",
  "PS Leftover: Rolled Up Amount LCU",
  ps_bridge,
  "PS Migration: Classification",
  "PS Leftover: Classification"
FROM sandbox.churn_migration_main_pathways
WHERE mcid NOT IN ('-');
DROP TABLE IF EXISTS sandbox.PS_migration_default;
CREATE TABLE sandbox.PS_migration_default AS
SELECT a.*,
  "PS Migration: Rolled Up Amount",
  "PS Leftover: Rolled Up Amount",
  "PS Migration: Rolled Up Amount LCU",
  "PS Leftover: Rolled Up Amount LCU",
  ps_bridge,
  "PS Migration: Classification"
FROM sandbox.PS_modded_pathways AS a
  JOIN sandbox.churn_migration_test_ps AS b ON a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.currency_code = b.currency_code
  AND a.current_product_solution = b.current_product_solution
  AND a.prior_product_solution = b.prior_product_solution
  AND a.product_bridge = b.ps_bridge
WHERE "PS Migration: Classification" ILIKE ('%migration')
  AND "PS Leftover: Rolled Up Amount" IS NULL;
DROP TABLE IF EXISTS sandbox.PS_migration_split;
CREATE TABLE sandbox.PS_migration_split AS
SELECT a.*,
  "PS Migration: Rolled Up Amount",
  "PS Leftover: Rolled Up Amount",
  "PS Migration: Rolled Up Amount LCU",
  "PS Leftover: Rolled Up Amount LCU",
  "PS Migration: Classification",
  "PS Leftover: Classification"
FROM sandbox.PS_modded_pathways AS a
  JOIN sandbox.churn_migration_test_ps AS b ON a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.currency_code = b.currency_code
  AND a.prior_product_solution = b.prior_product_solution
  AND a.current_product_solution = b.current_product_solution
  AND a.product_bridge = b.ps_bridge
WHERE "PS Migration: Classification" ILIKE ('%migration')
  AND "PS Leftover: Rolled Up Amount" IS NOT NULL;
DELETE FROM sandbox.PS_modded_pathways AS a USING sandbox.PS_migration_default AS b
WHERE a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.currency_code = b.currency_code
  AND a.prior_product_solution = b.prior_product_solution
  AND a.current_product_solution = b.current_product_solution
  AND a.product_bridge = b.product_bridge;
INSERT INTO sandbox.PS_modded_pathways AS a (
    evaluation_period,
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
    winback_period_days,
    wip_flag,
    price_increase_amount,
    subsidiary_entity_name,
    churn_period,
    customer_bridge
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
  --    product_bridge ,
  COALESCE("PS Migration: Classification", product_bridge),
  winback_period_days,
  wip_flag,
  price_increase_amount,
  subsidiary_entity_name,
  churn_period,
  customer_bridge
FROM sandbox.PS_migration_default AS b
WHERE mcid = b.mcid
  AND evaluation_period = b.evaluation_period
  AND currency_code = b.currency_code
  AND prior_product_solution = b.prior_product_solution
  AND current_product_solution = b.current_product_solution
  AND product_bridge = b.product_bridge;
DELETE FROM sandbox.PS_modded_pathways AS a USING sandbox.PS_migration_split AS b
WHERE a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.currency_code = b.currency_code
  AND a.prior_product_solution = b.prior_product_solution
  AND a.current_product_solution = b.current_product_solution
  AND a.product_bridge = b.product_bridge;
INSERT INTO sandbox.PS_modded_pathways AS a (
    evaluation_period,
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
    winback_period_days,
    wip_flag,
    price_increase_amount,
    subsidiary_entity_name,
    churn_period,
    customer_bridge
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
  prior_period_product_arr_usd_ccfx * abs(
    "PS Migration: Rolled Up Amount" /CASE
      WHEN round(product_arr_change_ccfx) = 0
      OR product_arr_change_ccfx IS NULL THEN 1
      ELSE product_arr_change_ccfx
    END
  ),
  current_period_product_arr_usd_ccfx * abs(
    "PS Migration: Rolled Up Amount" /CASE
      WHEN round(product_arr_change_ccfx) = 0
      OR product_arr_change_ccfx IS NULL THEN 1
      ELSE product_arr_change_ccfx
    END
  ),
  --  product_arr_change_ccfx ,
  "PS Migration: Rolled Up Amount",
  prior_period_product_arr_lcu * abs(
    "PS Migration: Rolled Up Amount LCU" /CASE
      WHEN round(product_arr_change_lcu) = 0
      OR product_arr_change_lcu IS NULL THEN 1
      ELSE product_arr_change_lcu
    END
  ),
  current_period_product_arr_lcu * abs(
    "PS Migration: Rolled Up Amount LCU" /CASE
      WHEN round(product_arr_change_lcu) = 0
      OR product_arr_change_lcu IS NULL THEN 1
      ELSE product_arr_change_lcu
    END
  ),
  "PS Migration: Rolled Up Amount LCU",
  --    product_bridge ,
  COALESCE("PS Migration: Classification", product_bridge),
  winback_period_days,
  wip_flag,
  price_increase_amount,
  subsidiary_entity_name,
  churn_period,
  customer_bridge
FROM sandbox.PS_migration_split AS b
WHERE mcid = b.mcid
  AND evaluation_period = b.evaluation_period
  AND currency_code = b.currency_code
  AND prior_product_solution = b.prior_product_solution
  AND current_product_solution = b.current_product_solution
  AND product_bridge = b.product_bridge;
INSERT INTO sandbox.PS_modded_pathways AS a(
    evaluation_period,
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
    winback_period_days,
    wip_flag,
    price_increase_amount,
    subsidiary_entity_name,
    churn_period,
    customer_bridge
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
  prior_period_product_arr_usd_ccfx * abs(
    "PS Leftover: Rolled Up Amount" /CASE
      WHEN round(product_arr_change_ccfx) = 0
      OR product_arr_change_ccfx IS NULL THEN 1
      ELSE product_arr_change_ccfx
    END
  ),
  current_period_product_arr_usd_ccfx * abs(
    "PS Leftover: Rolled Up Amount" / CASE
      WHEN round(product_arr_change_ccfx) = 0
      OR product_arr_change_ccfx IS NULL THEN 1
      ELSE product_arr_change_ccfx
    END
  ),
  --  product_arr_change_ccfx ,
  -- change this to default once and then to migrated value
  "PS Leftover: Rolled Up Amount",
  prior_period_product_arr_lcu * abs(
    "PS Leftover: Rolled Up Amount LCU" / CASE
      WHEN round(product_arr_change_lcu) = 0
      OR product_arr_change_lcu IS NULL THEN 1
      ELSE product_arr_change_lcu
    END
  ),
  current_period_product_arr_lcu * abs(
    "PS Leftover: Rolled Up Amount LCU" / CASE
      WHEN round(product_arr_change_lcu) = 0
      OR product_arr_change_lcu IS NULL THEN 1
      ELSE product_arr_change_lcu
    END
  ),
  "PS Leftover: Rolled Up Amount LCU",
  --    default_value_lcu ,
  --    product_bridge ,
  COALESCE("PS Leftover: Classification", product_bridge),
  winback_period_days,
  wip_flag,
  price_increase_amount,
  subsidiary_entity_name,
  churn_period,
  customer_bridge
FROM sandbox.PS_migration_split AS b
WHERE mcid = b.mcid
  AND evaluation_period = b.evaluation_period
  AND currency_code = b.currency_code
  AND prior_product_solution = b.prior_product_solution
  AND current_product_solution = b.current_product_solution
  AND product_bridge = b.product_bridge;
-- CUSTOMER BRIDGE MODDED 
DROP TABLE IF EXISTS sandbox.CB_modded_pathways;
CREATE TABLE sandbox.CB_modded_pathways AS
SELECT *
FROM ufdm.sst_customer_bridge scb;
ALTER TABLE sandbox.CB_modded_pathways DROP COLUMN id;
DROP TABLE IF EXISTS sandbox.churn_migration_test_cb;
CREATE TABLE sandbox.churn_migration_test_cb AS
SELECT DISTINCT mcid,
  evaluation_period,
  currency_code,
  customer_bridge,
  "CB Migration: Rolled Up Amount",
  "CB Migration: Rolled Up Amount LCU",
  "CB Leftover: Rolled Up Amount",
  "CB Leftover: Rolled Up Amount LCU",
  "CB Migration: Classification",
  "CB Leftover: Classification"
FROM sandbox.churn_migration_main_pathways
WHERE mcid NOT IN ('-');
DROP TABLE IF EXISTS sandbox.CB_migration_default;
CREATE TABLE sandbox.CB_migration_default AS
SELECT a.*,
  "CB Migration: Rolled Up Amount",
  "CB Migration: Rolled Up Amount LCU",
  "CB Leftover: Rolled Up Amount",
  "CB Leftover: Rolled Up Amount LCU",
  "CB Migration: Classification"
FROM sandbox.CB_modded_pathways AS a
  JOIN sandbox.churn_migration_test_cb AS b ON a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.baseline_currency = b.currency_code
  AND a.customer_bridge = b.customer_bridge
WHERE "CB Migration: Classification" ILIKE ('%migration')
  AND "CB Leftover: Rolled Up Amount" IS NULL;
DROP TABLE IF EXISTS sandbox.CB_migration_split;
CREATE TABLE sandbox.CB_migration_split AS
SELECT a.*,
  "CB Migration: Rolled Up Amount",
  "CB Migration: Rolled Up Amount LCU",
  "CB Leftover: Rolled Up Amount",
  "CB Leftover: Rolled Up Amount LCU",
  "CB Migration: Classification",
  "CB Leftover: Classification"
FROM sandbox.CB_modded_pathways AS a
  JOIN sandbox.churn_migration_test_cb AS b ON a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.baseline_currency = b.currency_code
  AND a.customer_bridge = b.customer_bridge
WHERE "CB Migration: Classification" ILIKE ('%migration')
  AND "CB Leftover: Rolled Up Amount" IS NOT NULL;
DELETE FROM sandbox.CB_modded_pathways AS a USING sandbox.CB_migration_default AS b
WHERE a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.baseline_currency = b.baseline_currency
  AND a.customer_bridge = b.customer_bridge;
INSERT INTO sandbox.CB_modded_pathways AS a (
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    "name",
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
SELECT evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  "name",
  baseline_currency,
  subsidiary_entity_name,
  prior_period_customer_arr_usd_ccfx,
  current_period_customer_arr_usd_ccfx,
  customer_arr_change_ccfx,
  prior_period_customer_arr_lcu,
  current_period_customer_lcu,
  customer_arr_change_lcu,
  --    customer_bridge, 
  COALESCE (
    "CB Migration: Classification",
    customer_bridge
  ),
  winback_period_days,
  wip_flag
FROM sandbox.CB_migration_default AS b
WHERE mcid = b.mcid
  AND evaluation_period = b.evaluation_period
  AND baseline_currency = b.baseline_currency
  AND customer_bridge = b.customer_bridge;
DELETE FROM sandbox.CB_modded_pathways AS a USING sandbox.CB_migration_split AS b
WHERE a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.baseline_currency = b.baseline_currency
  AND a.customer_bridge = b.customer_bridge;
INSERT INTO sandbox.CB_modded_pathways AS a (
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    "name",
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
SELECT evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  "name",
  baseline_currency,
  subsidiary_entity_name,
  prior_period_customer_arr_usd_ccfx * abs(
    "CB Migration: Rolled Up Amount" /CASE
      WHEN round(customer_arr_change_ccfx) = 0
      OR customer_arr_change_ccfx IS NULL THEN 1
      ELSE customer_arr_change_ccfx
    END
  ),
  current_period_customer_arr_usd_ccfx * abs(
    "CB Migration: Rolled Up Amount" /CASE
      WHEN round(customer_arr_change_ccfx) = 0
      OR customer_arr_change_ccfx IS NULL THEN 1
      ELSE customer_arr_change_ccfx
    END
  ),
  --    customer_arr_change_ccfx, 
  "CB Migration: Rolled Up Amount",
  prior_period_customer_arr_lcu * abs(
    "CB Migration: Rolled Up Amount LCU" /CASE
      WHEN round(customer_arr_change_lcu) = 0
      OR customer_arr_change_lcu IS NULL THEN 1
      ELSE customer_arr_change_lcu
    END
  ),
  current_period_customer_lcu * abs(
    "CB Migration: Rolled Up Amount LCU" /CASE
      WHEN round(customer_arr_change_lcu) = 0
      OR customer_arr_change_lcu IS NULL THEN 1
      ELSE customer_arr_change_lcu
    END
  ),
  --    customer_arr_change_lcu, 
  "CB Migration: Rolled Up Amount LCU",
  --    customer_bridge, 
  COALESCE("CB Migration: Classification", customer_bridge),
  winback_period_days,
  wip_flag
FROM sandbox.CB_migration_split AS b
WHERE mcid = b.mcid
  AND evaluation_period = b.evaluation_period
  AND baseline_currency = b.baseline_currency
  AND customer_bridge = b.customer_bridge;
INSERT INTO sandbox.CB_modded_pathways AS a (
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    "name",
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
SELECT evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  "name",
  baseline_currency,
  subsidiary_entity_name,
  prior_period_customer_arr_usd_ccfx * abs(
    "CB Leftover: Rolled Up Amount" /CASE
      WHEN round(customer_arr_change_ccfx) = 0
      OR customer_arr_change_ccfx IS NULL THEN 1
      ELSE customer_arr_change_ccfx
    END
  ),
  current_period_customer_arr_usd_ccfx * abs(
    "CB Leftover: Rolled Up Amount" /CASE
      WHEN round(customer_arr_change_ccfx) = 0
      OR customer_arr_change_ccfx IS NULL THEN 1
      ELSE customer_arr_change_ccfx
    END
  ),
  --    customer_arr_change_ccfx, 
  "CB Leftover: Rolled Up Amount",
  prior_period_customer_arr_lcu * abs(
    "CB Leftover: Rolled Up Amount LCU" /CASE
      WHEN round(customer_arr_change_lcu) = 0
      OR customer_arr_change_lcu IS NULL THEN 1
      ELSE customer_arr_change_lcu
    END
  ),
  current_period_customer_lcu * abs(
    "CB Leftover: Rolled Up Amount LCU" /CASE
      WHEN round(customer_arr_change_lcu) = 0
      OR customer_arr_change_lcu IS NULL THEN 1
      ELSE customer_arr_change_lcu
    END
  ),
  --    customer_arr_change_lcu, 
  "CB Leftover: Rolled Up Amount LCU",
  --    customer_bridge, 
  COALESCE("CB Leftover: Classification", customer_bridge),
  winback_period_days,
  wip_flag
FROM sandbox.CB_migration_split AS b
WHERE mcid = b.mcid
  AND evaluation_period = b.evaluation_period
  AND baseline_currency = b.baseline_currency
  AND customer_bridge = b.customer_bridge;