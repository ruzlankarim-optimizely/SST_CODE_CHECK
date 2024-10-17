-- New script in dw-prod-rds-master.cr9dekxonyuj.us-east-1.rds.amaz.
-- Date: May 31, 2024
-- Time: 9:33:22 PM
DROP TABLE IF EXISTS ryzlan.sst_ending_arr_tester_reversals;
CREATE TABLE ryzlan.sst_ending_arr_tester_reversals AS WITH main AS (
  SELECT *,
    round(
      sum(arr) OVER(
        PARTITION BY mcid,
        snapshot_date,
        base_currency,
        updated_product_group
      )
    ) AS sum_arr,
    round(
      sum(baseline_arr_local_currency) OVER(
        PARTITION BY mcid,
        snapshot_date,
        base_currency,
        updated_product_group
      )
    ) AS sum_baseline_arr_local_currency
  FROM ufdm_archive.sst_lcoked_05062024_1704 -- TO BE CHANGED 
)
SELECT *,
  arr / CASE
    WHEN sum_arr = 0
    OR sum_arr IS NULL THEN 1
    ELSE sum_arr
  END AS ratio_arr,
  baseline_arr_local_currency / CASE
    WHEN sum_baseline_arr_local_currency = 0
    OR sum_baseline_arr_local_currency IS NULL THEN 1
    ELSE sum_baseline_arr_local_currency
  END AS ratio_arr_local_currency,
  '' AS ending_arr_comment
FROM main --WHERE mcid IN ( '4e128cce-793a-e811-8124-70106faab5f1' , 'e412863b-fd0a-4234-953b-188bc6f848fe')
;
DROP TABLE IF EXISTS ending_arr_marker_base;
CREATE TEMP TABLE ending_arr_marker_base AS WITH main AS (
  SELECT --id,
    evaluation_period,
    TO_CHAR(
      (
        CASE
          WHEN current_period IS NULL
          AND prior_period IS NOT NULL THEN date(
            date_trunc('month', prior_period) + INTERVAL '1 month' + INTERVAL '1 month - 1 day'
          )
          ELSE current_period
        END
      )::DATE,
      'YYYY-MM'
    ) AS eval_period,
    CASE
      WHEN current_period IS NULL
      AND prior_period IS NOT NULL THEN date(
        date_trunc('month', prior_period) + INTERVAL '1 month' + INTERVAL '1 month - 1 day'
      )
      ELSE current_period
    END AS current_period,
    CASE
      WHEN prior_period IS NULL
      AND current_period IS NOT NULL THEN date(
        date_trunc('month', current_period) - INTERVAL '1 month' + INTERVAL '1 month - 1 day'
      )
      ELSE prior_period
    END AS prior_period,
    mcid,
    currency_code,
    prior_product_group,
    current_product_group,
    prior_period_product_arr_usd_ccfx,
    current_period_product_arr_usd_ccfx,
    product_arr_change_ccfx,
    product_arr_change_lcu,
    product_bridge
  FROM ufdm_archive.sst_product_bridge_product_group_lcoked_05062024_1704 -- TO BE CHANGED
   
)
SELECT --    id,
  mcid,
  evaluation_period,
  eval_period,
  currency_code,
  current_period,
  prior_period,
  prior_product_group,
  current_product_group,
  current_period_product_arr_usd_ccfx,
  prior_period_product_arr_usd_ccfx,
  product_arr_change_ccfx,
  product_arr_change_lcu,
  dense_rank() OVER (
    --        PARTITION BY mcid,
    --        eval_period
    ORDER BY mcid,
      eval_period
  ) AS row_num,
  product_bridge
FROM main;
DROP TABLE IF EXISTS ending_arr_marker_history;
CREATE TEMP TABLE ending_arr_marker_history AS
SELECT *
FROM ending_arr_marker_base
WHERE product_bridge IN (
    'Price Uplift',
    'Downsell',
    'Downgrade',
    'Cross-sell',
    'Up Sell',
    'Churn'
  );
DROP TABLE IF EXISTS ending_arr_market_cpi_part;
CREATE TEMP TABLE ending_arr_market_cpi_part AS
SELECT *
FROM (
    SELECT a.*,
      b.current_period_product_arr_usd_ccfx AS cpiR_start_arr,
      b.current_period AS cpiR_start_date,
      a.prior_period AS cpiR_end_date,
      a.product_arr_change_ccfx AS cpiR_delta_arr,
      a.product_arr_change_lcu AS cpiR_delta_arr_lcu,
      --      b.product_bridge , 
      ROW_NUMBER () OVER(
        PARTITION BY a.mcid,
        a.eval_period,
        concat(
          a.prior_product_group,
          '->',
          a.current_product_group
        ),
        a.product_bridge
        ORDER BY b.current_period desc
      ) AS cpi_rnk
    FROM ending_arr_marker_base AS a
      JOIN ending_arr_marker_history AS b ON a.mcid = b.mcid
      AND concat(
        a.prior_product_group,
        '->',
        a.current_product_group
      ) = concat(
        b.prior_product_group,
        '->',
        b.current_product_group
      )
      AND a.product_bridge = 'Price Uplift Reversal'
      AND b.product_bridge = 'Price Uplift'
      AND b.current_period <= a.current_period
      AND DATE_PART(
        'Day',
        a.current_period::TIMESTAMP - b.current_period::TIMESTAMP
      ) < 186
  ) AS a
WHERE cpi_rnk = 1;
DROP TABLE IF EXISTS ending_arr_market_upsell_part;
CREATE TEMP TABLE ending_arr_market_upsell_part AS
SELECT *
FROM (
    SELECT a.*,
      b.current_period_product_arr_usd_ccfx AS upsellR_start_arr,
      b.current_period AS upsellR_start_date,
      a.prior_period AS upsellR_end_date,
      a.product_arr_change_ccfx AS upsellR_delta_arr,
      a.product_arr_change_lcu AS upsellR_delta_arr_lcu,
      --      b.product_bridge , 
      ROW_NUMBER () OVER(
        PARTITION BY a.mcid,
        a.eval_period,
        concat(
          a.prior_product_group,
          '->',
          a.current_product_group
        ),
        a.product_bridge
        ORDER BY b.current_period desc
      ) AS upsell_rnk
    FROM ending_arr_marker_base AS a
      JOIN ending_arr_marker_history AS b ON a.mcid = b.mcid
      AND concat(
        a.prior_product_group,
        '->',
        a.current_product_group
      ) = concat(
        b.prior_product_group,
        '->',
        b.current_product_group
      )
      AND a.product_bridge = 'Up Sell Reversal'
      AND b.product_bridge = 'Up Sell'
      AND b.current_period <= a.current_period
      AND DATE_PART(
        'Day',
        a.current_period::TIMESTAMP - b.current_period::TIMESTAMP
      ) < 186
  ) AS a
WHERE upsell_rnk = 1;
DROP TABLE IF EXISTS ending_arr_market_crosssell_part;
CREATE TEMP TABLE ending_arr_market_crosssell_part AS
SELECT *
FROM (
    SELECT a.*,
      b.current_period_product_arr_usd_ccfx AS crossellR_start_arr,
      b.current_period AS crossellR_start_date,
      a.prior_period AS crossellR_end_date,
      a.product_arr_change_ccfx AS crossellR_delta_arr,
      a.product_arr_change_lcu AS crossellR_delta_arr_lcu,
      --      b.product_bridge ,
      ROW_NUMBER () OVER(
        PARTITION BY a.mcid,
        a.eval_period,
        concat(
          a.prior_product_group,
          '->',
          a.current_product_group
        ),
        a.product_bridge
        ORDER BY b.current_period desc
      ) AS crosssell_rnk
    FROM ending_arr_marker_base AS a
      JOIN ending_arr_marker_history AS b ON a.mcid = b.mcid
      AND a.prior_product_group = b.current_product_group
      AND a.product_bridge = 'Cross-sell Reversal'
      AND b.product_bridge = 'Cross-sell'
      AND b.current_period <= a.current_period
      AND DATE_PART(
        'Day',
        a.current_period::TIMESTAMP - b.current_period::TIMESTAMP
      ) < 186
  ) AS a --  WHERE crosssell_rnk = 1
;
DROP TABLE IF EXISTS ending_arr_market_winback_downgrade_part;
CREATE TEMP TABLE ending_arr_market_winback_downgrade_part AS
SELECT *
FROM (
    SELECT a.*,
      b.current_period_product_arr_usd_ccfx AS winback_downgrade_start_arr,
      b.current_period AS winback_downgrade_start_date,
      b.prior_period AS winback_downgrade_pull_back_date,
      a.prior_period AS winback_downgrade_end_date,
      a.product_arr_change_ccfx AS winback_downgrade_delta_arr,
      a.product_arr_change_lcu AS winback_downgrade_delta_arr_lcu,
      --      b.product_bridge ,
      ROW_NUMBER () OVER(
        PARTITION BY a.mcid,
        a.eval_period,
        concat(
          a.prior_product_group,
          '->',
          a.current_product_group
        ),
        a.product_bridge
        ORDER BY b.current_period desc
      ) AS winback_downgrade_rnk
    FROM ending_arr_marker_base AS a
      JOIN ending_arr_marker_history AS b ON a.mcid = b.mcid
      AND concat(
        a.prior_product_group,
        '->',
        a.current_product_group
      ) = concat(
        b.prior_product_group,
        '->',
        b.current_product_group
      )
      AND a.product_bridge = 'Win back Downgrade'
      AND b.product_bridge = 'Downgrade' --      AND round(a.product_arr_change_ccfx) = round(b.product_arr_change_ccfx)
      AND b.current_period <= a.current_period
      AND DATE_PART(
        'Day',
        a.current_period::TIMESTAMP - b.current_period::TIMESTAMP
      ) < 186
  ) AS a
WHERE winback_downgrade_rnk = 1;
DROP TABLE IF EXISTS ending_arr_market_winback_downsell_part;
CREATE TEMP TABLE ending_arr_market_winback_downsell_part AS
SELECT *
FROM (
    SELECT a.*,
      b.current_period_product_arr_usd_ccfx AS winback_downsell_start_arr,
      b.current_period AS winback_downsell_start_date,
      b.prior_period AS winback_downsell_pull_back_date,
      a.prior_period AS winback_downsell_end_date,
      a.product_arr_change_ccfx AS winback_downsell_delta_arr,
      a.product_arr_change_lcu AS winback_downsell_delta_arr_lcu,
      --      b.product_bridge ,
      ROW_NUMBER () OVER(
        PARTITION BY a.mcid,
        a.eval_period,
        concat(
          a.prior_product_group,
          '->',
          a.current_product_group
        ),
        a.product_bridge
        ORDER BY b.current_period desc
      ) AS winback_downsell_rnk
    FROM ending_arr_marker_base AS a
      JOIN ending_arr_marker_history AS b ON a.mcid = b.mcid
      AND concat(
        a.prior_product_group,
        '->',
        a.current_product_group
      ) = concat(
        b.prior_product_group,
        '->',
        b.current_product_group
      )
      AND a.product_bridge = 'Win back Downsell'
      AND b.product_bridge IN ('Churn', 'Downsell') --      AND round(a.product_arr_change_ccfx) = round(b.product_arr_change_ccfx)
      AND b.current_period <= a.current_period
      AND DATE_PART(
        'Day',
        a.current_period::TIMESTAMP - b.current_period::TIMESTAMP
      ) < 186
  ) AS a
WHERE winback_downsell_rnk = 1;
DROP TABLE IF EXISTS ending_arr_market_winback_part;
CREATE TEMP TABLE ending_arr_market_winback_part AS
SELECT *
FROM (
    SELECT a.*,
      b.current_period_product_arr_usd_ccfx AS winback_start_arr,
      b.current_period AS winback_start_date,
      b.prior_period AS winback_pull_back_date,
      a.prior_period AS winback_end_date,
      a.product_arr_change_ccfx AS winback_delta_arr,
      a.product_arr_change_lcu AS winback_delta_arr_lcu,
      --      b.product_bridge ,
      ROW_NUMBER () OVER(
        PARTITION BY a.mcid,
        a.eval_period,
        a.current_product_group,
        b.prior_product_group,
        a.product_bridge
        ORDER BY b.current_period desc
      ) AS winback_rnk
    FROM ending_arr_marker_base AS a
      JOIN ending_arr_marker_history AS b ON a.mcid = b.mcid
      AND a.current_product_group = b.prior_product_group
      AND a.product_bridge = 'Winback'
      AND b.product_bridge IN ('Churn', 'Downsell') --      AND round(a.product_arr_change_ccfx) = round(b.product_arr_change_ccfx)
      AND b.current_period <= a.current_period
      AND DATE_PART(
        'Day',
        a.current_period::TIMESTAMP - b.current_period::TIMESTAMP
      ) < 186
  ) AS a
WHERE winback_rnk = 1;
--SELECT snapshot_date  ,
--    mcid , 
--    base_currency  , 
--    sku,
--    updated_product_group  , 
--    arr 
----    sum(arr) OVER(
----        PARTITION BY mcid,
----        snapshot_date,
----        base_currency,
----        updated_product_group  
----      ),
--    sum_arr,
--    ratio_arr
--    
--    FROM ryzlan.sst_ending_arr_tester_reversals  --ufdm.sst
--WHERE mcid = '30f35937-33a5-e811-814d-70106fa55dc1' AND snapshot_date  >=  '2023-05-30' AND snapshot_date  <= '2023-09-30'
--ORDER BY 1 , 5 
--
--SELECT snapshot_date  ,
--    mcid , 
--    base_currency  , 
--    updated_product_group  , 
--    arr ,
----    sum(arr) OVER(
----        PARTITION BY mcid,
----        snapshot_date,
----        base_currency,
----        updated_product_group  
----      ),
--    sum_arr,
--    ratio_arr
--    
--    FROM ryzlan.sst_ending_arr_tester_reversals  
--WHERE mcid = 'a01d967b-ae8e-bb3b-3dd0-f5fdaaf27cf5' AND snapshot_date  >=  '2023-09-30' AND snapshot_date  <= '2024-03-31'
--ORDER BY 1 , 4 
--LIMIT 10 
-- Fix Upsell Reversal
UPDATE ryzlan.sst_ending_arr_tester_reversals AS m
SET arr = round(
    CAST(
      (sum_arr + a.upsellR_delta_arr) * ratio_arr AS NUMERIC
    ),
    3
  ),
  sum_arr = round(
    CAST(sum_arr + a.upsellR_delta_arr AS NUMERIC),
    3
  ),
  baseline_arr_local_currency = round(
    CAST(
      (
        sum_baseline_arr_local_currency + a.upsellR_delta_arr_lcu
      ) * ratio_arr_local_currency AS NUMERIC
    ),
    3
  ),
  sum_baseline_arr_local_currency = round(
    CAST(
      sum_baseline_arr_local_currency + a.upsellR_delta_arr_lcu AS NUMERIC
    ),
    3
  ),
  ending_arr_comment = concat(
    ending_arr_comment,
    'Upsell Reversal Ending arr Fix applied'
  )
FROM ending_arr_market_upsell_part AS a
WHERE m.mcid = a.mcid
  AND m.updated_product_group = a.current_product_group
  AND m.snapshot_date >= a.upsellR_start_date
  AND m.snapshot_date <= a.upsellR_end_date;
-- Fix cross-sell  Reversal
UPDATE ryzlan.sst_ending_arr_tester_reversals AS m
SET arr = round(
    CAST(
      (sum_arr + a.crossellR_delta_arr) * ratio_arr AS NUMERIC
    ),
    3
  ),
  sum_arr = round(
    CAST(sum_arr + a.crossellR_delta_arr AS NUMERIC),
    3
  ),
  baseline_arr_local_currency = round(
    CAST(
      (
        sum_baseline_arr_local_currency + a.crossellR_delta_arr_lcu
      ) * ratio_arr_local_currency AS NUMERIC
    ),
    3
  ),
  sum_baseline_arr_local_currency = round(
    CAST(
      sum_baseline_arr_local_currency + a.crossellR_delta_arr_lcu AS NUMERIC
    ),
    3
  ),
  ending_arr_comment = concat(
    ending_arr_comment,
    'Cross sell  Reversal Ending arr Fix applied'
  )
FROM ending_arr_market_crosssell_part AS a
WHERE m.mcid = a.mcid
  AND m.updated_product_group = a.current_product_group
  AND m.snapshot_date >= a.crossellR_start_date
  AND m.snapshot_date <= a.crossellR_end_date;
-- winback downgrade fix
UPDATE ryzlan.sst_ending_arr_tester_reversals AS m
SET arr = round(
    CAST(
      (sum_arr + a.winback_downgrade_delta_arr) * ratio_arr AS NUMERIC
    ),
    3
  ),
  sum_arr = round(
    CAST(
      sum_arr + a.winback_downgrade_delta_arr AS NUMERIC
    ),
    3
  ),
  baseline_arr_local_currency = round(
    CAST(
      (
        sum_baseline_arr_local_currency + a.winback_downgrade_delta_arr_lcu
      ) * ratio_arr_local_currency AS NUMERIC
    ),
    3
  ),
  sum_baseline_arr_local_currency = round(
    CAST(
      sum_baseline_arr_local_currency + a.winback_downgrade_delta_arr_lcu AS NUMERIC
    ),
    3
  ),
  ending_arr_comment = concat(
    ending_arr_comment,
    'Winback Downgrade Ending arr Fix applied'
  )
FROM ending_arr_market_winback_downgrade_part AS a
WHERE m.mcid = a.mcid
  AND m.updated_product_group = a.current_product_group
  AND m.snapshot_date >= a.winback_downgrade_start_date
  AND m.snapshot_date <= a.winback_downgrade_end_date;
-- winback downsell fix
UPDATE ryzlan.sst_ending_arr_tester_reversals AS m
SET arr = round(
    CAST(
      (sum_arr + a.winback_downsell_delta_arr) * CASE
        WHEN sum_arr = 0 THEN 1
        ELSE ratio_arr
      END AS NUMERIC
    ),
    3
  ),
  sum_arr = round(
    CAST(
      sum_arr + a.winback_downsell_delta_arr AS NUMERIC
    ),
    3
  ),
  baseline_arr_local_currency = round(
    CAST(
      (
        sum_baseline_arr_local_currency + a.winback_downsell_delta_arr_lcu
      ) * CASE
        WHEN sum_arr = 0 THEN 1
        ELSE ratio_arr_local_currency
      END AS NUMERIC
    ),
    3
  ),
  sum_baseline_arr_local_currency = round(
    CAST(
      sum_baseline_arr_local_currency + a.winback_downsell_delta_arr_lcu AS NUMERIC
    ),
    3
  ),
  ending_arr_comment = concat(
    ending_arr_comment,
    'Winback Downsell Ending arr Fix applied'
  )
FROM ending_arr_market_winback_downsell_part AS a
WHERE m.mcid = a.mcid
  AND m.updated_product_group = a.current_product_group
  AND m.snapshot_date >= a.winback_downsell_start_date
  AND m.snapshot_date <= a.winback_downsell_end_date;
-- Fix CPI Reversal
UPDATE ryzlan.sst_ending_arr_tester_reversals AS m
SET arr = round(
    CAST(
      (sum_arr + a.cpiR_delta_arr) * ratio_arr AS NUMERIC
    ),
    3
  ),
  sum_arr = round(CAST(sum_arr + a.cpiR_delta_arr AS NUMERIC), 3),
  baseline_arr_local_currency = round(
    CAST(
      (
        sum_baseline_arr_local_currency + a.cpiR_delta_arr_lcu
      ) * ratio_arr_local_currency AS NUMERIC
    ),
    3
  ),
  sum_baseline_arr_local_currency = round(
    CAST(
      sum_baseline_arr_local_currency + a.cpiR_delta_arr_lcu AS NUMERIC
    ),
    3
  ),
  ending_arr_comment = concat(
    ending_arr_comment,
    'CPI Reversal Ending arr Fix applied'
  )
FROM ending_arr_market_cpi_part AS a
WHERE m.mcid = a.mcid
  AND m.updated_product_group = a.current_product_group
  AND m.snapshot_date >= a.cpiR_start_date
  AND m.snapshot_date <= a.cpiR_end_date;
DROP TABLE IF EXISTS ryzlan.sst_winbacks_missing_records;
CREATE TABLE ryzlan.sst_winbacks_missing_records AS (
  WITH pull_back_records AS (
    SELECT a.*
    FROM ryzlan.sst_ending_arr_tester_reversals AS a
      JOIN ending_arr_market_winback_part AS b ON a.mcid = b.mcid
      AND a.updated_product_group = b.current_product_group
      AND a.snapshot_date = b.winback_pull_back_date
  )
  SELECT CAST(
      DATE_TRUNC('MONTH', custom_dates) + INTERVAL '1 MONTH' - INTERVAL '1 DAY' AS date
    ) AS snapshot_date,
    ultimate_parent_id,
    ultimate_parent_name,
    duns_name,
    duns_number,
    parent_duns_name,
    parent_duns_number,
    domesticultimatedunsnumber,
    globalultimatedunsnumber,
    new_product_solution,
    new_product_line,
    updated_product_group,
    new_product,
    new_line_of_business,
    new_line_of_business_sub_category,
    c_name,
    parent_ns_id,
    end_ns_id,
    "name",
    parent_name,
    end_name,
    a.mcid,
    parent_mcid,
    end_mcid,
    subsidiary_entity_name,
    overage_flag,
    segment,
    region,
    product_family,
    sku,
    base_currency,
    cc_fx_rate,
    fx_date,
    arr,
    baseline_arr_local_currency,
    dw_modified_date,
    dw_created_date,
    parent_sf_id,
    parent_sf_name,
    record_source,
    modified_comments,
    cohort_actions,
    id,
    reference_number,
    updated_product_group_manual,
    updated_product_solution_manual,
    icp_account,
    lob,
    lob_sub_category,
    temp_product_solution_li,
    temp_product_group_li,
    temp_product_line_li,
    exists_in_customer_detail,
    industry,
    sub_industry,
    digital_maturity,
    under_audit,
    migration_from,
    migration_to,
    acquire_product_group,
    acquire_product_solution,
    acquire_product_group_li,
    acquire_product_solution_li,
    sum_arr,
    sum_baseline_arr_local_currency,
    ratio_arr,
    ratio_arr_local_currency,
    ending_arr_comment
  FROM pull_back_records AS a
    JOIN ending_arr_market_winback_part AS b ON a.mcid = b.mcid
    AND a.updated_product_group = b.current_product_group
    AND a.snapshot_date = b.winback_pull_back_date
    CROSS JOIN generate_series(
      b.winback_start_date,
      b.winback_end_date,
      INTERVAL '1 month'
    ) AS custom_dates
);
INSERT INTO ryzlan.sst_ending_arr_tester_reversals (
    snapshot_date,
    ultimate_parent_id,
    ultimate_parent_name,
    duns_name,
    duns_number,
    parent_duns_name,
    parent_duns_number,
    domesticultimatedunsnumber,
    globalultimatedunsnumber,
    new_product_solution,
    new_product_line,
    updated_product_group,
    new_product,
    new_line_of_business,
    new_line_of_business_sub_category,
    c_name,
    parent_ns_id,
    end_ns_id,
    "name",
    parent_name,
    end_name,
    mcid,
    parent_mcid,
    end_mcid,
    subsidiary_entity_name,
    overage_flag,
    segment,
    region,
    product_family,
    sku,
    base_currency,
    cc_fx_rate,
    fx_date,
    arr,
    baseline_arr_local_currency,
    dw_modified_date,
    dw_created_date,
    parent_sf_id,
    parent_sf_name,
    record_source,
    modified_comments,
    cohort_actions,
    id,
    reference_number,
    updated_product_group_manual,
    updated_product_solution_manual,
    icp_account,
    lob,
    lob_sub_category,
    temp_product_solution_li,
    temp_product_group_li,
    temp_product_line_li,
    exists_in_customer_detail,
    industry,
    sub_industry,
    digital_maturity,
    under_audit,
    migration_from,
    migration_to,
    acquire_product_group,
    acquire_product_solution,
    acquire_product_group_li,
    acquire_product_solution_li,
    sum_arr,
    sum_baseline_arr_local_currency,
    ratio_arr,
    ratio_arr_local_currency,
    ending_arr_comment
  )
SELECT snapshot_date,
  ultimate_parent_id,
  ultimate_parent_name,
  duns_name,
  duns_number,
  parent_duns_name,
  parent_duns_number,
  domesticultimatedunsnumber,
  globalultimatedunsnumber,
  new_product_solution,
  new_product_line,
  updated_product_group,
  new_product,
  new_line_of_business,
  new_line_of_business_sub_category,
  c_name,
  parent_ns_id,
  end_ns_id,
  "name",
  parent_name,
  end_name,
  mcid,
  parent_mcid,
  end_mcid,
  subsidiary_entity_name,
  overage_flag,
  segment,
  region,
  product_family,
  sku,
  base_currency,
  cc_fx_rate,
  fx_date,
  arr,
  baseline_arr_local_currency,
  dw_modified_date,
  dw_created_date,
  parent_sf_id,
  parent_sf_name,
  record_source,
  modified_comments,
  cohort_actions,
  id,
  reference_number,
  updated_product_group_manual,
  updated_product_solution_manual,
  icp_account,
  lob,
  lob_sub_category,
  temp_product_solution_li,
  temp_product_group_li,
  temp_product_line_li,
  exists_in_customer_detail,
  industry,
  sub_industry,
  digital_maturity,
  under_audit,
  migration_from,
  migration_to,
  acquire_product_group,
  acquire_product_solution,
  acquire_product_group_li,
  acquire_product_solution_li,
  sum_arr,
  sum_baseline_arr_local_currency,
  ratio_arr,
  ratio_arr_local_currency,
  concat(
    ending_arr_comment,
    'Inserted as part of ending arr fix'
  ) AS ending_arr_comment
FROM ryzlan.sst_winbacks_missing_records;

DROP TABLE IF EXISTS ryzlan.sst_ending_arr_tester_final;
CREATE TABLE ryzlan.sst_ending_arr_tester_final AS
SELECT snapshot_date,
  ultimate_parent_id,
  ultimate_parent_name,
  duns_name,
  duns_number,
  parent_duns_name,
  parent_duns_number,
  domesticultimatedunsnumber,
  globalultimatedunsnumber,
  new_product_solution,
  new_product_line,
  updated_product_group,
  new_product,
  new_line_of_business,
  new_line_of_business_sub_category,
  c_name,
  parent_ns_id,
  end_ns_id,
  "name",
  parent_name,
  end_name,
  mcid,
  parent_mcid,
  end_mcid,
  subsidiary_entity_name,
  overage_flag,
  segment,
  region,
  product_family,
  sku,
  base_currency,
  cc_fx_rate,
  fx_date,
  arr,
  baseline_arr_local_currency,
  dw_modified_date,
  dw_created_date,
  parent_sf_id,
  parent_sf_name,
  record_source,
  modified_comments,
  cohort_actions,
  id,
  reference_number,
  updated_product_group_manual,
  updated_product_solution_manual,
  icp_account,
  lob,
  lob_sub_category,
  temp_product_solution_li,
  temp_product_group_li,
  temp_product_line_li,
  exists_in_customer_detail,
  industry,
  sub_industry,
  digital_maturity,
  under_audit,
  migration_from,
  migration_to,
  acquire_product_group,
  acquire_product_solution,
  acquire_product_group_li,
  acquire_product_solution_li
FROM ryzlan.sst_ending_arr_tester_reversals;