-- New script in dw-prod-rds-master.cr9dekxonyuj.us-east-1.rds.amaz.
-- Date: Jun 4, 2024
-- Time: 2:57:33 PM
DROP TABLE IF EXISTS ryzlan.sst_ending_arr_tester_reversals;
CREATE TABLE ryzlan.sst_ending_arr_tester_reversals AS WITH main AS (
  SELECT *,
    round(
      sum(arr) OVER(
        PARTITION BY mcid,
        snapshot_date,
        base_currency,
        new_product_solution
      )
    ) AS sum_arr,
    round(
      sum(baseline_arr_local_currency) OVER(
        PARTITION BY mcid,
        snapshot_date,
        base_currency,
        new_product_solution
      )
    ) AS sum_baseline_arr_local_currency
  FROM ryzlan.sst_ending_arr_1_a_6 --ryzlan.sst_ending_arr_2_d --ryzlan.sst_ending_arr_3_e--ryzlan.sst_ending_arr_2_d --ufdm.sst 
    --  ufdm_archive.sst_lcoked_20052024_0012
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
    prior_product_solution,
    current_product_solution,
    prior_period_product_arr_usd_ccfx,
    current_period_product_arr_usd_ccfx,
    product_arr_change_ccfx,
    product_arr_change_lcu,
    product_bridge
  FROM ryzlan.sst_ending_arr_ps_1_a_2 --ryzlan.sst_ending_arr_ps_2_d --ryzlan.sst_ending_arr_ps_3_e --ryzlan.sst_ending_arr_ps_2_d --ufdm.sst_product_bridge_product_solution -
    --    WHERE mcid ='34f5dc63-763f-0161-06c1-5577527358d6'
)
SELECT --    id,
  mcid,
  evaluation_period,
  eval_period,
  currency_code,
  current_period,
  prior_period,
  prior_product_solution,
  current_product_solution,
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
--  SELECT * FROM ending_arr_marker_base  
--  WHERE mcid = '40e3c7ac-edf6-db11-94ce-0018717a8c82' 
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
          a.prior_product_solution,
          '->',
          a.current_product_solution
        ),
        a.product_bridge
        ORDER BY b.current_period DESC
      ) AS cpi_rnk
    FROM ending_arr_marker_base AS a
      JOIN ending_arr_marker_history AS b ON a.mcid = b.mcid
      AND concat(
        a.prior_product_solution,
        '->',
        a.current_product_solution
      ) = concat(
        b.prior_product_solution,
        '->',
        b.current_product_solution
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
          a.prior_product_solution,
          '->',
          a.current_product_solution
        ),
        a.product_bridge
        ORDER BY b.current_period DESC
      ) AS upsell_rnk
    FROM ending_arr_marker_base AS a
      JOIN ending_arr_marker_history AS b ON a.mcid = b.mcid
      AND concat(
        a.prior_product_solution,
        '->',
        a.current_product_solution
      ) = concat(
        b.prior_product_solution,
        '->',
        b.current_product_solution
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
          a.prior_product_solution,
          '->',
          a.current_product_solution
        ),
        a.product_bridge
        ORDER BY b.current_period DESC
      ) AS crosssell_rnk
    FROM ending_arr_marker_base AS a
      JOIN ending_arr_marker_history AS b ON a.mcid = b.mcid
      AND a.prior_product_solution = b.current_product_solution
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
          a.prior_product_solution,
          '->',
          a.current_product_solution
        ),
        a.product_bridge
        ORDER BY b.current_period DESC
      ) AS winback_downgrade_rnk
    FROM ending_arr_marker_base AS a
      JOIN ending_arr_marker_history AS b ON a.mcid = b.mcid
      AND concat(
        a.prior_product_solution,
        '->',
        a.current_product_solution
      ) = concat(
        b.prior_product_solution,
        '->',
        b.current_product_solution
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
          a.prior_product_solution,
          '->',
          a.current_product_solution
        ),
        a.product_bridge
        ORDER BY b.current_period desc
      ) AS winback_downsell_rnk
    FROM ending_arr_marker_base AS a
      JOIN ending_arr_marker_history AS b ON a.mcid = b.mcid
      AND concat(
        a.prior_product_solution,
        '->',
        a.current_product_solution
      ) = concat(
        b.prior_product_solution,
        '->',
        b.current_product_solution
      )
      AND a.product_bridge = 'Win back Downsell'
      AND b.product_bridge = 'Downsell' --      AND round(a.product_arr_change_ccfx) = round(b.product_arr_change_ccfx)
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
        --        a.eval_period ,
        a.current_product_solution,
        b.prior_product_solution,
        a.product_bridge,
        b.current_period
        ORDER BY a.current_period
      ) AS winback_rnk
    FROM ending_arr_marker_base AS a
      JOIN ending_arr_marker_history AS b ON a.mcid = b.mcid
      AND a.current_product_solution = b.prior_product_solution
      AND a.product_bridge IN ('Winback', 'Winback ST', 'Winback LT')
      AND b.product_bridge IN ('Churn', 'Downsell') --      AND round(a.product_arr_change_ccfx) = round(b.product_arr_change_ccfx)
      AND b.current_period <= a.current_period
      AND DATE_PART(
        'Day',
        a.current_period::TIMESTAMP - b.current_period::TIMESTAMP
      ) < 186
  ) AS a
WHERE winback_rnk = 1;
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
  AND m.new_product_solution = a.current_product_solution
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
  AND m.new_product_solution = a.current_product_solution
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
  AND m.new_product_solution = a.current_product_solution
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
  AND m.new_product_solution = a.current_product_solution
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
  AND m.new_product_solution = a.current_product_solution
  AND m.snapshot_date >= a.cpiR_start_date
  AND m.snapshot_date <= a.cpiR_end_date;
DROP TABLE IF EXISTS ryzlan.sst_winbacks_missing_records;
CREATE TABLE ryzlan.sst_winbacks_missing_records AS (
  WITH pull_back_records AS (
    SELECT a.*
    FROM ryzlan.sst_ending_arr_tester_reversals AS a
      JOIN ending_arr_market_winback_part AS b ON a.mcid = b.mcid
      AND a.new_product_solution = b.current_product_solution
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
    AND a.new_product_solution = b.current_product_solution
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
--SELECT * FROM ending_arr_market_cpi_part 
--WHERE mcid = '821effa3-6d89-ede8-0e35-4f82940a368c'  
--
--SELECT evaluation_period  , mcid , prior_product_solution  , current_product_solution  , product_bridge  , product_arr_change_ccfx   
--FROM ryzlan.sst_ending_arr_ps_3_f --ryzlan.sst_ending_arr_ps_3_e--ryzlan.sst_ending_arr_ps_3_a
--WHERE  mcid ='c0254bfa-8c94-e311-a1cd-0050568d2da8'
----mcid = 'fb4c0c57-7650-e711-810c-3863bb3640e8'  
----mcid = 'd37a1181-45d7-e211-9350-0050568d002c'  
--product_bridge  = 'Price Uplift Reversal'
--
--
--
--product_bridge  = 'Win back Downgrade'
--
--SELECT snapshot_date  ,
--    mcid , 
--    base_currency  , 
--    sku,
--    new_product_solution  , 
--    arr ,
--round(
--      sum(arr) OVER(
--        PARTITION BY mcid,
--        snapshot_date,
--        base_currency,
--        new_product_solution
--      )
--    ) AS sum_arr,
--    round(
--      sum(arr) OVER(
--        PARTITION BY mcid,
--        snapshot_date,
--        base_currency
--      )
--    ) AS sum_arr2
----    sum_arr,
----    ratio_arr
--    
--    FROM ufdm.sst -- ryzlan.sst_ending_arr_3_f--ryzlan.sst_ending_arr_tester_final -- ryzlan.sst_ending_arr_tester_reversals  --ufdm.sst
--WHERE mcid ='c0254bfa-8c94-e311-a1cd-0050568d2da8' AND snapshot_date  >=  '2019-12-31' AND snapshot_date  <= '2020-02-29' 
--ORDER BY 1 , 5 
--
--
--UPDATE ryzlan.sst_ending_arr_tester_reversals AS m
--SET arr = round(
--    CAST(
--      (sum_arr + a.cpiR_delta_arr) * ratio_arr AS NUMERIC
--    ),
--    3
--  ),
--  sum_arr = round(CAST(sum_arr + a.cpiR_delta_arr AS NUMERIC), 3),
--  baseline_arr_local_currency = round(
--    CAST(
--      (
--        sum_baseline_arr_local_currency + a.cpiR_delta_arr_lcu
--      ) * ratio_arr_local_currency AS NUMERIC
--    ),
--    3
--  ),
--  sum_baseline_arr_local_currency = round(
--    CAST(
--      sum_baseline_arr_local_currency + a.cpiR_delta_arr_lcu AS NUMERIC
--    ),
--    3
--  ),
--  ending_arr_comment = concat(
--    ending_arr_comment,
--    'CPI Reversal Ending arr Fix applied'
--  )
--FROM ending_arr_market_cpi_part AS a
--WHERE m.mcid = a.mcid
--  AND m.new_product_solution = a.current_product_solution
--  AND m.snapshot_date >= a.cpiR_start_date
--  AND m.snapshot_date <= a.cpiR_end_date;
--  
--  SELECT 
--    snapshot_date  ,
--    m.mcid , 
--    m.base_currency  , 
--    m.sku,
--    m.new_product_solution  , 
--    m.arr ,
----    sum(arr) OVER(
----        PARTITION BY mcid,
----        snapshot_date,
----        base_currency,
----        updated_product_group  
----      ),
--    m.sum_arr,
--    m.ratio_arr,
--    round(
--    CAST(
--      (m.sum_arr + a.cpiR_delta_arr) * ratio_arr AS NUMERIC
--    ),
--    3
--  ) AS new_arr ,
--round(CAST(m.sum_arr + a.cpiR_delta_arr AS NUMERIC), 3) AS new_sum_arr 
--FROM ryzlan.sst_ending_arr_tester_reversals AS m 
----ryzlan. 
--JOIN ending_arr_market_cpi_part AS a
--ON  m.mcid = a.mcid
--  AND m.new_product_solution = a.current_product_solution
--  AND m.snapshot_date >= a.cpiR_start_date
--  AND m.snapshot_date <= a.cpiR_end_date
--WHERE   m.mcid = 'c0254bfa-8c94-e311-a1cd-0050568d2da8'   AND m.snapshot_date  >=  '2019-12-31' AND m.snapshot_date  <= '2020-02-29' 
--
--
--
--SELECT 
--    snapshot_date  ,
--    m.mcid , 
--    m.base_currency  , 
--    m.sku,
--    m.new_product_solution  , 
--    m.arr ,
--    m.sum_arr,
--    m.ratio_arr,
--      round(  sum(arr) OVER(
--        PARTITION BY mcid,
--        snapshot_date,
--        base_currency
--      )) AS total_sum 
--FROM ryzlan.sst_ending_arr_tester_reversals AS m 
--WHERE   m.mcid = 'c0254bfa-8c94-e311-a1cd-0050568d2da8'   AND m.snapshot_date >= '2023-01-01'
--JOIN ending_arr_market_cpi_part AS a