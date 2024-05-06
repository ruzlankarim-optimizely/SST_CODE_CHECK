

DROP TABLE IF EXISTS ryzlan.sst_winbacks_missing_records;
CREATE TABLE ryzlan.sst_winbacks_missing_records AS (
  WITH pull_back_records AS (
    SELECT a.*
    FROM ufdm.sst AS a
      JOIN ryzlan.ending_arr_marker_two AS b ON a.mcid = b.mcid
      AND a.snapshot_date = b.winback_pull_back_date
  )
  SELECT cast(
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
    a.id,
    updated_product_group_manual,
    updated_product_solution_manual,
    icp_account,
    lob,
    lob_sub_category,
    exists_in_customer_detail,
    created_datetime,
    sku,
    base_currency_old,
    baseline_arr_local_currency_old,
    industry,
    sub_industry,
    digital_maturity,
    temp_product_solution_li,
    temp_product_group_li,
    migration_from,
    migration_to,
    under_audit
  FROM pull_back_records AS a
    JOIN ryzlan.ending_arr_marker_two AS b ON a.mcid = b.mcid
    AND a.snapshot_date = b.winback_pull_back_date
    CROSS JOIN generate_series(
      b.winback_start_date,
      b.winback_end_date,
      interval '1 month'
    ) AS custom_dates
);








DROP TABLE IF EXISTS ryzlan.sst_ending_arr_tester_winbacks_base;
CREATE TABLE ryzlan.sst_ending_arr_tester_winbacks_base AS
SELECT *,
  NULL AS ending_arr_comment
FROM ufdm.sst;




INSERT INTO ryzlan.sst_ending_arr_tester_winbacks_base (
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
    updated_product_group_manual,
    updated_product_solution_manual,
    icp_account,
    lob,
    lob_sub_category,
    exists_in_customer_detail,
    created_datetime,
    sku,
    base_currency_old,
    baseline_arr_local_currency_old,
    industry,
    sub_industry,
    digital_maturity,
    temp_product_solution_li,
    temp_product_group_li,
    migration_from,
    migration_to,
    under_audit,
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
  updated_product_group_manual,
  updated_product_solution_manual,
  icp_account,
  lob,
  lob_sub_category,
  exists_in_customer_detail,
  created_datetime,
  sku,
  base_currency_old,
  baseline_arr_local_currency_old,
  industry,
  sub_industry,
  digital_maturity,
  temp_product_solution_li,
  temp_product_group_li,
  migration_from,
  migration_to,
  under_audit,
  'Inserted as part of ending arr fix' AS ending_arr_comment
FROM ryzlan.sst_winbacks_missing_records;




DROP TABLE IF EXISTS ryzlan.sst_ending_arr_tester_winbacks;
CREATE TABLE ryzlan.sst_ending_arr_tester_winbacks AS WITH main AS (
  SELECT *,
    round(
      sum(arr) over(
        PARTITION BY mcid,
        snapshot_date,
        base_currency
      )
    ) AS sum_arr,
    round(
      sum(baseline_arr_local_currency) over(
        PARTITION BY mcid,
        snapshot_date,
        base_currency
      )
    ) AS sum_baseline_arr_local_currency
  FROM ryzlan.sst_ending_arr_tester_winbacks_base
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
  END AS ratio_arr_local_currency
FROM main;


UPDATE ryzlan.sst_ending_arr_tester_winbacks AS m
SET arr = round((sum_arr + a.winback_delta_arr) * ratio_arr),
  sum_arr = round(sum_arr + a.winback_delta_arr),
  baseline_arr_local_currency = round(
    (
      sum_baseline_arr_local_currency + a.winback_delta_arr_lcu
    ) * ratio_arr_local_currency
  ),
  sum_baseline_arr_local_currency = round(
    sum_baseline_arr_local_currency + a.winback_delta_arr_lcu
  ),
  ending_arr_comment = concat(
    ending_arr_comment,
    ' --- winback Ending arr Fix applied'
  )
FROM ryzlan.ending_arr_marker_two AS a
WHERE m.mcid = a.mcid
  AND m.snapshot_date >= a.winback_start_date
  AND m.snapshot_date <= a.winback_end_date
  AND a.winback_reversal_flag > 0;

  