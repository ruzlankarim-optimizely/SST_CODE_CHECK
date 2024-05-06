
DROP TABLE IF EXISTS ryzlan.sst_ending_arr_tester;
CREATE TABLE ryzlan.sst_ending_arr_tester AS WITH main AS (
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
  FROM ufdm.sst
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
FROM main;








-- Fix upsell Reversal 
UPDATE ryzlan.sst_ending_arr_tester AS m
SET arr = round((sum_arr + a.upsellR_delta_arr) * ratio_arr),
  sum_arr = round(sum_arr + a.upsellR_delta_arr),
  baseline_arr_local_currency = round(
    (
      sum_baseline_arr_local_currency + a.upsellR_delta_arr_lcu
    ) * ratio_arr_local_currency
  ),
  sum_baseline_arr_local_currency = round(
    sum_baseline_arr_local_currency + a.upsellR_delta_arr_lcu
  ),
  ending_arr_comment = concat(
    ending_arr_comment,
    'Upsell Reversal Ending arr Fix applied'
  )
FROM ryzlan.ending_arr_marker AS a
WHERE m.mcid = a.mcid
  AND m.snapshot_date >= a.upsellR_start_date
  AND m.snapshot_date <= a.upsellR_end_date
  AND a.upsell_reversal_flag > 0;
-- Fix CPI Reversal 
UPDATE ryzlan.sst_ending_arr_tester AS m
SET arr = round((sum_arr + a.cpiR_delta_arr) * ratio_arr),
  sum_arr = round(sum_arr + a.cpiR_delta_arr),
  baseline_arr_local_currency = round(
    (
      sum_baseline_arr_local_currency + a.cpiR_delta_arr_lcu
    ) * ratio_arr_local_currency
  ),
  sum_baseline_arr_local_currency = round(
    sum_baseline_arr_local_currency + a.cpiR_delta_arr_lcu
  ),
  ending_arr_comment = concat(
    ending_arr_comment,
    'CPI Reversal Ending arr Fix applied'
  )
FROM ryzlan.ending_arr_marker AS a
WHERE m.mcid = a.mcid
  AND m.snapshot_date >= a.cpiR_start_date
  AND m.snapshot_date <= a.cpiR_end_date
  AND a.cpi_reversal_flag > 0;