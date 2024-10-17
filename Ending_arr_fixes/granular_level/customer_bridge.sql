-- New script in dw-prod-rds-master.cr9dekxonyuj.us-east-1.rds.amaz.
-- Date: May 29, 2024
-- Time: 4:25:35 PM  
DROP TABLE IF EXISTS ryzlan.sst_ending_arr_tester_reversals;
CREATE TABLE ryzlan.sst_ending_arr_tester_reversals AS WITH main AS (
  SELECT *,
    round(
      sum(arr) OVER(
        PARTITION BY mcid,
        snapshot_date,
        base_currency
      )
    ) AS sum_arr,
    round(
      sum(baseline_arr_local_currency) OVER(
        PARTITION BY mcid,
        snapshot_date,
        base_currency
      )
    ) AS sum_baseline_arr_local_currency
  FROM ryzlan.sst_ending_arr_1_a_11 -- TO BE CHANGED
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
DROP TABLE IF EXISTS ryzlan.ending_arr_marker;
CREATE TABLE ryzlan.ending_arr_marker AS (
  WITH main AS (
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
      baseline_currency,
      prior_period_customer_arr_usd_ccfx,
      current_period_customer_arr_usd_ccfx,
      customer_arr_change_ccfx,
      customer_arr_change_lcu,
      customer_bridge
    FROM ryzlan.sst_ending_arr_cb_1_a_5 -- TO BE CHANGED
  ),
  base AS (
    SELECT --    id,
      mcid,
      evaluation_period,
      eval_period,
      baseline_currency,
      current_period,
      prior_period,
      current_period_customer_arr_usd_ccfx,
      prior_period_customer_arr_usd_ccfx,
      customer_arr_change_ccfx,
      customer_arr_change_lcu,
      ROW_NUMBER() OVER (
        PARTITION BY mcid,
        eval_period
        ORDER BY eval_period
      ) AS row_num,
      customer_bridge
    FROM main
  ),
  base_two AS (
    SELECT *,
      LAG(customer_bridge, 1) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num DESC
      ) AS "n - 1 bridge_movement",
      LAG(customer_bridge, 2) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num DESC
      ) AS "n - 2 bridge_movement",
      LAG(customer_bridge, 3) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num DESC
      ) AS "n - 3 bridge_movement",
      LAG(customer_bridge, 4) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num DESC
      ) AS "n - 4 bridge_movement",
      LAG(customer_bridge, 5) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num DESC
      ) AS "n - 5 bridge_movement",
      LAG(customer_bridge, 6) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num DESC
      ) AS "n - 6 bridge_movement",
      LAG(customer_bridge, 7) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num DESC
      ) AS "n - 7 bridge_movement",
      LAG(customer_bridge, 8) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num DESC
      ) AS "n - 8 bridge_movement",
      LAG(customer_bridge, 9) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num DESC
      ) AS "n - 9 bridge_movement",
      LAG(customer_bridge, 10) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num DESC
      ) AS "n - 10 bridge_movement",
      LAG(customer_bridge, 11) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num DESC
      ) AS "n - 11 bridge_movement",
      LAG(customer_bridge, 12) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num DESC
      ) AS "n - 12 bridge_movement"
    FROM base
  ),
  base_checker AS (
    SELECT *,
      CASE
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 1 bridge_movement" = 'Up Sell' THEN 1
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 2 bridge_movement" = 'Up Sell' THEN 2
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 3 bridge_movement" = 'Up Sell' THEN 3
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 4 bridge_movement" = 'Up Sell' THEN 4
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 5 bridge_movement" = 'Up Sell' THEN 5
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 6 bridge_movement" = 'Up Sell' THEN 6
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 7 bridge_movement" = 'Up Sell' THEN 7
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 8 bridge_movement" = 'Up Sell' THEN 8
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 9 bridge_movement" = 'Up Sell' THEN 9
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 10 bridge_movement" = 'Up Sell' THEN 10
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 11 bridge_movement" = 'Up Sell' THEN 11
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 12 bridge_movement" = 'Up Sell' THEN 12
        ELSE 0
      END AS upsell_reversal_flag,
      CASE
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 1 bridge_movement" = 'Cross-sell' THEN 1
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 2 bridge_movement" = 'Cross-sell' THEN 2
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 3 bridge_movement" = 'Cross-sell' THEN 3
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 4 bridge_movement" = 'Cross-sell' THEN 4
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 5 bridge_movement" = 'Cross-sell' THEN 5
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 6 bridge_movement" = 'Cross-sell' THEN 6
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 7 bridge_movement" = 'Cross-sell' THEN 7
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 8 bridge_movement" = 'Cross-sell' THEN 8
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 9 bridge_movement" = 'Cross-sell' THEN 9
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 10 bridge_movement" = 'Cross-sell' THEN 10
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 11 bridge_movement" = 'Cross-sell' THEN 11
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 12 bridge_movement" = 'Cross-sell' THEN 12
        ELSE 0
      END AS crosssell_reversal_flag,
      CASE
        WHEN (
          customer_bridge = 'Winback ST'
          OR customer_bridge = 'Winback'
          OR customer_bridge = 'Winback LT'
        )
        AND "n - 1 bridge_movement" = 'Churn' THEN 1
        WHEN (
          customer_bridge = 'Winback ST'
          OR customer_bridge = 'Winback'
          OR customer_bridge = 'Winback LT'
        )
        AND "n - 2 bridge_movement" = 'Churn' THEN 2
        WHEN (
          customer_bridge = 'Winback ST'
          OR customer_bridge = 'Winback'
          OR customer_bridge = 'Winback LT'
        )
        AND "n - 3 bridge_movement" = 'Churn' THEN 3
        WHEN (
          customer_bridge = 'Winback ST'
          OR customer_bridge = 'Winback'
          OR customer_bridge = 'Winback LT'
        )
        AND "n - 4 bridge_movement" = 'Churn' THEN 4
        WHEN (
          customer_bridge = 'Winback ST'
          OR customer_bridge = 'Winback'
          OR customer_bridge = 'Winback LT'
        )
        AND "n - 5 bridge_movement" = 'Churn' THEN 5
        WHEN (
          customer_bridge = 'Winback ST'
          OR customer_bridge = 'Winback'
          OR customer_bridge = 'Winback LT'
        )
        AND "n - 6 bridge_movement" = 'Churn' THEN 6
        WHEN (
          customer_bridge = 'Winback ST'
          OR customer_bridge = 'Winback'
          OR customer_bridge = 'Winback LT'
        )
        AND "n - 7 bridge_movement" = 'Churn' THEN 7
        WHEN (
          customer_bridge = 'Winback ST'
          OR customer_bridge = 'Winback'
          OR customer_bridge = 'Winback LT'
        )
        AND "n - 8 bridge_movement" = 'Churn' THEN 8
        WHEN (
          customer_bridge = 'Winback ST'
          OR customer_bridge = 'Winback'
          OR customer_bridge = 'Winback LT'
        )
        AND "n - 9 bridge_movement" = 'Churn' THEN 9
        WHEN (
          customer_bridge = 'Winback ST'
          OR customer_bridge = 'Winback'
          OR customer_bridge = 'Winback LT'
        )
        AND "n - 10 bridge_movement" = 'Churn' THEN 10
        WHEN (
          customer_bridge = 'Winback ST'
          OR customer_bridge = 'Winback'
          OR customer_bridge = 'Winback LT'
        )
        AND "n - 11 bridge_movement" = 'Churn' THEN 11
        WHEN (
          customer_bridge = 'Winback ST'
          OR customer_bridge = 'Winback'
          OR customer_bridge = 'Winback LT'
        )
        AND "n - 12 bridge_movement" = 'Churn' THEN 12
        ELSE 0
      END AS winback_reversal_flag,
      CASE
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 1 bridge_movement" = 'Downgrade' THEN 1
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 2 bridge_movement" = 'Downgrade' THEN 2
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 3 bridge_movement" = 'Downgrade' THEN 3
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 4 bridge_movement" = 'Downgrade' THEN 4
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 5 bridge_movement" = 'Downgrade' THEN 5
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 6 bridge_movement" = 'Downgrade' THEN 6
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 7 bridge_movement" = 'Downgrade' THEN 7
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 8 bridge_movement" = 'Downgrade' THEN 8
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 9 bridge_movement" = 'Downgrade' THEN 9
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 10 bridge_movement" = 'Downgrade' THEN 10
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 11 bridge_movement" = 'Downgrade' THEN 11
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 12 bridge_movement" = 'Downgrade' THEN 12
        ELSE 0
      END AS winback_downgrade_flag,
      CASE
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 1 bridge_movement" = 'Downsell' THEN 1
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 2 bridge_movement" = 'Downsell' THEN 2
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 3 bridge_movement" = 'Downsell' THEN 3
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 4 bridge_movement" = 'Downsell' THEN 4
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 5 bridge_movement" = 'Downsell' THEN 5
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 6 bridge_movement" = 'Downsell' THEN 6
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 7 bridge_movement" = 'Downsell' THEN 7
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 8 bridge_movement" = 'Downsell' THEN 8
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 9 bridge_movement" = 'Downsell' THEN 9
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 10 bridge_movement" = 'Downsell' THEN 10
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 11 bridge_movement" = 'Downsell' THEN 11
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 12 bridge_movement" = 'Downsell' THEN 12
        ELSE 0
      END AS winback_downsell_flag,
      CASE
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 1 bridge_movement" = 'Price Uplift' THEN 1
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 2 bridge_movement" = 'Price Uplift' THEN 2
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 3 bridge_movement" = 'Price Uplift' THEN 3
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 4 bridge_movement" = 'Price Uplift' THEN 4
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 5 bridge_movement" = 'Price Uplift' THEN 5
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 6 bridge_movement" = 'Price Uplift' THEN 6
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 7 bridge_movement" = 'Price Uplift' THEN 7
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 8 bridge_movement" = 'Price Uplift' THEN 8
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 9 bridge_movement" = 'Price Uplift' THEN 9
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 10 bridge_movement" = 'Price Uplift' THEN 10
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 11 bridge_movement" = 'Price Uplift' THEN 11
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 12 bridge_movement" = 'Price Uplift' THEN 12
        ELSE 0
      END AS cpi_reversal_flag
    FROM base_two
  ) --SELECT * FROM base_checker   WHERE mcid =  'e87fcbb2-d6d3-5a1e-f9df-7eb3f4b482ce'
,
  base_checker_two AS (
    SELECT a.*,
      --- Up Sell Reversal
      CASE
        WHEN (a.upsell_reversal_flag > 0) THEN LAG(
          a.current_period_customer_arr_usd_ccfx,
          a.upsell_reversal_flag
        ) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num DESC
        )
        ELSE NULL
      END AS upsellR_start_arr,
      CASE
        WHEN (a.upsell_reversal_flag > 0) THEN LAG(a.current_period, a.upsell_reversal_flag) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num DESC
        )
        ELSE NULL
      END AS upsellR_start_date,
      CASE
        WHEN a.upsell_reversal_flag > 0 THEN prior_period
        ELSE NULL
      END AS upsellR_end_date,
      CASE
        WHEN (a.upsell_reversal_flag > 0) THEN customer_arr_change_ccfx
        ELSE NULL
      END AS upsellR_delta_arr,
      CASE
        WHEN (a.upsell_reversal_flag > 0) THEN customer_arr_change_lcu
        ELSE NULL
      END AS upsellR_delta_arr_lcu,
      --- cross selll Reversal
      CASE
        WHEN (a.crosssell_reversal_flag > 0) THEN LAG(
          a.current_period_customer_arr_usd_ccfx,
          a.crosssell_reversal_flag
        ) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num DESC
        )
        ELSE NULL
      END AS crossellR_start_arr,
      CASE
        WHEN (a.crosssell_reversal_flag > 0) THEN LAG(a.current_period, a.crosssell_reversal_flag) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num DESC
        )
        ELSE NULL
      END AS crossellR_start_date,
      CASE
        WHEN a.crosssell_reversal_flag > 0 THEN prior_period
        ELSE NULL
      END AS crossellR_end_date,
      CASE
        WHEN (a.crosssell_reversal_flag > 0) THEN customer_arr_change_ccfx
        ELSE NULL
      END AS crossellR_delta_arr,
      CASE
        WHEN (a.crosssell_reversal_flag > 0) THEN customer_arr_change_lcu
        ELSE NULL
      END AS crossellR_delta_arr_lcu,
      -- Price Uplift Reversal
      CASE
        WHEN (a.cpi_reversal_flag > 0) THEN LAG(
          a.current_period_customer_arr_usd_ccfx,
          a.cpi_reversal_flag
        ) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num DESC
        )
        ELSE NULL
      END AS cpiR_start_arr,
      CASE
        WHEN a.cpi_reversal_flag > 0 THEN LAG(a.current_period, a.cpi_reversal_flag) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num DESC
        )
        ELSE NULL
      END AS cpiR_start_date,
      CASE
        WHEN a.cpi_reversal_flag > 0 THEN prior_period
        ELSE NULL
      END AS cpiR_end_date,
      CASE
        WHEN a.cpi_reversal_flag > 0 THEN customer_arr_change_ccfx
        ELSE NULL
      END AS cpiR_delta_arr,
      CASE
        WHEN a.cpi_reversal_flag > 0 THEN customer_arr_change_lcu
        ELSE NULL
      END AS cpiR_delta_arr_lcu,
      -- Winback LT/ ST
      CASE
        WHEN (a.winback_reversal_flag > 0) THEN LAG(
          a.current_period_customer_arr_usd_ccfx,
          a.winback_reversal_flag
        ) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num DESC
        )
        ELSE NULL
      END AS winback_start_arr,
      CASE
        WHEN a.winback_reversal_flag > 0 THEN LAG(a.current_period, a.winback_reversal_flag) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num DESC
        )
        ELSE NULL
      END AS winback_start_date,
      CASE
        WHEN a.winback_reversal_flag > 0 THEN LAG(a.prior_period, a.winback_reversal_flag) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num DESC
        )
        ELSE NULL
      END AS winback_pull_back_date,
      CASE
        WHEN a.winback_reversal_flag > 0 THEN prior_period
        ELSE NULL
      END AS winback_end_date,
      CASE
        WHEN a.winback_reversal_flag > 0 THEN (
          customer_arr_change_ccfx + LAG(
            a.customer_arr_change_ccfx,
            a.winback_reversal_flag
          ) OVER(
            PARTITION BY a.mcid
            ORDER BY eval_period,
              row_num DESC
          )
        )
        ELSE NULL
      END AS winback_delta_arr,
      CASE
        WHEN a.winback_reversal_flag > 0 THEN (
          customer_arr_change_lcu + LAG(
            a.customer_arr_change_lcu,
            a.winback_reversal_flag
          ) OVER(
            PARTITION BY a.mcid
            ORDER BY eval_period,
              row_num DESC
          )
        )
        ELSE NULL
      END AS winback_delta_arr_lcu,
      -- Winback downgrade
      CASE
        WHEN (a.winback_downgrade_flag > 0) THEN LAG(
          a.current_period_customer_arr_usd_ccfx,
          a.winback_downgrade_flag
        ) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num DESC
        )
        ELSE NULL
      END AS winback_downgrade_start_arr,
      CASE
        WHEN a.winback_downgrade_flag > 0 THEN LAG(a.current_period, a.winback_downgrade_flag) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num DESC
        )
        ELSE NULL
      END AS winback_downgrade_start_date,
      CASE
        WHEN a.winback_downgrade_flag > 0 THEN LAG(a.prior_period, a.winback_downgrade_flag) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num DESC
        )
        ELSE NULL
      END AS winback_downgrade_pull_back_date,
      CASE
        WHEN a.winback_downgrade_flag > 0 THEN prior_period
        ELSE NULL
      END AS winback_downgrade_end_date,
      CASE
        WHEN a.winback_downgrade_flag > 0 THEN customer_arr_change_ccfx
        ELSE NULL
      END AS winback_downgrade_delta_arr,
      CASE
        WHEN a.winback_downgrade_flag > 0 THEN customer_arr_change_lcu
        ELSE NULL
      END AS winback_downgrade_delta_arr_lcu,
      -- Winback downsell
      CASE
        WHEN (a.winback_downsell_flag > 0) THEN LAG(
          a.current_period_customer_arr_usd_ccfx,
          a.winback_downsell_flag
        ) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num DESC
        )
        ELSE NULL
      END AS winback_downsell_start_arr,
      CASE
        WHEN a.winback_downsell_flag > 0 THEN LAG(a.current_period, a.winback_downsell_flag) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num DESC
        )
        ELSE NULL
      END AS winback_downsell_start_date,
      CASE
        WHEN a.winback_downsell_flag > 0 THEN LAG(a.prior_period, a.winback_downsell_flag) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num DESC
        )
        ELSE NULL
      END AS winback_downsell_pull_back_date,
      CASE
        WHEN a.winback_downsell_flag > 0 THEN prior_period
        ELSE NULL
      END AS winback_downsell_end_date,
      CASE
        WHEN a.winback_downsell_flag > 0 THEN customer_arr_change_ccfx
        ELSE NULL
      END AS winback_downsell_delta_arr,
      CASE
        WHEN a.winback_downsell_flag > 0 THEN customer_arr_change_lcu
        ELSE NULL
      END AS winback_downsell_delta_arr_lcu
    FROM base_checker AS a
  ),
  day_filter AS (
    SELECT a.*,
      DATE_PART(
        'Day',
        winback_downsell_end_date::TIMESTAMP - winback_downsell_start_date::TIMESTAMP
      ) AS winback_downsell_day_diff,
      DATE_PART(
        'Day',
        winback_downgrade_end_date::TIMESTAMP - winback_downgrade_start_date::TIMESTAMP
      ) AS winback_downgrade_day_diff,
      DATE_PART(
        'Day',
        winback_end_date::TIMESTAMP - winback_start_date::TIMESTAMP
      ) AS winback_day_diff,
      DATE_PART(
        'Day',
        cpiR_end_date::TIMESTAMP - cpiR_start_date::TIMESTAMP
      ) AS cpiR_day_diff,
      DATE_PART(
        'Day',
        crossellR_end_date::TIMESTAMP - crossellR_start_date::TIMESTAMP
      ) AS crossellR_day_diff,
      DATE_PART(
        'Day',
        upsellR_end_date::TIMESTAMP - upsellR_start_date::TIMESTAMP
      ) AS upsellR_day_diff,
      CASE
        WHEN upsell_reversal_flag > 0 THEN ROW_NUMBER () OVER(
          PARTITION BY mcid,
          customer_bridge,
          upsellr_start_date
          ORDER BY current_period
        )
        ELSE NULL
      END AS upsell_rnk,
      CASE
        WHEN crosssell_reversal_flag > 0 THEN ROW_NUMBER () OVER(
          PARTITION BY mcid,
          customer_bridge,
          crossellR_start_date
          ORDER BY current_period
        )
        ELSE NULL
      END AS crosssell_rnk,
      CASE
        WHEN cpi_reversal_flag > 0 THEN ROW_NUMBER () OVER(
          PARTITION BY mcid,
          customer_bridge,
          cpiR_start_date
          ORDER BY current_period
        )
        ELSE NULL
      END AS cpi_rnk,
      CASE
        WHEN winback_reversal_flag > 0 THEN ROW_NUMBER () OVER(
          PARTITION BY mcid,
          customer_bridge,
          winback_start_date
          ORDER BY current_period
        )
        ELSE NULL
      END AS winback_rnk,
      CASE
        WHEN winback_downgrade_flag > 0 THEN ROW_NUMBER () OVER(
          PARTITION BY mcid,
          customer_bridge,
          winback_downgrade_start_date
          ORDER BY current_period
        )
        ELSE NULL
      END AS winback_downgrade_rnk,
      CASE
        WHEN winback_downsell_flag > 0 THEN ROW_NUMBER () OVER(
          PARTITION BY mcid,
          customer_bridge,
          winback_downsell_start_date
          ORDER BY current_period
        )
        ELSE NULL
      END AS winback_downsell_rnk
    FROM base_checker_two AS a
  )
  SELECT a.*
  FROM day_filter AS a
  WHERE --  mcid = '4e128cce-793a-e811-8124-70106faab5f1' AND
    --  mcid IN ( '30f35937-33a5-e811-814d-70106fa55dc1'
    --  '4e128cce-793a-e811-8124-70106faab5f1' , 'e412863b-fd0a-4234-953b-188bc6f848fe'
    --  ) AND
    (
      (
        upsell_reversal_flag > 0
        AND upsellR_day_diff < 186
      )
      OR (
        crosssell_reversal_flag > 0
        AND crossellR_day_diff < 186
      )
      OR (
        cpi_reversal_flag > 0
        AND cpiR_day_diff < 186
      )
      OR (
        winback_reversal_flag > 0
        AND winback_day_diff < 186
      )
      OR (
        winback_downsell_flag > 0
        AND winback_downsell_day_diff < 186
      )
      OR (
        winback_downgrade_flag > 0
        AND winback_downgrade_day_diff < 186
      )
    )
    AND (
      upsell_rnk = 1
      OR crosssell_rnk = 1
      OR cpi_rnk = 1
      OR winback_rnk = 1
      OR winback_downgrade_rnk = 1
      OR winback_downsell_rnk = 1
    ) --  AND  mcid IN ( '4e128cce-793a-e811-8124-70106faab5f1' , 'e412863b-fd0a-4234-953b-188bc6f848fe')
    --   AND mcid IN ('f6e7e4fa-2b48-e81a-bcb4-46dfd569d878' , 'f2c843b1-605f-e7b5-71a3-402283550691' , 'fb462315-cce6-e411-9afb-0050568d2da8')
);
--SELECT snapshot_date  , mcid , ending_arr_comment  , sum(arr) AS arr    FROM ryzlan.sst_ending_arr_tester_reversals
--WHERE mcid = '4e128cce-793a-e811-8124-70106faab5f1'
--GROUP BY 1 ,2 ,3
--LIMIT 1
--
--SELECT
--    snapshot_date ,
--    mcid ,
--    sku ,
--    arr ,
--    ratio_arr ,
--    sum_arr
--FROM  ryzlan.sst_ending_arr_tester_reversals
--WHERE mcid = '4e128cce-793a-e811-8124-70106faab5f1' AND arr <> 0.0
--
--SELECT
--*,
--ROW_NUMBER () over(PARTITION BY mcid , customer_bridge ,  upsellr_start_date)  AS rnk
--
--FROM ryzlan.ending_arr_marker
--
--SELECT
--    snapshot_date ,
--    m.mcid ,
--    arr ,
--    ratio_arr ,
--    sum_arr ,
----    upsellR_delta_arr AS ch ,
----    sum(upsellR_delta_arr) upsellR_delta_arr,
----    round((sum_arr + sum(upsellR_delta_arr)) * ratio_arr) AS new_Arr
----    OVER(PARTITION BY m.snapshot_date , m.mcid , m.arr ) AS  upsellR_delta_arr
--
--    round(CAST((sum_arr + a.upsellR_delta_arr) * ratio_arr AS NUMERIC) ,2 ) AS new_arr ,
----    sum_arr ,
--     round(CAST(sum_arr + a.upsellR_delta_arr AS NUMERIC ) , 2  )
--FROM  ryzlan.sst_ending_arr_tester_reversals AS m
--JOIN    ryzlan.ending_arr_marker AS a
--on m.mcid = a.mcid
--  AND m.snapshot_date >= a.upsellR_start_date
--  AND m.snapshot_date <= a.upsellR_end_date
--  AND a.upsell_reversal_flag > 0
--  WHERE  rnk = 1
---- and  m.mcid = '4e128cce-793a-e811-8124-70106faab5f1'
----  AND arr <> 0.0
--GROUP BY 1,2,3,4,5 ,6
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
FROM ryzlan.ending_arr_marker AS a
WHERE m.mcid = a.mcid
  AND m.snapshot_date >= a.upsellR_start_date
  AND m.snapshot_date <= a.upsellR_end_date
  AND a.upsell_reversal_flag > 0;
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
FROM ryzlan.ending_arr_marker AS a
WHERE m.mcid = a.mcid
  AND m.snapshot_date >= a.crossellR_start_date
  AND m.snapshot_date <= a.crossellR_end_date
  AND a.crosssell_reversal_flag > 0;
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
FROM ryzlan.ending_arr_marker AS a
WHERE m.mcid = a.mcid
  AND m.snapshot_date >= a.winback_downgrade_start_date
  AND m.snapshot_date <= a.winback_downgrade_end_date
  AND a.winback_downgrade_flag > 0;
-- winback downsell fix
UPDATE ryzlan.sst_ending_arr_tester_reversals AS m
SET arr = round(
    CAST(
      (sum_arr + a.winback_downsell_delta_arr) * ratio_arr AS NUMERIC
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
      ) * ratio_arr_local_currency AS NUMERIC
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
FROM ryzlan.ending_arr_marker AS a
WHERE m.mcid = a.mcid
  AND m.snapshot_date >= a.winback_downsell_start_date
  AND m.snapshot_date <= a.winback_downsell_end_date
  AND a.winback_downsell_flag > 0;
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
FROM ryzlan.ending_arr_marker AS a
WHERE m.mcid = a.mcid
  AND m.snapshot_date >= a.cpiR_start_date
  AND m.snapshot_date <= a.cpiR_end_date
  AND a.cpi_reversal_flag > 0;
DROP TABLE IF EXISTS ryzlan.sst_winbacks_missing_records;
CREATE TABLE ryzlan.sst_winbacks_missing_records AS (
  WITH pull_back_records AS (
    SELECT a.*
    FROM ryzlan.sst_ending_arr_tester_reversals AS a
      JOIN ryzlan.ending_arr_marker AS b ON a.mcid = b.mcid
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
    JOIN ryzlan.ending_arr_marker AS b ON a.mcid = b.mcid
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