DROP TABLE IF EXISTS ryzlan.ending_arr_marker;
CREATE TABLE ryzlan.ending_arr_marker AS (
  WITH main AS (
    SELECT --id,
      evaluation_period,
      TO_CHAR(
        (
          CASE
            WHEN current_period IS NULL
            AND prior_period IS NOT NULL then date(
              date_trunc('month', prior_period) + INTERVAL '1 month' + interval '1 month - 1 day'
            )
            ELSE current_period
          END
        )::DATE,
        'YYYY-MM'
      ) AS eval_period,
      CASE
        WHEN current_period IS NULL
        AND prior_period IS NOT NULL then date(
          date_trunc('month', prior_period) + INTERVAL '1 month' + interval '1 month - 1 day'
        )
        ELSE current_period
      END AS current_period,
      CASE
        WHEN prior_period IS NULL
        AND current_period IS NOT NULL then date(
          date_trunc('month', current_period) - INTERVAL '1 month' + interval '1 month - 1 day'
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
    FROM ryzlan.sst_customer_bridge_reversal_fix --    ryzlan.sst_customer_bridge_reversal_fix
      --    ufdm_archive.sst_customer_bridge_lcoked_07052024_1157
      --    ryzlan.sst_customer_bridge_pp
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
      Lag(customer_bridge, 1) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num desc
      ) AS "n - 1 bridge_movement",
      Lag(customer_bridge, 2) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num desc
      ) AS "n - 2 bridge_movement",
      Lag(customer_bridge, 3) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num desc
      ) AS "n - 3 bridge_movement",
      Lag(customer_bridge, 4) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num desc
      ) AS "n - 4 bridge_movement",
      Lag(customer_bridge, 5) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num desc
      ) AS "n - 5 bridge_movement",
      Lag(customer_bridge, 6) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num desc
      ) AS "n - 6 bridge_movement",
      Lag(customer_bridge, 7) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num desc
      ) AS "n - 7 bridge_movement",
      Lag(customer_bridge, 8) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num desc
      ) AS "n - 8 bridge_movement",
      Lag(customer_bridge, 9) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num desc
      ) AS "n - 9 bridge_movement",
      Lag(customer_bridge, 10) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num desc
      ) AS "n - 10 bridge_movement",
      Lag(customer_bridge, 11) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num desc
      ) AS "n - 11 bridge_movement",
      Lag(customer_bridge, 12) OVER(
        PARTITION BY mcid
        ORDER BY eval_period,
          row_num desc
      ) AS "n - 12 bridge_movement"
    FROM base
  ),
  base_checker AS (
    SELECT *,
      CASE
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 1 bridge_movement" = 'Up Sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 1) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 2 bridge_movement" = 'Up Sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 2) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 3 bridge_movement" = 'Up Sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 3) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 4 bridge_movement" = 'Up Sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 4) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 5 bridge_movement" = 'Up Sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 5) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 6 bridge_movement" = 'Up Sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 6) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 7 bridge_movement" = 'Up Sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 7) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 8 bridge_movement" = 'Up Sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 8) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 9 bridge_movement" = 'Up Sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 9) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 10 bridge_movement" = 'Up Sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 10) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 11 bridge_movement" = 'Up Sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 11) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Up Sell Reversal'
        AND "n - 12 bridge_movement" = 'Up Sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 12) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        ELSE 0
      END AS upsell_reversal_flag,
      CASE
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 1 bridge_movement" = 'Cross-sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 1) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 2 bridge_movement" = 'Cross-sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 2) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 3 bridge_movement" = 'Cross-sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 3) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 4 bridge_movement" = 'Cross-sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 4) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 5 bridge_movement" = 'Cross-sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 5) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 6 bridge_movement" = 'Cross-sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 6) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 7 bridge_movement" = 'Cross-sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 7) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 8 bridge_movement" = 'Cross-sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 8) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 9 bridge_movement" = 'Cross-sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 9) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 10 bridge_movement" = 'Cross-sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 10) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 11 bridge_movement" = 'Cross-sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 11) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Cross-sell Reversal'
        AND "n - 12 bridge_movement" = 'Cross-sell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 12) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        ELSE 0
      END AS crosssell_reversal_flag,
      CASE
        WHEN (
          customer_bridge = 'Winback ST'
          or customer_bridge = 'Winback'
          or customer_bridge = 'Winback LT'
        )
        AND "n - 1 bridge_movement" = 'Churn' then DATE_PART(
          'Day',
          (
            Lag(current_period, 1) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN (
          customer_bridge = 'Winback ST'
          or customer_bridge = 'Winback'
          or customer_bridge = 'Winback LT'
        )
        AND "n - 2 bridge_movement" = 'Churn' then DATE_PART(
          'Day',
          (
            Lag(current_period, 2) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN (
          customer_bridge = 'Winback ST'
          or customer_bridge = 'Winback'
          or customer_bridge = 'Winback LT'
        )
        AND "n - 3 bridge_movement" = 'Churn' then DATE_PART(
          'Day',
          (
            Lag(current_period, 3) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN (
          customer_bridge = 'Winback ST'
          or customer_bridge = 'Winback'
          or customer_bridge = 'Winback LT'
        )
        AND "n - 4 bridge_movement" = 'Churn' then DATE_PART(
          'Day',
          (
            Lag(current_period, 4) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN (
          customer_bridge = 'Winback ST'
          or customer_bridge = 'Winback'
          or customer_bridge = 'Winback LT'
        )
        AND "n - 5 bridge_movement" = 'Churn' then DATE_PART(
          'Day',
          (
            Lag(current_period, 5) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN (
          customer_bridge = 'Winback ST'
          or customer_bridge = 'Winback'
          or customer_bridge = 'Winback LT'
        )
        AND "n - 6 bridge_movement" = 'Churn' then DATE_PART(
          'Day',
          (
            Lag(current_period, 6) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN (
          customer_bridge = 'Winback ST'
          or customer_bridge = 'Winback'
          or customer_bridge = 'Winback LT'
        )
        AND "n - 7 bridge_movement" = 'Churn' then DATE_PART(
          'Day',
          (
            Lag(current_period, 7) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN (
          customer_bridge = 'Winback ST'
          or customer_bridge = 'Winback'
          or customer_bridge = 'Winback LT'
        )
        AND "n - 8 bridge_movement" = 'Churn' then DATE_PART(
          'Day',
          (
            Lag(current_period, 8) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN (
          customer_bridge = 'Winback ST'
          or customer_bridge = 'Winback'
          or customer_bridge = 'Winback LT'
        )
        AND "n - 9 bridge_movement" = 'Churn' then DATE_PART(
          'Day',
          (
            Lag(current_period, 9) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN (
          customer_bridge = 'Winback ST'
          or customer_bridge = 'Winback'
          or customer_bridge = 'Winback LT'
        )
        AND "n - 10 bridge_movement" = 'Churn' then DATE_PART(
          'Day',
          (
            Lag(current_period, 10) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN (
          customer_bridge = 'Winback ST'
          or customer_bridge = 'Winback'
          or customer_bridge = 'Winback LT'
        )
        AND "n - 11 bridge_movement" = 'Churn' then DATE_PART(
          'Day',
          (
            Lag(current_period, 11) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN (
          customer_bridge = 'Winback ST'
          or customer_bridge = 'Winback'
          or customer_bridge = 'Winback LT'
        )
        AND "n - 12 bridge_movement" = 'Churn' then DATE_PART(
          'Day',
          (
            Lag(current_period, 12) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        ELSE 0
      END AS winback_reversal_flag,
      CASE
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 1 bridge_movement" = 'Downgrade' then DATE_PART(
          'Day',
          (
            Lag(current_period, 1) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 2 bridge_movement" = 'Downgrade' then DATE_PART(
          'Day',
          (
            Lag(current_period, 2) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 3 bridge_movement" = 'Downgrade' then DATE_PART(
          'Day',
          (
            Lag(current_period, 3) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 4 bridge_movement" = 'Downgrade' then DATE_PART(
          'Day',
          (
            Lag(current_period, 4) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 5 bridge_movement" = 'Downgrade' then DATE_PART(
          'Day',
          (
            Lag(current_period, 5) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 6 bridge_movement" = 'Downgrade' then DATE_PART(
          'Day',
          (
            Lag(current_period, 6) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 7 bridge_movement" = 'Downgrade' then DATE_PART(
          'Day',
          (
            Lag(current_period, 7) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 8 bridge_movement" = 'Downgrade' then DATE_PART(
          'Day',
          (
            Lag(current_period, 8) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 9 bridge_movement" = 'Downgrade' then DATE_PART(
          'Day',
          (
            Lag(current_period, 9) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 10 bridge_movement" = 'Downgrade' then DATE_PART(
          'Day',
          (
            Lag(current_period, 10) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 11 bridge_movement" = 'Downgrade' then DATE_PART(
          'Day',
          (
            Lag(current_period, 11) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downgrade'
        AND "n - 12 bridge_movement" = 'Downgrade' then DATE_PART(
          'Day',
          (
            Lag(current_period, 12) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        ELSE 0
      END AS winback_downgrade_flag,
      CASE
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 1 bridge_movement" = 'Downsell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 1) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 2 bridge_movement" = 'Downsell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 2) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 3 bridge_movement" = 'Downsell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 3) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 4 bridge_movement" = 'Downsell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 4) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 5 bridge_movement" = 'Downsell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 5) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 6 bridge_movement" = 'Downsell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 6) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 7 bridge_movement" = 'Downsell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 7) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 8 bridge_movement" = 'Downsell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 8) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 9 bridge_movement" = 'Downsell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 9) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 10 bridge_movement" = 'Downsell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 10) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 11 bridge_movement" = 'Downsell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 11) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Win back Downsell'
        AND "n - 12 bridge_movement" = 'Downsell' then DATE_PART(
          'Day',
          (
            Lag(current_period, 12) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        ELSE 0
      END AS winback_downsell_flag,
      CASE
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 1 bridge_movement" = 'Price Uplift' then DATE_PART(
          'Day',
          (
            Lag(current_period, 1) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 2 bridge_movement" = 'Price Uplift' then DATE_PART(
          'Day',
          (
            Lag(current_period, 2) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 3 bridge_movement" = 'Price Uplift' then DATE_PART(
          'Day',
          (
            Lag(current_period, 3) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 4 bridge_movement" = 'Price Uplift' then DATE_PART(
          'Day',
          (
            Lag(current_period, 4) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 5 bridge_movement" = 'Price Uplift' then DATE_PART(
          'Day',
          (
            Lag(current_period, 5) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 6 bridge_movement" = 'Price Uplift' then DATE_PART(
          'Day',
          (
            Lag(current_period, 6) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 7 bridge_movement" = 'Price Uplift' then DATE_PART(
          'Day',
          (
            Lag(current_period, 7) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 8 bridge_movement" = 'Price Uplift' then DATE_PART(
          'Day',
          (
            Lag(current_period, 8) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 9 bridge_movement" = 'Price Uplift' then DATE_PART(
          'Day',
          (
            Lag(current_period, 9) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 10 bridge_movement" = 'Price Uplift' then DATE_PART(
          'Day',
          (
            Lag(current_period, 10) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 11 bridge_movement" = 'Price Uplift' then DATE_PART(
          'Day',
          (
            Lag(current_period, 11) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        WHEN customer_bridge = 'Price Uplift Reversal'
        AND "n - 12 bridge_movement" = 'Price Uplift' then DATE_PART(
          'Day',
          (
            Lag(current_period, 12) OVER(
              PARTITION BY mcid
              ORDER BY eval_period,
                row_num desc
            )
          )::TIMESTAMP - current_period::TIMESTAMP
        ) * -1
        ELSE 0
      END AS cpi_reversal_flag
    FROM base_two
  ) --SELECT * FROM base_checker   WHERE mcid =  'e87fcbb2-d6d3-5a1e-f9df-7eb3f4b482ce'
,
  base_checker_two AS (
    SELECT a.*,
      --- Up Sell Reversal
      CASE
        WHEN (a.upsell_reversal_flag > 0) THEN Lag(
          a.current_period_customer_arr_usd_ccfx,
          a.upsell_reversal_flag
        ) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num desc
        )
        ELSE NULL
      END AS upsellR_start_arr,
      CASE
        WHEN (a.upsell_reversal_flag > 0) THEN Lag(a.current_period, a.upsell_reversal_flag) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num desc
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
        WHEN (a.crosssell_reversal_flag > 0) THEN Lag(
          a.current_period_customer_arr_usd_ccfx,
          a.crosssell_reversal_flag
        ) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num desc
        )
        ELSE NULL
      END AS crossellR_start_arr,
      CASE
        WHEN (a.crosssell_reversal_flag > 0) THEN Lag(a.current_period, a.crosssell_reversal_flag) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num desc
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
        WHEN (a.cpi_reversal_flag > 0) THEN Lag(
          a.current_period_customer_arr_usd_ccfx,
          a.cpi_reversal_flag
        ) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num desc
        )
        ELSE NULL
      END AS cpiR_start_arr,
      CASE
        WHEN a.cpi_reversal_flag > 0 THEN Lag(a.current_period, a.cpi_reversal_flag) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num desc
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
        WHEN (a.winback_reversal_flag > 0) THEN Lag(
          a.current_period_customer_arr_usd_ccfx,
          a.winback_reversal_flag
        ) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num desc
        )
        ELSE NULL
      END AS winback_start_arr,
      CASE
        WHEN a.winback_reversal_flag > 0 THEN Lag(a.current_period, a.winback_reversal_flag) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num desc
        )
        ELSE NULL
      END AS winback_start_date,
      CASE
        WHEN a.winback_reversal_flag > 0 THEN Lag(a.prior_period, a.winback_reversal_flag) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num desc
        )
        ELSE NULL
      END AS winback_pull_back_date,
      CASE
        WHEN a.winback_reversal_flag > 0 THEN prior_period
        ELSE NULL
      END AS winback_end_date,
      CASE
        WHEN a.winback_reversal_flag > 0 THEN (
          customer_arr_change_ccfx + Lag(
            a.customer_arr_change_ccfx,
            a.winback_reversal_flag
          ) OVER(
            PARTITION BY a.mcid
            ORDER BY eval_period,
              row_num desc
          )
        )
        ELSE NULL
      END AS winback_delta_arr,
      CASE
        WHEN a.winback_reversal_flag > 0 THEN (
          customer_arr_change_lcu + Lag(
            a.customer_arr_change_lcu,
            a.winback_reversal_flag
          ) OVER(
            PARTITION BY a.mcid
            ORDER BY eval_period,
              row_num desc
          )
        )
        ELSE NULL
      END AS winback_delta_arr_lcu,
      -- Winback downgrade
      CASE
        WHEN (a.winback_downgrade_flag > 0) THEN Lag(
          a.current_period_customer_arr_usd_ccfx,
          a.winback_downgrade_flag
        ) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num desc
        )
        ELSE NULL
      END AS winback_downgrade_start_arr,
      CASE
        WHEN a.winback_downgrade_flag > 0 THEN Lag(a.current_period, a.winback_downgrade_flag) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num desc
        )
        ELSE NULL
      END AS winback_downgrade_start_date,
      CASE
        WHEN a.winback_downgrade_flag > 0 THEN Lag(a.prior_period, a.winback_downgrade_flag) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num desc
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
        WHEN (a.winback_downsell_flag > 0) THEN Lag(
          a.current_period_customer_arr_usd_ccfx,
          a.winback_downsell_flag
        ) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num desc
        )
        ELSE NULL
      END AS winback_downsell_start_arr,
      CASE
        WHEN a.winback_downsell_flag > 0 THEN Lag(a.current_period, a.winback_downsell_flag) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num desc
        )
        ELSE NULL
      END AS winback_downsell_start_date,
      CASE
        WHEN a.winback_downsell_flag > 0 THEN Lag(a.prior_period, a.winback_downsell_flag) OVER(
          PARTITION BY a.mcid
          ORDER BY eval_period,
            row_num desc
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
  )
  SELECT a.*
  FROM base_checker_two AS a
  WHERE mcid = '6e2dddfc-4f35-837f-6912-ed2b9bc5dab0' (
      upsell_reversal_flag > 0
      OR crosssell_reversal_flag > 0
      OR cpi_reversal_flag > 0
      OR winback_reversal_flag > 0
      OR winback_downsell_flag > 0
      OR winback_downgrade_flag > 0
    ) --   AND mcid IN ('f6e7e4fa-2b48-e81a-bcb4-46dfd569d878' , 'f2c843b1-605f-e7b5-71a3-402283550691' , 'fb462315-cce6-e411-9afb-0050568d2da8')
);
DROP TABLE IF EXISTS ryzlan.sst_ending_arr_tester_reversals;
CREATE TABLE ryzlan.sst_ending_arr_tester_reversals AS WITH main AS (
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
  FROM ufdm_archive.sst_lcoked_07052024_1157 --  ryzlan.sst_adhoc
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
UPDATE ryzlan.sst_ending_arr_tester_reversals AS m
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
-- Fix cross-sell  Reversal 
UPDATE ryzlan.sst_ending_arr_tester_reversals AS m
SET arr = round((sum_arr + a.crossellR_delta_arr) * ratio_arr),
  sum_arr = round(sum_arr + a.crossellR_delta_arr),
  baseline_arr_local_currency = round(
    (
      sum_baseline_arr_local_currency + a.crossellR_delta_arr_lcu
    ) * ratio_arr_local_currency
  ),
  sum_baseline_arr_local_currency = round(
    sum_baseline_arr_local_currency + a.crossellR_delta_arr_lcu
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
    (sum_arr + a.winback_downgrade_delta_arr) * ratio_arr
  ),
  sum_arr = round(sum_arr + a.winback_downgrade_delta_arr),
  baseline_arr_local_currency = round(
    (
      sum_baseline_arr_local_currency + a.winback_downgrade_delta_arr_lcu
    ) * ratio_arr_local_currency
  ),
  sum_baseline_arr_local_currency = round(
    sum_baseline_arr_local_currency + a.winback_downgrade_delta_arr_lcu
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
    (sum_arr + a.winback_downsell_delta_arr) * ratio_arr
  ),
  sum_arr = round(sum_arr + a.winback_downsell_delta_arr),
  baseline_arr_local_currency = round(
    (
      sum_baseline_arr_local_currency + a.winback_downsell_delta_arr_lcu
    ) * ratio_arr_local_currency
  ),
  sum_baseline_arr_local_currency = round(
    sum_baseline_arr_local_currency + a.winback_downsell_delta_arr_lcu
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
DROP TABLE IF EXISTS ryzlan.sst_winbacks_missing_records;
CREATE TABLE ryzlan.sst_winbacks_missing_records AS (
  WITH pull_back_records AS (
    SELECT a.*
    FROM ryzlan.sst_ending_arr_tester_reversals AS a
      JOIN ryzlan.ending_arr_marker AS b ON a.mcid = b.mcid
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
      interval '1 month'
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
  acquire_product_solution_li,
  sum_arr,
  ending_arr_comment
FROM ryzlan.sst_ending_arr_tester_reversals;