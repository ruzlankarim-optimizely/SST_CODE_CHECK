WITH base AS (
  SELECT evaluation_period,
    current_period,
    prior_period,
    mcid,
    customer_bridge,
    customer_arr_change_ccfx,
    current_period_customer_arr_usd_ccfx,
    prior_period_customer_arr_usd_ccfx,
    winback_period_days
  FROM ufdm.sst_customer_bridge
  WHERE customer_bridge In ('Winback')
  ORDER BY current_period DESC
),
base_arr AS (
  SELECT a.*,
    DATE_PART(
      'Day',
      a.snapshot_date::Timestamp - a.start_date::Timestamp
    ) AS day_diff
  FROM (
      SELECT snapshot_date,
        mcid,
        sum(arr_usd_ccfx),
        max(date_start) AS start_date,
        max(date_end) AS end_date,
        ARRAY_AGG(DISTINCT date_start) arr_start_date,
        ARRAY_AGG(DISTINCT date_end) arr_end_date
      FROM ufdm_blue.monthly_metrics mm
      WHERE included_in_arr = 'Y'
      GROUP BY 1,
        2
    ) AS a
),
final_table AS (
  SELECT a.*,
    --    b.* ,
    CASE
      WHEN day_diff < 94 THEN 'Lapsed Renewal'
      ELSE customer_bridge
    END AS lapse_flag
  FROM base AS a
    JOIN base_arr AS b ON a.mcid = b.mcid
    AND a.current_period = b.snapshot_date
)
update ufdm.sst_customer_bridge a
set customer_bridge = lapse_flag
from final_table b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.evaluation_period = var_period
  and a.customer_bridge in ('Winback')