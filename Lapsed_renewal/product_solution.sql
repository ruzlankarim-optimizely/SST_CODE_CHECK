WITH base AS (
  SELECT evaluation_period,
    current_period,
    prior_period,
    mcid,
    current_product_solution,
    prior_product_solution,
    product_bridge,
    product_arr_change_ccfx,
    current_period_product_arr_usd_ccfx,
    prior_period_product_arr_usd_ccfx
  FROM ufdm.sst_product_bridge_product_solution spbpg
  WHERE product_bridge In ('Winback ST', 'Winback LT', 'Winback')
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
        new_product_solution,
        sum(arr_usd_ccfx) sum_arr,
        max(date_start) AS start_date,
        max(date_end) AS end_date,
        ARRAY_AGG(DISTINCT date_start) arr_start_date,
        ARRAY_AGG(DISTINCT date_end) arr_end_date
      FROM ufdm.arr
      GROUP BY 1,
        2,
        3
    ) AS a
),
final_table AS (
  SELECT a.*,
    --    b.snapshot_date  ,
    --    b.new_product_solution  , 
    --    b.sum_arr  ,
    --    b.start_date , 
    --    b.end_date , 
    --    b.day_diff ,
    --    b.arr_start_date ,
    --    b.arr_end_date , 
    CASE
      WHEN day_diff < 94 THEN 'Lapsed Renewal'
      ELSE product_bridge
    END AS lapse_flag
  FROM base AS a
    LEFT JOIN base_arr AS b ON a.mcid = b.mcid
    AND a.current_period = b.snapshot_date
    AND a.current_product_solution = b.new_product_solution
)
update ufdm.sst_product_bridge_product_solution a
set product_bridge = lapse_flag
from final_table b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.current_product_solution = b.current_product_solution
  and a.evaluation_period = var_period
  and a.product_bridge in ('Winback', 'Winback ST', 'Winback LT')