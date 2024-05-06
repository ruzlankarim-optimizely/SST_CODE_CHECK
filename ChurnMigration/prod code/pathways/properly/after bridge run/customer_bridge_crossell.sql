--############################
--cross sell
--############################
--RAISE NOTICE 'Running crossell update on sst_customer_bridge...';
drop table if exists temp_pb_crosssell;
create temp table temp_pb_crosssell as
select mcid,
  product_bridge,
  currency_code,
  evaluation_period,
  sum(product_arr_change_ccfx) as product_arr_change_ccfx,
  sum(product_arr_change_lcu) as product_arr_change_lcu
from sandbox.sst_product_group_bridge
where 1 = 1
  and product_bridge IN ('Cross-sell', 'Cross-sell - migration')
  and evaluation_period = '2022M10'
group by 1,
  2,
  3,
  4;
drop table if exists temp_pb_crosssell_final;
create temp table temp_pb_crosssell_final as with temp as (
  SELECT a.*,
    b.product_bridge,
    b.product_arr_change_ccfx,
    b.product_arr_change_lcu,
    count(a.mcid) over(
      PARTITION BY a.mcid,
      a.evaluation_period,
      a.baseline_currency,
      a.customer_bridge
    ) AS count_pb_records,
    count(b.mcid) over(
      PARTITION BY b.mcid,
      b.evaluation_period,
      b.currency_code,
      b.product_bridge
    ) AS count_pb_bridges,
    sum(product_arr_change_ccfx) over(
      PARTITION BY a.mcid,
      a.evaluation_period,
      a.baseline_currency,
      a.customer_bridge
    ) AS total_pb_amount_ccfx,
    sum(product_arr_change_lcu) over(
      PARTITION BY a.mcid,
      a.evaluation_period,
      a.baseline_currency,
      a.customer_bridge
    ) AS total_pb_amount_lcu
  from arr_bridge_tmp a
    LEFT join temp_pb_crosssell b on a.mcid = b.mcid
    AND b.currency_code = a.baseline_currency
  where a.customer_bridge not in ('Flat', 'Rounding')
    and a.evaluation_period = '2022M10'
    and customer_arr_change_ccfx > 0 --    
),
classifier AS (
  select *,
    CASE
      WHEN abs(round(total_pb_amount_ccfx)) = abs(round(customer_arr_change_ccfx)) THEN 'EQUAL'
      ELSE CASE
        WHEN abs(round(total_pb_amount_ccfx)) < abs(round(customer_arr_change_ccfx)) THEN 'LESS'
        ELSE CASE
          WHEN abs(round(total_pb_amount_ccfx)) > abs(round(customer_arr_change_ccfx)) THEN CASE
            WHEN count_pb_records > count_pb_bridges THEN 'GREATER WITH MIGRATION'
            ELSE CASE
              WHEN product_bridge NOT ILIKE ('%migration%') THEN 'GREATER WITHOUT MIGRATION'
              ELSE NULL
            END
          END
        END
      END
    END AS flag
  from TEMP
)
SELECT a.*,
  CASE
    WHEN abs(round(total_pb_amount_ccfx)) < abs(round(customer_arr_change_ccfx)) THEN round(
      (customer_arr_change_ccfx - total_pb_amount_ccfx) / count_pb_records
    )
    ELSE NULL
  END AS leftover_ccfx,
  CASE
    WHEN abs(round(total_pb_amount_lcu)) < abs(round(customer_arr_change_lcu)) THEN round(
      customer_arr_change_lcu - total_pb_amount_lcu / count_pb_records
    )
    ELSE NULL
  END AS leftover_lcu
FROM classifier AS a;
DELETE FROM arr_bridge_tmp AS a USING temp_pb_crosssell_final AS b
WHERE a.evaluation_period = b.evaluation_period
  AND a.mcid = b.mcid
  AND a.baseline_currency = b.baseline_currency
  AND a.customer_bridge = b.customer_bridge
  AND b.flag IN ('EQUAL', 'LESS');
insert into arr_bridge_tmp (
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    name,
    baseline_currency,
    subsidiary_entity_name,
    prior_arr_usd_ccfx,
    current_arr_usd_ccfx,
    customer_arr_change_ccfx,
    prior_arr_lcu,
    current_arr_lcu,
    customer_arr_change_lcu,
    customer_bridge --, winback_period_days, wip_flag
  )
select evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  name,
  baseline_currency,
  subsidiary_entity_name,
  prior_arr_usd_ccfx * abs(
    product_arr_change_ccfx / customer_arr_change_ccfx
  ) AS prior_arr_usd_ccfx,
  current_arr_usd_ccfx * abs(
    product_arr_change_ccfx / customer_arr_change_ccfx
  ) AS current_arr_usd_ccfx,
  product_arr_change_ccfx AS customer_arr_change_ccfx,
  --customer_arr_change_ccfx,
  prior_arr_lcu * abs(
    product_arr_change_lcu / customer_arr_change_lcu
  ) AS prior_arr_lcu,
  current_arr_lcu * abs(
    product_arr_change_lcu / customer_arr_change_lcu
  ) AS current_arr_lcu,
  product_arr_change_lcu AS customer_arr_change_lcu,
  --  customer_arr_change_lcu,
  product_bridge AS customer_bridge --customer_bridge --, winback_period_days, wip_flag
from temp_pb_crosssell_final AS b
WHERE b.flag IN ('EQUAL', 'LESS');
insert into arr_bridge_tmp (
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    name,
    baseline_currency,
    subsidiary_entity_name,
    prior_arr_usd_ccfx,
    current_arr_usd_ccfx,
    customer_arr_change_ccfx,
    prior_arr_lcu,
    current_arr_lcu,
    customer_arr_change_lcu,
    customer_bridge --, winback_period_days, wip_flag
  )
SELECT evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  name,
  baseline_currency,
  subsidiary_entity_name,
  sum(prior_arr_usd_ccfx) AS prior_arr_usd_ccfx,
  sum(current_arr_usd_ccfx) AS current_arr_usd_ccfx,
  sum(customer_arr_change_ccfx) AS customer_arr_change_ccfx,
  sum(prior_arr_lcu) AS prior_arr_lcu,
  sum(current_arr_lcu) AS current_arr_lcu,
  sum(customer_arr_change_lcu) AS customer_arr_change_lcu,
  customer_bridge
FROM (
    select evaluation_period,
      prior_period,
      current_period,
      current_master_customer_id,
      prior_master_customer_id,
      mcid,
      name,
      baseline_currency,
      subsidiary_entity_name,
      prior_arr_usd_ccfx * abs(leftover_ccfx / customer_arr_change_ccfx) AS prior_arr_usd_ccfx,
      current_arr_usd_ccfx * abs(leftover_ccfx / customer_arr_change_ccfx) AS current_arr_usd_ccfx,
      leftover_ccfx AS customer_arr_change_ccfx,
      --customer_arr_change_ccfx,
      prior_arr_lcu * abs(leftover_lcu / customer_arr_change_lcu) AS prior_arr_lcu,
      current_arr_lcu * abs(leftover_lcu / customer_arr_change_lcu) AS current_arr_lcu,
      leftover_lcu AS customer_arr_change_lcu,
      --  customer_arr_change_lcu,
      customer_bridge --, winback_period_days, wip_flag ('Cross-sell', 'Cross-sell - migration')
    from temp_pb_crosssell_final AS b
    WHERE b.flag IN ('LESS')
  ) AS a
GROUP BY 1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  16;
UPDATE arr_bridge_tmp AS a
SET customer_bridge = CASE
    WHEN b.flag = 'GREATER WITH MIGRATION' THEN 'Cross-sell - migration'
    ELSE 'Cross-sell'
  END
FROM temp_pb_crosssell_final AS b
WHERE a.mcid = b.mcid
  AND a.evaluation_period = b.evaluation_period
  AND a.customer_bridge = b.customer_bridge
  AND b.flag IN (
    'GREATER WITH MIGRATION',
    'GREATER WITHOUT MIGRATION'
  );