CREATE TABLE ryzlan.sst_customer_bridge_ending_arr_test AS
SELECT *
FROM ufdm.sst_customer_bridge scb
LIMIT 1;
TRUNCATE TABLE ryzlan.sst_customer_bridge_ending_arr_test;
CREATE OR REPLACE FUNCTION ryzlan.sp_populate_sst_customer_bridge_ending_arr_test(var_period text) RETURNS void LANGUAGE plpgsql AS $function$ BEGIN
DELETE from ryzlan.sst_customer_bridge_ending_arr_test
where evaluation_period = var_period;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--SST customer Bridge
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists prior_period_customer_arr;
create temp table prior_period_customer_arr as
SELECT snapshot_date,
  a.mcid as master_customer_id,
  a.base_currency as baseline_currency,
  sum(arr) AS arr_usd_ccfx,
  SUM(baseline_arr_local_currency) as arr_lcu
FROM ryzlan.sst_ending_arr_tester_final a
WHERE 1 = 1
  AND snapshot_date = (
    SELECT prior_period
    from ufdm_grey.periods
    WHERE evaluation_period = var_period
  )
  and a.overage_flag ilike '%N%'
GROUP BY 1,
  2,
  3;
drop table if exists current_period_customer_arr;
create temp table current_period_customer_arr as
SELECT snapshot_date,
  a.mcid as master_customer_id,
  a.base_currency as baseline_currency,
  sum(arr) AS arr_usd_ccfx,
  SUM(baseline_arr_local_currency) as arr_lcu
FROM ryzlan.sst_ending_arr_tester_final a
WHERE 1 = 1
  AND snapshot_date = (
    SELECT current_period
    from ufdm_grey.periods
    WHERE evaluation_period = var_period
  )
  and a.overage_flag ilike '%N%'
GROUP BY 1,
  2,
  3;
drop table if exists customer_level_arr;
create temp table customer_level_arr as
SELECT c1.master_customer_id AS current_cust_id,
  c2.master_customer_id AS prior_cust_id,
  c2.snapshot_date AS prior_period,
  c1.snapshot_date AS current_period,
  c1.baseline_currency as current_baseline_currency,
  c2.baseline_currency as prior_baseline_currency,
  coalesce(c1.arr_usd_ccfx, 0) AS current_arr_usd_ccfx,
  coalesce(c2.arr_usd_ccfx, 0) AS prior_arr_usd_ccfx,
  coalesce(c1.arr_lcu, 0) AS current_arr_lcu,
  coalesce(c2.arr_lcu, 0) AS prior_arr_lcu,
  COALESCE(c1.baseline_currency, c2.baseline_currency) AS baseline_currency
FROM current_period_customer_arr c1
  FULL OUTER JOIN prior_period_customer_arr c2 ON c1.master_customer_id = c2.master_customer_id
  and c1.baseline_currency = c2.baseline_currency;
drop table if exists account;
create temp table account as
SELECT coalesce(a.dynamics_id_c, a.sf_guid_c) as master_customer_id,
  a.name,
  row_number() over (
    partition by coalesce(a.dynamics_id_c, a.sf_guid_c)
  ) as rn
FROM opti_salesforce.account a
WHERE a.is_deleted IS DISTINCT
FROM TRUE;
------------------------------------------
-- Evaluate bridge categories
-- New
-- Upsell
-- Flat
-- Churn
-- Partial Churn
--------------------------------------------------
drop table if exists arr_bridge_tmp;
create temp table arr_bridge_tmp as with arr_bridge AS (
  SELECT per.evaluation_period,
    cla.prior_period,
    cla.current_period,
    cla.current_cust_id as current_master_customer_id,
    cla.prior_cust_id as prior_master_customer_id,
    coalesce(current_cust_id, prior_cust_id) as mcid,
    a.name,
    cla.baseline_currency,
    null as subsidiary_entity_name,
    round(
      (coalesce(cla.current_arr_usd_ccfx::numeric, 0)),
      2
    ) as current_arr_usd_ccfx,
    round(
      (coalesce(cla.prior_arr_usd_ccfx::numeric, 0)),
      2
    ) as prior_arr_usd_ccfx,
    round(
      (
        coalesce(cla.current_arr_usd_ccfx::numeric, 0) - coalesce(cla.prior_arr_usd_ccfx::numeric, 0)
      ),
      2
    ) AS customer_arr_change_ccfx,
    round((coalesce(cla.current_arr_lcu::numeric, 0)), 2) as current_arr_lcu,
    round((coalesce(cla.prior_arr_lcu::numeric, 0)), 2) as prior_arr_lcu,
    round(
      (
        coalesce(cla.current_arr_lcu::numeric, 0) - coalesce(cla.prior_arr_lcu::numeric, 0)
      ),
      2
    ) AS customer_arr_change_lcu,
    CASE
      WHEN (
        coalesce (cla.prior_arr_usd_ccfx, 0) = 0
        AND cla.current_arr_usd_ccfx > 0
      ) THEN 'New'
      WHEN cla.current_arr_usd_ccfx - cla.prior_arr_usd_ccfx BETWEEN -1 and 1 THEN 'Flat'
      WHEN cla.current_arr_usd_ccfx - cla.prior_arr_usd_ccfx > 1 THEN 'Up Sell'
      WHEN cla.current_arr_usd_ccfx - cla.prior_arr_usd_ccfx < - 1
      AND cla.current_arr_usd_ccfx > 0 THEN 'Partial Churn' -- different products, lower ARR
      WHEN cla.prior_arr_usd_ccfx > 0
      AND (
        cla.current_arr_usd_ccfx = 0
        OR cla.current_arr_usd_ccfx IS NULL
      ) THEN 'Churn'
      ELSE 'N/A'
    END AS customer_bridge
  FROM customer_level_arr cla
    LEFT JOIN account a on a.master_customer_id = cla.current_cust_id
    and a.rn = 1
    CROSS JOIN ufdm_grey.periods per
  WHERE 1 = 1
    AND per.evaluation_period = var_period
  ORDER BY coalesce(a.name)
)
select *
from arr_bridge;
--Downgrade
update arr_bridge_tmp
set customer_bridge = 'Downgrade'
where customer_bridge = 'Partial Churn';
/*
 --Cross-sell
 WITH PG_F_C AS (
 SELECT
 mcid,
 COUNT(distinct customer_bridge)  as product_family_count
 FROM arr_bridge_tmp
 WHERE current_arr_usd_ccfx > 0 and evaluation_period=var_period
 group by mcid,evaluation_period
 )
 UPDATE arr_bridge_tmp AS t
 SET customer_bridge = CASE
 WHEN pfc.product_family_count >1
 THEN 'Cross-sell'
 ELSE customer_bridge
 END
 FROM PG_F_C AS pfc
 WHERE t.mcid = pfc.mcid
 AND t.customer_bridge = 'New'
 and t.evaluation_period=var_period;
 
 
 --Downsell
 WITH PG_F_C AS (
 SELECT
 mcid,
 COUNT( distinct customer_bridge)  as product_family_count
 FROM arr_bridge_tmp
 WHERE prior_arr_usd_ccfx > 0 and evaluation_period=var_period
 group by mcid,evaluation_period
 )
 --  select * from PG_F_C
 
 UPDATE arr_bridge_tmp AS t
 SET customer_bridge = CASE
 WHEN pfc.product_family_count >1
 THEN 'Downsell'
 ELSE customer_bridge
 END
 FROM PG_F_C AS pfc
 WHERE t.mcid = pfc.mcid
 AND t.customer_bridge = 'Churn'
 and t.evaluation_period=var_period;
 */
--#############################################
--WIP/WINBACK
--#############################################
drop table if exists arr_new_products_tmp;
create temp table arr_new_products_tmp AS
select distinct a.mcid as mcid,
  a.current_period as snapshot_date,
  a.current_arr_usd_ccfx as arr_at_new,
  a.current_arr_lcu as arr_lcu_at_new,
  baseline_currency
from arr_bridge_tmp a
where customer_bridge = 'New';
--get most recent postivie arr for above new product which should have been churned
drop table if exists arr_churned_products_tmp;
create temp table arr_churned_products_tmp AS with temp as (
  select b.snapshot_date,
    b.mcid --, b.product_family
,
    a.baseline_currency,
    a.snapshot_date as snapshot_date_at_new,
    sum(b.arr) as arr_at_churn,
    sum(b.baseline_arr_local_currency) as arr_lcu_at_churn,
    max(a.arr_at_new) as arr_at_new,
    max(a.arr_lcu_at_new) as arr_lcu_at_new,
    row_number() over (
      partition by b.mcid
      order by b.snapshot_date desc
    ) as rnk
  from arr_new_products_tmp a
    join ryzlan.sst_ending_arr_tester_final b on a.mcid = b.mcid
    and a.baseline_currency = b.base_currency
  where b.snapshot_date < a.snapshot_date
    and b.overage_flag ilike '%N%'
    and b.arr > 0
  group by 1,
    2,
    3,
    4
)
select *,
  (
    DATE_PART('year', snapshot_date_at_new::date) - DATE_PART('year', snapshot_date::date)
  ) * 12 + (
    DATE_PART('month', snapshot_date_at_new::date) - DATE_PART('month', snapshot_date::date)
  ) as months_diff,
  case
    when arr_at_new > arr_at_churn then 'Upsell'
    else case
      when extract(
        day
        from snapshot_date_at_new::timestamp - snapshot_date::date
      ) <= 90 then 'Winback ST'
      else 'Winback LT'
    end
  end as customer_bridge_new,
  arr_at_new - arr_at_churn as arr_diff,
  arr_lcu_at_new - arr_lcu_at_churn as arr_lcu_diff,
  extract(
    day
    from snapshot_date_at_new::timestamp - snapshot_date::date
  ) as days_diff,
  snapshot_date as churn_period
from temp
where rnk = 1
  and extract(
    day
    from snapshot_date_at_new::timestamp - snapshot_date::date
  ) < 181;
INSERT INTO ryzlan.sst_customer_bridge_ending_arr_test (
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    Prior_master_customer_id,
    "name",
    prior_period_customer_arr_usd_ccfx,
    current_period_customer_arr_usd_ccfx,
    customer_arr_change_ccfx,
    prior_period_customer_arr_lcu,
    current_period_customer_lcu,
    customer_arr_change_lcu,
    customer_bridge,
    subsidiary_entity_name,
    mcid,
    baseline_currency,
    Winback_period_days,
    Wip_Flag
  )
SELECT a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.Prior_master_customer_id,
  a."name",
  ---usd ccfx ----
  round(a.prior_arr_usd_ccfx::numeric, 2) AS prior_period_customer_arr_usd_ccfx,
  case
    when b.mcid is not null then case
      when b.arr_at_new > b.arr_at_churn then b.arr_at_churn
      else b.arr_at_new
    end
    else round(a.current_arr_usd_ccfx::numeric, 2)
  end as current_period_customer_arr_usd_ccfx,
  case
    when b.mcid is not null then case
      when b.arr_at_new > b.arr_at_churn then b.arr_at_churn
      else b.arr_at_new
    end
    else a.customer_arr_change_ccfx
  end as customer_arr_change_ccfx,
  ------------------------lcu----------------------------
  round(a.prior_arr_lcu::numeric, 2) AS prior_period_customer_arr_lcu,
  case
    when b.mcid is not null then case
      when b.arr_lcu_at_new > b.arr_lcu_at_churn then b.arr_lcu_at_churn
      else b.arr_lcu_at_new
    end
    else round(a.current_arr_lcu::numeric, 2)
  end as current_period_customer_arr_lcu,
  case
    when b.mcid is not null then case
      when b.arr_lcu_at_new > b.arr_lcu_at_churn then b.arr_lcu_at_churn
      else b.arr_lcu_at_new
    end
    else a.customer_arr_change_lcu
  end as customer_arr_change_lcu,
  case
    when b.mcid is not null then case
      when b.days_diff <= 90 then 'Winback ST'
      else 'Winback LT'
    end
    else a.customer_bridge
  end as customer_bridge,
  a.subsidiary_entity_name,
  a.mcid,
  a.baseline_currency,
  b.days_diff as Winback_period_days,
  case
    when b.days_diff <= 90 then 'Y'
    else 'N'
  end as Wip_Flag
FROM arr_bridge_tmp a
  left join arr_churned_products_tmp b on a.current_master_customer_id = b.mcid
  and a.baseline_currency = b.baseline_currency
  and a.current_period = b.snapshot_date_at_new
union all
SELECT a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.Prior_master_customer_id,
  a."name",
  round(a.prior_arr_usd_ccfx::numeric, 2) AS prior_period_customer_arr_usd_ccfx,
  b.arr_diff as current_period_customer_arr_usd_ccfx,
  b.arr_diff as customer_arr_change_ccfx,
  round(a.prior_arr_lcu::numeric, 2) AS prior_period_customer_arr_lcu,
  b.arr_lcu_diff as current_period_customer_arr_lcu,
  b.arr_lcu_diff,
  'Up Sell' as customer_bridge,
  a.subsidiary_entity_name,
  a.mcid,
  a.baseline_currency,
  null,
  null
FROM arr_bridge_tmp a
  join arr_churned_products_tmp b on a.current_master_customer_id = b.mcid
  and a.baseline_currency = b.baseline_currency
  and a.current_period = b.snapshot_date_at_new
where b.arr_at_new > b.arr_at_churn;
RAISE NOTICE 'Running subsidiary entity name insert on sst customer bridge...';
drop table if exists sub_entity_tmp;
create temp table sub_entity_tmp as --update subsidiary_entity_name
with mcid_list as (
  select distinct mcid as master_customer_id
  from arr_bridge_tmp
  where evaluation_period = var_period
),
total_arr as (
  select a.mcid as mcid,
    a.snapshot_date,
    a.subsidiary_entity_name,
    sum(a.arr) as total_arr
  from ryzlan.sst_ending_arr_tester_final a
    join mcid_list b on a.mcid = b.master_customer_id
    and a.snapshot_date in (
      SELECT prior_period
      from ufdm_grey.periods
      WHERE evaluation_period = var_period
      union
      SELECT current_period
      from ufdm_grey.periods
      WHERE evaluation_period = var_period
    )
  group by a.mcid,
    a.snapshot_date,
    a.subsidiary_entity_name
),
sub_entity as (
  select *,
    row_number () over (
      partition by mcid
      order by total_arr desc
    ) as rnk
  from total_arr
)
select *
from sub_entity
where rnk = 1;
RAISE NOTICE 'Running sub entity update on sst customer bridge...';
create index nci_sub_entity_tmp_mcid on sub_entity_tmp(mcid);
update ryzlan.sst_customer_bridge_ending_arr_test a
set subsidiary_entity_name = b.subsidiary_entity_name
from sub_entity_tmp b
where a.mcid = b.mcid
  and a.evaluation_period = var_period;
RAISE NOTICE 'Running Price increase update on sst customer bridge...';
--Price Increase updates
update ryzlan.sst_customer_bridge_ending_arr_test
set customer_bridge = 'CPI'
where customer_bridge = 'Up Sell'
  and (
    (
      customer_arr_change_ccfx / prior_period_customer_arr_usd_ccfx
    ) * 100
  )::numeric(10, 2) < case
    when evaluation_period < '2023-01-01' then 5.5
    else 10.5
  end
  and prior_period_customer_arr_usd_ccfx > 0
  and evaluation_period = var_period;
END;
$function$;
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2019M01');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2019M02');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2019M03');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2019M04');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2019M05');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2019M06');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2019M07');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2019M08');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2019M09');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2019M10');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2019M11');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2019M12');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2020M01');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2020M02');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2020M03');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2020M04');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2020M05');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2020M06');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2020M07');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2020M08');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2020M09');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2020M10');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2020M11');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2020M12');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2021M01');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2021M02');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2021M03');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2021M04');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2021M05');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2021M06');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2021M07');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2021M08');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2021M09');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2021M10');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2021M11');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2021M12');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2022M01');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2022M02');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2022M03');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2022M04');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2022M05');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2022M06');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2022M07');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2022M08');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2022M09');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2022M10');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2022M11');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2022M12');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2023M01');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2023M02');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2023M03');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2023M04');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2023M05');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2023M06');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2023M07');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2023M08');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2023M09');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2023M10');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2023M11');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2023M12');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2024M01');
select ryzlan.sp_populate_sst_customer_bridge_ending_arr_test('2024M02');


-- ryzlan.sst_customer_bridge_ending_arr_test  CUSTOMER BRIDGE TABLE TO BE TESTED 
