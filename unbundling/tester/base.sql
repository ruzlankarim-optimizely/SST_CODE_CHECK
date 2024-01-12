create or replace function ryzlan.sp_populate_product_family_churn_migration(var_period text) returns void language plpgsql as $$ BEGIN
DELETE from ryzlan.sst_product_bridge_product_family_CM
where evaluation_period = var_period;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--SST product Bridge
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists prior_period_customer_arr_tmp;
create temp table prior_period_customer_arr_tmp as
SELECT snapshot_date,
  a.mcid as master_customer_id,
  product_family as product_family,
  updated_product_group as product_group,
  a.base_currency as baseline_currency,
  max(coalesce(a.end_name, a.parent_name)) as end_customer,
  sum(arr) AS arr_usd_ccfx,
  sum(baseline_arr_local_currency) AS arr_lcu
FROM sandbox_pd.sst a
WHERE 1 = 1
  AND snapshot_date = (
    SELECT prior_period
    from ufdm_grey.periods
    WHERE evaluation_period = var_period
  )
  AND a.overage_flag IS DISTINCT
FROM 'Y'
GROUP BY 1,
  2,
  3,
  4,
  5;
drop table if exists current_period_customer_arr_tmp;
create temp table current_period_customer_arr_tmp as
SELECT snapshot_date,
  a.mcid as master_customer_id,
  product_family as product_family,
  updated_product_group as product_group,
  a.base_currency as baseline_currency,
  max(coalesce(a.end_name, a.parent_name)) as end_customer,
  sum(arr) AS arr_usd_ccfx,
  sum(baseline_arr_local_currency) AS arr_lcu
FROM sandbox_pd.sst a
WHERE 1 = 1
  AND snapshot_date = (
    SELECT current_period
    from ufdm_grey.periods
    WHERE evaluation_period = var_period
  )
  AND a.overage_flag IS DISTINCT
FROM 'Y'
GROUP BY 1,
  2,
  3,
  4,
  5;
drop table if exists customer_level_arr_tmp;
create temp table customer_level_arr_tmp as
SELECT c1.master_customer_id AS current_cust_id,
  c2.master_customer_id AS prior_cust_id,
  c1.end_customer as current_end_customer,
  c2.end_customer as prior_end_customer,
  c2.snapshot_date AS prior_period,
  c1.snapshot_date AS current_period,
  c1.baseline_currency as current_baseline_currency,
  c2.baseline_currency as prior_baseline_currency,
  COALESCE(c1.baseline_currency, c2.baseline_currency) AS baseline_currency,
  c2.product_family AS prior_product_family,
  c1.product_family AS current_product_family,
  c2.product_group as prior_product_group,
  c1.product_group as current_product_group,
  coalesce(c1.arr_usd_ccfx, 0) AS current_arr_usd_ccfx,
  coalesce(c2.arr_usd_ccfx, 0) AS prior_arr_usd_ccfx,
  coalesce(c1.arr_lcu, 0) AS current_arr_lcu,
  coalesce(c2.arr_lcu, 0) AS prior_arr_lcu --c3.arr AS prior2_arr, --WIP
FROM current_period_customer_arr_tmp c1
  FULL OUTER JOIN prior_period_customer_arr_tmp c2 ON c1.master_customer_id = c2.master_customer_id
  and c1.product_family = c2.product_family
  and c1.baseline_currency = c2.baseline_currency;
------------------------------------------
-- Evaluate
------------------------------------------
drop table if exists arr_product_bridge_tmp;
create temp table arr_product_bridge_tmp AS
SELECT per.evaluation_period,
  cla.prior_period,
  cla.current_period,
  cla.current_cust_id as current_master_customer_id,
  cla.prior_cust_id as prior_master_customer_id,
  coalesce(cla.current_cust_id, cla.prior_cust_id) as mcid,
  cla.current_product_family as current_product_family,
  cla.prior_product_family as prior_product_family,
  cla.current_product_group,
  cla.prior_product_group,
  cla.current_end_customer,
  cla.prior_end_customer,
  cla.baseline_currency,
  round(
    (coalesce(cla.current_arr_usd_ccfx::numeric, 0)),
    2
  ) as current_arr_usd_ccfx,
  round((coalesce(cla.prior_arr_usd_ccfx::numeric, 0)), 2) as prior_arr_usd_ccfx,
  round(
    (
      coalesce(cla.current_arr_usd_ccfx::numeric, 0) - coalesce(cla.prior_arr_usd_ccfx::numeric, 0)
    ),
    2
  ) AS product_arr_change_ccfx,
  round((coalesce(cla.current_arr_lcu::numeric, 0)), 2) as current_arr_lcu,
  round((coalesce(cla.prior_arr_lcu::numeric, 0)), 2) as prior_arr_lcu,
  round(
    (
      coalesce(cla.current_arr_lcu::numeric, 0) - coalesce(cla.prior_arr_lcu::numeric, 0)
    ),
    2
  ) AS product_arr_change_lcu,
  CASE
    WHEN (
      (
        coalesce (cla.prior_arr_usd_ccfx, 0) = 0
        or coalesce (cla.prior_arr_usd_ccfx, 0) = 0.00
      ) --OR (cla.prior_arr_usd_ccfx = 0 and clp.prior_product_family_agg is null))
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
  END AS product_bridge
FROM customer_level_arr_tmp cla
  CROSS JOIN ufdm_grey.periods per
WHERE 1 = 1
  AND per.evaluation_period = var_period;
--Downgrade
update arr_product_bridge_tmp
set product_bridge = 'Downgrade'
where product_bridge = 'Partial Churn';
--Cross-sell
WITH PG_F_C AS (
  SELECT mcid,
    COUNT(distinct product_bridge) as product_family_count
  FROM arr_product_bridge_tmp
  WHERE current_arr_usd_ccfx > 0
    and evaluation_period = var_period
  group by mcid,
    evaluation_period
)
UPDATE arr_product_bridge_tmp AS t
SET product_bridge = CASE
    WHEN pfc.product_family_count > 1 THEN 'Cross-sell'
    ELSE product_bridge
  END
FROM PG_F_C AS pfc
WHERE t.mcid = pfc.mcid
  AND t.product_bridge = 'New'
  and t.evaluation_period = var_period;
-------
--Downsell
WITH PG_F_C AS (
  SELECT mcid,
    COUNT(distinct product_bridge) as product_family_count
  FROM arr_product_bridge_tmp
  WHERE prior_arr_usd_ccfx > 0
    and evaluation_period = var_period
  group by mcid,
    evaluation_period
) --  select * from PG_F_C
UPDATE arr_product_bridge_tmp AS t
SET product_bridge = CASE
    WHEN pfc.product_family_count > 1 THEN 'Downsell'
    ELSE product_bridge
  END
FROM PG_F_C AS pfc
WHERE t.mcid = pfc.mcid
  AND t.product_bridge = 'Churn'
  and t.evaluation_period = var_period;
--#############################################
--WIP/WINBACK
--#############################################
drop table if exists arr_new_products_tmp;
create temp table arr_new_products_tmp AS
select a.current_master_customer_id as mcid,
  a.current_product_family as product_family,
  a.current_product_group as product_group,
  a.current_period as snapshot_date,
  a.current_arr_usd_ccfx as arr_at_new,
  a.current_arr_lcu as arr_lcu_at_new,
  baseline_currency
from arr_product_bridge_tmp a
where product_bridge in ('New', 'Cross-sell');
--get most recent postivie arr for above new product which should have been churned
drop table if exists arr_churned_products_tmp;
create temp table arr_churned_products_tmp AS with temp as (
  select b.snapshot_date,
    b.mcid,
    b.product_family as product_family,
    b.updated_product_group as product_group,
    a.baseline_currency,
    a.snapshot_date as snapshot_date_at_new,
    sum(b.arr) as arr_at_churn,
    sum(b.baseline_arr_local_currency) as arr_lcu_at_churn,
    sum(a.arr_at_new) as arr_at_new,
    sum(a.arr_lcu_at_new) as arr_lcu_at_new,
    row_number() over (
      partition by b.mcid,
      b.product_family
      order by b.snapshot_date desc
    ) as rnk
  from arr_new_products_tmp a
    join (
      select sb.mcid,
        sb.snapshot_date,
        sb.overage_flag,
        sb.product_family,
        sb.updated_product_group,
        sb.base_currency,
        sum(arr) as arr,
        sum(baseline_arr_local_currency) as baseline_arr_local_currency
      from sandbox_pd.sst sb
      group by 1,
        2,
        3,
        4,
        5,
        6
    ) b on a.mcid = b.mcid --and a.product_family = b.product_family
    and a.product_family = b.product_family
    and a.product_group = b.updated_product_group
    and a.baseline_currency = b.base_currency
  where b.snapshot_date < a.snapshot_date
    and b.overage_flag ilike '%N%'
    and b.arr > 0
  group by 1,
    2,
    3,
    4,
    5,
    6
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
        from snapshot_date_at_new::timestamp - (snapshot_date + INTERVAL '1 month')::date
      ) <= 90 then 'Winback ST'
      else 'Winback LT'
    end
  end as product_bridge_new,
  arr_at_new - arr_at_churn as arr_diff,
  arr_lcu_at_new - arr_lcu_at_churn as arr_lcu_diff,
  extract(
    day
    from snapshot_date_at_new::timestamp - (snapshot_date + INTERVAL '1 month')::date
  ) as days_diff,
  snapshot_date as churn_period
from temp
where rnk = 1
  and extract(
    day
    from snapshot_date_at_new::timestamp - (snapshot_date + INTERVAL '1 month')::date
  ) < 186;
-- create index if not exists nci_arr_churned_products_tmp_tmp_composite on arr_churned_products_tmp(mcid,product_family,baseline_currency,snapshot_date_at_new) include(arr_at_new, arr_at_churn);
INSERT INTO ryzlan.sst_product_bridge_product_family_CM (
    evaluation_period,
    prior_period,
    current_period,
    current_end_customer,
    prior_end_customer,
    mcid,
    current_master_customer_id,
    Prior_master_customer_id,
    current_product_family,
    prior_product_family,
    current_product_group,
    prior_product_group,
    --         "name",
    prior_period_product_arr_usd_ccfx,
    current_period_product_arr_usd_ccfx,
    product_arr_change_ccfx,
    prior_period_product_arr_lcu,
    current_period_product_arr_lcu,
    product_arr_change_lcu,
    product_bridge,
    Winback_period_days,
    Wip_Flag,
    price_increase_amount,
    subsidiary_entity_name,
    churn_period,
    currency_code
  )
SELECT a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_end_customer,
  a.prior_end_customer,
  a.mcid,
  a.current_master_customer_id,
  a.Prior_master_customer_id,
  a.current_product_family,
  a.prior_product_family,
  a.current_product_group,
  a.prior_product_group,
  --         "name",
  round(a.prior_arr_usd_ccfx::numeric, 2) AS prior_period_customer_arr_usd_ccfx,
  --round(a.current_arr_usd_ccfx::numeric,2) AS current_period_customer_arr_usd_ccfx,
  case
    when b.mcid is not null then case
      when b.arr_at_new > b.arr_at_churn then b.arr_at_churn
      else b.arr_at_new
    end --round(b.arr_at_churn::numeric,2)
    else round(a.current_arr_usd_ccfx::numeric, 2)
  end as current_period_customer_arr_usd_ccfx,
  case
    when b.mcid is not null then case
      when b.arr_at_new > b.arr_at_churn then b.arr_at_churn
      else b.arr_at_new
    end
    else a.product_arr_change_ccfx
  end as product_arr_change_ccfx,
  ------------------------lcu----------------------------
  round(a.prior_arr_lcu::numeric, 2) AS prior_period_product_arr_lcu,
  case
    when b.mcid is not null then case
      when b.arr_lcu_at_new > b.arr_lcu_at_churn then b.arr_lcu_at_churn
      else b.arr_lcu_at_new
    end --round(b.arr_lcu_at_churn::numeric,2)
    else round(a.current_arr_lcu::numeric, 2)
  end as current_period_product_arr_lcu,
  case
    when b.mcid is not null then case
      when b.arr_lcu_at_new > b.arr_lcu_at_churn then b.arr_lcu_at_churn
      else b.arr_lcu_at_new
    end
    else a.product_arr_change_lcu
  end as product_arr_change_lcu,
  case
    when b.mcid is not null then case
      when b.days_diff <= 90 then 'Winback ST'
      else 'Winback LT'
    end --b.product_bridge_new --'Winback' --/WIP
    else a.product_bridge
  end as product_bridge,
  b.days_diff as Winback_period_days,
  case
    when b.days_diff <= 90 then 'Y'
    else 'N'
  end as Wip_Flag,
  null::numeric as price_increase_amount,
  null::text as subsidiary_entity_name,
  b.churn_period,
  a.baseline_currency
FROM arr_product_bridge_tmp a
  left join arr_churned_products_tmp b on a.current_master_customer_id = b.mcid
  and a.current_product_family = b.product_family
  and a.current_product_group = b.product_group
  and a.baseline_currency = b.baseline_currency
  and a.current_period = b.snapshot_date_at_new
union all
SELECT a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_end_customer,
  a.prior_end_customer,
  a.mcid,
  a.current_master_customer_id,
  a.Prior_master_customer_id,
  a.current_product_family,
  a.prior_product_family,
  a.current_product_group,
  a.prior_product_group,
  --         "name",
  round(a.prior_arr_usd_ccfx::numeric, 2) AS prior_period_customer_arr_usd_ccfx,
  b.arr_diff as current_period_customer_arr_usd_ccfx,
  b.arr_diff,
  round(a.prior_arr_lcu::numeric, 2) AS prior_period_product_arr_lcu,
  b.arr_lcu_diff as current_period_product_arr_lcu,
  b.arr_lcu_diff,
  'Up Sell' as product_bridge,
  null as Winback_period_days,
  null as Wip_Flag,
  null::numeric as price_increase_amount,
  null::text as subsidiary_entity_name,
  null::date as churn_period,
  a.baseline_currency
FROM arr_product_bridge_tmp a
  join arr_churned_products_tmp b on a.current_master_customer_id = b.mcid
  and a.current_product_family = b.product_family
  and a.current_product_group = b.product_group
  and a.baseline_currency = b.baseline_currency
  and a.current_period = b.snapshot_date_at_new
where b.arr_at_new > b.arr_at_churn;
RAISE NOTICE 'Running customer bridge update on sst product bridge...';
--update customer bridge and subsidiary entity
update ryzlan.sst_product_bridge_product_family_CM a
set customer_bridge = b.customer_bridge
from sandbox_pd.sst_customer_bridge b
where 1 = 1
  and a.evaluation_period = b.evaluation_period
  and a.mcid = b.mcid
  and a.evaluation_period = var_period;
RAISE NOTICE 'Running subsidiary entity name insert on sst product bridge...';
drop table if exists sub_entity_tmp;
create temp table sub_entity_tmp as --update subsidiary_entity_name
with mcid_list as (
  select distinct mcid as master_customer_id
  from arr_product_bridge_tmp
  where evaluation_period = var_period
),
total_arr as (
  select a.mcid as mcid,
    a.snapshot_date,
    a.subsidiary_entity_name,
    sum(a.arr) as total_arr
  from sandbox_pd.sst_adhoc a
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
RAISE NOTICE 'Running sub entity update on sst product bridge...';
create index nci_sub_entity_tmp_mcid on sub_entity_tmp(mcid);
update ryzlan.sst_product_bridge_product_family_CM a
set subsidiary_entity_name = b.subsidiary_entity_name
from sub_entity_tmp b
where a.mcid = b.mcid
  and a.evaluation_period = var_period;
RAISE NOTICE 'Running Price Increase update on sst product bridge...';
--Price Increase updates
update ryzlan.sst_product_bridge_product_family_CM
set product_bridge = 'CPI'
where product_bridge = 'Up Sell'
  and prior_period_product_arr_usd_ccfx > 0
  and (
    (
      product_arr_change_ccfx / prior_period_product_arr_usd_ccfx
    ) * 100
  )::numeric <= case
    when current_period < '2023-01-01' then 5.5
    else 10.5
  end
  and evaluation_period = var_period;
--###########################################
--WINBACK Downgrade
--###########################################
RAISE NOTICE 'Running WINBACK Downgrade update on sst product bridge group 1...';
drop table if exists temp_win_downgrade_upsell;
create temp table temp_win_downgrade_upsell as with temp1 as (
  select a.mcid,
    a.product_bridge,
    a.evaluation_period as evaluation_period_at_upsell,
    a.current_period as snapshot_date_at_upsell,
    a.product_arr_change_ccfx as Upsell_arr,
    a.current_product_family,
    a.current_product_group
  from ryzlan.sst_product_bridge_product_family_CM a
  where 1 = 1
    and a.product_bridge = 'Up Sell'
    and a.evaluation_period = var_period
),
temp2 as (
  select a.mcid,
    a.product_bridge,
    a.evaluation_period_at_upsell,
    a.snapshot_date_at_upsell,
    b.current_period as snapshot_date_Downgrade,
    a.Upsell_arr,
    b.product_arr_change_ccfx as Downgrade_arr,
    b.evaluation_period as Downgrade_evaluation_period,
    b.product_bridge as Downgrade_bridge,
    a.current_product_family,
    a.current_product_group,
    row_number() over (
      partition by a.mcid,
      a.evaluation_period_at_upsell,
      a.current_product_family
      order by b.current_period desc,
        a.snapshot_date_at_upsell
    ) as rnk
  from ryzlan.sst_product_bridge_product_family_CM b
    join temp1 a on a.mcid = b.mcid
    and a.current_product_family = b.current_product_family
    and a.current_product_group = b.current_product_group
  where 1 = 1
    and b.product_bridge = 'Downgrade'
    and b.current_period < (
      select current_period
      from ufdm_grey.periods
      where evaluation_period = var_period
    )
)
select *
from temp2;
RAISE NOTICE 'Running WINBACK Downgrade update on sst product group bridge 1.1 ...';
drop table if exists temp_windowngrade_final;
create temporary table temp_windowngrade_final as with temp1 as (
  select *,
    row_number() over (
      partition by mcid,
      Downgrade_evaluation_period,
      current_product_family
      order by snapshot_date_at_upsell
    ) as rnk2
  from temp_win_downgrade_upsell
  where rnk = 1
    and snapshot_date_at_upsell::date - snapshot_date_Downgrade::date < 186
),
temp2 as (
  select *
  from temp1
  where rnk2 = 1
)
select a.mcid,
  a.evaluation_period,
  b.Upsell_arr,
  b.Downgrade_arr,
  a.customer_bridge,
  case
    when b.Upsell_arr > abs(b.Downgrade_arr) then 1
    else 0
  end as Split_record,
  b.Downgrade_evaluation_period,
  b.Downgrade_bridge,
  a.current_product_family,
  a.current_product_group
from ryzlan.sst_product_bridge_product_family_CM a,
  temp2 b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period_at_upsell
  and a.current_product_family = b.current_product_family
  and a.current_product_group = b.current_product_group
  and a.product_bridge in ('Up Sell')
  and a.evaluation_period = var_period;
RAISE NOTICE 'Running WINBACK Downgrade update on sst product group bridge 2...';
update ryzlan.sst_product_bridge_product_family_CM a
set product_bridge = 'Win back Downgrade'
from temp_windowngrade_final b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.current_product_family = b.current_product_family
  and a.current_product_group = b.current_product_group
  and a.evaluation_period = var_period
  and a.product_bridge = 'Up Sell'
  and Split_record = 0;
RAISE NOTICE 'Running WINBACK Downgrade update on sst product group bridge 3...';
drop table if exists temp_windowngrade_split;
create temp table temp_windowngrade_split as
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_end_customer,
  a.prior_end_customer,
  a.mcid,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.current_product_family,
  a.prior_product_family,
  a.current_product_group,
  a.prior_product_group,
  a.currency_code,
case
    when a.product_bridge = 'Churn' then a.prior_period_product_arr_usd_ccfx - c.product_arr_change_ccfx
    else a.prior_period_product_arr_usd_ccfx
  end as prior_period_product_arr_usd_ccfx,
case
    when a.product_bridge = 'Churn' then 0
    else a.current_period_product_arr_usd_ccfx + c.product_arr_change_ccfx
  end as current_period_product_arr_usd_ccfx,
  a.product_arr_change_ccfx + c.product_arr_change_ccfx as product_arr_change_ccfx ---lcu
,
case
    when a.product_bridge = 'Churn' then a.prior_period_product_arr_lcu - c.product_arr_change_lcu
    else a.prior_period_product_arr_lcu
  end as prior_period_product_arr_lcu,
case
    when a.product_bridge = 'Churn' then 0
    else a.current_period_product_arr_lcu + c.product_arr_change_lcu
  end as current_period_product_arr_lcu,
  a.product_arr_change_lcu + c.product_arr_change_lcu as product_arr_change_lcu,
  a.product_bridge,
  a.winback_period_days,
  a.wip_flag,
  a.price_increase_amount,
  a.subsidiary_entity_name,
  a.churn_period,
  a.customer_bridge
from ryzlan.sst_product_bridge_product_family_CM a
  join temp_windowngrade_final b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.current_product_family = b.current_product_family
  and a.current_product_group = b.current_product_group
  join ryzlan.sst_product_bridge_product_family_CM c on c.mcid = b.mcid
  and c.evaluation_period = b.Downgrade_evaluation_period
  and c.current_product_family = b.current_product_family
  and a.current_product_group = b.current_product_group
  and c.product_bridge = 'Downgrade'
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.product_bridge = 'Up Sell'
union all
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_end_customer,
  a.prior_end_customer,
  a.mcid,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.current_product_family,
  a.prior_product_family,
  a.current_product_group,
  a.prior_product_group,
  a.currency_code,
  c.current_period_product_arr_usd_ccfx as prior_period_product_arr_usd_ccfx,
  c.prior_period_product_arr_usd_ccfx as current_period_product_arr_usd_ccfx,
  - c.product_arr_change_ccfx as product_arr_change_ccfx,
  c.current_period_product_arr_lcu as prior_period_product_arr_lcu,
  c.prior_period_product_arr_lcu as current_period_product_arr_lcu,
  - c.product_arr_change_lcu as product_arr_change_lcu,
  'Win back Downgrade' as product_bridge,
  a.winback_period_days,
  a.wip_flag,
  a.price_increase_amount,
  a.subsidiary_entity_name,
  a.churn_period,
  a.customer_bridge
from ryzlan.sst_product_bridge_product_family_CM a
  join temp_windowngrade_final b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.current_product_family = b.current_product_family
  and a.current_product_group = b.current_product_group
  join ryzlan.sst_product_bridge_product_family_CM c on c.mcid = b.mcid
  and c.evaluation_period = b.Downgrade_evaluation_period
  and c.current_product_family = b.current_product_family
  and a.current_product_group = b.current_product_group
  and c.product_bridge = 'Downgrade'
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.product_bridge = 'Up Sell'
order by mcid;
RAISE NOTICE 'Running WINBACK Downgrade update on sst product group bridge 4...';
delete from ryzlan.sst_product_bridge_product_family_CM a using temp_windowngrade_final b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and b.Split_record = 1
  and a.evaluation_period = var_period
  and a.current_product_family = b.current_product_family
  and a.current_product_group = b.current_product_group
  and a.product_bridge = 'Up Sell';
insert into ryzlan.sst_product_bridge_product_family_CM (
    evaluation_period,
    prior_period,
    current_period,
    current_end_customer,
    prior_end_customer,
    mcid,
    current_master_customer_id,
    prior_master_customer_id,
    current_product_family,
    prior_product_family,
    current_product_group,
    prior_product_group,
    currency_code,
    prior_period_product_arr_usd_ccfx,
    current_period_product_arr_usd_ccfx,
    product_arr_change_ccfx,
    prior_period_product_arr_lcu,
    current_period_product_arr_lcu,
    product_arr_change_lcu,
    product_bridge,
    winback_period_days,
    wip_flag,
    price_increase_amount,
    subsidiary_entity_name,
    churn_period,
    customer_bridge
  )
select evaluation_period,
  prior_period,
  current_period,
  current_end_customer,
  prior_end_customer,
  mcid,
  current_master_customer_id,
  prior_master_customer_id,
  current_product_family,
  prior_product_family,
  current_product_group,
  prior_product_group,
  currency_code,
  prior_period_product_arr_usd_ccfx,
  current_period_product_arr_usd_ccfx,
  product_arr_change_ccfx,
  prior_period_product_arr_lcu,
  current_period_product_arr_lcu,
  product_arr_change_lcu,
  product_bridge,
  winback_period_days,
  wip_flag,
  price_increase_amount,
  subsidiary_entity_name,
  churn_period,
  customer_bridge
from temp_windowngrade_split;
--###########################################
--CPI Reversal
--###########################################
RAISE NOTICE 'Running CPI Reversal update on sst customer bridge...';
drop table if exists temp_CPI_Rev_pg;
create table temp_CPI_Rev_pg as with temp1 as (
  select a.mcid,
    a.product_bridge,
    a.evaluation_period as evaluation_period_at_Downgrade_Churn,
    p.current_period as snapshot_date_at_Downgrade_Churn,
    product_arr_change_ccfx as current_arr_at_Downgrade_Churn,
    coalesce(a.current_product_family, a.prior_product_family) as product_group_at_Downgrade_Churn
  from ryzlan.sst_product_bridge_product_family_CM a
    join ufdm_grey.periods p on a.evaluation_period = p.evaluation_period
  where 1 = 1
    and a.evaluation_period = var_period
    and a.product_bridge in ('Downgrade', 'Churn')
),
temp2 as (
  select a.mcid,
    a.product_bridge,
    a.evaluation_period_at_Downgrade_Churn,
    a.snapshot_date_at_Downgrade_Churn,
    b.current_period as snapshot_date_CPI,
    a.current_arr_at_Downgrade_Churn,
    b.product_arr_change_ccfx as CPI_arr,
    b.product_bridge as CPI_bridge,
    b.evaluation_period as CPI_evaluation_period,
    a.product_group_at_Downgrade_Churn,
    b.current_product_family as product_group_at_CPI,
    row_number() over (
      partition by b.mcid,
      a.evaluation_period_at_Downgrade_Churn,
      current_product_family
      order by b.current_period desc
    ) as rnk
  from ryzlan.sst_product_bridge_product_family_CM b
    join temp1 a on a.mcid = b.mcid
    and a.product_group_at_Downgrade_Churn = b.current_product_family
  where 1 = 1
    and b.product_bridge = 'CPI'
    and b.current_period < (
      select current_period
      from ufdm_grey.periods
      where evaluation_period = var_period
    )
)
select *
from temp2;
RAISE NOTICE 'Running CPI Reversal update on sst customer bridge 1.1 ...';
drop table if exists temp_CPI_Reversal;
create table temp_CPI_Reversal as with temp_windowngrade as (
  select *
  from temp_CPI_Rev_pg
  where rnk = 1
    and snapshot_date_at_Downgrade_Churn::date - snapshot_date_CPI::date < 186
)
select a.mcid,
  a.evaluation_period,
  b.current_arr_at_Downgrade_Churn,
  b.CPI_arr,
  a.customer_bridge,
  case
    when - b.current_arr_at_Downgrade_Churn > b.CPI_arr then 1
    else 0
  end as Split_record,
  b.CPI_evaluation_period,
  b.CPI_bridge,
  a.current_product_family
from ryzlan.sst_product_bridge_product_family_CM a,
  temp_windowngrade b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period_at_Downgrade_Churn
  and coalesce (a.current_product_family, a.prior_product_family) = b.product_group_at_CPI
  and a.product_bridge in ('Downgrade', 'Churn')
  and a.evaluation_period = var_period;
RAISE NOTICE 'Running CPI Reversal update on sst customer bridge 2...';
update ryzlan.sst_product_bridge_product_family_CM a
set product_bridge = 'CPI Reversal'
from temp_CPI_Reversal b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and coalesce(a.current_product_family, a.prior_product_family) = b.current_product_family
  and a.evaluation_period = var_period
  and b.Split_record = 0
  and a.product_bridge in ('Downgrade', 'Churn');
--
drop table if exists temp_cpireversal_split;
create temp table temp_cpireversal_split as
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_end_customer,
  a.prior_end_customer,
  a.mcid,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.current_product_family,
  a.prior_product_family,
  a.current_product_group,
  a.prior_product_group,
  a.currency_code,
case
    when a.product_bridge = 'Churn' then a.prior_period_product_arr_usd_ccfx - c.product_arr_change_ccfx
    else a.prior_period_product_arr_usd_ccfx
  end as prior_period_product_arr_usd_ccfx,
case
    when a.product_bridge = 'Churn' then 0
    else a.current_period_product_arr_usd_ccfx + c.product_arr_change_ccfx
  end as current_period_product_arr_usd_ccfx,
  a.product_arr_change_ccfx + c.product_arr_change_ccfx as product_arr_change_ccfx ---lcu
,
case
    when a.product_bridge = 'Churn' then a.prior_period_product_arr_lcu - c.product_arr_change_lcu
    else a.prior_period_product_arr_lcu
  end as prior_period_product_arr_lcu,
case
    when a.product_bridge = 'Churn' then 0
    else a.current_period_product_arr_lcu + c.product_arr_change_lcu
  end as current_period_product_arr_lcu,
  a.product_arr_change_lcu + c.product_arr_change_lcu as product_arr_change_lcu,
  a.product_bridge,
  a.winback_period_days,
  a.wip_flag,
  a.price_increase_amount,
  a.subsidiary_entity_name,
  a.churn_period,
  a.customer_bridge
from ryzlan.sst_product_bridge_product_family_CM a
  join temp_CPI_Reversal b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and coalesce(a.current_product_family, a.prior_product_family) = b.current_product_family
  join ryzlan.sst_product_bridge_product_family_CM c on c.mcid = b.mcid
  and c.evaluation_period = b.CPI_evaluation_period
  and coalesce(c.current_product_family, c.prior_product_family) = b.current_product_family
  and c.product_bridge = 'CPI'
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.product_bridge in ('Downgrade', 'Churn')
union all
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_end_customer,
  a.prior_end_customer,
  a.mcid,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.current_product_family,
  a.prior_product_family,
  a.current_product_group,
  a.prior_product_group,
  a.currency_code,
  c.current_period_product_arr_usd_ccfx as prior_period_product_arr_usd_ccfx,
  c.prior_period_product_arr_usd_ccfx as current_period_product_arr_usd_ccfx,
  - c.product_arr_change_ccfx as product_arr_change_ccfx,
  c.current_period_product_arr_lcu as prior_period_product_arr_lcu,
  c.prior_period_product_arr_lcu as current_period_product_arr_lcu,
  - c.product_arr_change_lcu as product_arr_change_lcu,
  'CPI Reversal' as product_bridge,
  a.winback_period_days,
  a.wip_flag,
  a.price_increase_amount,
  a.subsidiary_entity_name,
  a.churn_period,
  a.customer_bridge
from ryzlan.sst_product_bridge_product_family_CM a
  join temp_CPI_Reversal b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and coalesce(a.current_product_family, a.prior_product_family) = b.current_product_family
  join ryzlan.sst_product_bridge_product_family_CM c on c.mcid = b.mcid
  and c.evaluation_period = b.CPI_evaluation_period
  and coalesce(c.current_product_family, c.prior_product_family) = b.current_product_family
  and c.product_bridge = 'CPI'
where b.Split_record = 1
  and a.evaluation_period = var_period
  and a.product_bridge in ('Downgrade', 'Churn')
order by mcid;
delete from ryzlan.sst_product_bridge_product_family_CM a using temp_CPI_Reversal b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and coalesce(a.current_product_family, a.prior_product_family) = b.current_product_family
  and b.Split_record = 1
  and a.evaluation_period = var_period
  and a.product_bridge in ('Downgrade', 'Churn');
insert into ryzlan.sst_product_bridge_product_family_CM (
    evaluation_period,
    prior_period,
    current_period,
    current_end_customer,
    prior_end_customer,
    mcid,
    current_master_customer_id,
    prior_master_customer_id,
    current_product_family,
    prior_product_family,
    current_product_group,
    prior_product_group,
    currency_code,
    prior_period_product_arr_usd_ccfx,
    current_period_product_arr_usd_ccfx,
    product_arr_change_ccfx,
    prior_period_product_arr_lcu,
    current_period_product_arr_lcu,
    product_arr_change_lcu,
    product_bridge,
    winback_period_days,
    wip_flag,
    price_increase_amount,
    subsidiary_entity_name,
    churn_period,
    customer_bridge
  )
select evaluation_period,
  prior_period,
  current_period,
  current_end_customer,
  prior_end_customer,
  mcid,
  current_master_customer_id,
  prior_master_customer_id,
  current_product_family,
  prior_product_family,
  current_product_group,
  prior_product_group,
  currency_code,
  prior_period_product_arr_usd_ccfx,
  current_period_product_arr_usd_ccfx,
  product_arr_change_ccfx,
  prior_period_product_arr_lcu,
  current_period_product_arr_lcu,
  product_arr_change_lcu,
  product_bridge,
  winback_period_days,
  wip_flag,
  price_increase_amount,
  subsidiary_entity_name,
  churn_period,
  customer_bridge
from temp_cpireversal_split;
END;
$$;