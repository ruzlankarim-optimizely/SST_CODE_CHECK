drop table if exists sst_pb_temp;
create temp table sst_pb_temp as
select *
from sandbox_pd.sst_product_bridge
limit 1;
truncate table sst_pb_temp;
ALTER TABLE sst_pb_temp
ADD COLUMN prior_product_group text;
ALTER TABLE sst_pb_temp
ADD COLUMN current_product_group text;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--SST product Bridge
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists prior_period_customer_arr_tmp;
create temp table prior_period_customer_arr_tmp as
SELECT snapshot_date,
  a.mcid as master_customer_id,
  product_family,
  updated_product_group AS product_group,
  a.base_currency as baseline_currency,
  max(coalesce(a.end_name, a.parent_name)) as end_customer,
  sum(arr) AS arr_usd_ccfx,
  sum(baseline_arr_local_currency) AS arr_lcu
FROM sandbox_pd.sst a
WHERE 1 = 1
  AND snapshot_date = (
    SELECT prior_period
    from ufdm_grey.periods
    WHERE evaluation_period = '2022M06'
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
  product_family,
  updated_product_group AS product_group,
  a.base_currency as baseline_currency,
  max(coalesce(a.end_name, a.parent_name)) as end_customer,
  sum(arr) AS arr_usd_ccfx,
  sum(baseline_arr_local_currency) AS arr_lcu
FROM sandbox_pd.sst a
WHERE 1 = 1
  AND snapshot_date = (
    SELECT current_period
    from ufdm_grey.periods
    WHERE evaluation_period = '2022M06'
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
  c2.product_group AS prior_product_group,
  c1.product_group AS current_product_group,
  coalesce(c1.arr_usd_ccfx, 0) AS current_arr_usd_ccfx,
  coalesce(c2.arr_usd_ccfx, 0) AS prior_arr_usd_ccfx,
  coalesce(c1.arr_lcu, 0) AS current_arr_lcu,
  coalesce(c2.arr_lcu, 0) AS prior_arr_lcu --c3.arr AS prior2_arr, --WIP
FROM current_period_customer_arr_tmp c1
  FULL OUTER JOIN prior_period_customer_arr_tmp c2 ON c1.master_customer_id = c2.master_customer_id
  and c1.product_family = c2.product_family
  AND c1.product_group = c2.product_group
  and c1.baseline_currency = c2.baseline_currency;
------------------------------------------
-- Evaluate
--------------------------------------------------
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
  cla.prior_product_group,
  cla.current_product_group,
  cla.current_end_customer,
  cla.prior_end_customer,
  cla.baseline_currency,
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
  AND per.evaluation_period = '2022M06';
--#############################################
--WIP/WINBACK
--#############################################
drop table if exists arr_new_products_tmp;
create temp table arr_new_products_tmp AS
select a.current_master_customer_id as mcid,
  a.current_product_family as product_family,
  a.current_product_group AS product_group,
  a.current_period as snapshot_date,
  a.current_arr_usd_ccfx as arr_at_new,
  a.current_arr_lcu as arr_lcu_at_new,
  baseline_currency
from arr_product_bridge_tmp a
where product_bridge = 'New';
--get most recent postivie arr for above new product which should have been churned
drop table if exists arr_churned_products_tmp;
create temp table arr_churned_products_tmp AS with temp as (
  select b.snapshot_date,
    b.mcid,
    b.product_family,
    b.product_group,
    a.baseline_currency,
    a.snapshot_date as snapshot_date_at_new,
    sum(b.arr) as arr_at_churn,
    sum(b.baseline_arr_local_currency) as arr_lcu_at_churn,
    sum(a.arr_at_new) as arr_at_new,
    sum(a.arr_lcu_at_new) as arr_lcu_at_new,
    row_number() over (
      partition by b.mcid,
      b.product_family,
      b.product_group
      order by b.snapshot_date desc
    ) as rnk
  from arr_new_products_tmp a
    join (
      select sb.mcid,
        sb.snapshot_date,
        sb.overage_flag,
        sb.product_family,
        sb.updated_product_group AS product_group,
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
    ) b on a.mcid = b.mcid
    and a.product_family = b.product_family
    and a.product_group = b.product_group
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
        from snapshot_date_at_new::timestamp - snapshot_date::date
      ) <= 90 then 'WIP'
      else 'Winback'
    end
  end as product_bridge_new,
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
-- create index if not exists nci_arr_churned_products_tmp_tmp_composite on arr_churned_products_tmp(mcid,product_family,baseline_currency,snapshot_date_at_new) include(arr_at_new, arr_at_churn);
INSERT INTO sst_pb_temp (
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
    prior_product_group,
    current_product_group,
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
  a.prior_product_group,
  a.current_product_group,
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
      when b.days_diff <= 90 then 'WIP'
      else 'Winback'
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
  AND a.current_product_group = b.product_group
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
  a.prior_product_group,
  a.current_product_group,
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
  AND a.current_product_group = b.product_group
  and a.baseline_currency = b.baseline_currency
  and a.current_period = b.snapshot_date_at_new
where b.arr_at_new > b.arr_at_churn;
--RAISE NOTICE 'Running customer bridge update on sst product bridge...';
--update customer bridge and subsidiary entity
update sst_pb_temp a
set customer_bridge = b.customer_bridge
from sandbox_pd.sst_customer_bridge b
where 1 = 1
  and a.evaluation_period = b.evaluation_period
  and a.mcid = b.mcid
  and a.evaluation_period = '2022M06';
--RAISE NOTICE 'Running subsidiary entity name insert on sst product bridge...';
drop table if exists sub_entity_tmp;
create temp table sub_entity_tmp as --update subsidiary_entity_name
with mcid_list as (
  select distinct mcid as master_customer_id
  from arr_product_bridge_tmp
  where evaluation_period = '2022M06'
),
total_arr as (
  select a.mcid as mcid,
    a.snapshot_date,
    a.subsidiary_entity_name,
    sum(a.arr) as total_arr
  from sandbox_pd.sst a
    join mcid_list b on a.mcid = b.master_customer_id
    and a.snapshot_date in (
      SELECT prior_period
      from ufdm_grey.periods
      WHERE evaluation_period = '2022M06'
      union
      SELECT current_period
      from ufdm_grey.periods
      WHERE evaluation_period = '2022M06'
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
--RAISE NOTICE 'Running sub entity update on sst product bridge...';
--create index nci_sub_entity_tmp_mcid on sub_entity_tmp(mcid);
update sst_pb_temp a
set subsidiary_entity_name = b.subsidiary_entity_name
from sub_entity_tmp b
where a.mcid = b.mcid
  and a.evaluation_period = '2022M06';
--RAISE NOTICE 'Running Price Increase update on sst product bridge...';
--Price Increase updates
update sst_pb_temp
set product_bridge = 'CPI'
where product_bridge = 'Up Sell'
  and prior_period_product_arr_usd_ccfx > 0
  and (
    abs(
      product_arr_change_ccfx / prior_period_product_arr_usd_ccfx
    ) * 100
  )::numeric(10, 2) <= case
    when current_period < '2023-01-01' then 5.5
    else 10.5
  end
  and evaluation_period = '2022M06';
--SELECT * FROM tmp_product_family_hierarchy
--Churn Migration
drop table if exists tmp_product_family_hierarchy;
create temporary table tmp_product_family_hierarchy as
select 'Non-Recurring: Perpetual License' as product_family,
  1 as attach_arr_hierarchy,
  'Perpetual License' as Product_Family_short
union all
select 'Recurring: Subscription License',
  2,
  'Subscription License'
union all
select 'Recurring: Cloud: Content Cloud: Content PaaS',
  3,
  'Content PaaS'
union all
select 'Recurring: Cloud: Content Cloud: Content SaaS',
  4,
  'Content SaaS'
union all
select 'Recurring: Cloud: Commerce Cloud: B2C Commerce (incl. Headless)',
  5,
  'B2C Commerce (incl. Headless)'
union all
select 'Recurring: Cloud: Commerce Cloud: B2B Commerce (incl. Headless)',
  6,
  'B2B Commerce (incl. Headless)'
union all
select 'Recurring: Cloud: Developer Cloud: Full Stack',
  7,
  'Full Stack'
union all
select 'Full Stack',
  7,
  'Full Stack'
union all
select 'Recurring: Cloud: Intelligence Cloud: Web Experimentation and Personalization',
  8,
  'Web Experimentation and Personalization'
union all
select 'Web',
  8,
  'Web Experimentation and Personalization'
union all
select 'Recurring: Cloud: Intelligence Cloud: CDP (incl. Visitor Intelligence)',
  9,
  'CDP (incl. Visitor Intelligence)'
union all
select 'Recurring: Cloud: Other Bookings: Campaign',
  10,
  'Campaign'
union all
select 'Recurring: Cloud: Intelligence Cloud: Content Recommendations (incl. E-mail)',
  11,
  'Content Recommendations (incl. E-mail)'
union all
select 'Recurring: Cloud: Intelligence Cloud: Product Recommendations (incl. E-mail)',
  12,
  'Product Recommendations (incl. E-mail)'
union all
select 'Recurring: Cloud: Other Bookings: Other Bookings',
  13,
  'Other'
union all
select 'Recurring: Intelligence Cloud: Marketing Orchestration',
  14,
  'Welcome'
union all
select '- Not Applicable -',
  1,
  'Perpetual License';
--  SELECT * FROM tmp_churn_migration_mappings 
drop table if exists tmp_churn_migration_mappings;
create temporary table tmp_churn_migration_mappings as
select 'Perpetual License' as Product_Family_short,
  'Content PaaS' as Product_Family_Change
union all
select 'Perpetual License',
  'B2C Commerce (incl. Headless)'
union all
select 'Perpetual License',
  'B2B Commerce (incl. Headless)' --union all select 'Web Experimentation and Personalization' , 'Full Stack'
union all
select 'Other',
  'Content PaaS'
union all
select 'Other',
  'B2C Commerce (incl. Headless)'
union all
select 'Other',
  'B2B Commerce (incl. Headless)';
--  SELECT * FROM tmp_churn_migration_mappings
--step1: get all churn/par churn records which are on old product family in churn migration pathway
drop table if exists temp_chm_old_product_family;
create temporary table temp_chm_old_product_family AS
SELECT *
FROM (
    select distinct a.MCID AS master_customer_id,
      case
        when current_product_family ilike '%Perpetual License%' then 'Perpetual License'
        else b.Product_Family_short
      end as product_family_short_current,
      case
        when prior_product_family ilike '%Perpetual License%' then 'Perpetual License'
        else c.Product_Family_short
      end as product_family_short_prior,
      a.product_bridge,
      a.evaluation_period,
      a.current_product_family,
      a.prior_product_family,
      a.prior_product_group,
      a.current_product_group
    from sst_pb_temp a
      left join tmp_product_family_hierarchy b on a.current_product_family = b.product_family
      left join tmp_product_family_hierarchy c on a.prior_product_family = c.product_family
    where 1 = 1
      AND a.prior_period_product_arr_usd_ccfx > 0
      and evaluation_period = '2022M06' --  and product_bridge in ('Partial Churn', 'Churn')
  ) AS a
WHERE a.product_family_short_prior IN ('Perpetual License', 'Other');
drop table if exists temp_churn_migration_records;
create temporary table temp_churn_migration_records AS (
  SELECT DISTINCT mcid
  FROM (
      SELECT a.*,
        CASE
          WHEN a.current_product_family ILIKE '%Perpetual License%' THEN 'Perpetual License'
          ELSE b.Product_Family_short
        END AS product_family_short_current,
        CASE
          WHEN a.prior_product_family ILIKE '%Perpetual License%' THEN 'Perpetual License'
          ELSE c.Product_Family_short
        END AS product_family_short_prior
      from sst_pb_temp a
        JOIN temp_chm_old_product_family AS t1 ON t1.master_customer_id = a.mcid
        left join tmp_product_family_hierarchy b on a.current_product_family = b.product_family
        left join tmp_product_family_hierarchy c on a.prior_product_family = c.product_family
      where a.evaluation_period = '2022M06'
    ) AS a
    JOIN tmp_churn_migration_mappings AS b ON b.product_family_change = a.product_family_short_current
  WHERE a.current_period_product_arr_usd_ccfx > 0
    AND (a.product_arr_change_ccfx <> 0)
);
SELECT distinct mcid
FROM temp_churn_migration_records
update sst_pb_temp a
set product_bridge = concat(a.product_bridge, ' - Migration')
from temp_churn_migration_records b
where a.mcid = b.mcid
  AND a.product_bridge NOT IN ('Flat', 'CPI')
  and a.evaluation_period = '2022M06';
DROP TABLE IF EXISTS ryzlan.sst_pb_temp;
CREATE TABLE ryzlan.sst_pb_temp AS
SELECT *
FROM sst_pb_temp;
SELECT *
FROM ryzlan.sst_pb_temp
WHERE --  mcid = '3100ac14-1eaa-db11-8952-0018717a8c82'
  -- / mcid = '035c17f2-7b31-e411-9f63-0050568d2da8'
  --  mcid = '670920c7-185f-e411-9afb-0050568d2da8' 
  product_bridge ILIKE ('%- Migration%')