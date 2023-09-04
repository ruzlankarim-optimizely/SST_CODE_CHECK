-- New script in dw-prod-rds-master.cr9dekxonyuj.us-east-1.rds.amaz.
-- Date: Aug 30, 2023
-- Time: 12:41:24 PM
DROP TABLE IF EXISTS ryzlan.monthly_metrics;
CREATE TABLE ryzlan.monthly_metrics AS
SELECT *
FROM ufdm_blue.monthly_metrics;
DROP TABLE IF EXISTS sandbox.unbund;
CREATE TABLE sandbox.unbund AS (
  WITH cte AS (
    SELECT snapshot_date,
      COALESCE(
        end_customer_master_customer_id,
        parent_master_customer_id
      ) AS MCID,
      COALESCE(end_customer, parent_customer) AS NAME,
      product_family,
      reference_number,
      subline_id,
      line_number,
      change_order,
      line_type,
      ROUND(SUM(arr_usd_ccfx)) AS ARR --select distinct snapshot_date
    FROM ryzlan.monthly_metrics
    WHERE snapshot_date < '2022-01-31'
      AND line_type IN ('recurring', 'Recurring')
      and product_family <> 'Recurring: Intelligence Cloud: Marketing Orchestration'
      AND snapshot_date NOT IN (
        '2022-07-30',
        '2022-09-01',
        '2022-10-01',
        '2022-11-01',
        '2022-12-01',
        '2023-03-09',
        '2023-03-16',
        '2023-03-23',
        '2023-04-06',
        '2023-04-13',
        '2023-04-20',
        '2023-04-27'
      )
    GROUP BY 1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9
    ORDER BY 1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9
  ),
  data_final AS (
    SELECT *
    FROM cte
  ),
  prod_check AS (
    SELECT snapshot_date,
      MCID,
      NAME,
      reference_number,
      COUNT(DISTINCT product_family) AS prod_fam,
      COUNT(
        DISTINCT (
          CASE
            WHEN ARR > 0 THEN product_family
            ELSE NULL
          END
        )
      ) AS arr_prod_fam,
      SUM(ARR) AS SUM_ARR
    FROM data_final
    GROUP BY 1,
      2,
      3,
      4
    ORDER BY 1,
      2,
      3,
      4
  ),
  prod_diff AS (
    SELECT *,
      prod_fam - arr_prod_fam AS diff,
      ROW_NUMBER() OVER (
        ORDER BY SUM_ARR DESC
      ) AS row_number
    FROM prod_check
    WHERE SUM_ARR > 0
      AND (prod_fam - arr_prod_fam) > 0
    ORDER BY SUM_ARR DESC
  ),
  table1 AS (
    SELECT DISTINCT mm.snapshot_date,
      row_number,
      diff AS prod_fam_diff,
      COALESCE(
        end_customer_master_customer_id,
        parent_master_customer_id
      ) AS MCID,
      COALESCE(end_customer, parent_customer) AS NAME,
      baseline_currency,
      baseline_arr_local_currency,
      ROUND(arr_usd_ccfx) AS arr_usd_ccfx,
      recurring_amount,
      baseline_mrr_local_currency,
      mrr_usd_ccfx,
      mrr_usd_mefx,
      arr_usd_mefx,
      mrr_usd_actualfx,
      arr_usd_actualfx,
      mm.reference_number --                       ,mm.line_number 
,
      date_start,
      date_end,
      date_termination,
      sku,
      sku_name,
      product_name,
      product_group,
      product_family,
      SUM_ARR AS total_ARR
    FROM ryzlan.monthly_metrics mm
      INNER JOIN prod_diff d ON mm.reference_number = d.reference_number
      AND mm.snapshot_date = d.snapshot_date
    WHERE mm.snapshot_date < '2022-01-31'
      AND line_type IN ('recurring', 'Recurring')
      and product_family <> 'Recurring: Intelligence Cloud: Marketing Orchestration'
    ORDER BY row_number,
      product_family,
      arr_usd_ccfx DESC,
      SKU
  ),
  raw AS (
    SELECT snapshot_date,
      prod_fam_diff,
      MCID,
      NAME,
      baseline_currency,
      baseline_arr_local_currency,
      ROUND(arr_usd_ccfx) AS arr_usd_ccfx,
      recurring_amount,
      baseline_mrr_local_currency,
      mrr_usd_ccfx,
      mrr_usd_mefx,
      arr_usd_mefx,
      mrr_usd_actualfx,
      arr_usd_actualfx,
      reference_number --             ,line_number 
,
      date_start,
      date_end,
      date_termination,
      sku -- sku_name
      --,-- product_name
      --,-- product_group
,
      product_family,
      total_ARR
    FROM table1 mm
    GROUP BY snapshot_date,
      prod_fam_diff,
      MCID,
      NAME,
      baseline_currency,
      baseline_arr_local_currency,
      arr_usd_ccfx,
      recurring_amount,
      baseline_mrr_local_currency,
      mrr_usd_ccfx,
      mrr_usd_mefx,
      arr_usd_mefx,
      mrr_usd_actualfx,
      arr_usd_actualfx,
      reference_number --               ,line_number 
,
      date_start,
      date_end,
      date_termination,
      sku,
      product_family,
      total_ARR
  ),
  need_unbun as (
    SELECT *
    FROM raw
    ORDER BY 1
  ),
  refe as (
    select ee.*,
      uas.product_code,
      uas.updated_list_price as list_price
    from need_unbun ee
      left join ufdm_grey.unbundling_arr_skus uas on ee.sku = uas.product_code
  ),
  sum_list_prices as (
    select sum(list_price) as sum_list_price,
      sum(baseline_arr_local_currency) as baseline_arr_local_currency,
      sum(recurring_amount) as recurring_amount,
      sum(baseline_mrr_local_currency) as baseline_mrr_local_currency,
      sum(mrr_usd_ccfx) as mrr_usd_ccfx,
      sum(mrr_usd_mefx) as mrr_usd_mefx,
      sum(arr_usd_mefx) as arr_usd_mefx,
      sum(mrr_usd_actualfx) as mrr_usd_actualfx,
      sum(arr_usd_actualfx) as arr_usd_actualfx,
      reference_number,
      --           line_number,
      snapshot_date
    from refe
    group by 10,
      11
  ),
  trans as (
    select re.*,
      case
        when slp.sum_list_price = 0 then 0
        else total_arr * (re.list_price::decimal / slp.sum_list_price)
      end as proposed_arr,
      case
        when slp.sum_list_price = 0 then 0
        else slp.baseline_arr_local_currency * (re.list_price::decimal / slp.sum_list_price)
      end as proposed_baseline_arr_local_currency,
      case
        when slp.sum_list_price = 0 then 0
        else slp.recurring_amount * (re.list_price::decimal / slp.sum_list_price)
      end as proposed_recurring_amount,
      case
        when slp.sum_list_price = 0 then 0
        else slp.baseline_mrr_local_currency * (re.list_price::decimal / slp.sum_list_price)
      end as proposed_baseline_mrr_local_currency,
      case
        when slp.sum_list_price = 0 then 0
        else slp.mrr_usd_ccfx * (re.list_price::decimal / slp.sum_list_price)
      end as proposed_mrr_usd_ccfx,
      case
        when slp.sum_list_price = 0 then 0
        else slp.mrr_usd_mefx * (re.list_price::decimal / slp.sum_list_price)
      end as proposed_mrr_usd_mefx,
      case
        when slp.sum_list_price = 0 then 0
        else slp.arr_usd_mefx * (re.list_price::decimal / slp.sum_list_price)
      end as proposed_arr_usd_mefx,
      case
        when slp.sum_list_price = 0 then 0
        else slp.mrr_usd_actualfx * (re.list_price::decimal / slp.sum_list_price)
      end as proposed_mrr_usd_actualfx,
      case
        when slp.sum_list_price = 0 then 0
        else slp.arr_usd_actualfx * (re.list_price::decimal / slp.sum_list_price)
      end as proposed_arr_usd_actualfx
    from refe re
      left join sum_list_prices slp on re.reference_number = slp.reference_number
      and re.snapshot_date = slp.snapshot_date
  )
  select --*,
    snapshot_date,
    mcid,
    "name",
    sku,
    reference_number,
    --       line_number ,
    baseline_currency,
    baseline_mrr_local_currency,
    arr_usd_ccfx,
    proposed_arr::numeric as prop_arr_usd_ccfx_last,
    proposed_baseline_arr_local_currency::NUMERIC as proposed_baseline_arr_local_currency_last,
    proposed_recurring_amount::numeric as proposed_recurring_amount_last,
    proposed_baseline_mrr_local_currency::numeric as proposed_baseline_mrr_local_currency_last,
    proposed_mrr_usd_ccfx::numeric as proposed_mrr_usd_ccfx_last,
    proposed_mrr_usd_mefx::numeric as proposed_mrr_usd_mefx_last,
    proposed_arr_usd_mefx::numeric as proposed_arr_usd_mefx_last,
    proposed_mrr_usd_actualfx::numeric as proposed_mrr_usd_actualfx_last,
    proposed_arr_usd_actualfx::numeric as proposed_arr_usd_actualfx_last
  from trans
);
DROP TABLE IF EXISTS sandbox.unbund_mod;
CREATE TABLE sandbox.unbund_mod AS (
  with unbundled_data as (
    select distinct mcid,
      snapshot_date,
      sku,
      reference_number,
      sum(prop_arr_usd_ccfx_last) prop_arr_usd_ccfx_last,
      sum(proposed_baseline_arr_local_currency_last) proposed_baseline_arr_local_currency_last,
      sum(proposed_recurring_amount_last) proposed_recurring_amount_last,
      sum(proposed_baseline_mrr_local_currency_last) proposed_baseline_mrr_local_currency_last,
      sum(proposed_mrr_usd_ccfx_last) proposed_mrr_usd_ccfx_last,
      sum(proposed_mrr_usd_mefx_last) proposed_mrr_usd_mefx_last,
      sum(proposed_arr_usd_mefx_last) proposed_arr_usd_mefx_last,
      sum(proposed_mrr_usd_actualfx_last) proposed_mrr_usd_actualfx_last,
      sum(proposed_arr_usd_actualfx_last) proposed_arr_usd_actualfx_last
    from sandbox.unbund
    where mcid IS NOT NULL
      and mcid <> '-'
    group by 1,
      2,
      3,
      4
  )
  SELECT distinct a.mcid,
    a.snapshot_date,
    a.reference_number,
    a.sku,
    case
      when prop_arr_usd_ccfx_last is null then a.arr_usd_ccfx
      else (prop_arr_usd_ccfx_last) /(
        count(a.mcid) over(
          partition by a.mcid,
          a.snapshot_date,
          a.reference_number,
          a.sku
        )
      )
    end as arr_usd_ccfx_mod,
    --a.baseline_arr_local_currency ,
    --proposed_baseline_arr_local_currency_last, 
    case
      when proposed_baseline_arr_local_currency_last is null then a.baseline_arr_local_currency
      else (proposed_baseline_arr_local_currency_last) /(
        count(a.mcid) over(
          partition by a.mcid,
          a.snapshot_date,
          a.reference_number,
          a.sku
        )
      )
    end as baseline_arr_local_currency_mod,
    --a.recurring_amount , 
    --proposed_recurring_amount_last,
    case
      when proposed_recurring_amount_last is null then a.recurring_amount
      else (proposed_recurring_amount_last) /(
        count(a.mcid) over(
          partition by a.mcid,
          a.snapshot_date,
          a.reference_number,
          a.sku
        )
      )
    end as recurring_amount_mod,
    --a.baseline_mrr_local_currency, 
    --proposed_baseline_mrr_local_currency_last, 
    case
      when proposed_baseline_mrr_local_currency_last is null then a.baseline_mrr_local_currency
      else (proposed_baseline_mrr_local_currency_last) /(
        count(a.mcid) over(
          partition by a.mcid,
          a.snapshot_date,
          a.reference_number,
          a.sku
        )
      )
    end as baseline_mrr_local_currency_mod,
    --a.mrr_usd_ccfx, 
    --proposed_mrr_usd_ccfx_last, 
    case
      when proposed_mrr_usd_ccfx_last is null then a.mrr_usd_ccfx
      else (proposed_mrr_usd_ccfx_last) /(
        count(a.mcid) over(
          partition by a.mcid,
          a.snapshot_date,
          a.reference_number,
          a.sku
        )
      )
    end as mrr_usd_ccfx_mod,
    --a.mrr_usd_mefx, 
    --proposed_mrr_usd_mefx_last, 
    case
      when proposed_mrr_usd_mefx_last is null then a.mrr_usd_mefx
      else (proposed_mrr_usd_mefx_last) /(
        count(a.mcid) over(
          partition by a.mcid,
          a.snapshot_date,
          a.reference_number,
          a.sku
        )
      )
    end as mrr_usd_mefx_mod,
    --a.arr_usd_mefx ,
    --proposed_arr_usd_mefx_last, 
    case
      when proposed_arr_usd_mefx_last is null then a.arr_usd_mefx
      else (proposed_arr_usd_mefx_last) /(
        count(a.mcid) over(
          partition by a.mcid,
          a.snapshot_date,
          a.reference_number,
          a.sku
        )
      )
    end as arr_usd_mefx_mod,
    --a.mrr_usd_actualfx , 
    --proposed_mrr_usd_actualfx_last, 
    case
      when proposed_mrr_usd_actualfx_last is null then a.mrr_usd_actualfx
      else (proposed_mrr_usd_actualfx_last) /(
        count(a.mcid) over(
          partition by a.mcid,
          a.snapshot_date,
          a.reference_number,
          a.sku
        )
      )
    end as mrr_usd_actualfx_mod,
    --a.arr_usd_actualfx , 
    --proposed_arr_usd_actualfx_last ,
    case
      when proposed_arr_usd_actualfx_last is null then a.arr_usd_actualfx
      else (proposed_arr_usd_actualfx_last) /(
        count(a.mcid) over(
          partition by a.mcid,
          a.snapshot_date,
          a.reference_number,
          a.sku
        )
      )
    end as arr_usd_actualfx_mod
  FROM ryzlan.monthly_metrics a
    inner join unbundled_data b on a.mcid = b.mcid
    and a.snapshot_date = b.snapshot_date
    and a.sku = b.sku
    and a.reference_number = b.reference_number
  where a.line_type in ('recurring', 'Recurring')
    and a.product_family <> 'Recurring: Intelligence Cloud: Marketing Orchestration'
    AND b.mcid IS NOT NULL
    AND b.mcid <> '-'
    and a.mcid is not null
    AND a.mcid <> '-'
    and a.snapshot_date is not null
    and a.sku is not null
    and a.reference_number is not null
);
UPDATE ryzlan.monthly_metrics mm
SET arr_usd_ccfx = arr_usd_ccfx_mod,
  baseline_arr_local_currency = baseline_arr_local_currency_mod,
  recurring_amount = recurring_amount_mod,
  baseline_mrr_local_currency = baseline_mrr_local_currency_mod,
  mrr_usd_ccfx = mrr_usd_ccfx_mod,
  mrr_usd_mefx = mrr_usd_mefx_mod,
  arr_usd_mefx = arr_usd_mefx_mod,
  mrr_usd_actualfx = mrr_usd_actualfx_mod,
  arr_usd_actualfx = arr_usd_actualfx_mod,
  modified_comments = concat(
    COALESCE(modified_comments, ''),
    '; unbundling update: arr changed from ',
    COALESCE(mm.arr_usd_ccfx, '0')::TEXT
  ),
  modified_date = current_timestamp
FROM sandbox.unbund_mod ub
WHERE mm.mcid IS NOT NULL
  AND mm.mcid <> '-'
  AND mm.snapshot_date = ub.snapshot_date
  AND mm.mcid = ub.mcid
  AND mm.reference_number = ub.reference_number
  AND mm.sku = ub.sku;
DROP TABLE sandbox.arr_unbund;
CREATE TABLE sandbox.arr_unbund AS
SELECT *
FROM ryzlan.arr;