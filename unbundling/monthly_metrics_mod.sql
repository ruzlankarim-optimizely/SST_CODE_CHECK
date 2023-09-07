-- New script in dw-prod-rds-master.cr9dekxonyuj.us-east-1.rds.amaz.
-- Date: Sep 6, 2023
-- Time: 1:55:04 PM
DROP TABLE IF EXISTS ryzlan.mm_base;
CREATE TABLE ryzlan.mm_base AS
SELECT *
FROM ufdm_blue.monthly_metrics
WHERE snapshot_date IN (
    SELECT DISTINCT current_period
    FROM ufdm_grey.periods
  )
  AND mcid IS NOT NULL
  AND mcid <> '-'
  AND lower(line_type) IN (
    'recurring',
    'inflight',
    'gmbh',
    'usage'
  ) --AND (lower(sku) NOT LIKE 'opt%' OR lower(sku) IS NULL)
  AND (
    lower(sku) <> 'sub-lease'
    OR lower(sku) IS NULL
  )
  AND (
    lower(sku) not ilike '%consult%'
    OR lower(sku) IS NULL
  )
  AND (
    lower(sku) NOT ILIKE 'EDU%'
    OR lower(sku) IS NULL
  )
  AND (
    lower(product_group) not like '%educat%'
    OR lower(product_group) IS NULL
  )
  AND (
    lower(product_group) not like '%professional%'
    OR lower(product_group) IS NULL
  )
  AND (
    lower(product_group) <> 'other services'
    OR lower(product_group) IS NULL
  )
  AND (
    lower(product_group) <> 'cloud : rental license'
    OR lower(product_group) IS NULL
  )
  AND (
    lower(sku) not in (
      select lower(sku)
      from ufdm_grey.sku_mapping_allocation
      where product_category is not null
    )
    OR lower(sku) IS NULL
  )
  AND (
    lower(product_name) not in ('expert services', 'expense')
    OR lower(product_name) IS NULL
  );
DROP TABLE IF EXISTS unbund;
CREATE TEMP TABLE unbund AS (
  WITH base AS (
    SELECT snapshot_date,
      COALESCE(
        end_customer_master_customer_id,
        parent_master_customer_id
      ) AS MCID,
      sku,
      reference_number,
      line_number,
      uas.product_code,
      uas.updated_list_price as list_price,
      baseline_arr_local_currency,
      arr_usd_ccfx,
      recurring_amount,
      baseline_mrr_local_currency,
      mrr_usd_ccfx,
      mrr_usd_mefx,
      arr_usd_mefx,
      mrr_usd_actualfx,
      arr_usd_actualfx,
      count(*),
      COUNT(DISTINCT product_family) AS prod_fam,
      COUNT(
        DISTINCT (
          CASE
            WHEN arr_usd_ccfx > 0 THEN product_family
            ELSE NULL
          END
        )
      ) AS arr_prod_fam
    FROM ryzlan.mm_base mm
      left join ufdm_grey.unbundling_arr_skus uas on mm.sku = uas.product_code
    WHERE snapshot_date < '2022-01-31'
      AND line_type IN ('recurring', 'Recurring')
      and product_family <> 'Recurring: Intelligence Cloud: Marketing Orchestration'
    GROUP BY 1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16 --        HAVING count(*) < 2
    ORDER BY 1,
      2,
      3
  ),
  base_two AS (
    SELECT *,
      prod_fam - arr_prod_fam AS diff
    FROM (
        SELECT mcid,
          snapshot_date,
          reference_number,
          COUNT(DISTINCT product_family) AS prod_fam,
          COUNT(
            DISTINCT (
              CASE
                WHEN arr_usd_ccfx > 0 THEN product_family
                ELSE NULL
              END
            )
          ) AS arr_prod_fam,
          sum(arr_usd_ccfx) AS sum_arr
        FROM ryzlan.mm_base AS mm
        WHERE snapshot_date < '2022-01-31'
          AND line_type IN ('recurring', 'Recurring')
          and product_family <> 'Recurring: Intelligence Cloud: Marketing Orchestration'
        GROUP BY 1,
          2,
          3
      ) AS a
    WHERE sum_arr > 0
      AND (prod_fam - arr_prod_fam) > 0
    ORDER BY sum_arr DESC
  ),
  raw AS (
    SELECT a.*,
      sum(baseline_arr_local_currency) over(PARTITION BY a.mcid, a.snapshot_date) as total_baseline_arr_local_currency,
      sum(recurring_amount) over(PARTITION BY a.mcid, a.snapshot_date) as total_recurring_amount,
      sum(baseline_mrr_local_currency) over(PARTITION BY a.mcid, a.snapshot_date) as total_baseline_mrr_local_currency,
      sum(mrr_usd_ccfx) over(PARTITION BY a.mcid, a.snapshot_date) as total_mrr_usd_ccfx,
      sum(mrr_usd_mefx) over(PARTITION BY a.mcid, a.snapshot_date) as total_mrr_usd_mefx,
      sum(arr_usd_mefx) over(PARTITION BY a.mcid, a.snapshot_date) as total_arr_usd_mefx,
      sum(mrr_usd_actualfx) over(PARTITION BY a.mcid, a.snapshot_date) as total_mrr_usd_actualfx,
      sum(arr_usd_actualfx) over(PARTITION BY a.mcid, a.snapshot_date) as total_arr_usd_actualfx,
      sum(arr_usd_ccfx) OVER(PARTITION BY a.mcid, a.snapshot_date) AS total_arr_usd_ccfx,
      sum(list_price) over(PARTITION BY a.mcid, a.snapshot_date) AS sum_list_price
    FROM base AS a
      JOIN base_two AS b ON b.snapshot_date = a.snapshot_date
      AND b.reference_number = a.reference_number
      AND b.mcid = a.mcid
  ),
  staging AS (
    SELECT df.*,
      case
        when sum_list_price = 0 then 0
        else total_arr_usd_ccfx * (list_price::decimal / sum_list_price)
      end as proposed_arr_usd_ccfx,
      case
        when sum_list_price = 0 then 0
        else total_baseline_arr_local_currency * (list_price::decimal / sum_list_price)
      end as proposed_baseline_arr_local_currency,
      case
        when sum_list_price = 0 then 0
        else total_recurring_amount * (list_price::decimal / sum_list_price)
      end as proposed_recurring_amount,
      case
        when sum_list_price = 0 then 0
        else total_baseline_mrr_local_currency * (list_price::decimal / sum_list_price)
      end as proposed_baseline_mrr_local_currency,
      case
        when sum_list_price = 0 then 0
        else total_mrr_usd_ccfx * (list_price::decimal / sum_list_price)
      end as proposed_mrr_usd_ccfx,
      case
        when sum_list_price = 0 then 0
        else total_mrr_usd_mefx * (list_price::decimal / sum_list_price)
      end as proposed_mrr_usd_mefx,
      case
        when sum_list_price = 0 then 0
        else total_arr_usd_mefx * (list_price::decimal / sum_list_price)
      end as proposed_arr_usd_mefx,
      case
        when sum_list_price = 0 then 0
        else total_mrr_usd_actualfx * (list_price::decimal / sum_list_price)
      end as proposed_mrr_usd_actualfx,
      case
        when sum_list_price = 0 then 0
        else total_arr_usd_actualfx * (list_price::decimal / sum_list_price)
      end as proposed_arr_usd_actualfx
    FROM raw AS df --       WHERE total_arr_usd_ccfx > 0 
  )
  SELECT snapshot_date,
    mcid,
    sku,
    reference_number,
    line_number,
    baseline_arr_local_currency,
    arr_usd_ccfx,
    proposed_arr_usd_ccfx::numeric as proposed_arr_usd_ccfx_last,
    proposed_baseline_arr_local_currency::NUMERIC as proposed_baseline_arr_local_currency_last,
    proposed_recurring_amount::numeric as proposed_recurring_amount_last,
    proposed_baseline_mrr_local_currency::numeric as proposed_baseline_mrr_local_currency_last,
    proposed_mrr_usd_ccfx::numeric as proposed_mrr_usd_ccfx_last,
    proposed_mrr_usd_mefx::numeric as proposed_mrr_usd_mefx_last,
    proposed_arr_usd_mefx::numeric as proposed_arr_usd_mefx_last,
    proposed_mrr_usd_actualfx::numeric as proposed_mrr_usd_actualfx_last,
    proposed_arr_usd_actualfx::NUMERIC AS proposed_arr_usd_actualfx_last
  FROM staging --WHERE list_price > 0 
);
UPDATE ryzlan.mm_base mm
SET arr_usd_ccfx = proposed_arr_usd_ccfx_last,
  baseline_arr_local_currency = proposed_baseline_arr_local_currency_last,
  recurring_amount = proposed_recurring_amount_last,
  baseline_mrr_local_currency = proposed_baseline_mrr_local_currency_last,
  mrr_usd_ccfx = proposed_mrr_usd_ccfx_last,
  mrr_usd_mefx = proposed_mrr_usd_mefx_last,
  arr_usd_mefx = proposed_arr_usd_mefx_last,
  mrr_usd_actualfx = proposed_mrr_usd_actualfx_last,
  arr_usd_actualfx = proposed_arr_usd_actualfx_last,
  modified_comments = concat(
    COALESCE(modified_comments, ''),
    '; new unbundling update: arr changed from ',
    COALESCE(mm.arr_usd_ccfx, '0')::TEXT
  ),
  modified_date = current_timestamp
FROM unbund ub
WHERE mm.mcid IS NOT NULL
  AND mm.mcid <> '-'
  AND mm.snapshot_date = ub.snapshot_date
  AND mm.mcid = ub.mcid
  AND mm.reference_number = ub.reference_number
  AND mm.sku = ub.sku
  AND mm.line_number = ub.line_number;
DROP TABLE IF EXISTS ryzlan.mm_half;
CREATE TABLE ryzlan.mm_half AS (
  SELECT *
  FROM ufdm_blue.monthly_metrics
  WHERE snapshot_date IN (
      SELECT DISTINCT current_period
      FROM ufdm_grey.periods
    )
    AND NOT(
      mcid IS NOT NULL
      AND mcid <> '-'
      AND lower(line_type) IN (
        'recurring',
        'inflight',
        'gmbh',
        'usage'
      ) --AND (lower(sku) NOT LIKE 'opt%' OR lower(sku) IS NULL)
      AND (
        lower(sku) <> 'sub-lease'
        OR lower(sku) IS NULL
      )
      AND (
        lower(sku) not ilike '%consult%'
        OR lower(sku) IS NULL
      )
      AND (
        lower(sku) NOT ILIKE 'EDU%'
        OR lower(sku) IS NULL
      )
      AND (
        lower(product_group) not like '%educat%'
        OR lower(product_group) IS NULL
      )
      AND (
        lower(product_group) not like '%professional%'
        OR lower(product_group) IS NULL
      )
      AND (
        lower(product_group) <> 'other services'
        OR lower(product_group) IS NULL
      )
      AND (
        lower(product_group) <> 'cloud : rental license'
        OR lower(product_group) IS NULL
      )
      AND (
        lower(sku) not in (
          select lower(sku)
          from ufdm_grey.sku_mapping_allocation
          where product_category is not null
        )
        OR lower(sku) IS NULL
      )
      AND (
        lower(product_name) not in ('expert services', 'expense')
        OR lower(product_name) IS NULL
      )
    )
);
DROP TABLE IF EXISTS ryzlan.mm;
CREATE TABLE ryzlan.mm AS (
  SELECT *
  FROM ryzlan.mm_base
  UNION ALL
  SELECT *
  FROM ryzlan.mm_half
);
DROP TABLE IF EXISTS sandbox.mm_notun;
CREATE TABLE sandbox.mm_notun AS
SELECT *
FROM ryzlan.mm;