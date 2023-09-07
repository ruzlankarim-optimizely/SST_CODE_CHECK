DROP TABLE IF EXISTS ryzlan.arr;
CREATE TABLE ryzlan.arr AS
SELECT *
FROM ufdm.arr;


select ryzlan.re_enter_arr_mod('2019-01-31');
select ryzlan.re_enter_arr_mod('2019-02-28');
select ryzlan.re_enter_arr_mod('2019-03-31');
select ryzlan.re_enter_arr_mod('2019-04-30');
select ryzlan.re_enter_arr_mod('2019-05-31');
select ryzlan.re_enter_arr_mod('2019-06-30');
select ryzlan.re_enter_arr_mod('2019-07-31');
select ryzlan.re_enter_arr_mod('2019-08-31');
select ryzlan.re_enter_arr_mod('2019-09-30');
select ryzlan.re_enter_arr_mod('2019-10-31');
select ryzlan.re_enter_arr_mod('2019-11-30');
select ryzlan.re_enter_arr_mod('2019-12-31');
select ryzlan.re_enter_arr_mod('2020-01-31');
select ryzlan.re_enter_arr_mod('2020-02-29');
select ryzlan.re_enter_arr_mod('2020-03-31');
select ryzlan.re_enter_arr_mod('2020-04-30');
select ryzlan.re_enter_arr_mod('2020-05-31');
select ryzlan.re_enter_arr_mod('2020-06-30');
select ryzlan.re_enter_arr_mod('2020-07-31');
select ryzlan.re_enter_arr_mod('2020-08-31');
select ryzlan.re_enter_arr_mod('2020-09-30');
select ryzlan.re_enter_arr_mod('2020-10-31');
select ryzlan.re_enter_arr_mod('2020-11-30');
select ryzlan.re_enter_arr_mod('2020-12-31');
select ryzlan.re_enter_arr_mod('2021-01-31');
select ryzlan.re_enter_arr_mod('2021-02-28');
select ryzlan.re_enter_arr_mod('2021-03-31');
select ryzlan.re_enter_arr_mod('2021-04-30');
select ryzlan.re_enter_arr_mod('2021-05-31');
select ryzlan.re_enter_arr_mod('2021-06-30');
select ryzlan.re_enter_arr_mod('2021-07-31');
select ryzlan.re_enter_arr_mod('2021-08-31');
select ryzlan.re_enter_arr_mod('2021-09-30');
select ryzlan.re_enter_arr_mod('2021-10-31');
select ryzlan.re_enter_arr_mod('2021-11-30');
select ryzlan.re_enter_arr_mod('2021-12-31');
select ryzlan.re_enter_arr_mod('2022-01-31');
select ryzlan.re_enter_arr_mod('2022-02-28');
select ryzlan.re_enter_arr_mod('2022-03-31');
select ryzlan.re_enter_arr_mod('2022-04-30');
select ryzlan.re_enter_arr_mod('2022-05-31');
select ryzlan.re_enter_arr_mod('2022-06-30');
select ryzlan.re_enter_arr_mod('2022-07-31');
select ryzlan.re_enter_arr_mod('2022-08-31');
select ryzlan.re_enter_arr_mod('2022-09-30');
select ryzlan.re_enter_arr_mod('2022-10-31');
select ryzlan.re_enter_arr_mod('2022-11-30');
select ryzlan.re_enter_arr_mod('2022-12-31');
select ryzlan.re_enter_arr_mod('2023-01-31');
select ryzlan.re_enter_arr_mod('2023-02-28');
select ryzlan.re_enter_arr_mod('2023-03-31');
select ryzlan.re_enter_arr_mod('2023-04-30');
select ryzlan.re_enter_arr_mod('2023-05-31');
select ryzlan.re_enter_arr_mod('2023-06-30');
select ryzlan.re_enter_arr_mod('2023-07-31');
select ryzlan.re_enter_arr_mod('2023-08-31');
-- removing fopti duplicates
WITH fopti AS (
  SELECT a.snapshot_date,
    a.c_name,
    a.mcid,
    a.line_type,
    a.product_family,
    a.subsidiary_entity_name,
    a.arr_usd_ccfx
  FROM ryzlan.arr a
  WHERE a.subsidiary_entity_name ilike '%FOpti%'
),
non_fopti AS (
  SELECT a.snapshot_date,
    a.c_name,
    a.mcid,
    a.line_type,
    a.product_family,
    a.subsidiary_entity_name
  FROM ryzlan.arr a
  WHERE a.subsidiary_entity_name not ilike '%FOpti%'
),
fopti_removed as (
  SELECT f.*
  FROM fopti f
    INNER JOIN non_fopti nf ON f.c_name = nf.c_name
    AND (
      DATE_TRUNC('month', f.snapshot_date) + interval '1 month' - interval '1 day'
    )::DATE = nf.snapshot_date::date
    AND f.mcid = nf.mcid
    AND f.line_type = nf.line_type
    AND f.product_family = nf.product_family
)
DELETE FROM ryzlan.arr a USING fopti_removed f
WHERE a.c_name = f.c_name
  AND a.snapshot_date = f.snapshot_date
  AND a.mcid = f.mcid
  AND a.line_type = f.line_type
  AND a.product_family = f.product_family;


SELECT ryzlan.populate_arr_integration_period();

update ryzlan.arr uaa
set arr_usd_ccfx = fi.final_value,
  baseline_currency = fi.baseline_currency,
  subsidiary_base_currency = fi.subsidiary_base_currency,
  recurring_amount = fi.end_recurring_amount,
  baseline_mrr_local_currency = fi.end_baseline_mrr_local_currency,
  baseline_arr_local_currency = fi.end_baseline_arr_local_currency,
  ccfx_date = fi.ccfx_date,
  mefx_date = fi.mefx_date,
  fx_rate_ccfx = fi.fx_rate_ccfx,
  mrr_usd_ccfx = fi.end_mrr_usd_ccfx,
  fx_rate_mefx = fi.fx_rate_mefx,
  mrr_usd_mefx = fi.end_mrr_usd_mefx,
  arr_usd_mefx = fi.end_arr_usd_mefx,
  fx_rate_actualfx = fi.fx_rate_actualfx,
  mrr_usd_actualfx = fi.end_mrr_usd_actualfx,
  arr_usd_actualfx = fi.end_arr_usd_actualfx
from (
    select final_value,
      mcid,
      snapshot_date,
      baseline_currency,
      subsidiary_base_currency,
      end_recurring_amount,
      end_baseline_mrr_local_currency,
      end_baseline_arr_local_currency,
      ccfx_date,
      mefx_date,
      fx_rate_ccfx,
      end_mrr_usd_ccfx,
      fx_rate_mefx,
      end_mrr_usd_mefx,
      end_arr_usd_mefx,
      fx_rate_actualfx,
      end_mrr_usd_actualfx,
      end_arr_usd_actualfx
    from ufdm_grey.arr_inte_corrections fi
  ) as fi
where uaa.snapshot_date = fi.snapshot_date
  and uaa.mcid = fi.mcid
  and uaa.line_type ilike '%inflight%'
  and uaa.product_name <> 'Expert Services'
  and (
    uaa.subsidiary_entity_name ilike '%welcome%'
    or uaa.subsidiary_entity_name ilike '%Optimizely North America Inc (6)%'
  );

SELECT ryzlan.sp_ufdm_arr_updates_manual();

update ryzlan.arr
set baseline_arr_local_currency = (arr_usd_ccfx / fx_rate_ccfx)::numeric(15, 6),
  baseline_mrr_local_currency = ((arr_usd_ccfx / fx_rate_ccfx) / 12)::numeric(15, 6),
  modified_comments = concat(
    coalesce(modified_comments, ''),
    '; baseline_arr_local_currency updated'
  ) --Select * from ufdm.arr
where 1 = 1
  and arr_source = 'FOpti product; Experimentation'
  and baseline_arr_local_currency is null
  and arr_usd_ccfx > 0 DROP TABLE IF EXISTS sandbox.arr_mod3;

CREATE TABLE sandbox.arr_mod3 AS
SELECT *
FROM ryzlan.arr;