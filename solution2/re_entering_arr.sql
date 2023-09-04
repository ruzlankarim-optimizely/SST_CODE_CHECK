CREATE OR REPLACE FUNCTION ryzlan.re_enter_arr(var_date date) RETURNS VOID AS $BODY$ BEGIN
DELETE FROM ryzlan.arr
WHERE snapshot_date = var_date;
INSERT INTO ryzlan.arr (
    snapshot_date,
    c_name,
    parent_customer_ns_id,
    end_customer_ns_id,
    parent_customer,
    end_customer,
    parent_master_customer_id,
    end_customer_master_customer_id,
    parent_salesforce_id,
    end_customer_salesforce_id,
    line_type,
    baseline_currency,
    subsidiary_base_currency,
    recurring_amount,
    baseline_mrr_local_currency,
    baseline_arr_local_currency,
    ccfx_date,
    mefx_date,
    fx_rate_ccfx,
    mrr_usd_ccfx,
    arr_usd_ccfx,
    fx_rate_mefx,
    mrr_usd_mefx,
    arr_usd_mefx,
    fx_rate_actualfx,
    mrr_usd_actualfx,
    arr_usd_actualfx,
    bill_freq,
    term_months,
    date_start,
    date_end,
    date_termination,
    subline_id,
    reference_number,
    line_number,
    revision_number,
    change_order,
    status,
    catalog_type,
    sku,
    sku_name,
    product_name,
    product_group,
    product_family,
    arr_source,
    sco_action_id,
    sco_memo,
    sco_modification_type,
    subsidiary_entity_name,
    legacy_org,
    new_product_solution,
    new_product_line,
    updated_product_group,
    new_product,
    new_line_of_business,
    new_line_of_business_sub_category
  )
SELECT snapshot_date,
  c_name,
  parent_customer_ns_id,
  end_customer_ns_id,
  parent_customer,
  end_customer,
  parent_master_customer_id,
  end_customer_master_customer_id,
  parent_salesforce_id,
  end_customer_salesforce_id,
  line_type,
  baseline_currency,
  subsidiary_base_currency,
  recurring_amount,
  baseline_mrr_local_currency,
  baseline_arr_local_currency,
  ccfx_date,
  mefx_date,
  fx_rate_ccfx,
  mrr_usd_ccfx,
  -- DATA-5187
  arr_usd_ccfx,
  -- DATA-5187
  fx_rate_mefx,
  mrr_usd_mefx,
  -- DATA-5187
  arr_usd_mefx,
  -- DATA-5187
  fx_rate_actualfx,
  mrr_usd_actualfx,
  -- DATA-5187
  arr_usd_actualfx,
  -- DATA-5187
  bill_freq,
  term_months,
  date_start,
  date_end,
  date_termination,
  subline_id,
  reference_number,
  line_number,
  revision_number,
  change_order,
  status,
  catalog_type,
  sku,
  sku_name,
  product_name,
  product_group,
  product_family,
  arr_source,
  sco_action_id,
  sco_memo,
  sco_modification_type,
  subsidiary_entity_name,
  legacy_org,
  new_product_solution,
  new_product_line,
  updated_product_group,
  new_product,
  new_line_of_business,
  new_line_of_business_sub_category
FROM ryzlan.monthly_metrics
WHERE snapshot_date = var_date
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
--product allocaton
INSERT INTO ryzlan.arr (
    snapshot_date,
    c_name,
    parent_customer_ns_id,
    end_customer_ns_id,
    parent_customer,
    end_customer,
    parent_master_customer_id,
    end_customer_master_customer_id,
    parent_salesforce_id,
    end_customer_salesforce_id,
    line_type,
    baseline_currency,
    subsidiary_base_currency,
    recurring_amount,
    baseline_mrr_local_currency,
    baseline_arr_local_currency,
    ccfx_date,
    mefx_date,
    fx_rate_ccfx,
    mrr_usd_ccfx,
    arr_usd_ccfx,
    fx_rate_mefx,
    mrr_usd_mefx,
    arr_usd_mefx,
    fx_rate_actualfx,
    mrr_usd_actualfx,
    arr_usd_actualfx,
    bill_freq,
    term_months,
    date_start,
    date_end,
    date_termination,
    subline_id,
    reference_number,
    line_number,
    revision_number,
    change_order,
    status,
    catalog_type,
    sku,
    sku_name,
    product_name,
    product_group,
    product_family,
    arr_source,
    sco_action_id,
    sco_memo,
    sco_modification_type,
    subsidiary_entity_name,
    legacy_org,
    new_product_solution,
    new_product_line,
    updated_product_group,
    new_product,
    new_line_of_business,
    new_line_of_business_sub_category
  ) WITH totals AS (
    SELECT snapshot_date,
      COALESCE(
        end_customer_master_customer_id,
        parent_master_customer_id
      ) AS master_customer_id,
      sum(mrr_usd_ccfx) AS total_mrr,
      sum(arr_usd_ccfx) AS total_arr
    FROM ryzlan.product_allocated pa
    WHERE sku <> 'Support'
      AND pa.snapshot_date = var_date
    GROUP BY 1,
      2
  ),
  support_total AS (
    SELECT pa.snapshot_date,
      COALESCE(
        pa.end_customer_master_customer_id,
        pa.parent_master_customer_id
      ) AS master_customer_id,
      sum(COALESCE(pa.mrr_usd_ccfx, 0)) AS support_mrr,
      sum(COALESCE(pa.arr_usd_ccfx, 0)) AS support_arr
    FROM ryzlan.product_allocated pa
    WHERE pa.sku = 'Support'
      AND pa.snapshot_date = var_date
    GROUP BY 1,
      2
  ),
  to_split AS (
    SELECT t.*,
      st.support_mrr,
      st.support_arr
    FROM totals t
      LEFT JOIN support_total st ON t.snapshot_date = st.snapshot_date
      AND t.master_customer_id = st.master_customer_id
  )
SELECT pa.snapshot_date,
  pa.c_name,
  pa.parent_customer_ns_id,
  pa.end_customer_ns_id,
  pa.parent_customer,
  pa.end_customer,
  pa.parent_master_customer_id,
  pa.end_customer_master_customer_id,
  pa.parent_salesforce_id,
  pa.end_customer_salesforce_id,
  pa.line_type,
  pa.baseline_currency,
  pa.subsidiary_base_currency,
  pa.recurring_amount,
  pa.baseline_mrr_local_currency,
  pa.baseline_arr_local_currency,
  pa.ccfx_date,
  pa.mefx_date,
  pa.fx_rate_ccfx,
  (
    pa.mrr_usd_ccfx + COALESCE(
      (
        pa.mrr_usd_ccfx / ts.total_mrr * support_mrr
      ),
      0
    )
  ) AS mrr_usd_ccfx,
  (
    pa.arr_usd_ccfx + COALESCE(
      (
        pa.arr_usd_ccfx / ts.total_arr * support_arr
      ),
      0
    )
  ) AS arr_usd_ccfx,
  pa.fx_rate_mefx,
  (
    (
      pa.mrr_usd_ccfx + COALESCE(
        (
          pa.mrr_usd_ccfx / ts.total_mrr * support_mrr
        ),
        0
      )
    ) / pa.fx_rate_ccfx
  ) * pa.fx_rate_mefx AS mrr_usd_mefx,
  (
    (
      pa.arr_usd_ccfx + COALESCE(
        (
          pa.arr_usd_ccfx / ts.total_arr * support_arr
        ),
        0
      )
    ) / pa.fx_rate_ccfx
  ) * pa.fx_rate_mefx AS arr_usd_mefx,
  pa.fx_rate_actualfx,
  (
    (
      pa.mrr_usd_ccfx + COALESCE(
        (
          pa.mrr_usd_ccfx / ts.total_mrr * support_mrr
        ),
        0
      )
    ) / pa.fx_rate_ccfx
  ) * pa.fx_rate_actualfx AS mrr_usd_actualfx,
  (
    (
      pa.arr_usd_ccfx + COALESCE(
        (
          pa.arr_usd_ccfx / ts.total_arr * support_arr
        ),
        0
      )
    ) / pa.fx_rate_ccfx
  ) * pa.fx_rate_actualfx AS arr_usd_actualfx,
  pa.bill_freq,
  pa.term_months,
  pa.date_start,
  pa.date_end,
  pa.date_termination,
  pa.subline_id,
  pa.reference_number,
  pa.line_number,
  pa.revision_number,
  pa.change_order,
  pa.status,
  pa.catalog_type,
  pa.sku,
  pa.sku_name,
  pa.product_name,
  pa.product_group,
  pa.product_family,
  pa.arr_source,
  pa.sco_action_id,
  pa.sco_memo,
  pa.sco_modification_type,
  pa.subsidiary_entity_name,
  pa.legacy_org,
  pa.new_product_solution,
  pa.new_product_line,
  pa.updated_product_group,
  pa.new_product,
  pa.new_line_of_business,
  pa.new_line_of_business_sub_category
FROM ryzlan.product_allocated pa
  LEFT JOIN to_split ts ON pa.snapshot_date = ts.snapshot_date
  AND COALESCE(
    pa.end_customer_master_customer_id,
    pa.parent_master_customer_id
  ) = ts.master_customer_id
WHERE lower(sku) IN ('web', 'full stack')
  AND pa.snapshot_date = var_date;
UPDATE ryzlan.arr
SET MCID = coalesce(
    NULLIF(TRIM(end_customer_master_customer_id), ''),
    NULLIF(TRIM(parent_master_customer_id), '')
  )
WHERE snapshot_date = var_date;
--------------------------------------------------------------------------------
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
    and (
      DATE_TRUNC('month', a.snapshot_date) + interval '1 month' - interval '1 day'
    )::DATE = var_date
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
    and (
      DATE_TRUNC('month', a.snapshot_date) + interval '1 month' - interval '1 day'
    )::DATE = var_date
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
PERFORM ryzlan.populate_arr_integration_period();
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
RAISE NOTICE 'GREEN LAYER POPULATION COMPLETED FOR %...',
var_date;
--------------------------------------------------------------------------------
END;
$BODY$ LANGUAGE 'plpgsql'