CREATE OR REPLACE FUNCTION ryzlan.reenter_pa_reset(var_date date) RETURNS void LANGUAGE plpgsql AS $function$ BEGIN ----------------------------------------------------------------------------------------------------------------------------------------------------------------
  -- CHECK VAR_DATE FIRST THING
  --------------------------------------------------------------------------------
  IF var_date IS NULL THEN var_date := (
    DATE_TRUNC('month', NOW()::DATE) + interval '0 month' - interval '1 day'
  )::DATE;
END IF;
DELETE FROM ryzlan.pa
WHERE snapshot_date = var_date;
/************************************************************************
 *              Get the data from MM to be calculated
 *************************************************************************/
DROP TABLE IF EXISTS all_lines;
CREATE TEMP TABLE all_lines AS
SELECT DISTINCT COALESCE(mm.mcid) || '|' || mm.snapshot_date AS master_customer_id,
  sma.product_category,
  sum(mm.arr_usd_ccfx) AS arr_usd_ccfx -- DATA-5187
FROM ryzlan.mm mm
  INNER JOIN ufdm_grey.sku_mapping_allocation_older_version sma ON mm.sku = sma.sku
  AND sma.product_category IS NOT NULL
WHERE mm.line_type ilike 'recurring'
  AND product_category IS NOT NULL
  AND mm.snapshot_date = var_date
GROUP BY 1,
  2
UNION ALL
SELECT COALESCE(mm.mcid) || '|' || mm.snapshot_date AS master_customer_id,
  sma.product_category,
  mm.arr_usd_ccfx AS arr_usd_ccfx -- DATA-5187
FROM ryzlan.mm mm
  INNER JOIN ufdm_grey.sku_mapping_allocation_older_version sma ON sma.sku = mm.sku
  AND sma.product_category IS NOT NULL
WHERE line_type = 'inflight'
  AND product_category IS NOT NULL
  AND mm.snapshot_date = var_date;
/************************************************************************
 *           add product categories to the crosstab function
 *************************************************************************/
INSERT INTO all_lines (product_category)
SELECT DISTINCT sma.product_category
FROM ufdm_grey.sku_mapping_allocation_older_version sma
  LEFT JOIN all_lines al ON al.product_category = sma.product_category
WHERE sma.product_category IS NOT NULL
  AND al.master_customer_id IS NULL;
/************************************************************************
 *                   Calculate the product allocation
 *************************************************************************/
DROP TABLE IF EXISTS product_allocated;
CREATE TEMP TABLE product_allocated AS WITH base_arr_joined AS (
  SELECT *
  FROM crosstab(
      'SELECT
                         master_customer_id,
                         product_category,
                         sum(arr_usd_ccfx)
                     FROM all_lines
                     GROUP BY 1,2
                     ORDER BY 1,2 DESC',
      'SELECT DISTINCT smp.product_category
    FROM ufdm_grey.sku_mapping_allocation_older_version smp
    WHERE smp.product_category IS NOT NULL
    ORDER BY smp.product_category DESC'
    ) AS ct (
      master_customer_id TEXT,
      x_ott_arr float,
      x_mobile_arr float,
      x_full_stack_arr float,
      web_arr float,
      support_arr float,
      snowflakw_arr float,
      seats_arr float,
      sf_dna_arr float,
      program_management_arr float,
      platform_other_arr float,
      platform_ent_arr float,
      personalization_arr float,
      performance_edge_arr float,
      mau_arr float,
      impressions_arr float,
      full_stack_arr float,
      experimentation_arr float
    )
),
base_arr AS (
  SELECT DISTINCT split_part(master_customer_id, '|', 1) AS customer_id,
    split_part(master_customer_id, '|', 2) AS snapshot_date,
    x_ott_arr,
    x_mobile_arr,
    x_full_stack_arr,
    web_arr,
    support_arr,
    snowflakw_arr,
    sf_dna_arr,
    seats_arr,
    program_management_arr,
    platform_other_arr,
    platform_ent_arr,
    personalization_arr,
    performance_edge_arr,
    mau_arr,
    impressions_arr,
    full_stack_arr,
    experimentation_arr
  FROM base_arr_joined
),
agg_arr AS (
  SELECT customer_id,
    snapshot_date,
    --split the even arr from platfor to do the proportional calculations
    COALESCE(performance_edge_arr, 0) + COALESCE(personalization_arr, 0) + COALESCE(web_arr, 0) + COALESCE(experimentation_arr, 0) + COALESCE(platform_ent_arr, 0) / 2 AS web_products_arr,
    COALESCE(full_stack_arr, 0) + COALESCE(platform_ent_arr, 0) / 2 + COALESCE(x_ott_arr, 0) + COALESCE(x_full_stack_arr, 0) + COALESCE(x_mobile_arr, 0) AS full_stack_arr,
    COALESCE(sf_dna_arr, 0) + COALESCE(impressions_arr, 0) + COALESCE(seats_arr, 0) + COALESCE(mau_arr, 0) + COALESCE(program_management_arr, 0) AS total_porportional_arr,
    COALESCE(platform_other_arr, 0) AS platform_split_arr,
    COALESCE(platform_ent_arr, 0) AS platform_even_arr,
    COALESCE(support_arr, 0) + COALESCE(snowflakw_arr, 0) AS support_arr
  FROM base_arr
),
platform_arr AS (
  SELECT customer_id,
    snapshot_date,
.7 * platform_split_arr AS platform_fs_arr,
.3 * platform_split_arr AS platform_web_arr
  FROM agg_arr
),
proportional_arr AS (
  SELECT am.customer_id,
    am.snapshot_date,
    -- Add ARR for all products with proportional split now that full contract ARR has been calculated
    am.web_products_arr + am.total_porportional_arr * (
      (am.web_products_arr + pm.platform_web_arr) / GREATEST(
        am.web_products_arr + am.full_stack_arr + pm.platform_web_arr + pm.platform_fs_arr,
        1
      )::float
    ) AS web_arr,
    am.full_stack_arr + am.total_porportional_arr * (
      (am.full_stack_arr + pm.platform_fs_arr) / GREATEST(
        am.web_products_arr + am.full_stack_arr + + pm.platform_web_arr + pm.platform_fs_arr,
        1
      )::float
    ) AS fs_arr
  FROM agg_arr am
    JOIN platform_arr pm ON am.customer_id = pm.customer_id
    AND am.snapshot_date = pm.snapshot_date
),
allocated_arr AS (
  SELECT DISTINCT pm.customer_id,
    pm.snapshot_date,
    pm.web_arr + ptm.platform_web_arr AS total_web_arr,
    pm.fs_arr + ptm.platform_fs_arr AS total_fs_arr,
    am.support_arr
  FROM proportional_arr pm
    JOIN agg_arr am ON pm.customer_id = am.customer_id
    AND pm.snapshot_date = am.snapshot_date
    JOIN platform_arr ptm ON ptm.customer_id = pm.customer_id
    AND ptm.snapshot_date = pm.snapshot_date
)
SELECT am.customer_id,
  am.snapshot_date,
  v.*
FROM allocated_arr am
  CROSS JOIN lateral(
    VALUES ('Web', total_web_arr),
      ('Full Stack', total_fs_arr),
      ('Support', support_arr)
  ) AS v(sku, arr)
WHERE v.arr > 0
  AND customer_id IS NOT NULL;
/************************************************************************
 *               Base table to get all the complementary data
 * c_names need to be changed with the new fields of monthly metrics
 *************************************************************************/
DROP TABLE IF EXISTS complementary_fields;
CREATE TEMP TABLE complementary_fields AS
SELECT mm.snapshot_date,
  mm.mcid,
  mm.c_name,
  mm.parent_customer,
  mm.parent_customer_ns_id,
  mm.end_customer,
  mm.end_customer_ns_id,
  mm.parent_master_customer_id,
  mm.end_customer_master_customer_id,
  mm.parent_salesforce_id,
  mm.end_customer_salesforce_id,
  mm.baseline_currency,
  mm.subsidiary_base_currency,
  mm.ccfx_date,
  mm.mefx_date,
  mm.fx_rate_ccfx,
  mm.fx_rate_mefx,
  mm.fx_rate_actualfx,
  mm.term_months,
  mm.date_start,
  mm.date_end,
  mm.date_termination,
  mm.status,
  mm.catalog_type,
  mm.sco_action_id,
  mm.sco_memo,
  mm.sco_modification_type,
  mm.subsidiary_entity_name,
  mm.new_product_solution,
  mm.new_product_line,
  mm.updated_product_group,
  mm.new_product,
  mm.new_line_of_business,
  mm.new_line_of_business_sub_category
FROM ryzlan.mm mm
  INNER JOIN ufdm_grey.sku_mapping_allocation_older_version sma ON mm.sku = sma.sku
  AND sma.product_category IS NOT NULL
WHERE mm.line_type ilike 'recurring'
  AND product_category IS NOT NULL
  AND mm.snapshot_date = var_date
UNION ALL
SELECT mm.snapshot_date,
  mm.mcid,
  mm.c_name,
  mm.parent_customer,
  mm.parent_customer_ns_id,
  mm.end_customer,
  mm.end_customer_ns_id,
  mm.parent_master_customer_id,
  mm.end_customer_master_customer_id,
  mm.parent_salesforce_id,
  mm.end_customer_salesforce_id,
  mm.baseline_currency,
  mm.subsidiary_base_currency,
  mm.ccfx_date,
  mm.mefx_date,
  mm.fx_rate_ccfx,
  mm.fx_rate_mefx,
  mm.fx_rate_actualfx,
  mm.term_months,
  mm.date_start,
  mm.date_end,
  mm.date_termination,
  mm.status,
  mm.catalog_type,
  mm.sco_action_id,
  mm.sco_memo,
  mm.sco_modification_type,
  mm.subsidiary_entity_name,
  mm.new_product_solution,
  mm.new_product_line,
  mm.updated_product_group,
  mm.new_product,
  mm.new_line_of_business,
  mm.new_line_of_business_sub_category
FROM ryzlan.mm mm
  INNER JOIN ufdm_grey.sku_mapping_allocation_older_version sma ON sma.sku = mm.sku
  AND sma.product_category IS NOT NULL
WHERE mm.line_type = 'inflight'
  AND product_category IS NOT NULL
  AND mm.snapshot_date = var_date;
/************************************************************************
 *              getting the bill_frequency field
 *************************************************************************/
DROP TABLE IF EXISTS base_table;
CREATE TEMP TABLE base_table AS
SELECT mcid,
  c_name,
  line_type,
  bill_freq,
  snapshot_date,
  arr_usd_mefx
FROM ryzlan.mm mm
  INNER JOIN ufdm_grey.sku_mapping_allocation_older_version sma ON mm.sku = sma.sku
  and product_category IS NOT NULL --correction for line_type
WHERE lower(mm.line_type) in ('recurring', 'inflight')
  AND product_category IS NOT NULL
  AND mm.snapshot_date = var_date;
--------------Drop inflights as they do not have billing frequency
DROP TABLE IF EXISTS base_table_2;
CREATE TEMP TABLE base_table_2 AS
select *
from base_table
where bill_freq != '';
--------------For customers which have multiple billing frequencies for the same date, rank billing frequency descending by arr_usd_mefx
DROP TABLE IF EXISTS base_table_3;
CREATE TEMP TABLE base_table_3 AS
SELECT *,
  row_number() over(
    partition by c_name,
    snapshot_date
    order by arr_usd_mefx desc
  ) as rank_bill_freq
FROM base_table_2;
---Final table to join
DROP TABLE IF EXISTS plr;
CREATE TEMP TABLE plr as
SELECT mcid,
  c_name,
  bill_freq,
  snapshot_date
FROM base_table_3
WHERE rank_bill_freq = 1;
/************************************************************************
 *               Insert into the product allocation table
 *************************************************************************/
INSERT INTO ryzlan.pa (
    snapshot_date,
    mcid,
    c_name,
    parent_customer,
    parent_customer_ns_id,
    end_customer,
    end_customer_ns_id,
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
  ) WITH grouped_values AS (
    SELECT snapshot_date,
      mcid,
      max(c_name) AS c_name,
      max(parent_customer) AS parent_customer,
      max(parent_customer_ns_id) AS parent_customer_ns_id,
      max(end_customer_ns_id) AS end_customer_ns_id,
      max(end_customer) AS end_customer,
      max(parent_master_customer_id) AS parent_master_customer_id,
      max(end_customer_master_customer_id) AS end_customer_master_customer_id,
      max(parent_salesforce_id) AS parent_salesforce_id,
      max(end_customer_salesforce_id) AS end_customer_salesforce_id,
      max(baseline_currency) AS baseline_currency,
      max(subsidiary_base_currency) AS subsidiary_base_currency,
      max(ccfx_date) AS ccfx_date,
      max(mefx_date) AS mefx_date,
      max(fx_rate_ccfx) AS fx_rate_ccfx,
      max(fx_rate_mefx) AS fx_rate_mefx,
      max(fx_rate_actualfx) AS fx_rate_actualfx,
      max(status) AS status,
      max(catalog_type) AS catalog_type,
      max(sco_action_id) AS sco_action_id,
      max(sco_memo) AS sco_memo,
      max(sco_modification_type) AS sco_modification_type,
      max(term_months) AS term_months,
      min(date_start) AS date_start,
      max(date_end) AS date_end,
      max(date_termination) AS date_termination,
      max(subsidiary_entity_name) AS subsidiary_entity_name,
      max(new_product_solution) AS new_product_solution,
      max(new_product_line) AS new_product_line,
      max(updated_product_group) AS updated_product_group,
      max(new_product) AS new_product,
      max(new_line_of_business) AS new_line_of_business,
      max(new_line_of_business_sub_category) AS new_line_of_business_sub_category
    FROM complementary_fields
    GROUP BY 1,
      2
  )
SELECT pa.snapshot_date::date,
  pa.customer_id AS mcid,
  pv.c_name,
  pv.parent_customer,
  pv.parent_customer_ns_id,
  pv.end_customer,
  pv.end_customer_ns_id,
  pv.parent_master_customer_id,
  pv.end_customer_master_customer_id,
  pv.parent_salesforce_id,
  pv.end_customer_salesforce_id,
  'FOpti product; Experimentation' AS line_type,
  pv.baseline_currency,
  pv.subsidiary_base_currency,
  NULL AS recurring_amount,
  NULL AS baseline_mrr_local_currency,
  NULL AS baseline_arr_local_currency,
  pv.ccfx_date,
  pv.mefx_date,
  pv.fx_rate_ccfx,
  --change here for mrr_usd_ccfx
  pa.arr / 12 AS mrr_usd_ccfx,
  pa.arr AS arr_usd_ccfx,
  pv.fx_rate_mefx,
  --change here for mrr_usd_ccfx
  ((pa.arr / pv.fx_rate_ccfx) * pv.fx_rate_mefx) / 12 AS mrr_usd_mefx,
  (pa.arr / pv.fx_rate_ccfx) * pv.fx_rate_mefx AS arr_usd_mefx,
  pv.fx_rate_actualfx,
  --change here for mrr_usd_ccfx
  ((pa.arr / pv.fx_rate_ccfx) * pv.fx_rate_actualfx) / 12 AS mrr_usd_actualfx,
  (pa.arr / pv.fx_rate_ccfx) * pv.fx_rate_actualfx AS arr_usd_actualfx,
  pl.bill_freq AS bill_freq,
  pv.term_months,
  pv.date_start,
  pv.date_end,
  pv.date_termination,
  NULL AS subline_id,
  NULL AS reference_number,
  NULL AS line_number,
  NULL AS revision_number,
  NULL AS change_order,
  pv.status,
  pv.catalog_type,
  pa.sku,
  pa.sku AS sku_name,
  pa.sku AS product_name,
  pa.sku AS product_group,
  pa.sku AS product_family,
  'FOpti product; Experimentation' AS arr_source,
  pv.sco_action_id,
  pv.sco_memo,
  pv.sco_modification_type,
  subsidiary_entity_name AS subsidiary_entity_name,
  'FOpti' AS legacy_org,
  pv.new_product_solution,
  pv.new_product_line,
  pv.updated_product_group,
  pv.new_product,
  pv.new_line_of_business,
  pv.new_line_of_business_sub_category
FROM product_allocated pa
  LEFT JOIN grouped_values pv ON pa.snapshot_date::date = pv.snapshot_date
  AND pa.customer_id = pv.mcid
  LEFT JOIN plr pl ON pv.mcid = pl.mcid
  and pa.snapshot_date::date = pl.snapshot_date
WHERE parent_customer IS NOT NULL;
--not updating modified date for the below update as it is not major
--UPDATE ufdm_blue.product_allocated
--SET MCID = coalesce(
--        NULLIF(TRIM(end_customer_master_customer_id), ''),
--        NULLIF(TRIM(parent_master_customer_id), '')
--    )
--WHERE snapshot_date = var_date;
END;
$function$;