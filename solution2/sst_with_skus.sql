CREATE OR REPLACE FUNCTION ryzlan.sp_populate_snapshot_sst_with_sku(var_date date) RETURNS void LANGUAGE plpgsql AS $function$ BEGIN DROP TABLE IF EXISTS tat_static;
CREATE TEMP TABLE tat_static AS (
  WITH base AS (
    SELECT tat."customer_name_d&b" AS customer_name_dnb,
      NULLIF(TRIM(tat.parent_customer), '') AS parent_customer,
      tat.parent_master_customer_id,
      -- NULL
      NULL::text AS parent_customer_ns_id,
      NULLIF(TRIM(tat.customer_name), '') AS customer_name,
      NULLIF(TRIM(tat.end_customer), '') AS end_customer,
      NULL::text AS end_customer_master_customer_id,
      NULL::text AS end_customer_ns_id,
      COALESCE(mp.mcid_new, NULLIF(TRIM(tat.mcid), '')) AS mcid,
      COALESCE(NULLIF(TRIM(tat."Overage Y/N"), ''), 'N') AS overage_flag,
      NULLIF(TRIM(tat."NS ID"), '') AS ns_id,
      tat.currency,
      (
        DATE_TRUNC('month', tat.snapshot_date) + interval '1 month' - interval '1 day'
      )::DATE AS snapshot_date,
      COALESCE(tat.arr, 0) as arr,
      tat.fx_rate_ccfx,
      COALESCE(tat.arr_usd_ccfx, 0) as arr_usd_ccfx,
      tat.ccfx_date,
      CASE
        WHEN tat.subsidiary_name = 'Idio Inc.' THEN 'Idio Inc'
        ELSE tat.subsidiary_name
      END AS subsidiary_name,
      CASE
        WHEN tat.product_family = 'B2B Commerce (incl. Headless)' THEN 'Recurring: Cloud: Commerce Cloud: B2B Commerce (incl. Headless)'
        WHEN tat.product_family = 'B2C Commerce (incl. Headless)' THEN 'Recurring: Cloud: Commerce Cloud: B2C Commerce (incl. Headless)'
        WHEN tat.product_family = 'Content PaaS' THEN 'Recurring: Cloud: Content Cloud: Content PaaS'
        WHEN tat.product_family = 'CDP (incl. Visitor Intelligence)' THEN 'Recurring: Cloud: Intelligence Cloud: CDP (incl. Visitor Intelligence)'
        WHEN tat.product_family = 'Content Recommendations (incl. E-mail)' THEN 'Recurring: Cloud: Intelligence Cloud: Content Recommendations (incl. E-mail)'
        WHEN tat.product_family = 'Product Recommendations (incl. E-mail)' THEN 'Recurring: Cloud: Intelligence Cloud: Product Recommendations (incl. E-mail)'
        WHEN tat.product_family = 'Web Experimentation and Personalization' THEN 'Recurring: Cloud: Intelligence Cloud: Web Experimentation and Personalization'
        WHEN tat.product_family = 'Campaign' THEN 'Recurring: Cloud: Other Bookings: Campaign'
        WHEN tat.product_family = 'Other' THEN 'Recurring: Cloud: Other Bookings: Other Bookings'
        WHEN tat.product_family = 'Welcome' THEN 'Recurring: Intelligence Cloud: Marketing Orchestration'
        WHEN tat.product_family = 'Subscription License' THEN 'Recurring: Subscription License'
        WHEN tat.product_family = 'Perpetual License' THEN 'Non-Recurring: Perpetual License'
        WHEN tat.product_family = 'Full Stack' THEN 'Full Stack' --                 WHEN tat.product_family = 'Web Experimentation and Personalization' THEN 'Web'
        WHEN tat.product_family = 'Perpetual License' THEN '- Not Applicable -' --                 WHEN tat.product_family = 'Campaign' THEN 'Campaign'
        ELSE tat.product_family
      END AS product_family,
      tat.sku as sku
    FROM sandbox.tat_with_sku tat
      LEFT JOIN ufdm_grey.mcid_overrides_manual mp ON mp.mcid_old = tat.mcid
      left join ufdm_grey.sst_dates_lookup_manual c on tat.mcid = c.mcid
    WHERE tat.is_deleted IS DISTINCT
    FROM 1 --          and COALESCE(NULLIF(TRIM(tat."Overage Y/N"), ''), 'N') is distinct
      --            from 'N'
      and (
        c.mcid is null
        or (
          c.mcid is not null
          and (
            date_trunc('month', tat.snapshot_date::date) + interval '1 month' - interval '1 day'
          )::date < c.date_for_switching_from_arr
        )
      )
  ),
  agg AS (
    SELECT snapshot_date,
      product_family,
      sku,
      overage_flag,
      mcid,
      currency,
      parent_customer,
      customer_name_dnb,
      parent_master_customer_id,
      -- NULL
      parent_customer_ns_id,
      customer_name,
      end_customer,
      end_customer_master_customer_id,
      end_customer_ns_id,
      ns_id,
      arr,
      fx_rate_ccfx,
      arr_usd_ccfx,
      ccfx_date,
      subsidiary_name,
      GREATEST(
        0,
        SUM(arr_usd_ccfx) OVER (
          PARTITION BY snapshot_date,
          product_family,
          sku,
          overage_flag,
          mcid,
          currency
        )
      ) AS total_arr_usd_ccfx,
      GREATEST(
        0,
        SUM(arr) OVER (
          PARTITION BY snapshot_date,
          product_family,
          sku,
          overage_flag,
          mcid,
          currency
        )
      ) AS total_arr,
      ROW_NUMBER() OVER (
        PARTITION BY snapshot_date,
        product_family,
        sku,
        overage_flag,
        mcid,
        currency
        ORDER BY arr_usd_ccfx DESC,
          ns_id
      ) AS ranking
    FROM base
  )
  SELECT snapshot_date,
    product_family,
    sku,
    overage_flag,
    mcid,
    currency,
    parent_customer,
    customer_name_dnb,
    parent_master_customer_id,
    -- NULL
    parent_customer_ns_id,
    customer_name,
    end_customer,
    end_customer_master_customer_id,
    end_customer_ns_id,
    ns_id,
    total_arr AS arr,
    fx_rate_ccfx,
    total_arr_usd_ccfx AS arr_usd_ccfx,
    ccfx_date,
    subsidiary_name
  FROM agg
  WHERE ranking = 1
);
UPDATE tat_static ts
SET parent_customer = COALESCE(ts.parent_customer, ts.customer_name),
  parent_customer_ns_id = ts.ns_id,
  parent_master_customer_id = ts.mcid
WHERE ts.end_customer IS NULL
  AND ts.customer_name IS NOT NULL;
DROP TABLE IF EXISTS arr_base;
CREATE TEMP TABLE arr_base AS (
  WITH dates AS (
    SELECT DISTINCT snapshot_date
    FROM ufdm.arr
    WHERE snapshot_date > '2022-12-31'
  ),
  dates_rank AS (
    SELECT snapshot_date,
      DENSE_RANK() OVER (
        PARTITION BY DATE_TRUNC('month', snapshot_date)
        ORDER BY snapshot_date desc
      ) AS ranking
    FROM dates
  )
  SELECT (
      DATE_TRUNC('month', snapshot_date) + interval '1 month' - interval '1 day'
    )::DATE as snapshot_date,
    NULLIF(TRIM(arr_b.c_name), '') AS c_name,
    NULLIF(TRIM(parent_customer_ns_id), '') AS parent_customer_ns_id,
    NULLIF(TRIM(end_customer_ns_id), '') AS end_customer_ns_id,
    NULLIF(TRIM(parent_customer), '') AS parent_customer,
    NULLIF(TRIM(end_customer), '') AS end_customer,
    NULLIF(TRIM(parent_master_customer_id), '') AS parent_master_customer_id,
    NULLIF(TRIM(end_customer_master_customer_id), '') AS end_customer_master_customer_id,
    COALESCE(mp.mcid_new, NULLIF(TRIM(arr_b.mcid), '')) AS mcid,
    CASE
      WHEN arr_source = 'GMBH overages' THEN 'Y'
      ELSE 'N'
    END as overage_flag,
    line_type,
    baseline_currency,
    subsidiary_base_currency,
    ccfx_date,
    fx_rate_ccfx,
    arr_usd_ccfx,
    baseline_arr_local_currency,
    product_family,
    sku,
    arr_source,
    subsidiary_entity_name,
    legacy_org,
    --NULLIF(TRIM(mcid), '') AS mcid,
    created_date,
    modified_date,
    reference_number,
    new_product_solution,
    new_product_line,
    updated_product_group,
    new_product,
    new_line_of_business,
    new_line_of_business_sub_category
  FROM sandbox.arr_unbund arr_b
    LEFT JOIN ufdm_grey.mcid_overrides_manual mp ON mp.mcid_old = arr_b.mcid
  WHERE arr_source not ilike '%GMBH overages%'
    AND snapshot_date NOT IN (
      SELECT snapshot_date
      FROM dates_rank
      WHERE ranking > 1
    )
);
UPDATE arr_base
SET end_customer_ns_id = NULLIF(
    TRIM(
      SUBSTRING(
        SUBSTRING(
          end_customer,
          POSITION(' : C' in end_customer) + 3
        ),
        0,
        POSITION(
          ' ' in SUBSTRING(
            end_customer,
            POSITION(' : C' in end_customer) + 3
          )
        )
      )
    ),
    ''
  )
WHERE end_customer like 'C%: C%';
DROP TABLE IF EXISTS customer_detail;
CREATE TEMP TABLE customer_detail AS (
  WITH base AS (
    SELECT sf_guid_c AS mcid,
      "name",
      territory_c,
      number_of_employees,
      id,
      duns_number_c,
      is_deleted,
      last_modified_date,
      2 AS rank
    FROM opti_salesforce.account
    WHERE sf_guid_c IS NOT NULL
    UNION
    SELECT dynamics_id_c AS mcid,
      "name",
      territory_c,
      number_of_employees,
      id,
      duns_number_c,
      is_deleted,
      last_modified_date,
      1 AS rank
    FROM opti_salesforce.account
    WHERE dynamics_id_c IS NOT NULL
  )
  SELECT DISTINCT ON (e.mcid) e.mcid AS epi_universal_id,
    e."name",
    duns_number_c AS duns_id,
    e.id,
    CASE
      WHEN e.number_of_employees > 1499 THEN 'Enterprise'
      WHEN e.number_of_employees < 1500 THEN 'Mid-Market'
      WHEN e.number_of_employees IS NULL THEN 'N/A'
      ELSE 'N/A'
    END AS segment,
    CASE
      WHEN t."name" LIKE '%NA%' THEN 'North America'
      WHEN t."name" LIKE '%North%America%' THEN 'North America'
      WHEN t."name" LIKE '%EMEA%' THEN 'EMEA'
      WHEN t."name" LIKE '%Europe%' THEN 'EMEA'
      WHEN t."name" LIKE '%Sweden%' THEN 'EMEA'
      WHEN t."name" LIKE '%UK%' THEN 'EMEA'
      WHEN t."name" LIKE '%ANZ%' THEN 'APAC'
      WHEN t."name" LIKE '%APJ%' THEN 'APAC'
      WHEN t."name" LIKE '%DACH%' THEN 'DACH'
      ELSE NULL
    END AS region
  FROM base e
    LEFT JOIN opti_salesforce.territory_c t ON e.territory_c = t.id
    AND t.is_deleted IS DISTINCT
  FROM TRUE
  ORDER BY e.mcid,
    e.rank,
    e.is_deleted,
    e.last_modified_date DESC
);
DROP TABLE IF EXISTS ns_id_master_customer_id_map;
CREATE TEMP TABLE ns_id_master_customer_id_map AS
SELECT DISTINCT TRIM(name) AS ns_id,
  TRIM(master_customer_id) AS mcid
FROM epi_netsuite.companies
WHERE NULLIF(TRIM(name), '') IS NOT NULL
  AND NULLIF(TRIM(master_customer_id), '') IS NOT null;
UPDATE arr_base
SET mcid = COALESCE(
    NULLIF(end_customer_master_customer_id, ''),
    NULLIF(parent_master_customer_id, '')
  )
where nullif(trim(mcid), '') is null;
DROP TABLE IF EXISTS dnb_accounts;
CREATE TEMP TABLE dnb_accounts AS (
  WITH base AS (
    SELECT NULLIF(TRIM(da."Master Customer ID"), '') AS master_customer_id,
      NULLIF(TRIM(da."D-U-N-S Number"::TEXT), '') AS duns_number,
      da.db_name AS db_name,
      da1.db_name AS parent_db_name,
      NULLIF(TRIM(da.db_domesticultimatedunsnumber::TEXT), '') AS db_domesticultimatedunsnumber,
      NULLIF(TRIM(da.db_globalultimatedunsnumber::TEXT), '') AS db_globalultimatedunsnumber,
      NULLIF(TRIM(da.db_parentdunsnumber::TEXT), '') as db_parentdunsnumber
    FROM dnb_data.all_accounts_20230323 da
      LEFT JOIN dnb_data.all_accounts_20230323 da1 ON da1."D-U-N-S Number" = da.db_parentdunsnumber
  )
  SELECT DISTINCT ON (master_customer_id) master_customer_id,
    db_name,
    parent_db_name,
    duns_number::TEXT AS duns_number,
    db_domesticultimatedunsnumber::TEXT AS db_domesticultimatedunsnumber,
    db_globalultimatedunsnumber::TEXT AS db_globalultimatedunsnumber,
    db_parentdunsnumber::TEXT AS db_parentdunsnumber
  FROM base
);
DROP TABLE IF EXISTS arr_agg;
CREATE TEMP TABLE arr_agg AS (
  SELECT snapshot_date,
    c_name,
    mcid,
    product_family,
    sku,
    subsidiary_entity_name,
    baseline_currency,
    fx_rate_ccfx,
    ccfx_date,
    overage_flag,
    new_product_solution,
    new_product_line,
    updated_product_group,
    new_product,
    new_line_of_business,
    new_line_of_business_sub_category,
    NULL AS ultimate_parent_name,
    NULL AS ultimate_parent_id,
    max(up1.duns_number)::TEXT AS duns_number,
    max(up1.db_name) AS duns_name,
    max(up1.db_parentdunsnumber)::TEXT AS parent_duns_number,
    max(up1.parent_db_name) AS parent_duns_name,
    max(up1.db_domesticultimatedunsnumber)::TEXT AS domesticultimatedunsnumber,
    max(up1.db_globalultimatedunsnumber)::TEXT AS globalultimatedunsnumber,
    max(parent_master_customer_id) AS parent_master_customer_id,
    max(parent_customer_ns_id) AS parent_customer_nsid,
    max(parent_customer) AS parent_customer_name,
    max(end_customer_master_customer_id) AS end_master_customer_id,
    max(end_customer_ns_id) AS end_customer_nsid,
    max(end_customer) AS end_customer_name,
    max(cd."name") AS customer_name,
    max(cd.segment) AS segment,
    max(cd.region) AS region,
    sum(arr_usd_ccfx) AS arr_usd_ccfx,
    sum(baseline_arr_local_currency) AS baseline_arr_local_currency,
    min(created_date) AS min_created_date,
    max(modified_date) AS max_modified_date,
    max(cd1.id) as parent_sf_id,
    max(cd1."name") as parent_sf_name
  FROM arr_base arrb
    LEFT JOIN customer_detail cd ON cd.epi_universal_id = arrb.mcid
    LEFT JOIN customer_detail cd1 ON cd1.epi_universal_id = arrb.parent_master_customer_id
    LEFT JOIN dnb_accounts up1 ON up1.master_customer_id = arrb.mcid
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
    16
);
DROP TABLE IF EXISTS ufdm_fopti;
CREATE TEMP TABLE ufdm_fopti AS (
  SELECT snapshot_date,
    c_name,
    mcid,
    product_family,
    sku,
    subsidiary_entity_name,
    baseline_currency,
    fx_rate_ccfx,
    ccfx_date,
    overage_flag,
    new_product_solution,
    new_product_line,
    updated_product_group,
    new_product,
    new_line_of_business,
    new_line_of_business_sub_category,
    NULL AS ultimate_parent_name,
    NULL AS ultimate_parent_id,
    max(up1.duns_number)::TEXT AS duns_number,
    max(up1.db_name) AS duns_name,
    max(up1.db_parentdunsnumber)::TEXT AS parent_duns_number,
    max(up1.parent_db_name) AS parent_duns_name,
    max(up1.db_domesticultimatedunsnumber)::TEXT AS domesticultimatedunsnumber,
    max(up1.db_globalultimatedunsnumber)::TEXT AS globalultimatedunsnumber,
    max(parent_master_customer_id) AS parent_master_customer_id,
    max(parent_customer_ns_id) AS parent_customer_nsid,
    max(parent_customer) AS parent_customer_name,
    max(end_customer_master_customer_id) AS end_master_customer_id,
    max(end_customer_ns_id) AS end_customer_nsid,
    max(end_customer) AS end_customer_name,
    max(cd."name") AS customer_name,
    max(cd.segment) AS segment,
    max(cd.region) AS region,
    sum(arr_usd_ccfx) AS arr_usd_ccfx,
    sum(baseline_arr_local_currency) AS baseline_arr_local_currency,
    min(created_date) AS min_created_date,
    max(modified_date) AS max_modified_date,
    max(cd1.id) as parent_sf_id,
    max(cd1."name") as parent_sf_name
  FROM arr_base arr
    LEFT JOIN customer_detail cd ON cd.epi_universal_id = arr.mcid
    LEFT JOIN customer_detail cd1 ON cd1.epi_universal_id = arr.parent_master_customer_id
    LEFT JOIN dnb_accounts up1 ON up1.master_customer_id = arr.mcid
  WHERE arr.snapshot_date < '2022-01-01'::DATE
    AND line_type ILIKE '%Fopti%'
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
    16
);
--SELECT 
--    extract(YEAR FROM snapshot_date) AS yrs ,
--    round(sum(arr_usd_ccfx ))
--FROM ufdm_fopti  
--GROUP BY 1   
DROP TABLE IF EXISTS tat_agg;
CREATE TEMP TABLE tat_agg AS (
  SELECT tat.customer_name_dnb,
    tat.parent_customer,
    tat.parent_master_customer_id,
    -- NULL
    tat.parent_customer_ns_id,
    tat.customer_name,
    tat.end_customer,
    tat.end_customer_master_customer_id,
    tat.end_customer_ns_id,
    tat.mcid,
    tat.overage_flag,
    tat.ns_id,
    tat.currency,
    tat.snapshot_date,
    tat.arr,
    tat.fx_rate_ccfx,
    tat.arr_usd_ccfx,
    tat.ccfx_date,
    tat.subsidiary_name,
    tat.product_family,
    tat.sku,
    cd.segment,
    cd.region,
    NULL AS ultimate_parent_id,
    NULL AS ultimate_parent_name,
    up.db_name AS duns_name,
    up.duns_number::TEXT AS duns_number,
    up.parent_db_name AS parent_duns_name,
    up.db_parentdunsnumber::TEXT AS parent_duns_number,
    up.db_domesticultimatedunsnumber::TEXT AS domesticultimatedunsnumber,
    up.db_globalultimatedunsnumber::TEXT AS globalultimatedunsnumber,
    cd."name" AS parent_sf_name,
    cd.id AS parent_sf_id
  FROM tat_static tat
    LEFT JOIN customer_detail cd ON cd.epi_universal_id = tat.mcid
    LEFT JOIN dnb_accounts up ON up.master_customer_id = tat.mcid
);
DROP TABLE IF EXISTS sst_temp;
CREATE TEMP TABLE sst_temp AS (
  WITH ufdm_campaigns_dec2021 AS (
    SELECT snapshot_date,
      ultimate_parent_id AS ultimate_parent_id,
      ultimate_parent_name AS ultimate_parent_name,
      duns_name,
      duns_number,
      parent_duns_name,
      parent_duns_number,
      domesticultimatedunsnumber,
      globalultimatedunsnumber,
      new_product_solution,
      new_product_line,
      updated_product_group,
      new_product,
      new_line_of_business,
      new_line_of_business_sub_category,
      c_name AS c_name,
      parent_customer_nsid AS parent_ns_id,
      end_customer_nsid AS end_ns_id,
      customer_name AS name,
      parent_customer_name AS parent_name,
      end_customer_name AS end_name,
      mcid AS mcid,
      parent_master_customer_id AS parent_mcid,
      end_master_customer_id AS end_mcid,
      subsidiary_entity_name AS subsidiary_entity_name,
      overage_flag AS overage_flag,
      segment AS segment,
      region AS region,
      product_family AS product_family,
      sku as sku,
      baseline_currency AS base_currency,
      fx_rate_ccfx AS cc_fx_rate,
      ccfx_date AS fx_date,
      arr_usd_ccfx AS arr,
      baseline_arr_local_currency AS baseline_arr_local_currency,
      max_modified_date AS dw_modified_date,
      min_created_date AS dw_created_date,
      parent_sf_id,
      parent_sf_name
    FROM arr_agg
    WHERE date_trunc('month', snapshot_date) = '2021-12-01'::DATE
      AND product_family = 'Recurring: Cloud: Other Bookings: Campaign'
  ),
  sst_tat AS (
    SELECT snapshot_date,
      ultimate_parent_id AS ultimate_parent_id,
      ultimate_parent_name AS ultimate_parent_name,
      duns_name,
      duns_number,
      parent_duns_name,
      parent_duns_number,
      domesticultimatedunsnumber,
      globalultimatedunsnumber,
      NULL AS new_product_solution,
      NULL AS new_product_line,
      NULL AS updated_product_group,
      NULL AS new_product,
      NULL AS new_line_of_business,
      NULL AS new_line_of_business_sub_category,
      tat.ns_id AS c_name,
      tat.ns_id AS parent_ns_id,
      NULL AS end_ns_id,
      tat.customer_name AS name,
      tat.parent_customer AS parent_name,
      NULL AS end_name,
      tat.mcid AS mcid,
      tat.mcid AS parent_mcid,
      NULL AS end_mcid,
      tat.subsidiary_name AS subsidiary_entity_name,
      overage_flag,
      tat.segment AS segment,
      tat.region AS region,
      product_family,
      tat.sku as sku,
      tat.currency AS base_currency,
      tat.fx_rate_ccfx AS cc_fx_rate,
      tat.ccfx_date AS fx_date,
      tat.arr_usd_ccfx AS arr,
      tat.arr AS baseline_arr_local_currency,
      NULL::DATE AS dw_modified_date,
      NULL::DATE AS dw_created_date,
      parent_sf_id,
      parent_sf_name
    FROM tat_agg tat
    WHERE DATE_TRUNC('month', tat.snapshot_date) != '2021-12-01'::DATE
      OR product_family IS DISTINCT
    FROM 'Recurring: Cloud: Other Bookings: Campaign'
  ),
  ufdm_fopti_pre_2022 AS (
    SELECT snapshot_date,
      ultimate_parent_id AS ultimate_parent_id,
      ultimate_parent_name AS ultimate_parent_name,
      duns_name,
      duns_number,
      parent_duns_name,
      parent_duns_number,
      domesticultimatedunsnumber,
      globalultimatedunsnumber,
      new_product_solution,
      new_product_line,
      updated_product_group,
      new_product,
      new_line_of_business,
      new_line_of_business_sub_category,
      ufdm.c_name AS c_name,
      ufdm.parent_customer_nsid AS parent_ns_id,
      ufdm.end_customer_nsid AS end_ns_id,
      ufdm.customer_name AS name,
      ufdm.parent_customer_name AS parent_name,
      ufdm.end_customer_name AS end_name,
      ufdm.mcid AS mcid,
      ufdm.parent_master_customer_id AS parent_mcid,
      ufdm.end_master_customer_id AS end_mcid,
      ufdm.subsidiary_entity_name AS subsidiary_entity_name,
      ufdm.overage_flag AS overage_flag,
      ufdm.segment AS segment,
      ufdm.region AS region,
      ufdm.product_family AS product_family,
      ufdm.sku as sku,
      ufdm.baseline_currency AS base_currency,
      ufdm.fx_rate_ccfx AS cc_fx_rate,
      ufdm.ccfx_date AS fx_date,
      ufdm.arr_usd_ccfx AS arr,
      ufdm.baseline_arr_local_currency AS baseline_arr_local_currency,
      ufdm.max_modified_date AS dw_modified_date,
      ufdm.min_created_date AS dw_created_date,
      parent_sf_id,
      parent_sf_name
    from ufdm_fopti ufdm
  ),
  ufdm_2022 AS (
    SELECT snapshot_date,
      ultimate_parent_id AS ultimate_parent_id,
      ultimate_parent_name AS ultimate_parent_name,
      duns_name,
      duns_number,
      parent_duns_name,
      parent_duns_number,
      domesticultimatedunsnumber,
      globalultimatedunsnumber,
      new_product_solution,
      new_product_line,
      updated_product_group,
      new_product,
      new_line_of_business,
      new_line_of_business_sub_category,
      ufdm.c_name AS c_name,
      ufdm.parent_customer_nsid AS parent_ns_id,
      ufdm.end_customer_nsid AS end_ns_id,
      ufdm.customer_name AS name,
      ufdm.parent_customer_name AS parent_name,
      ufdm.end_customer_name AS end_name,
      ufdm.mcid AS mcid,
      ufdm.parent_master_customer_id AS parent_mcid,
      ufdm.end_master_customer_id AS end_mcid,
      ufdm.subsidiary_entity_name AS subsidiary_entity_name,
      ufdm.overage_flag AS overage_flag,
      ufdm.segment AS segment,
      ufdm.region AS region,
      ufdm.product_family AS product_family,
      ufdm.sku as sku,
      ufdm.baseline_currency AS base_currency,
      ufdm.fx_rate_ccfx AS cc_fx_rate,
      ufdm.ccfx_date AS fx_date,
      ufdm.arr_usd_ccfx AS arr,
      ufdm.baseline_arr_local_currency AS baseline_arr_local_currency,
      ufdm.max_modified_date AS dw_modified_date,
      ufdm.min_created_date AS dw_created_date,
      parent_sf_id,
      parent_sf_name
    FROM arr_agg ufdm
      left join ufdm_grey.sst_dates_lookup_manual c on ufdm.mcid = c.mcid
    WHERE 1 = 1
      and (
        (
          c.mcid is null
          and ufdm.snapshot_date > '2021-12-31'::DATE
        )
        or (
          c.mcid is not null
          and ufdm.snapshot_date >= c.date_for_switching_from_arr
        )
      )
  ),
  sst AS (
    SELECT *,
      'sst_tat' as record_source
    FROM sst_tat
    UNION ALL
    SELECT *,
      'ufdm_campaigns_dec2021' as record_source
    FROM ufdm_campaigns_dec2021
    UNION ALL
    SELECT *,
      'ufdm_fopti_pre_2022' as record_source
    FROM ufdm_fopti_pre_2022
    UNION ALL
    SELECT *,
      'ufdm_2022' as record_source
    FROM ufdm_2022
  )
  SELECT *
  FROM sst
);
-- deduping fopti items from SST table
DROP TABLE IF EXISTS fopti_dupes;
CREATE TEMP TABLE fopti_dupes as (
  WITH fopti AS (
    SELECT s.c_name,
      s.mcid,
      s.snapshot_date,
      s.product_family,
      s.sku,
      s.subsidiary_entity_name
    FROM sst_temp s
    WHERE s.subsidiary_entity_name ilike '%fopti%'
  ),
  non_fopti AS (
    SELECT s.c_name,
      s.mcid,
      s.snapshot_date,
      s.product_family,
      s.sku,
      s.subsidiary_entity_name
    FROM sst_temp s
    WHERE s.subsidiary_entity_name not ilike '%fopti%'
  )
  SELECT f.c_name,
    f.mcid,
    f.snapshot_date,
    f.product_family,
    f.sku,
    f.subsidiary_entity_name as f_sub,
    nf.subsidiary_entity_name nf_sub
  FROM fopti f
    INNER JOIN non_fopti nf ON f.c_name = nf.c_name
    AND f.mcid = nf.mcid
    AND f.snapshot_date = nf.snapshot_date
    AND f.product_family = nf.product_family
    AND f.sku = nf.sku
    AND f.subsidiary_entity_name ilike '%fopti%'
);
DELETE FROM sst_temp s USING fopti_dupes fd
WHERE s.c_name = fd.c_name
  AND s.mcid = fd.mcid
  AND s.snapshot_date = fd.snapshot_date
  AND s.product_family = fd.product_family
  AND s.sku = fd.sku
  AND s.subsidiary_entity_name = fd.f_sub;
/*
 SST CORRECTIONS
 */
DELETE FROM sst_temp s USING ufdm_grey.sst_corrections st
WHERE st.to_delete IS TRUE
  AND (
    st.match_cname IS DISTINCT
    FROM TRUE
      OR st.match_val_cname IS NOT DISTINCT
    FROM s.c_name
  )
  AND (
    st.match_parent_ns_id IS DISTINCT
    FROM TRUE
      OR st.match_val_parent_ns_id IS NOT DISTINCT
    FROM s.parent_ns_id
  )
  AND (
    st.match_product_family IS DISTINCT
    FROM TRUE
      OR st.match_val_product_family IS NOT DISTINCT
    FROM s.product_family
  )
  AND (
    st.match_snapshot_date IS DISTINCT
    FROM TRUE
      OR st.match_val_snapshot_date IS NOT DISTINCT
    FROM s.snapshot_date
  )
  AND (
    st.match_overage_flag IS DISTINCT
    FROM TRUE
      OR st.match_val_overage_flag IS NOT DISTINCT
    FROM s.overage_flag
  )
  AND (
    st.match_subsidiary_entity_name IS DISTINCT
    FROM TRUE
      OR st.match_val_subsidiary_entity_name IS NOT DISTINCT
    FROM s.subsidiary_entity_name
  )
  AND (
    st.match_base_currency IS DISTINCT
    FROM TRUE
      OR st.match_val_base_currency IS NOT DISTINCT
    FROM s.base_currency
  );
WITH tmp_update AS (
  SELECT COALESCE(st.c_name, s.c_name) AS c_name,
    COALESCE(st.parent_ns_id, s.parent_ns_id) AS parent_ns_id,
    COALESCE(st.end_ns_id, s.end_ns_id) AS end_ns_id,
    COALESCE(st."name", s."name") AS name,
    COALESCE(st.parent_name, s.parent_name) AS parent_name,
    COALESCE(st.end_name, s.end_name) AS end_name,
    COALESCE(
      COALESCE(nm1.mcid, st.end_mcid, s.end_mcid),
      COALESCE(nm.mcid, st.parent_mcid, s.parent_mcid)
    ) AS mcid,
    COALESCE(nm.mcid, st.parent_mcid, s.parent_mcid) AS parent_mcid,
    COALESCE(nm1.mcid, st.end_mcid, s.end_mcid) AS end_mcid,
    COALESCE(
      st.subsidiary_entity_name,
      s.subsidiary_entity_name
    ) AS subsidiary_entity_name,
    COALESCE(st.overage_flag, s.overage_flag) AS overage_flag,
    COALESCE(st.product_family, s.product_family) AS product_family,
    COALESCE(st.base_currency, s.base_currency) AS base_currency,
    COALESCE(st.cc_fx_rate, s.cc_fx_rate) AS cc_fx_rate,
    COALESCE(st.fx_date, s.fx_date) AS fx_date,
    COALESCE(st.arr, s.arr) AS arr,
    COALESCE(
      st.baseline_arr_local_currency,
      s.baseline_arr_local_currency
    ) AS baseline_arr_local_currency,
    dnb.db_name AS duns_name,
    dnb.duns_number AS duns_number,
    dnb.parent_db_name AS parent_duns_name,
    dnb.db_parentdunsnumber AS parent_duns_number,
    dnb.db_domesticultimatedunsnumber AS domesticultimatedunsnumber,
    dnb.db_globalultimatedunsnumber AS globalultimatedunsnumber,
    cd.segment AS segment,
    cd.region AS region,
    cd1.epi_universal_id AS parent_sf_id,
    cd1."name" AS parent_sf_name,
    st.match_cname,
    st.match_val_cname,
    st.match_parent_ns_id,
    st.match_val_parent_ns_id,
    st.match_product_family,
    st.match_val_product_family,
    st.match_snapshot_date,
    st.match_val_snapshot_date,
    st.match_overage_flag,
    st.match_val_overage_flag,
    st.match_subsidiary_entity_name,
    st.match_val_subsidiary_entity_name,
    st.match_base_currency,
    st.match_val_base_currency
  FROM sst_temp s
    JOIN ufdm_grey.sst_corrections st ON (
      st.to_merge IS TRUE
      AND (
        st.match_cname IS DISTINCT
        FROM TRUE
          OR st.match_val_cname IS NOT DISTINCT
        FROM s.c_name
      )
      AND (
        st.match_parent_ns_id IS DISTINCT
        FROM TRUE
          OR st.match_val_parent_ns_id IS NOT DISTINCT
        FROM s.parent_ns_id
      )
      AND (
        st.match_product_family IS DISTINCT
        FROM TRUE
          OR st.match_val_product_family IS NOT DISTINCT
        FROM s.product_family
      )
      AND (
        st.match_snapshot_date IS DISTINCT
        FROM TRUE
          OR st.match_val_snapshot_date IS NOT DISTINCT
        FROM s.snapshot_date
      )
      AND (
        st.match_overage_flag IS DISTINCT
        FROM TRUE
          OR st.match_val_overage_flag IS NOT DISTINCT
        FROM s.overage_flag
      )
      AND (
        st.match_subsidiary_entity_name IS DISTINCT
        FROM TRUE
          OR st.match_val_subsidiary_entity_name IS NOT DISTINCT
        FROM s.subsidiary_entity_name
      )
      AND (
        st.match_base_currency IS DISTINCT
        FROM TRUE
          OR st.match_val_base_currency IS NOT DISTINCT
        FROM s.base_currency
      )
    )
    LEFT JOIN ns_id_master_customer_id_map nm ON nm.ns_id = COALESCE(st.parent_ns_id, s.parent_ns_id)
    LEFT JOIN ns_id_master_customer_id_map nm1 ON nm1.ns_id = COALESCE(st.end_ns_id, s.end_ns_id)
    LEFT JOIN customer_detail cd ON cd.epi_universal_id = COALESCE(
      COALESCE(nm1.mcid, st.end_mcid, s.end_mcid),
      COALESCE(nm.mcid, st.parent_mcid, s.parent_mcid)
    )
    LEFT JOIN customer_detail cd1 ON cd1.epi_universal_id = COALESCE(nm.mcid, st.parent_mcid, s.parent_mcid)
    LEFT JOIN dnb_accounts dnb ON dnb.master_customer_id = COALESCE(
      COALESCE(nm1.mcid, st.end_mcid, s.end_mcid),
      COALESCE(nm.mcid, st.parent_mcid, s.parent_mcid)
    )
)
UPDATE sst_temp s
SET c_name = tmp.c_name,
  parent_ns_id = tmp.parent_ns_id,
  end_ns_id = tmp.end_ns_id,
  name = tmp."name",
  parent_name = tmp.parent_name,
  end_name = tmp.end_name,
  mcid = tmp.mcid,
  parent_mcid = tmp.parent_mcid,
  end_mcid = tmp.end_mcid,
  subsidiary_entity_name = tmp.subsidiary_entity_name,
  overage_flag = tmp.overage_flag,
  product_family = tmp.product_family,
  base_currency = tmp.base_currency,
  cc_fx_rate = tmp.cc_fx_rate,
  fx_date = tmp.fx_date,
  arr = tmp.arr,
  baseline_arr_local_currency = tmp.baseline_arr_local_currency,
  duns_name = tmp.duns_name,
  duns_number = tmp.duns_number,
  parent_duns_name = tmp.parent_duns_name,
  parent_duns_number = tmp.parent_duns_number,
  domesticultimatedunsnumber = tmp.domesticultimatedunsnumber,
  globalultimatedunsnumber = tmp.globalultimatedunsnumber,
  segment = tmp.segment,
  region = tmp.region,
  parent_sf_id = tmp.parent_sf_id,
  parent_sf_name = tmp.parent_sf_name
FROM tmp_update tmp
WHERE (
    tmp.match_cname IS DISTINCT
    FROM TRUE
      OR tmp.match_val_cname IS NOT DISTINCT
    FROM s.c_name
  )
  AND (
    tmp.match_parent_ns_id IS DISTINCT
    FROM TRUE
      OR tmp.match_val_parent_ns_id IS NOT DISTINCT
    FROM s.parent_ns_id
  )
  AND (
    tmp.match_product_family IS DISTINCT
    FROM TRUE
      OR tmp.match_val_product_family IS NOT DISTINCT
    FROM s.product_family
  )
  AND (
    tmp.match_snapshot_date IS DISTINCT
    FROM TRUE
      OR tmp.match_val_snapshot_date IS NOT DISTINCT
    FROM s.snapshot_date
  )
  AND (
    tmp.match_overage_flag IS DISTINCT
    FROM TRUE
      OR tmp.match_val_overage_flag IS NOT DISTINCT
    FROM s.overage_flag
  )
  AND (
    tmp.match_subsidiary_entity_name IS DISTINCT
    FROM TRUE
      OR tmp.match_val_subsidiary_entity_name IS NOT DISTINCT
    FROM s.subsidiary_entity_name
  )
  AND (
    tmp.match_base_currency IS DISTINCT
    FROM TRUE
      OR tmp.match_val_base_currency IS NOT DISTINCT
    FROM s.base_currency
  );
INSERT INTO sst_temp (
    snapshot_date,
    c_name,
    parent_ns_id,
    end_ns_id,
    "name",
    parent_name,
    end_name,
    mcid,
    parent_mcid,
    end_mcid,
    subsidiary_entity_name,
    overage_flag,
    product_family,
    base_currency,
    cc_fx_rate,
    fx_date,
    arr,
    baseline_arr_local_currency,
    duns_name,
    duns_number,
    parent_duns_name,
    parent_duns_number,
    domesticultimatedunsnumber,
    globalultimatedunsnumber,
    segment,
    region,
    parent_sf_id,
    parent_sf_name
  ) (
    SELECT st.snapshot_date,
      st.c_name,
      st.parent_ns_id,
      st.end_ns_id,
      st."name",
      st.parent_name,
      st.end_name,
      COALESCE(nm1.mcid, nm.mcid),
      nm.mcid,
      nm1.mcid,
      st.subsidiary_entity_name,
      st.overage_flag,
      st.product_family,
      st.base_currency,
      st.cc_fx_rate,
      st.fx_date,
      st.arr,
      st.baseline_arr_local_currency,
      dnb.db_name,
      dnb.duns_number,
      dnb.parent_db_name,
      dnb.db_parentdunsnumber,
      dnb.db_domesticultimatedunsnumber,
      dnb.db_globalultimatedunsnumber,
      cd.segment,
      cd.region,
      cd1.epi_universal_id,
      cd1."name"
    FROM ufdm_grey.sst_corrections st
      LEFT JOIN ns_id_master_customer_id_map nm ON nm.ns_id = st.parent_ns_id
      LEFT JOIN ns_id_master_customer_id_map nm1 ON nm1.ns_id = st.end_ns_id
      LEFT JOIN customer_detail cd ON cd.epi_universal_id = COALESCE(nm1.mcid, nm.mcid)
      LEFT JOIN customer_detail cd1 ON cd1.epi_universal_id = nm.mcid
      LEFT JOIN dnb_accounts dnb ON dnb.master_customer_id = COALESCE(nm1.mcid, nm.mcid)
    WHERE st.to_insert IS TRUE
  );
UPDATE sst_temp s
SET base_currency = sc."MISSING CURR CODE",
  cc_fx_rate = sc."LC AMOUNT RATE",
  baseline_arr_local_currency = s.arr / sc."LC AMOUNT RATE"
FROM ufdm_grey.sst_currency_corrections sc
WHERE sc."NS ID" = s.c_name
  AND sc.mcid = s.mcid
  AND (
    NULLIF(TRIM(s.base_currency), '') IS NULL
    OR s.baseline_arr_local_currency IS NULL
    OR s.cc_fx_rate IS NULL
  );
WITH base AS (
  SELECT snapshot_date,
    mcid
  FROM sst_temp
  GROUP BY snapshot_date,
    mcid
  HAVING sum(arr) < 50.0
)
UPDATE sst_temp st
SET arr = 0,
  baseline_arr_local_currency = 0
FROM base b
WHERE st.snapshot_date = b.snapshot_date
  AND st.mcid = b.mcid;
-------------------- Adding logic for sst region and segment -------
---changing region for only null regions
Update sst_temp
set region = (
    Case
      when subsidiary_entity_name in ('Episerver AB', 'Opti AB', 'Optimizely AB') then 'EMEA'
      when subsidiary_entity_name = 'Episerver GmbH' then 'EMEA'
      when subsidiary_entity_name = 'Episerver Inc.' then 'North America'
      when subsidiary_entity_name in ('Episerver UK Ltd (formerly Peerius Ltd)') then 'EMEA'
      when subsidiary_entity_name = 'Idio Inc' then 'EMEA'
      when subsidiary_entity_name = 'Idio Inc.' then 'EMEA'
      when subsidiary_entity_name = 'Idio Ltd' then 'EMEA'
      when subsidiary_entity_name in(
        'Insite Hosting Services',
        'Opti NA (2)',
        'Optimizely North America Inc (2)'
      ) then 'EMEA'
      when subsidiary_entity_name in(
        'Insite Inc',
        'Opti NA (4)',
        'Optimizely North America Inc (4)'
      ) then 'North America'
      when subsidiary_entity_name in(
        'Optimizely Inc',
        'Fopti',
        'FOpti',
        'Optimizely Operations Inc',
        'Opti NA (3)',
        'Optimizely North America Inc (3)'
      ) then 'North America'
      when subsidiary_entity_name in(
        'Welcome',
        'Welcome Inc.',
        'Opti NA (6)',
        'Optimizely North America Inc (6)'
      ) then 'North America'
      when subsidiary_entity_name in(
        'Zaius',
        'Opti NA (5)',
        'Optimizely North America Inc (5)'
      ) then 'North America'
      when subsidiary_entity_name in('Optimizely North America Inc') then 'North America'
    end
  )
where (
    region is null
    or region = ''
    or region = 'NA-DS'
    or region = 'Unassigned'
  );
---changing segments ----
with avg_total_customer_average as (
  select mcid,
    avg(arr) as average_arr
  from sst_temp
  where mcid is not null
    and mcid <> '-'
    and mcid <> ''
    and snapshot_date >= '2019-01-01'
  group by 1
)
update sst_temp as a
set segment = (
    case
      when b.average_arr >= 100000 then 'Enterprise'
      else 'Mid-Market'
    end
  )
from avg_total_customer_average as b
where a.mcid = b.mcid;
drop table if exists ryzlan.sku_sst;
create table ryzlan.sku_sst (
  snapshot_date date,
  ultimate_parent_id text,
  ultimate_parent_name text,
  duns_name text,
  duns_number text,
  parent_duns_name text,
  parent_duns_number text,
  domesticultimatedunsnumber text,
  globalultimatedunsnumber text,
  new_product_solution text,
  new_product_line text,
  updated_product_group text,
  new_product text,
  new_line_of_business text,
  new_line_of_business_sub_category text,
  c_name text,
  parent_ns_id text,
  end_ns_id text,
  name text,
  parent_name text,
  end_name text,
  mcid text,
  parent_mcid text,
  end_mcid text,
  subsidiary_entity_name text,
  overage_flag text,
  segment text,
  region text,
  product_family text,
  sku TEXT,
  base_currency text,
  cc_fx_rate double precision,
  fx_date date,
  arr double precision,
  baseline_arr_local_currency double precision,
  dw_modified_date timestamp,
  dw_created_date timestamp default CURRENT_TIMESTAMP,
  parent_sf_id varchar,
  parent_sf_name varchar,
  record_source text,
  modified_comments text,
  cohort_actions text,
  id serial primary key
);
--create index nci_sst_prod_snapshot_date on sandbox_pd.sst_prod (snapshot_date);
--create index nci_sst_prod_snapshot_date_mcid_pf on sandbox_pd.sst_prod (snapshot_date, mcid, product_family);
--create index nci_sst_prod_snapshot_date_mcid_pg on sandbox_pd.sst_prod (snapshot_date, mcid, updated_product_group);
--create index nci_sst_prod_snapshot_date_mcid_ps on sandbox_pd.sst_prod (snapshot_date, mcid, new_product_solution);
--create index nci_sst_prod_snapshot_date_mcid_pg_bc on sandbox_pd.sst_prod (
--  snapshot_date,
--  mcid,
--  updated_product_group,
--  base_currency
--);
--create index nci_sst_prod_snapshot_date_mcid_ps_bc on sandbox_pd.sst_prod (
--  snapshot_date,
--  mcid,
--  new_product_solution,
--  base_currency
--);
--delete specific snapshot or all snapshots
delete from ryzlan.sku_sst
where (
    snapshot_date = var_date
    and var_date is not null
  )
  or var_date is null;
insert into ryzlan.sku_sst (
    snapshot_date,
    ultimate_parent_id,
    ultimate_parent_name,
    duns_name,
    duns_number,
    parent_duns_name,
    parent_duns_number,
    domesticultimatedunsnumber,
    globalultimatedunsnumber,
    new_product_solution,
    new_product_line,
    updated_product_group,
    new_product,
    new_line_of_business,
    new_line_of_business_sub_category,
    c_name,
    parent_ns_id,
    end_ns_id,
    name,
    parent_name,
    end_name,
    mcid,
    parent_mcid,
    end_mcid,
    subsidiary_entity_name,
    overage_flag,
    segment,
    region,
    product_family,
    sku,
    base_currency,
    cc_fx_rate,
    fx_date,
    arr,
    baseline_arr_local_currency,
    dw_modified_date,
    dw_created_date,
    parent_sf_id,
    parent_sf_name,
    record_source
  )
select snapshot_date,
  ultimate_parent_id,
  ultimate_parent_name,
  duns_name,
  duns_number,
  parent_duns_name,
  parent_duns_number,
  domesticultimatedunsnumber,
  globalultimatedunsnumber,
  new_product_solution,
  new_product_line,
  updated_product_group,
  new_product,
  new_line_of_business,
  new_line_of_business_sub_category,
  c_name,
  parent_ns_id,
  end_ns_id,
  name,
  parent_name,
  end_name,
  mcid,
  parent_mcid,
  end_mcid,
  subsidiary_entity_name,
  overage_flag,
  segment,
  region,
  product_family,
  sku,
  base_currency,
  cc_fx_rate,
  fx_date,
  arr,
  baseline_arr_local_currency,
  dw_modified_date,
  current_timestamp as dw_created_date,
  parent_sf_id,
  parent_sf_name,
  record_source
from sst_temp
where (
    snapshot_date = var_date
    and var_date is not null
  )
  or var_date is null;
END;
$function$;