CREATE OR REPLACE FUNCTION public.sp_ufdm_arr_epi_subs('2024-01-31' date) RETURNS void LANGUAGE plpgsql AS $function$
DECLARE var_last_refresh TIMESTAMP WITHOUT TIME ZONE := NOW();
BEGIN --------------------------------------------------------------------------------
-- CHECK VAR_DATE FIRST THING
--------------------------------------------------------------------------------
IF '2024-01-31' IS NULL THEN '2024-01-31' := (
  DATE_TRUNC('month', NOW()::DATE) + interval '0 month' - interval '1 day'
)::DATE;
END IF;
SELECT DISTINCT snapshot_date
FROM ufdm_grey.arr_epi_subs;
RAISE NOTICE '';
RAISE NOTICE '';
RAISE NOTICE 'EXECUTING FOR DATE: %',
'2024-01-31';
RAISE NOTICE 'CLEANING UP OLD DATA...';
TRUNCATE TABLE ufdm_grey.arr_epi_subs;
DROP TABLE IF EXISTS tmp_epi_arrsub;
DROP TABLE IF EXISTS tmp_epi_arrsub_final;
DROP TABLE IF EXISTS tmp_epi_arrsub_term_memos;
--------------------------------------------------------------------------------
RAISE NOTICE 'EPISERVER SUBSCRIPTION DATA - MONTH OVER MONTH';
PERFORM func_ufdm_audit_logging (
  'sp_ufdm_arr_epi_subs',
  'EPISERVER SUBSCRIPTION DATA - MONTH OVER MONTH: ' || '2024-01-31'
);
--------------------------------------------------------------------------------
CREATE TEMPORARY TABLE tmp_epi_arrsub AS (
  SELECT DISTINCT c."name" as c_name,
    c.full_name as c_full_name,
    e."name" || ' ' || e.companyname AS "end_customer",
    c.master_customer_id,
    e.master_customer_id AS "end_customer_master_customer_id",
    esa.id AS salesforce_account_id,
    esb.id AS "end_customer_sfid",
    ba.billing_account_name,
    ua.region,
    i."name" AS "i_name",
    x.name as sku_name,
    x.product_category_c as product_name,
    x.netsuite_product_family_c as product_family,
    x.product_group_c as product_group,
    COALESCE(bs.advance_renewal_period_number, 0) AS "advance_renewal_period_number",
    COALESCE(bs.advance_renewal_period_unit_id, '') AS "advance_renewal_period_unit_id",
    bs.billing_account_id,
    bs.default_renewal_method_id,
    bs."name" AS "bs_name",
    bs.subscription_id AS "bs_subscription_id",
    bs.subscription_number AS "bs_subscription_num",
    bsl.subline_number AS "bsl_subline_number",
    bs.currency,
    func_datediff_pg(
      'month',
      COALESCE(
        slpi.date_start_inclusive::date,
        bsl.date_start::date
      ),
      COALESCE(
        slpi.date_end_exclusive::date,
        bsl.date_end::date
      ),
      0
    ) AS "term_months",
    func_datediff_pg(
      'month',
      COALESCE(
        slpi.date_start_inclusive::date,
        bsl.date_start::date
      ),
      COALESCE(
        slpi.date_end_exclusive::date,
        bsl.date_end::date
      ),
      0
    ) AS "term_months_when_terminated",
    --          EXTRACT(YEAR FROM age(COALESCE( slpi.date_end_exclusive , bsl.date_end ) , COALESCE( slpi.date_start_inclusive , bsl.date_start ))) * 12 +
    --          EXTRACT(MONTH FROM age(COALESCE( slpi.date_end_exclusive , bsl.date_end ) , COALESCE( slpi.date_start_inclusive , bsl.date_start ))) +
    --          CASE WHEN EXTRACT(DAYS FROM age(COALESCE( slpi.date_end_exclusive , bsl.date_end ) , COALESCE( slpi.date_start_inclusive , bsl.date_start ))) > 20 THEN 1 ELSE 0 END
    --          AS "term_months" ,
    --          EXTRACT(YEAR FROM age(COALESCE( slpi.date_end_exclusive , bsl.date_end ) , COALESCE( slpi.date_start_inclusive , bsl.date_start ))) * 12 +
    --          EXTRACT(MONTH FROM age(COALESCE( slpi.date_end_exclusive , bsl.date_end ) , COALESCE( slpi.date_start_inclusive , bsl.date_start ))) +
    --          CASE WHEN EXTRACT(DAYS FROM age(COALESCE( slpi.date_end_exclusive , bsl.date_end ) , COALESCE( slpi.date_start_inclusive , bsl.date_start ))) > 20 THEN 1 ELSE 0 END
    --          AS "term_months_when_terminated" ,
    s."name" AS "subsidiary_entity_name",
    COALESCE(bsl.period_amount, 0) AS "bsl_period_amount",
    COALESCE(bsl.recurring_amount, 0) AS "bsl_recurring_amount",
    COALESCE(slpi.total_interval_value, bsl.total, 0) AS "total_interval_value",
    slpi.charge_frequency_id AS "charge_frequency",
    slpi.date_start_inclusive,
    slpi.date_end_exclusive,
    '*************' AS "sep1",
    CONCAT(
      slpi.charge_frequency_id,
      ' - ',
      slpi.repeat_every
    ) AS "bsd_name",
    NULL AS "bsd_repeat_every",
    '*************' AS "sep2",
    bsl.*
  FROM epi_netsuite.billing_subscription_lines bsl
    LEFT JOIN epi_netsuite.billing_subscriptions bs ON bs.subscription_id = bsl.subscription_id
    AND bs."_fivetran_deleted" IS DISTINCT
  FROM TRUE
    LEFT JOIN epi_netsuite.subscript_line_price_intervals slpi ON slpi.subscription_line_id = bsl.subline_id
    AND slpi."_fivetran_deleted" IS DISTINCT
  FROM TRUE
    LEFT JOIN epi_netsuite.subsidiaries s ON s.subsidiary_id = bs.subsidiary_id
    AND s."_fivetran_deleted" IS DISTINCT
  FROM TRUE
    LEFT JOIN epi_netsuite.currencies cur ON bs.currency = cur.name
    AND cur._fivetran_deleted IS DISTINCT
  FROM TRUE
    LEFT JOIN epi_netsuite.billing_accounts ba ON ba.billing_account_id = bs.billing_account_id
    AND ba."_fivetran_deleted" IS DISTINCT
  FROM TRUE
    LEFT JOIN epi_netsuite.billing_schedule_descriptions bsd ON bsd.billing_schedule_id = ba.billing_schedule_id
    AND bsd."_fivetran_deleted" IS DISTINCT
  FROM TRUE
    LEFT JOIN epi_netsuite.companies c ON c.company_id = ba.customer_id
    AND c._fivetran_deleted IS DISTINCT
  FROM TRUE
    LEFT JOIN epi_netsuite.companies e ON e.company_id = bs.end_customer_id
    AND e."_fivetran_deleted" IS DISTINCT
  FROM TRUE
    LEFT JOIN epi_netsuite.items i ON i.item_id = bsl.item_id
    and i._fivetran_deleted IS DISTINCT
  FROM TRUE
    LEFT JOIN (
      SELECT *,
        ROW_NUMBER() over(
          PARTITION BY COALESCE(aa.dynamics_id_c, aa.sf_guid_c)
        ) AS "row_ranking"
      FROM epi_salesforce.account aa
    ) esa on COALESCE(esa.dynamics_id_c, esa.sf_guid_c) = c.master_customer_id
    and esa.is_deleted IS DISTINCT
  FROM TRUE
    AND esa.row_ranking = 1
    LEFT JOIN (
      SELECT *,
        ROW_NUMBER() over(
          PARTITION BY COALESCE(bb.dynamics_id_c, bb.sf_guid_c)
        ) AS "row_ranking"
      FROM epi_salesforce.account bb
    ) esb ON coalesce(esb.dynamics_id_c, esb.sf_guid_c) = e.master_customer_id
    AND esb.is_deleted IS DISTINCT
  FROM TRUE
    AND esb.row_ranking = 1
    LEFT JOIN ufdm.account ua on ua.id = esa.id
    LEFT JOIN (
      SELECT *,
        ROW_NUMBER() over(
          PARTITION BY p2.product_code
          ORDER BY (
              p2.is_active::int + (not p2.is_deleted)::int + (
                p2.don_t_sync_with_net_suite_c is distinct
                from true
              )::int
            ) desc
        ) AS "row_ranking"
      FROM opti_salesforce.product_2 p2
    ) x ON x.product_code = i."name"
    AND x.row_ranking = 1
  WHERE 1 = 1
    AND bsl."_fivetran_deleted" IS DISTINCT
  FROM TRUE
    AND (
      bsl.date_start::date <= '2024-01-31'
      AND bsl.date_end::date >= '2024-01-31'
      AND COALESCE(bsl.date_termination::date, '2099-12-31') >= '2024-01-31'
    )
    AND bsl.status_id IN (
      'CLOSED',
      'ACTIVE',
      'TERMINATED',
      'PENDING_ACTIVATION',
      'SUSPENDED'
    )
    AND (
      slpi.date_start_inclusive::date <= '2024-01-31'
      AND slpi.date_end_exclusive::date >= '2024-01-31'
      AND slpi.status_id = 'ACTIVE'
    )
);
--------------------------------------------------------------------------------
-- LOGIC TO EXCLUDE HISTORICAL GMBH DATA
--------------------------------------------------------------------------------
DELETE FROM tmp_epi_arrsub
WHERE subsidiary_entity_name ILIKE '%gmbh%'
  AND (
    --      (DATE_TRUNC('month' , '2021-06-02'::DATE) + interval '1 month' - interval '1 day')::DATE <=
    '2024-01-31' <= (
      SELECT max(snapshot_date)
      FROM ufdm_blue.gmbh_historical_data
    )
  );
--------------------------------------------------------------------------------
RAISE NOTICE 'ADDRESSES REVISIONS TO ORIGINAL SALES ORDERS';
PERFORM func_ufdm_audit_logging (
  'sp_ufdm_arr_epi_subs',
  'ADDRESSES REVISIONS TO ORIGINAL SALES ORDERS'
);
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS tmp_epi_arrsub_revs;
CREATE TEMPORARY TABLE tmp_epi_arrsub_revs AS (
  SELECT *
  FROM (
      SELECT ROW_NUMBER() OVER(
          PARTITION BY slr.subscription_line_id
          ORDER BY slr.subscription_revision DESC,
            fivetran_index DESC
        ) AS "rownum",
        sco.action_id AS "sco_action_id",
        slr.subscription_revision AS "slr_subscription_revision",
        sco.change_order_number AS "sco_change_order_number",
        slr.subscription_line_id AS "slr_subscription_line_id",
        CASE
          WHEN sco.requester_id = '-4.0'
          AND sco.action_id = 'MODIFY_PRICING'
          AND sco.status_id = 'ACTIVE' THEN 'cpi'
          ELSE 'not_cpi'
        END AS cpi_flag,
        slr.*
      FROM epi_netsuite.subscription_line_revisions slr
        LEFT JOIN epi_netsuite.subscription_change_orders sco ON sco.change_order_id = slr.change_order_id
        AND sco."_fivetran_deleted" IS DISTINCT
      FROM TRUE
        LEFT JOIN tmp_epi_arrsub tea ON tea.subline_id = slr.subscription_line_id
      WHERE 1 = 1
        AND slr."_fivetran_deleted" IS DISTINCT
      FROM TRUE
        AND sco.action_id IN ('MODIFY_PRICING', 'TERMINATE')
        AND sco.approval_status_id = 'APPROVED'
        AND sco.status_id <> 'VOIDED'
        AND slr.date_change_order_effective <= '2024-01-31'
    ) tear
  WHERE tear.rownum = 1
);
CREATE TEMPORARY TABLE tmp_epi_arrsub_final AS (
  SELECT t.*,
    r.recurring_amount AS "rev_recurring_amount",
    r.slr_subscription_revision,
    r.sco_change_order_number,
    r.cpi_flag
  FROM tmp_epi_arrsub t
    LEFT JOIN tmp_epi_arrsub_revs r ON r.subscription_line_id = t.subline_id
);
--------------------------------------------------------------------------------
-- ONLY REVIEW TERM'D SUBSCRIPTIONS TERM DATE IS IRRELAVENT FOR THIS QUESTION, 
-- WE ARE ONLY LOOKING FOR MATCHES AGAINST THE SPECIFIC SUBSCRIPTION
--------------------------------------------------------------------------------
RAISE NOTICE 'FIND MEMO DATA ASSOCIATED WITH CHANGE ORDERS';
PERFORM func_ufdm_audit_logging (
  'sp_ufdm_arr_epi_subs',
  'FIND MEMO DATA ASSOCIATED WITH CHANGE ORDERS'
);
--------------------------------------------------------------------------------
CREATE TEMPORARY TABLE tmp_epi_arrsub_term_memos AS (
  SELECT *
  FROM (
      SELECT ROW_NUMBER() OVER(
          PARTITION BY slr.subscription_line_id
          ORDER BY slr.subscription_revision DESC,
            fivetran_index DESC
        ) AS "rownum",
        sco.action_id AS "sco_action_id",
        slr.subscription_revision AS "slr_subscription_revision",
        sco.change_order_number AS "sco_change_order_number",
        slr.subscription_line_id AS "slr_subscription_line_id",
        sco.date_effective AS "termination_effective_date",
        regexp_replace(sco.memo, E'[\r\n\t]', ' ', 'g') AS "sco_memo",
        sco.modification_type AS "sco_modification_type"
      FROM epi_netsuite.subscription_line_revisions slr
        LEFT JOIN epi_netsuite.subscription_change_orders sco ON sco.change_order_id = slr.change_order_id
        AND sco."_fivetran_deleted" IS DISTINCT
      FROM TRUE
        LEFT JOIN tmp_epi_arrsub tea ON tea.subline_id = slr.subscription_line_id
      WHERE 1 = 1
        AND slr."_fivetran_deleted" IS DISTINCT
      FROM TRUE
        AND sco.action_id IN ('TERMINATE')
        AND sco.approval_status_id = 'APPROVED'
        AND sco.status_id <> 'VOIDED'
    ) teatm
  WHERE teatm.rownum = 1
);
--------------------------------------------------------------------------------
RAISE NOTICE 'INSERT FINAL DATA INTO GREY LAYER: ARR_EPI_SUBS...';
PERFORM func_ufdm_audit_logging (
  'sp_ufdm_arr_epi_subs',
  'INSERT FINAL DATA INTO GREY LAYER: ARR_EPI_SUBS...'
);
--------------------------------------------------------------------------------
INSERT INTO ufdm_grey.arr_epi_subs
SELECT '2024-01-31' AS "snapshot_date",
  t.c_name,
  t.c_full_name AS "parent_customer",
  t.end_customer,
  t.master_customer_id,
  t.end_customer_master_customer_id,
  t.salesforce_account_id,
  t.end_customer_sfid,
  t.line_type,
  t.currency AS "baseline_currency",
  NULL AS "subsidiary_base_currency",
  t.total_interval_value,
  t.charge_frequency,
  COALESCE(t.rev_recurring_amount, t.recurring_amount) AS "recurring_amount",
  CASE
    WHEN t.date_termination IS NOT NULL THEN (
      t.total_interval_value / CASE
        WHEN t.term_months_when_terminated > 0 THEN t.term_months_when_terminated
        ELSE 1
      END
    )
    ELSE (
      t.total_interval_value / CASE
        WHEN t.term_months > 0 THEN t.term_months
        ELSE 1
      END
    )
  END AS "baseline_mrr_local_currency",
  CASE
    WHEN t.date_termination IS NOT NULL THEN (
      t.total_interval_value / CASE
        WHEN t.term_months_when_terminated > 0 THEN t.term_months_when_terminated
        ELSE 1
      END
    )
    ELSE (
      t.total_interval_value / CASE
        WHEN t.term_months > 0 THEN t.term_months
        ELSE 1
      END
    )
  END * 12 AS "baseline_arr_local_currency",
  -- FX DATES....
  ccfx.fx_date AS "ccfx_date",
  qefx.fx_date AS "mefx_date",
  ccfx.fx_rate AS "fx_rate_ccfx",
  CASE
    WHEN t.date_termination IS NOT NULL THEN (
      t.total_interval_value / CASE
        WHEN t.term_months_when_terminated > 0 THEN t.term_months_when_terminated
        ELSE 1
      END
    ) * ccfx.fx_rate
    ELSE (
      t.total_interval_value / CASE
        WHEN t.term_months > 0 THEN t.term_months
        ELSE 1
      END
    ) * ccfx.fx_rate
  END AS "mrr_usd_ccfx",
  CASE
    WHEN t.date_termination IS NOT NULL THEN (
      t.total_interval_value / CASE
        WHEN t.term_months_when_terminated > 0 THEN t.term_months_when_terminated
        ELSE 1
      END
    ) * ccfx.fx_rate
    ELSE (
      t.total_interval_value / CASE
        WHEN t.term_months > 0 THEN t.term_months
        ELSE 1
      END
    ) * ccfx.fx_rate
  END * 12 AS "arr_usd_ccfx",
  qefx.fx_rate,
  -- fx_rate_mefx
  CASE
    WHEN t.date_termination IS NOT NULL THEN (
      t.total_interval_value / CASE
        WHEN t.term_months_when_terminated > 0 THEN t.term_months_when_terminated
        ELSE 1
      END
    ) * qefx.fx_rate
    ELSE (
      t.total_interval_value / CASE
        WHEN t.term_months > 0 THEN t.term_months
        ELSE 1
      END
    ) * qefx.fx_rate
  END AS "mrr_usd_mefx",
  CASE
    WHEN t.date_termination IS NOT NULL THEN (
      t.total_interval_value / CASE
        WHEN t.term_months_when_terminated > 0 THEN t.term_months_when_terminated
        ELSE 1
      END
    ) * qefx.fx_rate
    ELSE (
      t.total_interval_value / CASE
        WHEN t.term_months > 0 THEN t.term_months
        ELSE 1
      END
    ) * qefx.fx_rate
  END * 12 AS "arr_usd_mefx",
  mefx.fx_rate,
  -- fx_rate_actualfx
  CASE
    WHEN t.date_termination IS NOT NULL THEN (
      t.total_interval_value / CASE
        WHEN t.term_months_when_terminated > 0 THEN t.term_months_when_terminated
        ELSE 1
      END
    ) * mefx.fx_rate
    ELSE (
      t.total_interval_value / CASE
        WHEN t.term_months > 0 THEN t.term_months
        ELSE 1
      END
    ) * mefx.fx_rate
  END AS "mrr_usd_actualfx",
  CASE
    WHEN t.date_termination IS NOT NULL THEN (
      t.total_interval_value / CASE
        WHEN t.term_months_when_terminated > 0 THEN t.term_months_when_terminated
        ELSE 1
      END
    ) * mefx.fx_rate
    ELSE (
      t.total_interval_value / CASE
        WHEN t.term_months > 0 THEN t.term_months
        ELSE 1
      END
    ) * mefx.fx_rate
  END * 12 AS "arr_usd_actualfx",
  --------------------------------------------------------------------------------
  CASE
    WHEN t.date_termination IS NOT NULL THEN (
      t.total_interval_value / CASE
        WHEN t.term_months_when_terminated > 0 THEN t.term_months_when_terminated
        ELSE 1
      END
    ) * mefx.fx_rate
    ELSE (
      t.total_interval_value / CASE
        WHEN t.term_months > 0 THEN t.term_months
        ELSE 1
      END
    ) * mefx.fx_rate
  END AS "mrr_usd_suggested",
  CASE
    WHEN t.date_termination IS NOT NULL THEN (
      t.total_interval_value / CASE
        WHEN t.term_months_when_terminated > 0 THEN t.term_months_when_terminated
        ELSE 1
      END
    ) * 12 * mefx.fx_rate
    ELSE (
      t.total_interval_value / CASE
        WHEN t.term_months > 0 THEN t.term_months
        ELSE 1
      END
    ) * 12 * mefx.fx_rate
  END AS "arr_usd_suggested",
  --------------------------------------------------------------------------------
  t.bsd_name AS "bill_freq",
  t.bsd_repeat_every AS "repeat_every",
  CASE
    WHEN t.term_months > 0 THEN t.term_months
    ELSE 1
  END AS "term_months",
  t.term_months_when_terminated,
  COALESCE(t.date_start_inclusive, t.date_start)::date AS "date_start",
  COALESCE(t.date_end_exclusive, t.date_end)::date AS "date_end",
  t.date_termination,
  t.subline_id,
  t.bs_subscription_num AS "reference_number",
  t.bsl_subline_number::varchar AS "line_number",
  t.slr_subscription_revision AS "revision_number",
  t.sco_change_order_number AS "change_order",
  t.status_id AS "status",
  t.catalog_type AS "catalog_type",
  t.i_name AS "sku",
  t.sku_name,
  t.product_name,
  t.product_group,
  t.product_family,
  t.cpi_flag,
  'subs' AS "arr_source",
  tm.sco_action_id,
  tm.sco_memo,
  tm.sco_modification_type,
  CASE
    WHEN t.subsidiary_entity_name ILIKE '%gmbh%' THEN 'Episerver GmbH'
    ELSE t.subsidiary_entity_name
  END AS "subsidiary_entity_name"
FROM tmp_epi_arrsub_final t
  LEFT JOIN ufdm_grey.arr_fx_rates ccfx ON ccfx.trans_cur = t.currency
  AND ccfx.fx_type = 'ccfx'
  LEFT JOIN ufdm_grey.arr_fx_rates mefx ON mefx.trans_cur = t.currency
  AND mefx.fx_type = 'mefx'
  LEFT JOIN ufdm_grey.arr_fx_rates qefx ON qefx.trans_cur = t.currency
  AND qefx.fx_type = 'qefx'
  LEFT JOIN tmp_epi_arrsub_term_memos tm ON tm.slr_subscription_line_id = t.subline_id;
--------------------------------------------------------------------------------
RAISE NOTICE 'POPULATE GMBH OVERAGES...';
PERFORM func_ufdm_audit_logging (
  'sp_ufdm_arr_epi_subs',
  'POPULATE GMBH OVERAGES...'
);
--------------------------------------------------------------------------------
-----this function contains the full logic to include GMBH overages---
PERFORM public.populate_gmbh_overages('2024-01-31');
--------------------------------------------------------------------------------
RAISE NOTICE 'ARR UPDATES WHEN DATES NOT MATCH...';
PERFORM func_ufdm_audit_logging (
  'sp_ufdm_arr_epi_subs',
  'ARR UPDATES WHEN DATES NOT MATCH...'
);
--------------------------------------------------------------------------------
UPDATE ufdm_grey.arr_epi_subs AES
SET baseline_arr_local_currency = (
    total_interval_amount / (date_end::date - date_start::date)
  ) * 365,
  arr_usd_ccfx = (
    total_interval_amount / (date_end::date - date_start::date)
  ) * 365 * fx_rate_ccfx,
  arr_usd_mefx = (
    total_interval_amount / (date_end::date - date_start::date)
  ) * 365 * fx_rate_mefx -- this is previous quarter end  
,
  arr_usd_actualfx = (
    total_interval_amount / (date_end::date - date_start::date)
  ) * 365 * fx_rate_actualfx -- this is previous month end
,
  arr_usd_suggested = (
    total_interval_amount / (date_end::date - date_start::date)
  ) * 365 * fx_rate_actualfx -- this is previous month end
,
  baseline_mrr_local_currency = (
    (
      total_interval_amount / (date_end::date - date_start::date)
    ) * 365
  ) / 12,
  mrr_usd_ccfx = (
    (
      (
        total_interval_amount / (date_end::date - date_start::date)
      ) * 365
    ) / 12
  ) * fx_rate_ccfx,
  mrr_usd_mefx = (
    (
      (
        total_interval_amount / (date_end::date - date_start::date)
      ) * 365
    ) / 12
  ) * fx_rate_mefx --this is previous quarter end         
,
  mrr_usd_actualfx = (
    (
      (
        total_interval_amount / (date_end::date - date_start::date)
      ) * 365
    ) / 12
  ) * fx_rate_actualfx -- this is previous month end
,
  mrr_usd_suggested = (
    (
      (
        total_interval_amount / (date_end::date - date_start::date)
      ) * 365
    ) / 12
  ) * fx_rate_actualfx -- this is previous month end              
WHERE date_part('DAY', date_start) <> date_part('DAY', date_end)
  AND lower(line_type) <> 'one time';
INSERT INTO ufdm_grey.arr_epi_subs
SELECT *
FROM ufdm_grey.gmbh_overages
WHERE snapshot_date = '2024-01-31';
--------------------------------------------------------------------------------
RAISE NOTICE 'Update Productname and group where it contains numbers ...';
PERFORM func_ufdm_audit_logging (
  'sp_ufdm_arr_epi_subs',
  'ARR Product Name/Group Updates...'
);
--------------------------------------------------------------------------------
UPDATE ufdm_grey.arr_epi_subs AES
SET product_name = product_category_name
FROM ufdm_blue.Product_Category_Static pcs
WHERE AES.product_name = pcs.product_category_code;
UPDATE ufdm_grey.arr_epi_subs AES
SET product_group = pgs.product_group_name
FROM ufdm_blue.Product_Group_Static pgs
WHERE AES.product_group = pgs.product_group_code;
--------------------------------------------------------------------------------
RAISE NOTICE 'CLEAN UP ERRORS FROM MEMOS...';
PERFORM func_ufdm_audit_logging (
  'sp_ufdm_arr_epi_subs',
  'CLEAN UP ERRORS FROM MEMOS...'
);
--------------------------------------------------------------------------------
DELETE FROM ufdm_grey.arr_epi_subs
WHERE sco_memo ILIKE '%ERROR%';
DROP TABLE IF EXISTS tmp_epi_arrsub;
DROP TABLE IF EXISTS tmp_epi_arrsub_final;
DROP TABLE IF EXISTS tmp_epi_arrsub_term_memos;
--------------------------------------------------------------------------------
RAISE NOTICE 'ARR_EPI_SUBS...FINISHED.';
PERFORM func_ufdm_audit_logging (
  'sp_ufdm_arr_epi_subs',
  'ARR_EPI_SUBS...FINISHED'
);
--------------------------------------------------------------------------------
EXCEPTION
WHEN SQLSTATE 'P0000' THEN RAISE NOTICE 'ARR_EPI_SUBS...FINISHED.';
WHEN OTHERS THEN RAISE NOTICE 'ERROR %',
SQLSTATE;
PERFORM func_ufdm_audit_logging ('sp_ufdm_arr_epi_subs', 'ERROR: ' || SQLSTATE);
END;
$function$;