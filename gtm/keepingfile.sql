{ % snapshot gtm_opportunity_product_snapshot % } { { config(
  unique_key = 'OPPORTUNITY_ID',
  strategy = 'timestamp',
  updated_at = 'SNAPSHOT_LOAD_DATE',
  target_schema = generate_schema_name('gtm_analytics'),
) } } WITH source_data AS (
  SELECT opportunity_line_item.opportunity_id,
    opportunity_line_item.currency_iso_code,
    opportunity_line_item.product_2_id,
    opportunity_line_item.product_of_interest_c,
    opportunity_line_item.name AS "Opportunity Product Name",
    opportunity_line_item.quantity,
    product_2.name AS "Product Name",
    product_2.family AS "Product FAMILY",
    opportunity_line_item.product_code,
    opportunity_line_item.total_price,
    opportunity_line_item.unit_price,
    opportunity_line_item.list_price,
    COALESCE(
      opportunity_line_item.historical_current_subscription_software_c,
      0
    ) AS "HISTORICAL CURRENT SUBSCRIPTION SOFTWARE",
    COALESCE(qoute_line_c.sbqq_net_price_c, 0) AS "SBQQ_NET_PRICE_C",
    TO_NUMBER(
      CASE
        WHEN opportunity_line_item.subscription_term_calculated_c >= 12
        AND opportunity_line_item.exclude_from_1_st_year_mrr_c = 'False' THEN (
          (
            opportunity_line_item.total_price / opportunity_line_item.subscription_term_calculated_c
          ) * 12
        )
        WHEN opportunity_line_item.subscription_term_calculated_c >= 12
        AND opportunity_line_item.exclude_from_1_st_year_mrr_c = 'True' THEN 0
        ELSE opportunity_line_item.total_price
      END
    ) AS "Annual Price",
    CURRENT_TIMESTAMP() AS snapshot_load_date,
    ROW_NUMBER() OVER (
      PARTITION BY opportunity_line_item.opportunity_id,
      opportunity_line_item.product_code
      ORDER BY opportunity_line_item.created_date DESC
    ) AS rn
  FROM { { source("salesforce", "OPPORTUNITY_LINE_ITEM") } } AS opportunity_line_item
    INNER JOIN { { source("salesforce", "PRODUCT_2") } } AS product_2 ON opportunity_line_item.product_2_id = product_2.id
    LEFT JOIN { { source("salesforce", "SBQQ_QUOTE_LINE_C") } } AS qoute_line_c ON opportunity_line_item.id = qoute_line_c.id
)
SELECT opportunity_id,
  currency_iso_code,
  product_2_id,
  product_of_interest_c,
  "Opportunity Product Name",
  quantity,
  "Product Name",
  "Product FAMILY",
  product_code,
  total_price,
  snapshot_load_date,
  unit_price,
  list_price,
  TO_NUMBER(
    CASE
      WHEN "HISTORICAL CURRENT SUBSCRIPTION SOFTWARE" IS NOT NULL THEN "Annual Price" - "HISTORICAL CURRENT SUBSCRIPTION SOFTWARE"
      ELSE "Annual Price" - "SBQQ_NET_PRICE_C"
    END
  ) AS "Opp Product RSAC",
  FROM source_data
WHERE rn = 1 { % endsnapshot % }




create or replace transient table REPORTING_DEV.DBT_RYZLAN_gtm_analytics.opportunity_product_snapshot as (
    WITH previous_data AS (
      SELECT snapshot_data.opportunity_id,
        snapshot_data.currency_iso_code,
        snapshot_data.product_2_id,
        snapshot_data.product_of_interest_c,
        snapshot_data."Opportunity Product Name",
        snapshot_data.quantity,
        snapshot_data."Product.Name" AS "Product Name",
        snapshot_data."Product.FAMILY" AS "Product FAMILY",
        snapshot_data.product_code,
        snapshot_data.total_price,
        snapshot_data.snapshot_load_date,
        snapshot_data.unit_price,
        snapshot_data.list_price,
        NULL AS "Opp Product RSAC"
      FROM vault_fivetran.opti_salesforce.OPPORTUNITY_PRODUCT_SNAPSHOT as snapshot_data
    ),
    combining_data AS (
      SELECT snapshot_data.opportunity_id,
        snapshot_data.currency_iso_code,
        snapshot_data.product_2_id,
        snapshot_data.product_of_interest_c,
        snapshot_data."Opportunity Product Name",
        snapshot_data.quantity,
        snapshot_data."Product Name",
        snapshot_data."Product FAMILY",
        snapshot_data.product_code,
        snapshot_data.total_price,
        snapshot_data.snapshot_load_date::TIMESTAMP_TZ(9) AS snapshot_load_date,
        snapshot_data.unit_price,
        snapshot_data.list_price,
        snapshot_data."Opp Product RSAC"
      FROM REPORTING_DEV.DBT_RYZLAN_gtm_analytics.gtm_opportunity_product_snapshot as snapshot_data
      UNION ALL
      SELECT *
      FROM previous_data
    )
    SELECT *
    FROM combining_data
  );