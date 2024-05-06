create or replace task VAULT_FIVETRAN.OPTI_SALESFORCE.TASK_OPPORTUNITY_LINE_ITEM_ANALYTICS warehouse = ANALYST_WH schedule = 'using cron 0 0 * * * UTC' COMMENT = 'This task has been created as a temporary automated solution for DSSM-205.' as
create or replace table opportunity_line_item_analytics comment = 'Table created as per DSSM-205; this is a temporary solution and will be replaced once the necessary dbt models have been created.' as
select ol.OPPORTUNITY_ID "Opportunity ID",
  o.STAGE_NAME "Stage Name",
  to_date(o.CLOSE_DATE) close_date,
  date_part(year, o.CLOSE_DATE) "Close Year",
  concat(
    date_part(year, o.close_date),
    '-',
    LPAD(date_part(quarter, o.close_date), 2, 0)
  ) "Year/Quarter Close Date",
  o.LEGACY_RENEWAL_OPPORTUNITY_NEW_C,
  ol.PRODUCT_2_ID "Product ID",
  ol.PRODUCT_CODE "Product Code",
  ol.CURRENCY_ISO_CODE "Currency Code",
  ol.QUANTITY,
  ol.DISCOUNT,
  ol.TOTAL_PRICE,
  ol.UNIT_PRICE,
  ol.LIST_PRICE,
  ol.CREATED_DATE,
  ol.CREATED_BY_ID,
  ol.IS_DELETED,
  p.PRODUCT_CODE,
  p.FAMILY,
  p.PRODUCT_CATEGORY_C "Product Category",
  p.NETSUITE_PRODUCT_FAMILY_C "Product Family",
  p.PRODUCT_OF_INTEREST_C "Product of Interest",
  p.LOB_C "LOB",
  p.LOB_SUBCATEGORY_C "LOB Subcategory",
  p.PRODUCT_LINE_C "Product Line",
  p.PRODUCT_SOLUTION_C "Product Solution",
  p.PRODUCT_CATEGORY_C "Product",
  ol.SBQQ_QUOTE_LINE_C,
  ifnull(ol.HISTORICAL_CURRENT_SUBSCRIPTION_SOFTWARE_C, 0) "HISTORICAL CURRENT SUBSCRIPTION SOFTWARE",
  ol.SUBSCRIPTION_TERM_CALCULATED_C,
  ql.SBQQ_RENEWED_SUBSCRIPTION_C,
  ifnull(ql.SBQQ_NET_PRICE_C, 0) "SBQQ_NET_PRICE_C",
  to_number (
    case
      when ol.SUBSCRIPTION_TERM_CALCULATED_C >= 12
      and ol.EXCLUDE_FROM_1_ST_YEAR_MRR_C = 'False' then (
        (ol.TOTAL_PRICE / ol.SUBSCRIPTION_TERM_CALCULATED_C) * 12
      )
      when ol.SUBSCRIPTION_TERM_CALCULATED_C >= 12
      and ol.EXCLUDE_FROM_1_ST_YEAR_MRR_C = 'True' then 0
      else ol.TOTAL_PRICE
    end
  ) "Annual Price",
  to_number(
    case
      when o.HISTORICAL_CURRENT_CONTRACT_SOFTWARE_C is not null then o.RECURRING_SOFTWARE_AMOUNT_NEW_ROLLUP_C - o.HISTORICAL_CURRENT_CONTRACT_SOFTWARE_C
      else o.RECURRING_SOFTWARE_AMOUNT_NEW_ROLLUP_C - o.CURRENT_CONTRACT_SOFTWARE_NEW_C
    end
  ) "Opp RSAC",
  to_number (
    case
      when "HISTORICAL CURRENT SUBSCRIPTION SOFTWARE" is not null then "Annual Price" - "HISTORICAL CURRENT SUBSCRIPTION SOFTWARE"
      else "Annual Price" - "SBQQ_NET_PRICE_C"
    end
  ) "Opp Product RSAC",
  to_number(
    (
      case
        when "Currency Code" = 'GBP'
        and "Close Year" = '2021' then 1.3663 * "Opp Product RSAC"
        when "Currency Code" = 'SGD'
        and "Close Year" = '2021' then 0.7565 * "Opp Product RSAC"
        when "Currency Code" = 'AUD'
        and "Close Year" = '2021' then 0.7725 * "Opp Product RSAC"
        when "Currency Code" = 'EUR'
        and "Close Year" = '2021' then 1.2278 * "Opp Product RSAC"
        when "Currency Code" = 'CAD'
        and "Close Year" = '2021' then 0.7849 * "Opp Product RSAC"
        when "Currency Code" = 'SEK'
        and "Close Year" = '2021' then 0.1225 * "Opp Product RSAC"
        when "Currency Code" = 'DKK'
        and "Close Year" = '2021' then 0.165 * "Opp Product RSAC"
        when "Currency Code" = 'NOK'
        and "Close Year" = '2021' then 0.1173 * "Opp Product RSAC"
        when "Currency Code" = 'VND'
        and "Close Year" = '2021' then 0.0000433 * "Opp Product RSAC"
        when "Currency Code" = 'PLN'
        and "Close Year" = '2021' then 0.2691 * "Opp Product RSAC"
        when "Currency Code" = 'USD'
        and "Close Year" = '2021' then 1 * "Opp Product RSAC"
        when "Currency Code" = 'GBP'
        and "Close Year" = '2022' then 1.3497 * "Opp Product RSAC"
        when "Currency Code" = 'SGD'
        and "Close Year" = '2022' then 0.7409 * "Opp Product RSAC"
        when "Currency Code" = 'AUD'
        and "Close Year" = '2022' then 0.7262 * "Opp Product RSAC"
        when "Currency Code" = 'EUR'
        and "Close Year" = '2022' then 1.1325 * "Opp Product RSAC"
        when "Currency Code" = 'CAD'
        and "Close Year" = '2022' then 0.7859 * "Opp Product RSAC"
        when "Currency Code" = 'SEK'
        and "Close Year" = '2022' then 0.1105 * "Opp Product RSAC"
        when "Currency Code" = 'DKK'
        and "Close Year" = '2022' then 0.1523 * "Opp Product RSAC"
        when "Currency Code" = 'NOK'
        and "Close Year" = '2022' then 0.1173 * "Opp Product RSAC"
        when "Currency Code" = 'VND'
        and "Close Year" = '2022' then 0.00004375 * "Opp Product RSAC"
        when "Currency Code" = 'PLN'
        and "Close Year" = '2022' then 0.2464 * "Opp Product RSAC"
        when "Currency Code" = 'USD'
        and "Close Year" = '2022' then 1 * "Opp Product RSAC"
        when "Currency Code" = 'JPY'
        and "Close Year" = '2022' then 0.00868719 * "Opp Product RSAC"
        when (
          "Currency Code" = 'GBP'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'GBP'
          and "Close Year" is null
        ) then 1.2103 * "Opp Product RSAC"
        when (
          "Currency Code" = 'SGD'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'SGD'
          and "Close Year" is null
        ) then 0.7459 * "Opp Product RSAC"
        when (
          "Currency Code" = 'AUD'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'AUD'
          and "Close Year" is null
        ) then 0.6805 * "Opp Product RSAC"
        when (
          "Currency Code" = 'EUR'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'EUR'
          and "Close Year" is null
        ) then 1.072 * "Opp Product RSAC"
        when (
          "Currency Code" = 'CAD'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'CAD'
          and "Close Year" is null
        ) then 0.737 * "Opp Product RSAC"
        when (
          "Currency Code" = 'SEK'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'SEK'
          and "Close Year" is null
        ) then 0.0959 * "Opp Product RSAC"
        when (
          "Currency Code" = 'DKK'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'DKK'
          and "Close Year" is null
        ) then 0.1439 * "Opp Product RSAC"
        when (
          "Currency Code" = 'NOK'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'NOK'
          and "Close Year" is null
        ) then 0.1025 * "Opp Product RSAC"
        when (
          "Currency Code" = 'VND'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'VND'
          and "Close Year" is null
        ) then 0.00004231 * "Opp Product RSAC"
        when (
          "Currency Code" = 'PLN'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'PLN'
          and "Close Year" is null
        ) then 0.2282 * "Opp Product RSAC"
        when (
          "Currency Code" = 'USD'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'USD'
          and "Close Year" is null
        ) then 1 * "Opp Product RSAC"
        when (
          "Currency Code" = 'AED'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'AED'
          and "Close Year" is null
        ) then 0.2723 * "Opp Product RSAC"
      end
    )
  ) "Opp Product RSAC Converted",
  listagg(
    concat(
      "Product Family",
      ' $',
      "Opp Product RSAC Converted"
    ),
    ', '
  ) product_family_summarize
from "VAULT_FIVETRAN"."OPTI_SALESFORCE"."OPPORTUNITY_LINE_ITEM" ol
  left join "VAULT_FIVETRAN"."OPTI_SALESFORCE"."PRODUCT_2" p on ol.PRODUCT_2_ID = p.id
  left join "VAULT_FIVETRAN"."OPTI_SALESFORCE"."SBQQ_QUOTE_LINE_C" ql on ol.id = ql.id
  left join "VAULT_FIVETRAN"."OPTI_SALESFORCE"."OPPORTUNITY" o on "Opportunity ID" = o.id
where ol.IS_DELETED = 'FALSE'
  and "Opp RSAC" > 0
  and (
    o.LEGACY_RENEWAL_OPPORTUNITY_NEW_C is null
    OR o.LEGACY_RENEWAL_OPPORTUNITY_NEW_C = 'No'
  ) --and ol.OPPORTUNITY_ID ='0068d00000AdLkUAAV'
  and close_date >= '2022-01-01' --and close_date<'2023-06-01'
  --and rsac>0
  --order by close_date desc
GROUP BY ol.OPPORTUNITY_ID,
  "Stage Name",
  close_date,
  "Close Year",
  "Year/Quarter Close Date",
  o.LEGACY_RENEWAL_OPPORTUNITY_NEW_C,
  "Product ID",
  "Product Code",
  "Currency Code",
  ol.QUANTITY,
  ol.DISCOUNT,
  ol.TOTAL_PRICE,
  ol.UNIT_PRICE,
  ol.LIST_PRICE,
  ol.CREATED_DATE,
  ol.CREATED_BY_ID,
  ol.IS_DELETED,
  p.PRODUCT_CODE,
  p.FAMILY,
  "Product Category",
  "Product Family",
  "Product of Interest",
  ol.SBQQ_QUOTE_LINE_C,
  "HISTORICAL CURRENT SUBSCRIPTION SOFTWARE",
  ol.SUBSCRIPTION_TERM_CALCULATED_C,
  ql.SBQQ_RENEWED_SUBSCRIPTION_C,
  "SBQQ_NET_PRICE_C",
  "Annual Price",
  "Opp RSAC",
  "Opp Product RSAC",
  "Opp Product RSAC Converted",
  "LOB",
  "LOB Subcategory",
  "Product Line",
  "Product Solution",
  "Product";