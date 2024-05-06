create or replace task VAULT_FIVETRAN.OPTI_SALESFORCE.TASK_OPPORTUNITY_ANALYTICS warehouse = ANALYST_WH schedule = 'using cron 0 */3 * * * UTC' COMMENT = 'This task has been created as a temporary automated solution for DSSM-217.' as
create or replace table opportunity_analytics comment = 'Table created as per DSSM-217; this is a temporary solution and will be 		replaced once the necessary dbt models have been created.' as
select o.id "Opportunity ID",
  o.is_deleted "Is Deleted",
  o.ACCOUNT_ID "AccountId",
  o.name "Opportunity Name",
  o.STAGE_NAME "Stage Name",
  o.Amount "Amount",
  concat(
    date_part(year, o.close_date),
    '-',
    LPAD(date_part(quarter, o.close_date), 2, 0)
  ) "Year/Quarter Close Date",
  date_part(year, o.CLOSE_DATE) "Close Year",
  to_date(o.close_date) "Close Date",
  (
    case
      when "Close Date" >=(
        DATEADD(quarter, 1, DATE_TRUNC(quarter, CURRENT_DATE()))
      )
      and "Close Date" <= (
        DATEADD(
          day,
          -1,
          DATEADD(quarter, + 2, DATE_TRUNC(quarter, CURRENT_DATE()))
        )
      ) then 'Next Quarter'
      else ''
    end
  ) "Close Next Quarter",
  (
    case
      when "Close Date" >= DATEADD(quarter, -1, DATE_TRUNC(quarter, CURRENT_DATE()))
      and "Close Date" <= DATEADD(day, -1, DATE_TRUNC(quarter, CURRENT_DATE())) then 'Previous Quarter'
      else ''
    end
  ) "Close Previous Quarter",
  (
    case
      when "Close Date" >= DATE_TRUNC(QUARTER, CURRENT_DATE())
      and "Close Date" <= DATEADD(
        DAY,
        -1,
        DATEADD(QUARTER, 1, DATE_TRUNC(QUARTER, CURRENT_DATE()))
      ) then 'Current Quarter'
      else ''
    end
  ) "Close Current Quarter",
  (
    case
      when "Close Date" >= DATEADD(quarter, -1, DATE_TRUNC(quarter, CURRENT_DATE()))
      and "Close Date" <= DATEADD(day, -1, DATE_TRUNC(quarter, CURRENT_DATE())) then 'Previous Quarter'
      when "Close Date" >=(
        DATEADD(quarter, 1, DATE_TRUNC(quarter, CURRENT_DATE()))
      )
      and "Close Date" <= (
        DATEADD(
          day,
          -1,
          DATEADD(quarter, + 2, DATE_TRUNC(quarter, CURRENT_DATE()))
        )
      ) then 'Next Quarter'
      when "Close Date" >= DATE_TRUNC(QUARTER, CURRENT_DATE())
      and "Close Date" <= DATEADD(
        DAY,
        -1,
        DATEADD(QUARTER, 1, DATE_TRUNC(QUARTER, CURRENT_DATE()))
      ) then 'Current Quarter'
      else ''
    end
  ) "Recent Quarter Filter",
  o.Type "Opportunity Type",
  o.LEAD_SOURCE "LeadSource",
  o.SOURCE_C "Source",
  o.OPPORTUNITY_SOURCE_C "Opportunity Source",
  (
    CASE
      when o.OPPORTUNITY_SOURCE_C = 'AE/AM Generated' then 'AE'
      when o.OPPORTUNITY_SOURCE_C = 'Welcome' then 'AE'
      when o.OPPORTUNITY_SOURCE_C = 'Optimizely' then 'AE'
      when o.OPPORTUNITY_SOURCE_C = 'Empire Selling - AE' then 'AE'
      when o.OPPORTUNITY_SOURCE_C = 'CSM Generated' then 'CSM'
      when o.OPPORTUNITY_SOURCE_C = 'Support Generated' then 'CSM'
      when o.OPPORTUNITY_SOURCE_C = 'Expert Services' then 'CSM'
      when o.OPPORTUNITY_SOURCE_C = 'Education Services' then 'CSM'
      when o.OPPORTUNITY_SOURCE_C = 'Education Store' then 'CSM'
      when o.OPPORTUNITY_SOURCE_C = 'Content download (web)' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Website Direct' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Marketing' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Live Event' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Content Syndication' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'UserGems' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Demo request (web)' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Drift' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Webinar' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Tradeshow' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Inbound' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Paid Search' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Content Diagnostic Trial' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Purchased List' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Organic Search' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Event Partner' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Virtual Event' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Online Advertising' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Website Referral' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Organic Social' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Paid Social' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Partner' then 'Partner'
      when o.OPPORTUNITY_SOURCE_C = 'Partner Marketing' then 'Marketing'
      when o.OPPORTUNITY_SOURCE_C = 'Referral' then 'Partner'
      when o.OPPORTUNITY_SOURCE_C = 'Technology Partner' then 'Partner'
      when o.OPPORTUNITY_SOURCE_C = 'SDR Generated' then 'SDR'
      when o.OPPORTUNITY_SOURCE_C = 'Lead IQ' then 'SDR'
      when o.OPPORTUNITY_SOURCE_C = 'DiscoverOrg' then 'SDR'
      when o.OPPORTUNITY_SOURCE_C = 'Empire Selling - SDR' then 'SDR'
      when o.OPPORTUNITY_SOURCE_C = 'Zoominfo' then 'SDR'
      when o.OPPORTUNITY_SOURCE_C = 'Unknown' then 'Unknown'
      else 'Unknown'
    end
  ) "Opportunity Source Category",
  o.OWNER_ID "Owner ID",
  o.CREATED_DATE "CreatedDate",
  o.CREATED_BY_ID "CreatedById",
  o.CONTACT_ID "ContactId",
  o.LOSS_REASON_C "Loss Reason",
  (
    case
      when o.LEGACY_CUSTOMER_TYPE_C = 'New Customer' then 'New Customer'
      when o.LEGACY_CUSTOMER_TYPE_C = 'Existing Customer' then 'Existing Customer'
      when o.LEGACY_CUSTOMER_TYPE_C is NULL then 'Existing Customer'
    end
  ) "Customer Type",
  o.LEGACY_ORIGINATING_LEAD_C "Legacy Originating Lead",
  ifnull(o.LEGACY_RENEWAL_OPPORTUNITY_NEW_C, '') "Legacy Renewal Opportunity New",
  t.name "Territory Name",
  o.CUSTOM_FORECAST_C "Sales Defined Forecast",
  --"Salesforce ID",
  to_date(o.DATE_ENTERED_STAGE_SAL_C) "Date Entered SAL",
  to_date(o.DATE_ENTERED_STAGE_SQL_C) "Date Entered SQL",
  to_date(o.DATE_ENTERED_STAGE_DISCOVERY_C) "Date Entered Discovery",
  concat(
    date_part(year, o.DATE_ENTERED_STAGE_DISCOVERY_C),
    '-',
    LPAD(
      date_part(quarter, o.DATE_ENTERED_STAGE_DISCOVERY_C),
      2,
      0
    )
  ) quarter_month_discovery,
  to_date(o.DATE_ENTERED_STAGE_PROOF_OF_VALUE_C) "Date Entered Proof of Value",
  to_date(o.DATE_ENTERED_STAGE_PROPOSAL_C) "Date Entered Proposal",
  to_date(o.DATE_ENTERED_STAGE_CONTRACTS_C) "Date Entered Contracts",
  to_date(o.DATE_ENTERED_STAGE_CLOSED_WON_C) "Date Entered Closed Won",
  to_date(o.DATE_ENTERED_STAGE_CLOSED_WON_FINANCE_C) "Date Entered Closed Won Finance",
  to_date(o.DATE_ENTERED_STAGE_PROSPECTING_C) "Date Entered Prospecting",
  date_part(year, o.DATE_ENTERED_STAGE_CLOSED_WON_C) "Close Won Year",
  --"Recurring Software Amount",
  a.INDUSTRY "Account Industry",
  a.name "Account Name",
  a.ABM_ACCOUNT_C "ABM Account",
  a.D_B_EMPLOYEE_COUNT_C "Employee Count",
  a.type "Account Type",
  a.ICP_ACCOUNT_C "ICP Account",
  (
    case
      when a.ICP_ACCOUNT_C = 'Non-ICP' then FALSE
      when a.ICP_ACCOUNT_C is null then FALSE
      else TRUE
    end
  ) "ICP (True/False)",
  a.PRIMARY_ACCOUNT_EXECUTIVE_C "Primary Account Executive",
  a.SEGMENT_C "Segment",
  u.name "Opportunity Owner",
  r.name "Opportunity Owner Role Name",
  u.manager_name "Opportunity Owner Manager",
  r2.name "Opportunity Owner Manger Role Name",
  g.name "Generated By Name",
  g.name "Generated By Role Name",
  o.OPPORTUNITY_PRODUCT_OF_INTEREST_C "Opportunity Product of Interest",
  o.OPPORTUNITY_PRODUCT_OF_INTEREST_2_C "Opportunity Product Of Interest2",
  o.currency_iso_code "Currency Code",
  to_number(
    case
      when o.HISTORICAL_CURRENT_CONTRACT_SOFTWARE_C is not null then o.RECURRING_SOFTWARE_AMOUNT_NEW_ROLLUP_C - o.HISTORICAL_CURRENT_CONTRACT_SOFTWARE_C
      else o.RECURRING_SOFTWARE_AMOUNT_NEW_ROLLUP_C - o.CURRENT_CONTRACT_SOFTWARE_NEW_C
    end
  ) rsac,
  to_number(
    (
      case
        when "Currency Code" = 'GBP'
        and "Close Year" = '2021' then 1.3663 * rsac
        when "Currency Code" = 'SGD'
        and "Close Year" = '2021' then 0.7565 * rsac
        when "Currency Code" = 'AUD'
        and "Close Year" = '2021' then 0.7725 * rsac
        when "Currency Code" = 'EUR'
        and "Close Year" = '2021' then 1.2278 * rsac
        when "Currency Code" = 'CAD'
        and "Close Year" = '2021' then 0.7849 * rsac
        when "Currency Code" = 'SEK'
        and "Close Year" = '2021' then 0.1225 * rsac
        when "Currency Code" = 'DKK'
        and "Close Year" = '2021' then 0.165 * rsac
        when "Currency Code" = 'NOK'
        and "Close Year" = '2021' then 0.1173 * rsac
        when "Currency Code" = 'VND'
        and "Close Year" = '2021' then 0.0000433 * rsac
        when "Currency Code" = 'PLN'
        and "Close Year" = '2021' then 0.2691 * rsac
        when "Currency Code" = 'USD'
        and "Close Year" = '2021' then 1 * rsac
        when "Currency Code" = 'GBP'
        and "Close Year" = '2022' then 1.3497 * rsac
        when "Currency Code" = 'SGD'
        and "Close Year" = '2022' then 0.7409 * rsac
        when "Currency Code" = 'AUD'
        and "Close Year" = '2022' then 0.7262 * rsac
        when "Currency Code" = 'EUR'
        and "Close Year" = '2022' then 1.1325 * rsac
        when "Currency Code" = 'CAD'
        and "Close Year" = '2022' then 0.7859 * rsac
        when "Currency Code" = 'SEK'
        and "Close Year" = '2022' then 0.1105 * rsac
        when "Currency Code" = 'DKK'
        and "Close Year" = '2022' then 0.1523 * rsac
        when "Currency Code" = 'NOK'
        and "Close Year" = '2022' then 0.1173 * rsac
        when "Currency Code" = 'VND'
        and "Close Year" = '2022' then 0.00004375 * rsac
        when "Currency Code" = 'PLN'
        and "Close Year" = '2022' then 0.2464 * rsac
        when "Currency Code" = 'USD'
        and "Close Year" = '2022' then 1 * rsac
        when "Currency Code" = 'JPY'
        and "Close Year" = '2022' then 0.00868719 * rsac
        when (
          "Currency Code" = 'GBP'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'GBP'
          and "Close Year" is null
        ) then 1.2103 * rsac
        when (
          "Currency Code" = 'SGD'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'SGD'
          and "Close Year" is null
        ) then 0.7459 * rsac
        when (
          "Currency Code" = 'AUD'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'AUD'
          and "Close Year" is null
        ) then 0.6805 * rsac
        when (
          "Currency Code" = 'EUR'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'EUR'
          and "Close Year" is null
        ) then 1.072 * rsac
        when (
          "Currency Code" = 'CAD'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'CAD'
          and "Close Year" is null
        ) then 0.737 * rsac
        when (
          "Currency Code" = 'SEK'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'SEK'
          and "Close Year" is null
        ) then 0.0959 * rsac
        when (
          "Currency Code" = 'DKK'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'DKK'
          and "Close Year" is null
        ) then 0.1439 * rsac
        when (
          "Currency Code" = 'NOK'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'NOK'
          and "Close Year" is null
        ) then 0.1025 * rsac
        when (
          "Currency Code" = 'VND'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'VND'
          and "Close Year" is null
        ) then 0.00004231 * rsac
        when (
          "Currency Code" = 'PLN'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'PLN'
          and "Close Year" is null
        ) then 0.2282 * rsac
        when (
          "Currency Code" = 'USD'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'USD'
          and "Close Year" is null
        ) then 1 * rsac
        when (
          "Currency Code" = 'AED'
          and "Close Year" >= '2023'
        )
        or (
          "Currency Code" = 'AED'
          and "Close Year" is null
        ) then 0.2723 * rsac
      end
    )
  ) "RSAC Converted",
  to_number(
    case
      when "RSAC Converted" > 0 then "RSAC Converted"
    end
  ) "RSAC-All",
  to_number(
    case
      when "RSAC Converted" > 0
      and "Stage Name" in ('Closed Won', 'Closed Won - Finance') then "RSAC Converted"
    end
  ) "RSAC-Closed Won/Won Finance",
  to_number(
    case
      when "RSAC Converted" > 0
      and "Stage Name" in (
        'Contracts',
        'Discovery',
        'Proof of Value',
        'Proposal'
      )
      and "Sales Defined Forecast" in ('Commit', 'Most Likely', 'Best Case', 'Pipeline') then "RSAC Converted"
    end
  ) "RSAC-Pipeline",
  to_number(
    case
      when "RSAC Converted" > 0
      and "Stage Name" in (
        'Contracts',
        'Discovery',
        'Proof of Value',
        'Proposal'
      )
      and "Sales Defined Forecast" in ('Commit', 'Most Likely', 'Best Case', 'Pipeline')
      and "Close Next Quarter" = 'Next Quarter' then "RSAC Converted"
    end
  ) "RSAC-Pipeline Next Quarter",
  o.HISTORICAL_CURRENT_CONTRACT_SOFTWARE_C "Historical Current Contract Software",
  o.RECURRING_SOFTWARE_AMOUNT_NEW_ROLLUP_C "Recurring Software Amount New Rollup",
  o.CURRENT_CONTRACT_SOFTWARE_NEW_C "Current Contract Software",
  --"Total Arr (from Opp Product Table)",
  o.IN_MANAGER_CALL_C "In Manager Call",
  o.Legacy_Partner_role_c "Legacy Partner Role",
  o.PRIMARY_COMPETITOR_C "Primary Competitor",
  o.LEGACY_PARTNER_INVOLVED_C "Legacy Partner Involved",
  o.Reason_Closed_Won_Detail_c "reason Closed Won Detail",
  o.Reason_Closed_Won_c "Reason Closed Won",
  o.Reason_Lost_Detail_c "Reason Lost Detail" --,op."Product Family"
,
  listagg(
    concat(
      op."Product Family",
      ' $',
      "Opp Product RSAC Converted"
    ),
    ', '
  ) product_family_summarize
from "VAULT_FIVETRAN"."OPTI_SALESFORCE"."OPPORTUNITY" o
  left join (
    select ID,
      INDUSTRY,
      name,
      ABM_ACCOUNT_C,
      D_B_EMPLOYEE_COUNT_C,
      type,
      ICP_ACCOUNT_C,
      PRIMARY_ACCOUNT_EXECUTIVE_C,
      SEGMENT_C,
      TERRITORY_C
    from "VAULT_FIVETRAN"."OPTI_SALESFORCE"."ACCOUNT"
    where IS_DELETED = 'FALSE'
  ) a on o.ACCOUNT_ID = a.id
  left join "VAULT_FIVETRAN"."OPTI_SALESFORCE"."TERRITORY_C" t on a.territory_C = t.id
  left join(
    select u.id,
      u.username,
      u.name,
      u.USER_ROLE_ID,
      m.name manager_name,
      m.USER_ROLE_ID manager_role
    from "VAULT_FIVETRAN"."OPTI_SALESFORCE"."USER" u
      left join "VAULT_FIVETRAN"."OPTI_SALESFORCE"."USER" m on u.MANAGER_ID = m.id
  ) u on o.owner_id = u.id
  left join "VAULT_FIVETRAN"."OPTI_SALESFORCE"."USER_ROLE" r on u.USER_ROLE_ID = r.id
  left join "VAULT_FIVETRAN"."OPTI_SALESFORCE"."USER_ROLE" r2 on u.manager_role = r2.id
  left join "VAULT_FIVETRAN"."OPTI_SALESFORCE"."USER" g on o.GENERATED_BY_C = g.id
  left join "VAULT_FIVETRAN"."OPTI_SALESFORCE"."USER_ROLE" gr on u.USER_ROLE_ID = g.id
  left join (
    select "Opportunity ID",
      "Product Family",
      "Opp Product RSAC Converted"
    from "VAULT_FIVETRAN"."OPTI_SALESFORCE"."OPPORTUNITY_LINE_ITEM_ANALYTICS"
    where "Product Family" <> '- Not Applicable -'
      and "Opp Product RSAC Converted" > 0
  ) op on o.id = op."Opportunity ID"
where "Close Date" >= '2021-10-01' -- and "Opportunity ID"='0068d00000AdLkUAAV'
  --and o.is_deleted='FALSE'
group by o.id,
  "Is Deleted",
  "AccountId",
  "Opportunity Name",
  o.STAGE_NAME,
  "Amount",
  "Year/Quarter Close Date",
  "Close Year",
  O.CLOSE_DATE,
  "Recent Quarter Filter",
  "Opportunity Type",
  "LeadSource",
  "Source",
  "Opportunity Source",
  "Opportunity Source Category",
  "Owner ID",
  "CreatedDate",
  "CreatedById",
  "ContactId",
  "Loss Reason",
  "Customer Type",
  "Legacy Originating Lead",
  "Legacy Renewal Opportunity New",
  "Territory Name",
  "Sales Defined Forecast",
  "Date Entered SAL",
  "Date Entered SQL",
  "Date Entered Discovery",
  quarter_month_discovery,
  "Date Entered Proof of Value",
  "Date Entered Proposal",
  "Date Entered Contracts",
  "Date Entered Closed Won",
  "Date Entered Closed Won Finance",
  "Date Entered Prospecting",
  "Close Won Year",
  "Account Industry",
  "Account Name",
  "ABM Account",
  "Employee Count",
  "Account Type",
  "ICP Account",
  "ICP (True/False)",
  "Primary Account Executive",
  "Segment",
  "Opportunity Owner",
  "Opportunity Owner Role Name",
  "Opportunity Owner Manager",
  "Opportunity Owner Manger Role Name",
  "Generated By Name",
  "Generated By Role Name",
  "Opportunity Product of Interest",
  "Opportunity Product Of Interest2",
  O.CURRENCY_ISO_CODE,
  rsac,
  "RSAC Converted",
  "RSAC-All",
  "RSAC-Closed Won/Won Finance",
  "RSAC-Pipeline",
  "RSAC-Pipeline Next Quarter",
  "Historical Current Contract Software",
  "Recurring Software Amount New Rollup",
  "Current Contract Software",
  "In Manager Call",
  "Legacy Partner Role",
  "Primary Competitor",
  "Legacy Partner Involved",
  "reason Closed Won Detail",
  "Reason Closed Won",
  "Reason Lost Detail"
order by "Close Date" desc;