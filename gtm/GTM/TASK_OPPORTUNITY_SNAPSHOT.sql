create or replace task VAULT_FIVETRAN.OPTI_SALESFORCE.TASK_OPPORTUNITY_SNAPSHOT schedule = 'USING CRON 0 22 * * * America/Los_Angeles' USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' as
insert into opportunity_snapshot (
    id,
    contact_id,
    account_id,
    created_date,
    close_date,
    stage_name,
    legacy_customer_type_c,
    legacy_originating_lead_c,
    date_entered_stage_sal_c,
    date_entered_stage_sql_c,
    date_entered_stage_accepted_c,
    date_entered_stage_discovery_c,
    date_entered_stage_proof_of_value_c,
    date_entered_stage_proposal_c,
    date_entered_stage_contracts_c,
    date_entered_stage_closed_won_c,
    date_entered_stage_closed_lost_c,
    date_entered_stage_closed_won_finance_c,
    opportunity_source_c,
    opportunity_product_of_interest_c,
    source_c,
    owner_role_c,
    in_manager_call_c,
    territory_c,
    Opportunity_Owner__c,
    Opportunity_Owner_Manager__c,
    Opportunity_Source_Category__c,
    Quarter_Close_Date__c,
    Recurring_Software_Amount_Change__c,
    name,
    description,
    amount,
    LEGACY_RENEWAL_OPPORTUNITY_NEW_C,
    type,
    CURRENCY_ISO_CODE,
    SNAPSHOT_LOAD_DATE,
    CUSTOM_FORECAST_C,
    RECURRING_SOFTWARE_AMOUNT_NEW_ROLLUP_C
  )
select OPPORTUNITY.ID,
  OPPORTUNITY.contact_id,
  OPPORTUNITY.account_id,
  OPPORTUNITY.created_date,
  OPPORTUNITY.close_date,
  OPPORTUNITY.stage_name,
  OPPORTUNITY.legacy_customer_type_c,
  OPPORTUNITY.legacy_originating_lead_c,
  OPPORTUNITY.date_entered_stage_sal_c,
  OPPORTUNITY.date_entered_stage_sql_c,
  OPPORTUNITY.date_entered_stage_accepted_c,
  OPPORTUNITY.date_entered_stage_discovery_c,
  OPPORTUNITY.date_entered_stage_proof_of_value_c,
  OPPORTUNITY.date_entered_stage_proposal_c,
  OPPORTUNITY.date_entered_stage_contracts_c,
  OPPORTUNITY.date_entered_stage_closed_won_c,
  OPPORTUNITY.date_entered_stage_closed_lost_c,
  OPPORTUNITY.date_entered_stage_closed_won_finance_c,
  OPPORTUNITY.opportunity_source_c,
  OPPORTUNITY.opportunity_product_of_interest_c,
  OPPORTUNITY.source_c,
  OPPORTUNITY.owner_role_c,
  OPPORTUNITY.in_manager_call_c,
  OPPORTUNITY.territory_c,
  user.name as Opportunity_Owner__c,
  USER_MANAGER.name as Opportunity_Owner_Manager__c,
  CASE
    when OPPORTUNITY.Opportunity_Source_c in (
      'AE/AM Generated',
      'Welcome',
      'Optimizely',
      'Empire Selling - AE'
    ) then 'AE'
    when OPPORTUNITY.Opportunity_Source_c in (
      'CSM Generated',
      'Support Generated',
      'Expert Services',
      'Education Services',
      'Education Store'
    ) then 'CSM'
    when OPPORTUNITY.Opportunity_Source_c in (
      'Content download (web)',
      'Website Direct',
      'Marketing',
      'Live Event',
      'Content Syndication',
      'UserGems',
      'Demo request (web)',
      'Drift',
      'Webinar',
      'Tradeshow',
      'Inbound',
      'Paid Search',
      'Content Diagnostic Trial',
      'Purchased List',
      'Organic Search',
      'Event Partner',
      'Virtual Event',
      'Online Advertising',
      'Website Referral',
      'Organic Social',
      'Paid Social'
    ) then 'Marketing'
    when OPPORTUNITY.Opportunity_Source_c in (
      'Partner',
      'Partner Marketing',
      'Referral',
      'Technology Partner'
    ) then 'Partner'
    when OPPORTUNITY.Opportunity_Source_c in (
      'SDR Generated',
      'Lead IQ',
      'DiscoverOrg',
      'Empire Selling - SDR',
      'Zoominfo'
    ) then 'SDR'
    else 'Unknown'
  end as Opportunity_Source_Category__c,
  year(close_date) || '-' || 'Q' || ceil(month(close_date) / 3) as Quarter_Close_Date__c,
  case
    when OPPORTUNITY.HISTORICAL_CURRENT_CONTRACT_SOFTWARE_C is not null then OPPORTUNITY.RECURRING_SOFTWARE_AMOUNT_NEW_ROLLUP_C - OPPORTUNITY.HISTORICAL_CURRENT_CONTRACT_SOFTWARE_C
    else OPPORTUNITY.RECURRING_SOFTWARE_AMOUNT_NEW_ROLLUP_C - OPPORTUNITY.CURRENT_CONTRACT_SOFTWARE_NEW_C
  end as Recurring_Software_Amount_Change__c,
  OPPORTUNITY.name,
  OPPORTUNITY.description,
  OPPORTUNITY.amount,
  OPPORTUNITY.LEGACY_RENEWAL_OPPORTUNITY_NEW_C,
  OPPORTUNITY.type,
  OPPORTUNITY.CURRENCY_ISO_CODE,
  current_timestamp(),
  OPPORTUNITY.CUSTOM_FORECAST_C,
  OPPORTUNITY.RECURRING_SOFTWARE_AMOUNT_NEW_ROLLUP_C
from "VAULT_FIVETRAN"."OPTI_SALESFORCE"."OPPORTUNITY" OPPORTUNITY,
  "VAULT_FIVETRAN"."OPTI_SALESFORCE"."USER" User,
  "VAULT_FIVETRAN"."OPTI_SALESFORCE"."USER" User_Manager
where year(OPPORTUNITY.created_date) > 2020
  and user.id = OPPORTUNITY.owner_id
  and USER_MANAGER.ID = USER.manager_id;