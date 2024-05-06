create or replace task VAULT_FIVETRAN.OPTI_SALESFORCE.TASK_CONTACT_SNAPSHOT schedule = 'USING CRON 0 22 * * * America/Los_Angeles' USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' as
insert into "VAULT_FIVETRAN"."OPTI_SALESFORCE"."CONTACT_SNAPSHOT" (
    Id,
    account_id,
    lead_id_c,
    lead_source,
    territory_text_c,
    other_additional_information_c,
    status_c,
    SNAPSHOT_LOAD_DATE
  )
select Id,
  account_id,
  lead_id_c,
  lead_source,
  territory_text_c,
  other_additional_information_c,
  status_c,
  current_timestamp()
from "VAULT_FIVETRAN"."OPTI_SALESFORCE"."CONTACT";