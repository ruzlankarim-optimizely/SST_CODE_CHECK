create or replace task VAULT_FIVETRAN.OPTI_SALESFORCE.TASK_USER_ROLE_SNAPSHOT schedule = 'USING CRON 0 22 * * * America/Los_Angeles' USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' as
Insert into "VAULT_FIVETRAN"."OPTI_SALESFORCE"."USER_ROLE_SNAPSHOT" (
    id,
    name,
    parent_role_id,
    rollup_description,
    Forecast_User_ID,
    SNAPSHOT_LOAD_DATE
  )
select id,
  name,
  parent_role_id,
  rollup_description,
  Forecast_User_ID,
  current_timestamp()
from "VAULT_FIVETRAN"."OPTI_SALESFORCE"."USER_ROLE";