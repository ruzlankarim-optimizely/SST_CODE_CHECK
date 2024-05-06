create or replace task VAULT_FIVETRAN.OPTI_SALESFORCE.TASK_ACCOUNT_SNAPSHOT schedule = 'USING CRON 0 22 * * * America/Los_Angeles' USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' as
Insert into "VAULT_FIVETRAN"."OPTI_SALESFORCE"."ACCOUNT_SNAPSHOT" (
    ID,
    NAME,
    number_of_employees,
    primary_contact_c,
    record_type_id,
    type,
    D_B_EMPLOYEE_COUNT_C,
    SNAPSHOT_LOAD_DATE
  )
select ID,
  NAME,
  number_of_employees,
  primary_contact_c,
  record_type_id,
  type,
  D_B_EMPLOYEE_COUNT_C,
  current_timestamp()
from "VAULT_FIVETRAN"."OPTI_SALESFORCE"."ACCOUNT";