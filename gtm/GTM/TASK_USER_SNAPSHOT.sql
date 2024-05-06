create or replace task VAULT_FIVETRAN.OPTI_SALESFORCE.TASK_USER_SNAPSHOT schedule = 'USING CRON 0 22 * * * America/Los_Angeles' USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' as
Insert into "VAULT_FIVETRAN"."OPTI_SALESFORCE"."USER_SNAPSHOT" (
    id,
    username,
    last_name,
    first_name,
    name,
    user_role_id,
    user_type,
    manager_id,
    manager_name,
    department,
    territory_c,
    segment_c,
    SNAPSHOT_LOAD_DATE
  )
Select USER.id,
  USER.username,
  USER.last_name,
  USER.first_name,
  USER.name,
  USER.user_role_id,
  USER.user_type,
  USER.manager_id,
  USER_MANAGER.first_name || ' ' || USER_MANAGER.last_name as Manager_Name,
  USER.department,
  USER.territory_c,
  USER.segment_c,
  current_timestamp()
from "VAULT_FIVETRAN"."OPTI_SALESFORCE"."USER" USER,
  "VAULT_FIVETRAN"."OPTI_SALESFORCE"."USER" USER_MANAGER
where USER_MANAGER.manager_id = USER.ID;