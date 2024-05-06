create or replace task VAULT_FIVETRAN.OPTI_SALESFORCE.TASK_OPPORTUNITY_PRODUCT_SNAPSHOT
    schedule='USING CRON 0 22 * * * America/Los_Angeles'
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
    as insert into "VAULT_FIVETRAN"."OPTI_SALESFORCE"."OPPORTUNITY_PRODUCT_SNAPSHOT"
select 
 OPPORTUNITY_ID
,CURRENCY_ISO_CODE
,PRODUCT_2_ID
,PRODUCT_OF_INTEREST_C
,"Opportunity Product Name"
,Quantity
,"Product.Name"
,"Product.FAMILY"
,PRODUCT_CODE
,total_price
,SNAPSHOT_LOAD_DATE
,UNIT_PRICE
,LIST_PRICE
from (Select
 t1.OPPORTUNITY_ID
,t1.CURRENCY_ISO_CODE
,t1.PRODUCT_2_ID
,t1.PRODUCT_OF_INTEREST_C
,t1.NAME as "Opportunity Product Name"
,t1.Quantity
,t2.NAME as "Product.Name"
,t2.FAMILY as "Product.FAMILY"
,t1.PRODUCT_CODE
,t1.total_price
,t1.UNIT_PRICE
,t1.LIST_PRICE
,current_timestamp()  as SNAPSHOT_LOAD_DATE
, row_number()
  over (partition by t1.OPPORTUNITY_ID, t1.PRODUCT_CODE order by t1.created_date desc) as rn
from "VAULT_FIVETRAN"."OPTI_SALESFORCE"."OPPORTUNITY_LINE_ITEM" t1
join "VAULT_FIVETRAN"."OPTI_SALESFORCE"."PRODUCT_2" t2
where t1.PRODUCT_2_ID = t2.ID) where rn=1;