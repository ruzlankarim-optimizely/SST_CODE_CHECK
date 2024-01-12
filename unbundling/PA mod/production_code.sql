CREATE OR REPLACE FUNCTION sandbox_pd.sp_ufdm_arr_updates_manual()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

DECLARE sql_stmt TEXT;

BEGIN

    --remove weekly snapshots
    delete from sandbox_pd.arr a
        using ufdm_grey.periods b
    where a.snapshot_date = b.current_period
      and b.evaluation_period ilike '%W%'
      and snapshot_date < date_trunc('month', current_date)
    ;

    --remove invalid snapshots
    delete from sandbox_pd.arr
    where snapshot_date in ('2022-12-01','2022-10-01','2022-11-01','2022-09-01');

    --####################################################
    --update baseline_currency for fopti historic data
    --####################################################
    drop table if exists temp_base_currency_fopti;

    create temp table temp_base_currency_fopti as
    with temp_blank_currency as
        (
            select distinct mcid, snapshot_date
            from sandbox_pd.arr
            where baseline_currency is null
              and arr_usd_ccfx > 0
              and arr_source = 'FOpti product; Experimentation'
        )
       ,temp2 as
        (
            select a.mcid,a.snapshot_date,baseline_currency, rank() over (partition by a.mcid order by b.snapshot_date desc) as rnk
            from temp_blank_currency a
                     join sandbox_pd.arr b on a.mcid = b.mcid
                and arr_usd_ccfx > 0
                and arr_source = 'FOpti product; Experimentation'
                and baseline_currency is not null
        )
    select  distinct mcid, snapshot_date, baseline_currency
    from temp2
    where rnk = 1
    ;

    update sandbox_pd.arr a
    set baseline_currency = b.baseline_currency, modified_comments = 'baseline currency is updated from blank'
    from temp_base_currency_fopti b
         --select * from sandbox_pd.arr a, temp_base_currency_fopti b
    where a.mcid = b.mcid and a.snapshot_date = b.snapshot_date
      and a.baseline_currency is null
      and arr_usd_ccfx > 0
      and arr_source = 'FOpti product; Experimentation'
    ;

    --update remaining to USD
    update sandbox_pd.arr
    set baseline_currency = 'USD'
        --SELECT * FROM sandbox_pd.arr
    where baseline_currency is null
      and arr_usd_ccfx > 0
      and arr_source = 'FOpti product; Experimentation';

    --update lcu
    update sandbox_pd.arr
    set baseline_arr_local_currency = (arr_usd_ccfx / fx_rate_ccfx)::numeric(15,6)
      ,baseline_mrr_local_currency = ((arr_usd_ccfx / fx_rate_ccfx)/12)::numeric(15,6)
      ,modified_comments = concat(coalesce(modified_comments,''),'; baseline_arr_local_currency updated')
      --Select * from sandbox_pd.arr
    where 1=1
      and arr_source = 'FOpti product; Experimentation'
      and baseline_arr_local_currency is null
      and arr_usd_ccfx > 0
      and  baseline_arr_local_currency - (arr_usd_ccfx / fx_rate_ccfx)::numeric(15,6) not between -1 and 1
    ;

    --####################################################
    --MANUAL UPDATES
    --####################################################
    update sandbox_pd.arr set baseline_arr_local_currency = '66435',arr_usd_ccfx = '66435',modified_comments = 'arr,lcu updated FROM 398610'
                        --SELECT * FROM sandbox_pd.arr
    where mcid = '897145ea-48ca-89c6-9284-cb3e8bd3c17e'
      and snapshot_date = '2023-04-30' and arr_usd_ccfx > 0
      and baseline_arr_local_currency <>  '66435'
    ;

    update sandbox_pd.arr set baseline_arr_local_currency = '13602.333333333332',arr_usd_ccfx = '13602.333333333332' ,modified_comments = 'arr,lcu updated FROM 169596.3644262295'
                        --SELECT * FROM sandbox_pd.arr
    where mcid = '0d696e7c-1b37-7f98-589b-baacea4e9170'
      and snapshot_date = '2023-05-31' and arr_usd_ccfx > 0
      and baseline_arr_local_currency <>  '13602.333333333332'
    ;

    update sandbox_pd.arr a
    set parent_master_customer_id = case when a.parent_master_customer_id = b.mcid_old then b.mcid_new else a.parent_master_customer_id end
      , end_customer_master_customer_id = case when a.end_customer_master_customer_id = b.mcid_old then b.mcid_new else a.end_customer_master_customer_id end
      , mcid = b.mcid_new
    from ufdm_grey.mcid_overrides_manual b
         --select * from ufdm_grey.mcid_overrides_manual b,sandbox_pd.arr a
    where a.mcid = b.mcid_old
      and a.mcid <> b.mcid_new;

    update sandbox_pd.arr
    set snapshot_date_revised = (DATE_TRUNC('month' , snapshot_date) + interval '1 month' - interval '1 day')::DATE
        --select * from sandbox_pd.arr
    WHERE 1=1
      and snapshot_date_revised <> (DATE_TRUNC('month' , snapshot_date) + interval '1 month' - interval '1 day')::DATE
    ;

    update sandbox_pd.arr set  mcid = trim(COALESCE(nullif(end_customer_master_customer_id,''), nullif(parent_master_customer_id,'')))
                         --select * from sandbox_pd.arr
    where 1=1
      and coalesce(mcid,'') = ''
    ;

    --##########################################################
    --
    --##########################################################
    drop table if exists temp_mcid_mappings;

    create temp table temp_mcid_mappings as
    with temp1 as (
        select distinct mcid,sku,snapshot_date, a.product_group,a.product_family,a.product_name from sandbox_pd.arr a where snapshot_date >= '2023-04-30'
    )
       ,temp2 as
        (select distinct a.mcid,
                         a.sku,
                         a.snapshot_date, a.product_group,a.product_family,a.product_name,
                         row_number() over (partition by a.mcid,a.sku order by a.snapshot_date desc) as rnk
         FROM sandbox_pd.monthly_metrics a
                  join temp1 b on a.mcid = b.mcid and a.sku = b.sku
         WHERE a.snapshot_date between '2023-01-01' and '2023-03-31'
           and a.snapshot_date not in ('2023-05-04','2023-05-11','2023-05-18','2023-05-25','2023-06-13','2023-06-29')
           AND lower(line_type) IN (
                                    'recurring',
                                    'inflight',
                                    'gmbh',
                                    'usage'
             )
           and (lower(a.sku) = 'sub-lease'
             or lower(a.sku) ilike '%consult%'
             or lower(a.sku) ILIKE 'EDU%'
             or lower(a.product_group) like '%educat%'
             or lower(a.product_group) like '%professional%'
             or lower(a.product_group) = 'other services'
             or lower(a.product_group) = 'cloud : rental license'
             or lower(a.product_name) in ('expert services', 'expense')
             --or lower(a.sku) in (select lower(a.sku) from ufdm_grey.sku_mapping_allocation where product_category is not null)
             ))
    select a.*,b.snapshot_date as snapshot_date_previous,b.product_name as product_name_previous,b.product_group as product_group_previous,b.product_family as product_family_previous --,c.product_name,c.product_group,c.product_family
    from temp1 a
             join temp2 b on a.mcid = b.mcid and a.sku = b.sku and b.rnk = 1
    --join sandbox_pd.arr c on a.mcid = c.mcid and a.sku = c.sku and b.snapshot_date = c.snapshot_date
    --where a.mcid = '791c7910-594f-e411-9f63-0050568d2da8' and a.sku = 'FSNS'
    ;

    drop table if exists sandbox_pd.mcid_sku_exclusions_manual;
    create table sandbox_pd.mcid_sku_exclusions_manual as select * from ufdm_grey.mcid_sku_exclusions_manual;

    insert into sandbox_pd.mcid_sku_exclusions_manual
    select a.* from temp_mcid_mappings a
                        left join sandbox_pd.mcid_sku_exclusions_manual b on a.mcid = b.mcid and a.sku = b.sku
    where b.mcid is null
    ;

    delete from sandbox_pd.arr a
        using sandbox_pd.mcid_sku_exclusions_manual b
                --SELECT * FROM sandbox_pd.arr a,ufdm_grey.mcid_sku_exclusions_manual b
    where a.mcid = b.mcid and a.snapshot_date = b.snapshot_date and a.sku = b.sku
    ;

    delete from sandbox_pd.arr
                --SELECT * FROM sandbox_pd.arr
    where mcid =  '793d6093-1129-e8a8-5cbe-7e8c8a8f46a0' and snapshot_date > '2023-01-30' and sku in ('ISS-IC-MSS-3','ISS-IC-MSS-1');

    --####################################################
    --MANUAL UPDATES
    --####################################################
    update sandbox_pd.arr
    set baseline_arr_local_currency = arr_usd_ccfx,modified_comments = concat(coalesce(modified_comments,''),'baseline_arr_local_currency updated from ',baseline_arr_local_currency::text ,' to ',arr_usd_ccfx)
        --SELECT * FROM  ufdm_blue.monthly_metrics
    where mcid = '00ec9665-e386-18a4-2d14-40b5803bac2c'
      and arr_usd_ccfx > 0
      and snapshot_date between '2022-01-31' and '2022-06-30'
      and product_family = 'Recurring: Cloud: Intelligence Cloud: CDP (incl. Visitor Intelligence)'
      and baseline_currency = 'USD'
    ;

    update sandbox_pd.arr set baseline_arr_local_currency = arr_usd_ccfx,modified_comments = concat(coalesce(modified_comments,''),'baseline_arr_local_currency updated from ',baseline_arr_local_currency::text ,' to ',arr_usd_ccfx)
                        --select baseline_arr_local_currency , arr_usd_ccfx from sandbox_pd.arr
    where mcid = '00ec9665-e386-18a4-2d14-40b5803bac2c' and arr_usd_ccfx > 0
      and snapshot_date between '2022-01-31' and '2022-06-30'
      and product_family = 'Recurring: Cloud: Intelligence Cloud: CDP (incl. Visitor Intelligence)' and baseline_currency = 'USD'
    ;

    update sandbox_pd.arr
    set baseline_arr_local_currency = arr_usd_ccfx
      ,modified_comments = concat(coalesce(modified_comments,''),'baseline_arr_local_currency updated from ',baseline_arr_local_currency::text ,' to ',arr_usd_ccfx)
      --select baseline_arr_local_currency , arr_usd_ccfx,snapshot_date,baseline_currency,mcid from sandbox_pd.arr
    where mcid in ('18f4943f-49d6-73a0-0262-eba2c3da5729','30f35937-33a5-e811-814d-70106fa55dc1','683a93ed-727c-8e7f-ab67-44908a6da78d')
      and arr_usd_ccfx > 0
      and snapshot_date between '2022-09-30' and '2023-03-31'
      and baseline_currency = 'USD'
      and product_family = 'Recurring: Intelligence Cloud: Marketing Orchestration'
      and baseline_arr_local_currency <> arr_usd_ccfx;

    update sandbox_pd.arr
    set baseline_arr_local_currency = arr_usd_ccfx
      ,modified_comments = concat(coalesce(modified_comments,''),'baseline_arr_local_currency updated from ',baseline_arr_local_currency::text ,' to ',arr_usd_ccfx)
      --select  baseline_arr_local_currency , arr_usd_ccfx,baseline_currency from sandbox_pd.arr
    where mcid = 'f537f1ea-f32b-a960-a59d-3138a1530783' and snapshot_date between  '2022-09-30' and '2022-12-31'
      and baseline_currency = 'USD'
      and baseline_arr_local_currency <> arr_usd_ccfx
    ;

    update sandbox_pd.arr
    set baseline_arr_local_currency = arr_usd_ccfx
      ,modified_comments = concat(coalesce(modified_comments,''),'baseline_arr_local_currency updated from ',baseline_arr_local_currency::text ,' to ',arr_usd_ccfx)
      --select * from sandbox_pd.arr
    where mcid = '07454906-409e-dfdb-091a-69fa44d8c612' and snapshot_date between  '2021-08-31' and '2021-10-31' and baseline_currency = 'USD'
      and baseline_arr_local_currency <> arr_usd_ccfx
    ;

    update sandbox_pd.arr
    set baseline_arr_local_currency = arr_usd_ccfx,modified_comments = concat(coalesce(modified_comments,''),'baseline_arr_local_currency updated from ',baseline_arr_local_currency::text ,' to ',arr_usd_ccfx)
        --select * from sandbox_pd.arr
    where baseline_arr_local_currency - arr_usd_ccfx not between  -1 and 1
      and baseline_currency = 'USD' and arr_usd_ccfx > 0
      and baseline_arr_local_currency <> arr_usd_ccfx
    ;

    update sandbox_pd.arr set modified_comments = concat(coalesce(modified_comments,''),'baseline currency updated to USD'), baseline_currency = 'USD'
                        --select baseline_arr_local_currency - arr_usd_ccfx ,* from sandbox_pd.arr
    where baseline_arr_local_currency - arr_usd_ccfx between  -1 and 1
      and baseline_currency <> 'USD' and arr_usd_ccfx > 0
      and line_type = 'FOpti product; Experimentation';

    /*
    update sandbox_pd.arr
    set end_customer_master_customer_id = '167c66c7-dc2d-a579-7da9-b9268e730097'
      , mcid = '167c66c7-dc2d-a579-7da9-b9268e730097'
      ,modified_comments = concat(modified_comments,'; end mcid updated from ',coalesce(end_customer_master_customer_id,''),' to 167c66c7-dc2d-a579-7da9-b9268e730097')
      --select mcid,parent_master_customer_id,end_customer_master_customer_id,snapshot_date,c_name,modified_comments  from sandbox_pd.arr
    where (mcid = '167c66c7-dc2d-a579-7da9-b9268e730097' or c_name in ('C7550','C7550:7713'))
      and end_customer_master_customer_id <> '167c66c7-dc2d-a579-7da9-b9268e730097'
    ;
    */

END;
$function$
;
