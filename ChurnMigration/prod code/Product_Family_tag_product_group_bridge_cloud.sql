-- Product Family tag product group bridge cloud version 

create or replace function ryzlan.sp_populate_product_family_TAG_churn_migration_cloud(var_period text) returns void
    language plpgsql
as
$$

BEGIN

    DELETE from ryzlan.sst_product_bridge_product_family_Tag_CM_cloud
    where evaluation_period  = var_period ;

    
drop table if exists sst_temp;
    create temp table sst_temp as
    Select
        * ,
        CASE WHEN migration_from = 'Y' THEN 'Legacy'
             WHEN migration_to = 'Y' THEN 'Named'
             ELSE 'Usual'
        END AS tag
    from ufdm.sst ;

    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    --SST product Bridge
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    drop table if exists prior_period_customer_arr_tmp;

    create temp table prior_period_customer_arr_tmp as
    SELECT snapshot_date,
           a.mcid as master_customer_id,
           product_family as product_family,
           temp_product_group_li as product_group ,
           a.base_currency as baseline_currency,
           tag ,
           max(coalesce(a.end_name, a.parent_name)) as end_customer,
           sum(arr) AS arr_usd_ccfx,
           sum(baseline_arr_local_currency) AS arr_lcu
    FROM sst_temp a
    WHERE 1=1
      AND snapshot_date = (SELECT prior_period from ufdm_grey.periods WHERE evaluation_period = var_period)
      AND a.overage_flag IS DISTINCT FROM 'Y'
    GROUP BY 1, 2, 3 ,4,5,6
    ;

    drop table if exists current_period_customer_arr_tmp;

    create temp table current_period_customer_arr_tmp as
    SELECT  snapshot_date,
            a.mcid as master_customer_id,
            product_family as product_family,
            temp_product_group_li as product_group ,
            a.base_currency as baseline_currency,
            tag,
            max(coalesce(a.end_name, a.parent_name)) as end_customer,
            sum(arr) AS arr_usd_ccfx,
            sum(baseline_arr_local_currency) AS arr_lcu
    FROM sst_temp a
    WHERE  1=1
      AND snapshot_date = (SELECT current_period from ufdm_grey.periods WHERE evaluation_period = var_period)
      AND a.overage_flag IS DISTINCT FROM 'Y'
    GROUP BY 1, 2, 3 ,4,5,6
    ;

    drop table if exists customer_level_arr_tmp;

    create temp table customer_level_arr_tmp as
    SELECT c1.master_customer_id AS current_cust_id,
           c2.master_customer_id AS prior_cust_id,
           c1.end_customer as current_end_customer,
           c2.end_customer as prior_end_customer,
           c2.snapshot_date AS prior_period,
           c1.snapshot_date AS current_period,
           c1.baseline_currency as current_baseline_currency,
           c2.baseline_currency as prior_baseline_currency,
           COALESCE(c1.baseline_currency, c2.baseline_currency) AS baseline_currency ,
           c1.tag as current_tag ,
           c2.tag as prior_tag  ,
           c2.product_family AS prior_product_family,
           c1.product_family AS current_product_family,
           c2.product_group as prior_product_group ,
           c1.product_group as current_product_group ,
           coalesce(c1.arr_usd_ccfx,0) AS current_arr_usd_ccfx,
           coalesce(c2.arr_usd_ccfx,0) AS prior_arr_usd_ccfx,
           coalesce(c1.arr_lcu,0) AS current_arr_lcu,
           coalesce(c2.arr_lcu,0) AS prior_arr_lcu
           --c3.arr AS prior2_arr, --WIP
    FROM current_period_customer_arr_tmp c1
             FULL OUTER JOIN prior_period_customer_arr_tmp c2
                             ON c1.master_customer_id = c2.master_customer_id
                                    and c1.product_family = c2.product_family
                                    and c1.product_group = c2.product_group
                                    and c1.baseline_currency = c2.baseline_currency
                                    and c1.tag = c2.tag
    ;
    





drop table if exists arr_product_bridge_tmp;

    create temp table arr_product_bridge_tmp AS
    SELECT
        per.evaluation_period,
        cla.prior_period,
        cla.current_period,
        cla.current_cust_id as current_master_customer_id,
        cla.prior_cust_id as prior_master_customer_id,
        coalesce(cla.current_cust_id, cla.prior_cust_id) as mcid,
        cla.current_product_family as current_product_family,
        cla.prior_product_family as prior_product_family,
        cla.current_product_group ,
        cla.prior_product_group ,
        cla.current_tag ,
        cla.prior_tag ,
        cla.current_end_customer,
        cla.prior_end_customer,
        cla.baseline_currency,
        round((coalesce(cla.current_arr_usd_ccfx::numeric,0)),2) as current_arr_usd_ccfx,
        round((coalesce(cla.prior_arr_usd_ccfx::numeric,0)),2) as prior_arr_usd_ccfx,
        round((coalesce(cla.current_arr_usd_ccfx::numeric,0) - coalesce(cla.prior_arr_usd_ccfx::numeric,0)),2)  AS product_arr_change_ccfx,
        round((coalesce(cla.current_arr_lcu::numeric,0)),2) as current_arr_lcu,
        round((coalesce(cla.prior_arr_lcu::numeric,0)),2) as prior_arr_lcu,
        round((coalesce(cla.current_arr_lcu::numeric,0) - coalesce(cla.prior_arr_lcu::numeric,0)),2)  AS product_arr_change_lcu,
        CASE
            WHEN ((coalesce (cla.prior_arr_usd_ccfx ,0) = 0 or coalesce (cla.prior_arr_usd_ccfx ,0) = 0.00)
                --OR (cla.prior_arr_usd_ccfx = 0 and clp.prior_product_family_agg is null))
                AND cla.current_arr_usd_ccfx > 0 )
                THEN 'New'
            WHEN
                    cla.current_arr_usd_ccfx - cla.prior_arr_usd_ccfx  BETWEEN -1 and 1
                THEN 'Flat'
            WHEN
                        cla.current_arr_usd_ccfx - cla.prior_arr_usd_ccfx > 1
                THEN 'Up Sell'
            WHEN
                            cla.current_arr_usd_ccfx - cla.prior_arr_usd_ccfx < - 1
                    AND cla.current_arr_usd_ccfx > 0
                THEN 'Partial Churn' -- different products, lower ARR
            WHEN cla.prior_arr_usd_ccfx > 0
                AND (cla.current_arr_usd_ccfx = 0 OR cla.current_arr_usd_ccfx IS NULL)
                THEN 'Churn'
            ELSE 'N/A'
            END AS product_bridge
    FROM customer_level_arr_tmp  cla
             CROSS JOIN ufdm_grey.periods per
    WHERE 1 = 1
      AND per.evaluation_period = var_period
    ;

    --Downgrade
    update arr_product_bridge_tmp
    set product_bridge='Downgrade'
    where product_bridge='Partial Churn' ;

    --Cross-sell
    WITH PG_F_C AS (
        SELECT
            mcid,
            COUNT(distinct product_bridge)  as product_family_count
        FROM arr_product_bridge_tmp
        WHERE current_arr_usd_ccfx > 0 and evaluation_period=var_period
        group by mcid,evaluation_period
    )
    UPDATE arr_product_bridge_tmp AS t
    SET product_bridge = CASE
                             WHEN pfc.product_family_count >1
                                 THEN 'Cross-sell'
                             ELSE product_bridge
        END
    FROM PG_F_C AS pfc
    WHERE t.mcid = pfc.mcid
      AND t.product_bridge = 'New'
      and t.evaluation_period=var_period;

    -------
    --Downsell
    WITH PG_F_C AS (
        SELECT
            mcid,
            COUNT( distinct product_bridge)  as product_family_count
        FROM arr_product_bridge_tmp
        WHERE prior_arr_usd_ccfx > 0 and evaluation_period=var_period
        group by mcid,evaluation_period
    )
         --  select * from PG_F_C

    UPDATE arr_product_bridge_tmp AS t
    SET product_bridge = CASE
                             WHEN pfc.product_family_count >1
                                 THEN 'Downsell'
                             ELSE product_bridge
        END
    FROM PG_F_C AS pfc
    WHERE t.mcid = pfc.mcid
      AND t.product_bridge = 'Churn'
      and t.evaluation_period=var_period;

    --#############################################
    --WIP/WINBACK
    --#############################################
    drop table if exists arr_new_products_tmp;

    create temp table arr_new_products_tmp AS
    select a.current_master_customer_id as mcid,
           a.current_product_family as product_family,
           a.current_product_group as product_group,
           a.current_tag as tag ,
           a.current_period as snapshot_date,
           a.current_arr_usd_ccfx as arr_at_new,
           a.current_arr_lcu as arr_lcu_at_new,
           baseline_currency
    from arr_product_bridge_tmp a
    where product_bridge in ('New','Cross-sell')
    ;

    --get most recent postivie arr for above new product which should have been churned
    drop table if exists arr_churned_products_tmp;

    create temp table arr_churned_products_tmp AS
    with temp as
             (
                 select b.snapshot_date
                      , b.mcid
                      , b.product_family as product_family
                      , b.temp_product_group_li as product_group
                      , b.tag
                      , a.baseline_currency
                      , a.snapshot_date as snapshot_date_at_new
                      , sum(b.arr) as arr_at_churn
                      , sum(b.baseline_arr_local_currency) as arr_lcu_at_churn
                      , sum(a.arr_at_new) as arr_at_new
                      , sum(a.arr_lcu_at_new) as arr_lcu_at_new
                      , row_number() over (partition by b.mcid,b.product_family,b.tag  order by b.snapshot_date desc) as rnk
                 from arr_new_products_tmp a
                          join (select
                                    sb.mcid,
                                    sb.snapshot_date,
                                    sb.overage_flag,
                                    sb.product_family,
                                    sb.temp_product_group_li,
                                    sb.tag ,
                                    sb.base_currency ,
                                    sum(arr) as arr ,
                                    sum(baseline_arr_local_currency) as baseline_arr_local_currency
                                from sst_temp sb
                                group by 1,2,3,4,5,6,7
                 ) b
                               on a.mcid = b.mcid
                                   --and a.product_family = b.product_family
                                   and a.product_family = b.product_family
                                   and a.product_group = b.temp_product_group_li
                                   and a.tag = b.tag
                                   and a.baseline_currency = b.base_currency
                 where b.snapshot_date < a.snapshot_date
                   and b.overage_flag ilike '%N%'
                   and b.arr > 0
                 group by 1,2,3,4,5,6,7
             )
    select *
         ,(DATE_PART('year', snapshot_date_at_new::date) - DATE_PART('year', snapshot_date::date)) * 12 +
          (DATE_PART('month', snapshot_date_at_new::date) - DATE_PART('month', snapshot_date::date)) as months_diff
         ,case when arr_at_new > arr_at_churn then 'Upsell'
               else case when extract(day from snapshot_date_at_new::timestamp - (snapshot_date+INTERVAL '1 month')::date) <= 90 then 'Winback ST' else 'Winback LT' end
        end as product_bridge_new
         , arr_at_new - arr_at_churn as arr_diff
         , arr_lcu_at_new - arr_lcu_at_churn as arr_lcu_diff
         , extract(day from snapshot_date_at_new::timestamp - (snapshot_date+INTERVAL '1 month')::date) as days_diff
         , snapshot_date as churn_period
    from temp
    where rnk = 1
      and extract(day from snapshot_date_at_new::timestamp - (snapshot_date+INTERVAL '1 month')::date) < 186
    ;

    -- create index if not exists nci_arr_churned_products_tmp_tmp_composite on arr_churned_products_tmp(mcid,product_family,baseline_currency,snapshot_date_at_new) include(arr_at_new, arr_at_churn);

    INSERT INTO ryzlan.sst_product_bridge_product_family_Tag_CM_cloud
    (
        evaluation_period,
        prior_period,
        current_period,
        current_end_customer,
        prior_end_customer,
        mcid,
        current_master_customer_id,
        Prior_master_customer_id,
        current_product_family,
        prior_product_family,
        current_product_group,
        prior_product_group,
        current_tag ,
        prior_tag ,
--         "name",
        prior_period_product_arr_usd_ccfx,
        current_period_product_arr_usd_ccfx,
        product_arr_change_ccfx,
        prior_period_product_arr_lcu,
        current_period_product_arr_lcu,
        product_arr_change_lcu,
        product_bridge,
        Winback_period_days,
        Wip_Flag,
        price_increase_amount,
        subsidiary_entity_name,
        churn_period,
        currency_code
    )
    SELECT
        a.evaluation_period,
        a.prior_period,
        a.current_period,
        a.current_end_customer,
        a.prior_end_customer,
        a.mcid,
        a.current_master_customer_id,
        a.Prior_master_customer_id,
        a.current_product_family,
        a.prior_product_family,
        a.current_product_group,
        a.prior_product_group,
        a.current_tag ,
        a.prior_tag,
--         "name",
        round(a.prior_arr_usd_ccfx::numeric, 2) AS prior_period_customer_arr_usd_ccfx,
        --round(a.current_arr_usd_ccfx::numeric,2) AS current_period_customer_arr_usd_ccfx,
        case when b.mcid is not null then case when b.arr_at_new > b.arr_at_churn then b.arr_at_churn else b.arr_at_new end --round(b.arr_at_churn::numeric,2)
             else round(a.current_arr_usd_ccfx::numeric,2)
            end as current_period_customer_arr_usd_ccfx,
        case when b.mcid is not null then case when b.arr_at_new > b.arr_at_churn then b.arr_at_churn else b.arr_at_new end
             else a.product_arr_change_ccfx
            end as product_arr_change_ccfx,
        ------------------------lcu----------------------------
        round(a.prior_arr_lcu::numeric, 2) AS prior_period_product_arr_lcu,
        case when b.mcid is not null then case when b.arr_lcu_at_new > b.arr_lcu_at_churn then b.arr_lcu_at_churn else b.arr_lcu_at_new end --round(b.arr_lcu_at_churn::numeric,2)
             else round(a.current_arr_lcu::numeric,2)
            end as current_period_product_arr_lcu,
        case when b.mcid is not null then case when b.arr_lcu_at_new > b.arr_lcu_at_churn then b.arr_lcu_at_churn else b.arr_lcu_at_new end
             else a.product_arr_change_lcu
            end as product_arr_change_lcu,
        case when b.mcid is not null then case when b.days_diff <= 90 then 'Winback ST' else 'Winback LT' end --b.product_bridge_new --'Winback' --/WIP
             else a.product_bridge
            end as product_bridge,
        b.days_diff as Winback_period_days,
        case when b.days_diff <= 90 then 'Y' else 'N' end as Wip_Flag,
        null::numeric as price_increase_amount,
        null::text as subsidiary_entity_name,
        b.churn_period,
        a.baseline_currency
    FROM arr_product_bridge_tmp a
             left join arr_churned_products_tmp b
                       on a.current_master_customer_id = b.mcid
                           and a.current_product_family = b.product_family
                           and a.current_product_group = b.product_group
                           and a.current_tag = b.tag
                           and a.baseline_currency = b.baseline_currency
                           and a.current_period = b.snapshot_date_at_new

    union all

    SELECT
        a.evaluation_period,
        a.prior_period,
        a.current_period,
        a.current_end_customer,
        a.prior_end_customer,
        a.mcid,
        a.current_master_customer_id,
        a.Prior_master_customer_id,
        a.current_product_family,
        a.prior_product_family,
        a.current_product_group,
        a.prior_product_group,
        a.current_tag ,
        a.prior_tag ,
--         "name",
        round(a.prior_arr_usd_ccfx::numeric, 2) AS prior_period_customer_arr_usd_ccfx,
        b.arr_diff as current_period_customer_arr_usd_ccfx,
        b.arr_diff,
        round(a.prior_arr_lcu::numeric, 2) AS prior_period_product_arr_lcu,
        b.arr_lcu_diff as current_period_product_arr_lcu,
        b.arr_lcu_diff,
        'Up Sell' as product_bridge,
        null as Winback_period_days,
        null as Wip_Flag,
        null::numeric as price_increase_amount,
        null::text as subsidiary_entity_name,
        null::date as churn_period,
        a.baseline_currency
    FROM arr_product_bridge_tmp a
             join arr_churned_products_tmp b
                  on a.current_master_customer_id = b.mcid
                      and a.current_product_family = b.product_family
                      and a.current_product_group = b.product_group
                      and a.current_tag = b.tag
                      and a.baseline_currency = b.baseline_currency
                      and a.current_period = b.snapshot_date_at_new
    where b.arr_at_new > b.arr_at_churn
    ;

    RAISE NOTICE 'Running customer bridge update on sst product bridge...';

    --update customer bridge and subsidiary entity
    update ryzlan.sst_product_bridge_product_family_Tag_CM_cloud a
    set customer_bridge = b.customer_bridge
    from sandbox_pd.sst_customer_bridge b
    where 1=1
      and a.evaluation_period = b.evaluation_period
      and a.mcid =b.mcid
      and a.evaluation_period = var_period
    ;

    RAISE NOTICE 'Running subsidiary entity name insert on sst product bridge...';

    drop table if exists sub_entity_tmp;

    create temp table sub_entity_tmp as
        --update subsidiary_entity_name
    with mcid_list as
        (
            select distinct mcid as master_customer_id
            from arr_product_bridge_tmp
            where evaluation_period = var_period
        )
       ,total_arr as
        (
            select a.mcid  as mcid, a.snapshot_date ,a.subsidiary_entity_name , sum(a.arr) as total_arr
            from sst_temp a
                     join mcid_list b on a.mcid = b.master_customer_id
                and a.snapshot_date in
                    (
                        SELECT prior_period from ufdm_grey.periods WHERE evaluation_period = var_period
                        union SELECT current_period from ufdm_grey.periods WHERE evaluation_period = var_period
                    )
            group by a.mcid  , a.snapshot_date ,a.subsidiary_entity_name
        )
       ,sub_entity as
        (
            select *,
                   row_number () over (partition by mcid order by total_arr desc) as rnk
            from total_arr
        )
    select *
    from sub_entity
    where rnk = 1;

    RAISE NOTICE 'Running sub entity update on sst product bridge...';

    create index nci_sub_entity_tmp_mcid on sub_entity_tmp(mcid);

    update ryzlan.sst_product_bridge_product_family_Tag_CM_cloud a
    set subsidiary_entity_name  = b.subsidiary_entity_name
    from sub_entity_tmp b
    where a.mcid = b.mcid
      and a.evaluation_period = var_period
    ;

    RAISE NOTICE 'Running Price Increase update on sst product bridge...';

    --Price Increase updates
    update ryzlan.sst_product_bridge_product_family_Tag_CM_cloud
    set product_bridge = 'CPI'
    where product_bridge = 'Up Sell'
      and prior_period_product_arr_usd_ccfx > 0
      and ((product_arr_change_ccfx / prior_period_product_arr_usd_ccfx) * 100)::numeric <= case when current_period < '2023-01-01' then 5.5 else 10.5 end
      and evaluation_period = var_period
    ;




END;
$$

TRUNCATE TABLE ryzlan.sst_product_bridge_product_family_Tag_CM_cloud


select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2019M01');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2019M02');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2019M03');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2019M04');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2019M05');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2019M06');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2019M07');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2019M08');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2019M09');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2019M10');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2019M11');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2019M12');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2020M01');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2020M02');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2020M03');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2020M04');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2020M05');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2020M06');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2020M07');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2020M08');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2020M09');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2020M10');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2020M11');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2020M12');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2021M01');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2021M02');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2021M03');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2021M04');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2021M05');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2021M06');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2021M07');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2021M08');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2021M09');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2021M10');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2021M11');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2021M12');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2022M01');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2022M02');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2022M03');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2022M04');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2022M05');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2022M06');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2022M07');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2022M08');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2022M09');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2022M10');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2022M11');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2022M12');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2023M01');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2023M02');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2023M03');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2023M04');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2023M05');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2023M06');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2023M07');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2023M08');
select ryzlan.sp_populate_product_family_TAG_churn_migration_cloud('2023M09');



