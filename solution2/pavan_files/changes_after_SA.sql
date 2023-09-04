CREATE OR REPLACE FUNCTION sandbox_pd.sp_populate_sst_updates_manual_after_sensitivity_analysis()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

BEGIN

    ---##########################################
    --APPLY SHELL CUSTOMER FIXES
    ---##########################################
    update sandbox_pd.sst
    set arr = '218333.33',baseline_arr_local_currency ='218333.33'
      ,modified_comments = concat(coalesce(modified_comments,''),'==> arr and lcu updated from ',arr::text)
      --select * from sandbox_pd.sst
    where mcid= '8ef0bf48-f16c-72ce-6356-41f25b5aaaf2' and snapshot_date between '2022-09-30' and '2022-09-30' and arr > 0 and arr <> '218333.33';

    update sandbox_pd.sst
    set arr = '218333.33',baseline_arr_local_currency ='218333.33'
      ,modified_comments = concat(coalesce(modified_comments,''),'==> arr and lcu updated from ',arr::text)
      --select * from sandbox_pd.sst
    where mcid= '8ef0bf48-f16c-72ce-6356-41f25b5aaaf2' and snapshot_date between '2022-10-31' and '2022-12-31' and arr > 0 and arr <> '218333.33';

    update sandbox_pd.sst
    set arr = '252333.33',baseline_arr_local_currency ='252333.33'
      ,modified_comments = concat(coalesce(modified_comments,''),'==> arr and lcu updated from ',arr::text)
      --select * from sandbox_pd.sst
    where mcid= '8ef0bf48-f16c-72ce-6356-41f25b5aaaf2' and snapshot_date between '2023-01-31' and '2023-06-30' and arr > 0 and arr <> '252333.33';

    ---##########################################
    --UPDATE baseline vs arr discrepencies
    ---##########################################
    update sandbox_pd.sst a
    set baseline_arr_local_currency = (a.arr / b.fx_rate)
      ,modified_comments  =  concat(coalesce(modified_comments,';') , 'lcu update from ',baseline_arr_local_currency::Text, ' to ',(a.arr / b.fx_rate)::Text)
    from (select trans_cur,fx_rate from ufdm_grey.arr_fx_rates where fx_type = 'ccfx') b
         --select * from sandbox_pd.sst a, (select trans_cur,fx_rate from ufdm_grey.arr_fx_rates where fx_type = 'ccfx') b
    where a.base_currency = b.trans_cur
      and coalesce(a.arr,0) - (coalesce(a.baseline_arr_local_currency,0) * b.fx_rate) not between  -1 and 1
    ;
    ---##########################################
    -- update product group and product solution based on SKU
    ---##########################################
    /*
    SELECT a.snapshot_date,a.sku, count(*)
        --distinct a.sku,a.new_product,a.new_product_solution,a.new_line_of_business,a.new_product_line,a.updated_product_group,tmjs.*
    FROM sandbox_pd.sst a
    LEFT JOIN ufdm_grey.product_hierarchy_mappings tmjs ON tmjs."Product Code" = a.sku
    where a.new_product is null
    group by a.snapshot_date,a.sku
    ;

    SELECT distinct a.sku,a.new_product,a.new_product_solution,a.new_line_of_business,a.new_product_line,a.updated_product_group,tmjs.*
    FROM sandbox_pd.sst a
             LEFT JOIN ufdm_grey.product_hierarchy_mappings tmjs ON tmjs."Product Code" = a.sku
    where a.updated_product_group is null
    ;
    */

    update sandbox_pd.sst a
    set new_product = tmjs."NEW: Product"
      ,updated_product_group = tmjs."Updated: Product Group"
      ,new_product_line = tmjs."NEW:  Product Line"
      ,new_product_solution = tmjs."NEW: Product Solution"
      ,new_line_of_business = tmjs."NEW: Line of Business"
      ,new_line_of_business_sub_category = tmjs."NEW: Line of Business Subcategory"
      ,modified_comments = concat(coalesce(modified_comments,''),'==> new_product_hierarchy columns updated from blank based on SKU')
    FROM ufdm_grey.product_hierarchy_mappings tmjs
    WHERE tmjs."Product Code" = a.sku
    AND a.updated_product_group is null
    ;

    ---##########################################
    -- update product group and product solution based on new mapping tables
    ---##########################################
    drop table if exists tmp_pf_split;

    create temporary table tmp_pf_split as
    select a.snapshot_date,ultimate_parent_id,ultimate_parent_name,duns_name,duns_number,parent_duns_name,parent_duns_number,domesticultimatedunsnumber,globalultimatedunsnumber
         ,new_product_solution,new_product_line
         ,b.pg_mapping_1 as updated_product_group
         ,new_product,new_line_of_business,new_line_of_business_sub_category,c_name,parent_ns_id,end_ns_id
         ,name,parent_name,end_name,mcid,parent_mcid,end_mcid,subsidiary_entity_name,overage_flag,segment,region
         ,a.product_family
         ,base_currency
         ,cc_fx_rate,fx_date
         ,arr,baseline_arr_local_currency
         ,arr * (b.pg_arr_percentage::numeric) as arr_new
         ,baseline_arr_local_currency * (b.pg_arr_percentage::numeric) as baseline_arr_local_currency_new
         ,dw_modified_date,dw_created_date,parent_sf_id,parent_sf_name,record_source,modified_comments
         ,cohort_actions,id
         ,pg_mapping_1,pg_arr_percentage,subsidairy
    from sandbox_pd.sst a
             cross join ufdm_grey.sst_product_family_porduct_group_mappings_manual b
    where a.product_family = b.product_family
      and a.product_family in ('Recurring: Cloud: Other Bookings: Other Bookings')
      and (coalesce(updated_product_group,'') = '')
      and coalesce(arr,0) > 0

    union all

    select a.snapshot_date,ultimate_parent_id,ultimate_parent_name,duns_name,duns_number,parent_duns_name,parent_duns_number,domesticultimatedunsnumber,globalultimatedunsnumber
         ,new_product_solution,new_product_line
         ,b.pg_mapping_1 as updated_product_group
         ,new_product,new_line_of_business,new_line_of_business_sub_category,c_name,parent_ns_id,end_ns_id
         ,name,parent_name,end_name,mcid,parent_mcid,end_mcid,subsidiary_entity_name,overage_flag,segment,region
         ,a.product_family
         ,base_currency
         ,cc_fx_rate,fx_date
         ,arr,baseline_arr_local_currency
         ,arr * (b.pg_arr_percentage::numeric) as arr_new
         ,baseline_arr_local_currency * (b.pg_arr_percentage::numeric) as baseline_arr_local_currency_new
         ,dw_modified_date,dw_created_date,parent_sf_id,parent_sf_name,record_source,modified_comments
         ,cohort_actions,id
         ,pg_mapping_1,pg_arr_percentage,subsidairy
    from sandbox_pd.sst a
             cross join ufdm_grey.sst_product_family_porduct_group_mappings_manual b
    where a.product_family = b.product_family
      and a.product_family in ('Recurring: Subscription License',
                               'Non-Recurring: Perpetual License')
      and (coalesce(updated_product_group,'') = '')
      and coalesce(arr,0) > 0
      and coalesce(a.subsidiary_entity_name,'') ilike '%insite%'
      and b.subsidairy = 'Insite'

    union all

    select a.snapshot_date,ultimate_parent_id,ultimate_parent_name,duns_name,duns_number,parent_duns_name,parent_duns_number,domesticultimatedunsnumber,globalultimatedunsnumber
         ,new_product_solution,new_product_line
         ,b.pg_mapping_1 as updated_product_group
         ,new_product,new_line_of_business,new_line_of_business_sub_category,c_name,parent_ns_id,end_ns_id
         ,name,parent_name,end_name,mcid,parent_mcid,end_mcid,subsidiary_entity_name,overage_flag,segment,region
         ,a.product_family
         ,base_currency
         ,cc_fx_rate,fx_date
         ,arr,baseline_arr_local_currency
         ,arr * (b.pg_arr_percentage::numeric) as arr_new
         ,baseline_arr_local_currency * (b.pg_arr_percentage::numeric) as baseline_arr_local_currency_new
         ,dw_modified_date,dw_created_date,parent_sf_id,parent_sf_name,record_source,modified_comments
         ,cohort_actions,id
         ,pg_mapping_1
         ,pg_arr_percentage,subsidairy
    from sandbox_pd.sst a
             cross join ufdm_grey.sst_product_family_porduct_group_mappings_manual b
    where a.product_family = b.product_family
      and a.product_family in ('Recurring: Subscription License',
                               'Non-Recurring: Perpetual License')
      and  (coalesce(updated_product_group,'') = '')
      and coalesce(arr,0) > 0
      and coalesce(a.subsidiary_entity_name,'') not ilike '%insite%'
      and b.subsidairy = 'Not Insite'
    ;

    --update 1 to 1 mappings
    update sandbox_pd.sst a
    set updated_product_group = b.pg_mapping_1
      ,modified_comments = concat(coalesce(modified_comments,''),'==> updated_product_group updated from blank')
    from ufdm_grey.sst_product_family_porduct_group_mappings_manual b
    where 1=1
      and a.product_family = b.product_family
      and (coalesce(updated_product_group,'') = '')
      and coalesce(arr,0) > 0
      and a.product_family not in ('Recurring: Cloud: Other Bookings: Other Bookings','Recurring: Subscription License','Non-Recurring: Perpetual License')
    ;

    --delete multi product group records
    delete from sandbox_pd.sst a
    where 1=1
      and (coalesce(updated_product_group,'') = '')
      and coalesce(arr,0) > 0
      and a.product_family in ('Recurring: Cloud: Other Bookings: Other Bookings','Recurring: Subscription License','Non-Recurring: Perpetual License')
    ;

    --insert split records
    insert into sandbox_pd.sst
    (snapshot_date, ultimate_parent_id, ultimate_parent_name, duns_name, duns_number, parent_duns_name, parent_duns_number, domesticultimatedunsnumber, globalultimatedunsnumber
    , new_product_solution, new_product_line, updated_product_group, new_product, new_line_of_business, new_line_of_business_sub_category, c_name, parent_ns_id, end_ns_id, name, parent_name, end_name, mcid, parent_mcid, end_mcid, subsidiary_entity_name, overage_flag, segment, region, product_family, base_currency, cc_fx_rate, fx_date, arr, baseline_arr_local_currency
    , dw_modified_date, dw_created_date, parent_sf_id, parent_sf_name, record_source, modified_comments, cohort_actions
    )
    select
        snapshot_date, ultimate_parent_id, ultimate_parent_name, duns_name, duns_number, parent_duns_name, parent_duns_number, domesticultimatedunsnumber, globalultimatedunsnumber
         , new_product_solution, new_product_line, updated_product_group, new_product, new_line_of_business, new_line_of_business_sub_category, c_name, parent_ns_id, end_ns_id
         , name, parent_name, end_name, mcid, parent_mcid, end_mcid, subsidiary_entity_name, overage_flag, segment, region, product_family
         , base_currency, cc_fx_rate, fx_date, arr_new, baseline_arr_local_currency_new
         , dw_modified_date, dw_created_date, parent_sf_id, parent_sf_name, record_source
         , concat(coalesce(modified_comments, ''), '; updated product group changed from null to ',
                  updated_product_group
        , case
              when subsidairy = 'Not Insite' or product_family in ('Recurring: Cloud: Other Bookings: Other Bookings')
                  then concat('; arr updated from ', arr::Text, ' to ', arr_new::text,
                              '; baseline_arr_local_currency updated from ',
                              baseline_arr_local_currency::Text, ' to ',
                              baseline_arr_local_currency_new::text
                  )
              else '' end
        ) as modified_comments
         , cohort_actions
    from tmp_pf_split
    ;

    --finally update product solution based on product group mappings
    /*
     select distinct a.updated_product_group,b.*
     from sandbox_pd.sst_pg_pf_updates a
              join ufdm_grey.sst_product_group_porduct_solution_mappings_manual b
                   on a.updated_product_group = b.product_group
     where 1=1 and (coalesce(new_product_solution,'') = '')
       and coalesce(arr,0) > 0
     ;
     */

    update sandbox_pd.sst a
    set new_product_solution = b.product_solution
      ,modified_comments = concat(coalesce(modified_comments,''),'==> new_product_solution updated from blank')
    from ufdm_grey.sst_product_group_porduct_solution_mappings_manual b
    where 1=1
      and  a.updated_product_group = b.product_group
      and (coalesce(new_product_solution,'') = '')
      and coalesce(arr,0) > 0
    ;

    --

END;
$function$
;
