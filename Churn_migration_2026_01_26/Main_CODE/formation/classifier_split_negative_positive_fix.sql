drop table if exists sandbox.churn_migration_classifiers_max_value_v2_int_split;
create table   sandbox.churn_migration_classifiers_max_value_v2_int_split as (
  with initial_table as (
    select *
    from ryzlan.sst_product_pathways_bridge_split
    where mcid is not null and mcid <> '-'
-- and mcid = '7183df70-0c08-e511-9afb-0050568d2da8'
-- and evaluation_period  = '2025M03'
  )
   -- 513810
--     select count(*) from initial_table ;
  , initial_table_2 as (
    select
    *,
    -- downgraded product
    case when current_pathways = 'Commerce Connect LI'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as downgraded_commerce_connect_li_in_current_date ,
    case when current_pathways = 'Commerce Connect M&S'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as downgraded_commerce_connect_ms_in_current_date ,
    case when current_pathways = 'Configured Commerce LI'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as downgraded_configured_commerce_li_in_current_date ,
    case when current_pathways = 'Configured Commerce M&S'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as downgraded_configured_commerce_ms_in_current_date ,
    case when current_pathways = 'Content Management PaaS LI'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as downgraded_content_management_paas_li_in_current_date ,
    case when current_pathways = 'Content Management PaaS M&S'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as downgraded_content_management_paas_ms_in_current_date ,
    case when current_pathways = 'EOL - Everweb'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as downgraded_eol_everweb_in_current_date ,
    case when current_pathways = 'EOL - Legacy Visitor Intelligence'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as downgraded_visitor_intelligence_in_current_date ,
    case when current_pathways = 'EOL - Legacy Ektron'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as downgraded_eol_legacy_ektron_in_current_date ,
    case  when current_pathways = 'Search & Navigation - Standalone'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as downgraded_search_navigation_in_current_date ,
    case when current_pathways = 'Configured Commerce Subscription'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as downgraded_configured_commerce_subs_in_current_date ,
    case when current_pathways = 'EOL - Community API'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as downgraded_eol_community_api_in_current_date ,
    case when current_pathways = 'Commerce Connect Subscription'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as downgraded_commerce_connect_subs_in_current_date ,
    case when current_pathways = 'Content Management PaaS Subscription'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as downgraded_content_mgmt_paas_subs_in_current_date ,

    -- Churned product
    case when prior_pathways = 'Commerce Connect LI'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx = 0 then 1
    else 0 end as churned_commerce_connect_li_in_current_date ,
    case when prior_pathways = 'Commerce Connect M&S'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx = 0 then 1
    else 0 end as churned_commerce_connect_ms_in_current_date ,
    case when prior_pathways = 'Configured Commerce LI'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx = 0 then 1
    else 0 end as churned_configured_commerce_li_in_current_date ,
    case when prior_pathways = 'Configured Commerce M&S'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx = 0 then 1
    else 0 end as churned_configured_commerce_ms_in_current_date ,
    case when prior_pathways = 'Content Management PaaS LI'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx = 0 then 1
    else 0 end as churned_content_management_paas_li_in_current_date ,
    case when prior_pathways = 'Content Management PaaS M&S'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx = 0 then 1
    else 0 end as churned_content_management_paas_ms_in_current_date ,
    case when prior_pathways = 'EOL - Everweb'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx = 0 then 1
    else 0 end as churned_eol_everweb_in_current_date ,
    case when prior_pathways = 'EOL - Legacy Visitor Intelligence'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx = 0 then 1
    else 0 end as churned_visitor_intelligence_in_current_date ,
    case when prior_pathways = 'EOL - Legacy Ektron'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx = 0 then 1
    else 0 end as churned_eol_legacy_ektron_in_current_date ,
    case when prior_pathways = 'Search & Navigation - Standalone'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx = 0 then 1
    else 0 end as churned_search_navigation_in_current_date ,
    case when prior_pathways = 'Configured Commerce Subscription'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx = 0 then 1
    else 0 end as churned_configured_commerce_subs_in_current_date,
    case when prior_pathways = 'EOL - Community API'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx = 0 then 1
    else 0 end as churned_community_api_in_current_date,
    case when prior_pathways = 'Commerce Connect Subscription'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx = 0 then 1
    else 0 end as churned_commerce_connect_subs_in_current_date,
    case when prior_pathways = 'Content Management PaaS Subscription'
    and product_arr_change_ccfx < 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx = 0 then 1
    else 0 end as churned_content_mgmt_paas_subs_in_current_date,

    -- added products
    case when current_pathways = 'Other'
    and product_arr_change_ccfx > 0
    and prior_period_product_arr_usd_ccfx = 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as added_other_in_current_date,
    case when current_pathways = 'Experience Creation'
    and product_arr_change_ccfx > 0
    and prior_period_product_arr_usd_ccfx = 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as added_experience_creation_in_current_date,
    case when current_pathways = 'Segmentation'
    and product_arr_change_ccfx > 0
    and prior_period_product_arr_usd_ccfx = 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as added_segmentation_in_current_date,
    -- increased products
    case when current_pathways = 'Other'
    and product_arr_change_ccfx > 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as increased_other_in_current_date,
    case when current_pathways = 'Experience Creation'
    and product_arr_change_ccfx > 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as increased_experience_creation_in_current_date,
    case when current_pathways= 'Segmentation'
    and product_arr_change_ccfx > 0
    and prior_period_product_arr_usd_ccfx > 0
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as increased_segmentation_in_current_date,
    -- current_date with arr
    case when current_pathways = 'Other'
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as other_in_current_date_with_arr,
    case when current_pathways = 'Experience Creation'
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as experience_creation_in_current_date_with_arr,
    case when current_pathways = 'Segmentation'
    and current_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as segmentation_in_current_date_with_arr,
    -- previous date with arr
    case when prior_pathways = 'Commerce Connect LI'
    and prior_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as commerce_connect_li_in_previous_date_with_arr,
    case when prior_pathways = 'Commerce Connect M&S'
    and prior_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as commerce_connect_ms_in_previous_date_with_arr,
    case when prior_pathways = 'Configured Commerce LI'
    and prior_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as configured_commerce_li_in_previous_date_with_arr,
    case when prior_pathways = 'Configured Commerce M&S'
    and prior_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as configured_commerce_ms_in_previous_date_with_arr,
    case when prior_pathways = 'Content Management PaaS LI'
    and prior_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as content_management_paas_li_in_previous_date_with_arr,
    case when prior_pathways = 'Content Management PaaS M&S'
    and prior_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as content_management_paas_ms_in_previous_date_with_arr,
    case when prior_pathways = 'EOL - Everweb'
    and prior_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as eol_everweb_in_previous_date_with_arr,
    case when prior_pathways = 'EOL - Legacy Visitor Intelligence'
    and prior_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as eol_legacy_visitor_intelligence_in_previous_date_with_arr,
    case when prior_pathways = 'EOL - Legacy Ektron'
    and prior_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as eol_legacy_ektron_in_previous_date_with_arr,
    case when prior_pathways = 'Search & Navigation - Standalone'
    and prior_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as search_navigation_standalone_in_previous_date_with_arr,
    case when prior_pathways = 'Configured Commerce Subscription'
    and prior_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as configured_commerce_subs_in_previous_date_with_arr,
    case when prior_pathways = 'EOL - Community API'
    and prior_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as community_api_in_previous_date_with_arr,
    case when prior_pathways = 'Commerce Connect Subscription'
    and prior_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as commerce_connect_subs_in_previous_date_with_arr,
    case when prior_pathways = 'Content Management PaaS Subscription'
    and prior_period_product_arr_usd_ccfx > 0 then 1
    else 0 end as content_mgmt_paas_subs_in_previous_date_with_arr
    from initial_table
  )
  , initial_table_3 as (
    select
    *,
    -- SUM MIGRATION TO CURRENT DATE
    sum(other_in_current_date_with_arr)over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_other_in_current_date_with_arr,
    sum(experience_creation_in_current_date_with_arr)over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_experience_creation_in_current_date_with_arr,
    sum(segmentation_in_current_date_with_arr)over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_segmentation_in_current_date_with_arr,
    -- SUM DOWNGRADE
    sum(downgraded_commerce_connect_li_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_downgraded_commerce_connect_li_in_current_date,
    sum(downgraded_commerce_connect_ms_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_downgraded_commerce_connect_ms_in_current_date,
    sum(downgraded_configured_commerce_li_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_downgraded_configured_commerce_li_in_current_date,
    sum(downgraded_configured_commerce_ms_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_downgraded_configured_commerce_ms_in_current_date,
    sum(downgraded_content_management_paas_li_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_downgraded_content_management_paas_li_in_current_date,
    sum(downgraded_content_management_paas_ms_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_downgraded_content_management_paas_ms_in_current_date,
    sum(downgraded_eol_everweb_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_downgraded_eol_everweb_in_current_date,
    sum(downgraded_visitor_intelligence_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_downgraded_visitor_intelligence_in_current_date,
    sum(downgraded_eol_legacy_ektron_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_downgraded_eol_legacy_ektron_in_current_date,
    sum(downgraded_search_navigation_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_downgraded_search_navigation_in_current_date,
      sum(downgraded_configured_commerce_subs_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_downgraded_configured_commerce_subs_in_current_date,
      sum(downgraded_eol_community_api_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_downgraded_eol_community_api_in_current_date,
      sum(downgraded_commerce_connect_subs_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_downgraded_commerce_connect_subs_in_current_date,
      sum(downgraded_content_mgmt_paas_subs_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_downgraded_content_mgmt_paas_subs_in_current_date,
    -- SUM CHURNED
    sum(churned_commerce_connect_li_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_churned_commerce_connect_li_in_current_date,
    sum(churned_commerce_connect_ms_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_churned_commerce_connect_ms_in_current_date,
    sum(churned_configured_commerce_li_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_churned_configured_commerce_li_in_current_date,
    sum(churned_configured_commerce_ms_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_churned_configured_commerce_ms_in_current_date,
    sum(churned_content_management_paas_li_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_churned_content_management_paas_li_in_current_date,
    sum(churned_content_management_paas_ms_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_churned_content_management_paas_ms_in_current_date,
    sum(churned_eol_everweb_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_churned_eol_everweb_in_current_date,
    sum(churned_visitor_intelligence_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_churned_visitor_intelligence_in_current_date,
    sum(churned_eol_legacy_ektron_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_churned_eol_legacy_ektron_in_current_date,
    sum(churned_search_navigation_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_churned_search_navigation_in_current_date,
      sum(churned_configured_commerce_subs_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_churned_configured_commerce_subs_in_current_date,
      sum(churned_community_api_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_churned_community_api_in_current_date,
      sum(churned_commerce_connect_subs_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_churned_commerce_connect_subs_in_current_date,
      sum(churned_content_mgmt_paas_subs_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_churned_content_mgmt_paas_subs_in_current_date,

    -- SUM MIGRATION FROM PREVIOUS DATE
    sum(commerce_connect_li_in_previous_date_with_arr) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_commerce_connect_li_in_previous_date_with_arr,
    sum(commerce_connect_ms_in_previous_date_with_arr) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_commerce_connect_ms_in_previous_date_with_arr,
    sum(configured_commerce_li_in_previous_date_with_arr) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_configured_commerce_li_in_previous_date_with_arr,
    sum(configured_commerce_ms_in_previous_date_with_arr) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_configured_commerce_ms_in_previous_date_with_arr,
    sum(content_management_paas_li_in_previous_date_with_arr) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_content_management_paas_li_in_previous_date_with_arr,
    sum(content_management_paas_ms_in_previous_date_with_arr) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_content_management_paas_ms_in_previous_date_with_arr,
    sum(eol_everweb_in_previous_date_with_arr) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_eol_everweb_in_previous_date_with_arr,
    sum(eol_legacy_visitor_intelligence_in_previous_date_with_arr) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_visitor_intelligence_in_previous_date_with_arr,
    sum(eol_legacy_ektron_in_previous_date_with_arr) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_eol_legacy_ektron_in_previous_date_with_arr,
    sum(search_navigation_standalone_in_previous_date_with_arr) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_search_navigation_in_previous_date_with_arr,
      sum(configured_commerce_subs_in_previous_date_with_arr) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_configured_commerce_subs_in_previous_date_with_arr,
      sum(community_api_in_previous_date_with_arr) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_community_api_in_previous_date_with_arr,
      sum(commerce_connect_subs_in_previous_date_with_arr) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_commerce_connect_subs_in_previous_date_with_arr,
      sum(content_mgmt_paas_subs_in_previous_date_with_arr) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_content_mgmt_paas_subs_in_previous_date_with_arr,
    -- SUM ADDED in current date
    sum(added_other_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_added_other_in_current_date,
    sum(added_experience_creation_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_added_experience_creation_in_current_date,
    sum(added_segmentation_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_added_segmentation_in_current_date,
    -- SUM INCREASED in current date
    sum(increased_other_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_increased_other_in_current_date,
    sum(increased_experience_creation_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_increased_experience_creation_in_current_date,
    sum(increased_segmentation_in_current_date) over(
      partition by mcid,evaluation_period,currency_code
      ) as sum_increased_segmentation_in_current_date
    from initial_table_2
    )
  ,  initial_table_4 as (
    select
    *,
    -- downgraded commerce connect li to to product
    case when downgraded_commerce_connect_li_in_current_date = 1 THEN
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_commerce_connect_li_to_other,

    case when downgraded_commerce_connect_li_in_current_date= 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_commerce_connect_li_to_experience_creation,
    -- downgrade commerce connect ms to product
    case when downgraded_commerce_connect_ms_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_commerce_connect_ms_to_other,

    case when downgraded_commerce_connect_ms_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_commerce_connect_ms_to_experience_creation,
    -- downgrade configured commerce li to product
    case when downgraded_configured_commerce_li_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_configured_commerce_li_to_other,

    case when downgraded_configured_commerce_li_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_configured_commerce_li_to_experience_creation,
    -- downgrade configured commerce ms to product
    case when downgraded_configured_commerce_ms_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_configured_commerce_ms_to_other,

    case when downgraded_configured_commerce_ms_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_configured_commerce_ms_to_experience_creation,
    -- downgrade content management paas li to product
    case when downgraded_content_management_paas_li_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_content_management_paas_li_to_other,

    case when downgraded_content_management_paas_li_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_content_management_paas_li_to_experience_creation,
    -- downgrade content management paas ms to product
    case when downgraded_content_management_paas_ms_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_content_management_paas_ms_to_other,

    case when downgraded_content_management_paas_ms_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_content_management_paas_ms_to_experience_creation,
    -- downgrade eol everweb to product
    case when downgraded_eol_everweb_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_eol_everweb_to_other,

    case when downgraded_eol_everweb_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_eol_everweb_to_experience_creation,
    -- downgrade eol legacy visitor intelligence to product
    case when downgraded_visitor_intelligence_in_current_date = 1 then
    case when sum_segmentation_in_current_date_with_arr > 0 then 1
    when added_segmentation_in_current_date > 0 or sum_increased_segmentation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_visitor_intelligence_to_segmentation,
    -- downgrade legacy ektron to product
    case when downgraded_eol_legacy_ektron_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_eol_legacy_ektron_to_other,

    case when downgraded_eol_legacy_ektron_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_eol_legacy_ektron_to_experience_creation,
    -- downgrade search navigation standalone to product
    case when downgraded_search_navigation_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_search_navigation_to_other,

    case when downgraded_search_navigation_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_search_navigation_to_experience_creation,
    -- downgrade configured commerce subscription to product
    case when downgraded_configured_commerce_subs_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_configured_commerce_subs_to_other,

    case when downgraded_configured_commerce_subs_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_configured_commerce_subs_to_experience_creation,

    -- downgrade eol community api to product
    case when downgraded_eol_community_api_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_eol_community_api_to_other,

    case when downgraded_eol_community_api_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_eol_community_api_to_experience_creation,

    -- downgrade Commerce Connect subscription to product
    case when downgraded_commerce_connect_subs_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_commerce_connect_subs_to_other,

    case when downgraded_commerce_connect_subs_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_commerce_connect_subs_to_experience_creation,

    -- downgrade Content Management Paas Subs to product
    case when downgraded_content_mgmt_paas_subs_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_content_mgmt_paas_subs_to_other,

    case when downgraded_content_mgmt_paas_subs_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downgrade_migration_content_mgmt_paas_subs_to_experience_creation,
    -- churned commerce connect li to product
    case when churned_commerce_connect_li_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_commerce_connect_li_to_other,
    case when churned_commerce_connect_li_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_commerce_connect_li_to_experience_creation,
    -- churned commerce connect ms to product
    case when churned_commerce_connect_ms_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_commerce_connect_ms_to_other,
    case when churned_commerce_connect_ms_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_commerce_connect_ms_to_experience_creation,
    -- churned configured commerce li to product
    case when churned_configured_commerce_li_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_configured_commerce_li_to_other,
    case when churned_configured_commerce_li_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_configured_commerce_li_to_experience_creation,
    -- churned configured commerce ms to product
    case when churned_configured_commerce_ms_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_configured_commerce_ms_to_other,
    case when churned_configured_commerce_ms_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_configured_commerce_ms_to_experience_creation,
    -- churned content management paas li to product
    case when churned_content_management_paas_li_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_content_management_paas_li_to_other,
    case when churned_content_management_paas_li_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_content_management_paas_li_to_experience_creation,
    -- churned content management paas ms to product
    case when churned_content_management_paas_ms_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_content_management_paas_ms_to_other,
    case when churned_content_management_paas_ms_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_content_management_paas_ms_to_experience_creation,
    -- churned eol everweb to product
    case when churned_eol_everweb_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_eol_everweb_to_other,
    case when churned_eol_everweb_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_eol_everweb_to_experience_creation,
    -- churned eol legacy visitor intelligence to product
    case when churned_visitor_intelligence_in_current_date = 1 then
    case when sum_segmentation_in_current_date_with_arr > 0 then 1
    when added_segmentation_in_current_date > 0 or sum_increased_segmentation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_visitor_intelligence_to_segmentation,
    -- churned legacy ektron to product
    case when churned_eol_legacy_ektron_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_eol_legacy_ektron_to_other,
    case when churned_eol_legacy_ektron_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_eol_legacy_ektron_to_experience_creation,
    -- churned search navigation standalone to product
    case when churned_search_navigation_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_search_navigation_to_other,
    case when churned_search_navigation_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_search_navigation_to_experience_creation,
    -- churned configured commerce subscription
    case when churned_configured_commerce_subs_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_configured_commerce_subs_to_other,
    case when churned_configured_commerce_subs_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_configured_commerce_subs_to_experience_creation,
    -- churned eol community api
    case when churned_community_api_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_community_api_to_other,
    case when churned_community_api_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_community_api_to_experience_creation,

    -- churned Commerce Connect Subs
    case when churned_commerce_connect_subs_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_commerce_connect_subs_to_other,
    case when churned_commerce_connect_subs_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_commerce_connect_subs_to_experience_creation,

    -- churned Content Management PaAS subs
    case when churned_content_mgmt_paas_subs_in_current_date = 1 then
    case when sum_other_in_current_date_with_arr > 0 then 1
    when sum_added_other_in_current_date > 0 or sum_increased_other_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_content_mgmt_paas_subs_to_other,
    case when churned_content_mgmt_paas_subs_in_current_date = 1 then
    case when sum_experience_creation_in_current_date_with_arr > 0 then 1
    when sum_added_experience_creation_in_current_date > 0 or sum_increased_experience_creation_in_current_date > 0
    then 1 else 0 end
    else 0 end as downsell_migration_content_mgmt_paas_subs_to_experience_creation,
    -- CROSS SELL other to migration from products
    case when added_other_in_current_date=1 then
    case when sum_downgraded_commerce_connect_li_in_current_date > 0
    or sum_churned_commerce_connect_li_in_current_date > 0 then 1
    when sum_commerce_connect_li_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_commerce_connect_li_to_other,
    case when added_other_in_current_date=1 then
    case when sum_downgraded_commerce_connect_ms_in_current_date > 0
    or sum_churned_commerce_connect_ms_in_current_date > 0 then 1
    when sum_commerce_connect_ms_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_commerce_connect_ms_to_other,
    case when added_other_in_current_date=1 then
    case when sum_downgraded_configured_commerce_li_in_current_date > 0
    or sum_churned_configured_commerce_li_in_current_date > 0 then 1
    when sum_configured_commerce_li_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_configured_commerce_li_to_other,
    case when added_other_in_current_date=1 then
    case when sum_downgraded_configured_commerce_ms_in_current_date > 0
    or sum_churned_configured_commerce_ms_in_current_date > 0 then 1
    when sum_configured_commerce_ms_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_configured_commerce_ms_to_other,
    case when added_other_in_current_date=1 then
    case when sum_downgraded_content_management_paas_li_in_current_date > 0
    or sum_churned_content_management_paas_li_in_current_date > 0 then 1
    when sum_content_management_paas_li_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_content_management_paas_li_to_other,
    case when added_other_in_current_date=1 then
    case when sum_downgraded_content_management_paas_ms_in_current_date > 0
    or sum_churned_content_management_paas_ms_in_current_date > 0 then 1
    when sum_content_management_paas_ms_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_content_management_paas_ms_to_other,
    case when added_other_in_current_date=1 then
    case when sum_downgraded_eol_everweb_in_current_date > 0
    or sum_churned_eol_everweb_in_current_date > 0 then 1
    when sum_eol_everweb_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_eol_everweb_to_other,
    case when added_other_in_current_date=1 then
    case when sum_downgraded_eol_legacy_ektron_in_current_date > 0
    or sum_churned_eol_legacy_ektron_in_current_date > 0 then 1
    when sum_eol_legacy_ektron_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_eol_legacy_ektron_to_other,
    case when added_other_in_current_date=1 then
    case when sum_downgraded_search_navigation_in_current_date > 0
    or sum_churned_search_navigation_in_current_date > 0 then 1
    when sum_search_navigation_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_search_navigation_to_other,
    case when added_other_in_current_date=1 then
    case when sum_downgraded_configured_commerce_subs_in_current_date > 0
    or sum_churned_configured_commerce_subs_in_current_date > 0 then 1
    when sum_configured_commerce_subs_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_configured_commerce_subs_to_other,
    case when added_other_in_current_date=1 then
    case when sum_downgraded_eol_community_api_in_current_date > 0
    or sum_churned_community_api_in_current_date > 0 then 1
    when sum_community_api_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_eol_community_api_to_other,

    case when added_other_in_current_date=1 then
    case when sum_downgraded_commerce_connect_subs_in_current_date > 0
    or sum_churned_commerce_connect_subs_in_current_date > 0 then 1
    when sum_commerce_connect_subs_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_commerce_connect_subs_to_other,

    case when added_other_in_current_date=1 then
    case when sum_downgraded_content_mgmt_paas_subs_in_current_date > 0
    or sum_churned_content_mgmt_paas_subs_in_current_date > 0 then 1
    when sum_content_mgmt_paas_subs_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_content_mgmt_paas_subs_to_other,
    -- cross sell content management cms to migration from products
    case when added_experience_creation_in_current_date=1 then
    case when sum_downgraded_commerce_connect_li_in_current_date > 0
    or sum_churned_commerce_connect_li_in_current_date > 0 then 1
    when sum_commerce_connect_li_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_commerce_connect_li_to_experience_creation,
    case when added_experience_creation_in_current_date=1 then
    case when sum_downgraded_commerce_connect_ms_in_current_date > 0
    or sum_churned_commerce_connect_ms_in_current_date > 0 then 1
    when sum_commerce_connect_ms_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_commerce_connect_ms_to_experience_creation,
    case when added_experience_creation_in_current_date=1 then
    case when sum_downgraded_configured_commerce_li_in_current_date > 0
    or sum_churned_configured_commerce_li_in_current_date > 0 then 1
    when sum_configured_commerce_li_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_configured_commerce_li_to_experience_creation,
    case when added_experience_creation_in_current_date=1 then
    case when sum_downgraded_configured_commerce_ms_in_current_date > 0
    or sum_churned_configured_commerce_ms_in_current_date > 0 then 1
    when sum_configured_commerce_ms_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_configured_commerce_ms_to_experience_creation,
    case when added_experience_creation_in_current_date=1 then
    case when sum_downgraded_content_management_paas_li_in_current_date > 0
    or sum_churned_content_management_paas_li_in_current_date > 0 then 1
    when sum_content_management_paas_li_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_content_management_paas_li_to_experience_creation,
    case when added_experience_creation_in_current_date=1 then
    case when sum_downgraded_content_management_paas_ms_in_current_date > 0
    or sum_churned_content_management_paas_ms_in_current_date > 0 then 1
    when sum_content_management_paas_ms_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_content_management_paas_ms_to_experience_creation,
    case when added_experience_creation_in_current_date=1 then
    case when sum_downgraded_eol_everweb_in_current_date > 0
    or sum_churned_eol_everweb_in_current_date > 0 then 1
    when sum_eol_everweb_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_eol_everweb_to_experience_creation,
    case when added_experience_creation_in_current_date=1 then
    case when sum_downgraded_eol_legacy_ektron_in_current_date > 0
    or sum_churned_eol_legacy_ektron_in_current_date > 0 then 1
    when sum_eol_legacy_ektron_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_eol_legacy_ektron_to_experience_creation,

    case when added_experience_creation_in_current_date=1 then
    case when sum_downgraded_search_navigation_in_current_date > 0
    or sum_churned_search_navigation_in_current_date > 0 then 1
    when sum_search_navigation_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_search_navigation_to_experience_creation,
    case when added_experience_creation_in_current_date=1 then
    case when sum_downgraded_configured_commerce_subs_in_current_date > 0
    or sum_churned_configured_commerce_subs_in_current_date > 0 then 1
    when sum_configured_commerce_subs_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_configured_commerce_subs_to_experience_creation,
    case when added_experience_creation_in_current_date=1 then
    case when sum_downgraded_eol_community_api_in_current_date > 0
    or sum_churned_community_api_in_current_date > 0 then 1
    when sum_community_api_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_eol_community_api_to_experience_creation,

    case when added_experience_creation_in_current_date=1 then
    case when sum_downgraded_commerce_connect_subs_in_current_date > 0
    or sum_churned_commerce_connect_subs_in_current_date > 0 then 1
    when sum_commerce_connect_subs_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_commerce_connect_subs_to_experience_creation,
    case when added_experience_creation_in_current_date=1 then
    case when sum_downgraded_content_mgmt_paas_subs_in_current_date > 0
    or sum_churned_content_mgmt_paas_subs_in_current_date > 0 then 1
    when sum_content_mgmt_paas_subs_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_content_mgmt_paas_subs_to_experience_creation,
    -- cross sell segmentation to migration from products
    case when added_segmentation_in_current_date=1 then
    case when sum_downgraded_visitor_intelligence_in_current_date > 0
    or sum_churned_visitor_intelligence_in_current_date > 0 then 1
    when sum_visitor_intelligence_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as cross_sell_migration_visitor_intelligence_to_segmentation,
    -- UPSELL migration from products to other
    -- upsell migration other to migration from products
    case when increased_other_in_current_date = 1 then
    case when sum_downgraded_commerce_connect_li_in_current_date > 0
    or sum_churned_commerce_connect_li_in_current_date >0 then 1
    when sum_commerce_connect_li_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_commerce_connect_li_to_other,
    case when increased_other_in_current_date = 1 then
    case when sum_downgraded_commerce_connect_ms_in_current_date > 0
    or sum_churned_commerce_connect_ms_in_current_date > 0 then 1
    when sum_commerce_connect_ms_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_commerce_connect_ms_to_other,
    case when increased_other_in_current_date = 1 then
    case when sum_downgraded_configured_commerce_li_in_current_date > 0
    or sum_churned_configured_commerce_li_in_current_date > 0 then 1
    when sum_configured_commerce_li_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_configured_commerce_li_to_other,
    case when increased_other_in_current_date = 1 then
    case when sum_downgraded_configured_commerce_ms_in_current_date > 0
    or sum_churned_configured_commerce_ms_in_current_date > 0 then 1
    when sum_configured_commerce_ms_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_configured_commerce_ms_to_other,
    case when increased_other_in_current_date = 1 then
    case when sum_downgraded_content_management_paas_li_in_current_date > 0
    or sum_churned_content_management_paas_li_in_current_date > 0 then 1
    when sum_content_management_paas_li_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_content_management_paas_li_to_other,
    case when increased_other_in_current_date = 1 then
    case when sum_downgraded_content_management_paas_ms_in_current_date > 0
    or sum_churned_content_management_paas_ms_in_current_date > 0 then 1
    when sum_content_management_paas_ms_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_content_management_paas_ms_to_other,
    case when increased_other_in_current_date = 1 then
    case when sum_downgraded_eol_everweb_in_current_date > 0
    or sum_churned_eol_everweb_in_current_date > 0 then 1
    when sum_eol_everweb_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_eol_everweb_to_other,
    case when increased_other_in_current_date = 1 then
    case when sum_downgraded_eol_legacy_ektron_in_current_date > 0
    or sum_churned_eol_legacy_ektron_in_current_date > 0 then 1
    when sum_eol_legacy_ektron_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_eol_legacy_ektron_to_other,
    case when increased_other_in_current_date = 1 then
    case when sum_downgraded_search_navigation_in_current_date > 0
    or sum_churned_search_navigation_in_current_date > 0 then 1
    when sum_search_navigation_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_search_navigation_to_other,
    case when increased_other_in_current_date = 1 then
    case when sum_downgraded_configured_commerce_subs_in_current_date > 0
    or sum_churned_configured_commerce_subs_in_current_date > 0 then 1
    when configured_commerce_subs_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_configured_commerce_subs_to_other,
    case when increased_other_in_current_date = 1 then
    case when sum_downgraded_eol_community_api_in_current_date > 0
    or sum_churned_community_api_in_current_date > 0 then 1
    when sum_community_api_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_community_api_to_other,

    case when increased_other_in_current_date = 1 then
    case when sum_downgraded_commerce_connect_subs_in_current_date > 0
    or sum_churned_commerce_connect_subs_in_current_date > 0 then 1
    when sum_commerce_connect_subs_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_commerce_connect_subs_to_other,

    case when increased_other_in_current_date = 1 then
    case when sum_downgraded_content_mgmt_paas_subs_in_current_date > 0
    or sum_churned_content_mgmt_paas_subs_in_current_date > 0 then 1
    when sum_content_mgmt_paas_subs_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_content_mgmt_paas_subs_to_other,
    -- upsell migration content management cms to migration from products
    case when increased_experience_creation_in_current_date = 1 then
    case when sum_downgraded_commerce_connect_li_in_current_date > 0
    or sum_churned_commerce_connect_li_in_current_date > 0 then 1
    when sum_commerce_connect_li_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_commerce_connect_li_to_experience_creation,
    case when increased_experience_creation_in_current_date = 1 then
    case when sum_downgraded_commerce_connect_ms_in_current_date > 0
    or sum_churned_commerce_connect_ms_in_current_date > 0 then 1
    when sum_commerce_connect_ms_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_commerce_connect_ms_to_experience_creation,
    case when increased_experience_creation_in_current_date = 1 then
    case when sum_downgraded_configured_commerce_li_in_current_date > 0
    or sum_churned_configured_commerce_li_in_current_date > 0 then 1
    when sum_configured_commerce_li_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_configured_commerce_li_to_experience_creation,
    case when increased_experience_creation_in_current_date = 1 then
    case when sum_downgraded_configured_commerce_ms_in_current_date > 0
    or sum_churned_configured_commerce_ms_in_current_date > 0 then 1
    when sum_configured_commerce_ms_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_configured_commerce_ms_to_experience_creation,
    case when increased_experience_creation_in_current_date = 1 then
    case when sum_downgraded_content_management_paas_li_in_current_date > 0
    or sum_churned_content_management_paas_li_in_current_date > 0 then 1
    when sum_content_management_paas_li_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_content_management_paas_li_to_experience_creation,
    case when increased_experience_creation_in_current_date = 1 then
    case when sum_downgraded_content_management_paas_ms_in_current_date > 0
    or sum_churned_content_management_paas_ms_in_current_date > 0 then 1
    when sum_content_management_paas_ms_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_content_management_paas_ms_to_experience_creation,
    case when increased_experience_creation_in_current_date = 1 then
    case when sum_downgraded_eol_everweb_in_current_date > 0
    or sum_churned_eol_everweb_in_current_date > 0 then 1
    when sum_eol_everweb_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_eol_everweb_to_experience_creation,
    case when increased_experience_creation_in_current_date = 1 then
    case when sum_downgraded_eol_legacy_ektron_in_current_date > 0
    or sum_churned_eol_legacy_ektron_in_current_date > 0 then 1
    when sum_eol_legacy_ektron_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_eol_legacy_ektron_to_experience_creation,
    case when increased_experience_creation_in_current_date = 1 then
    case when sum_downgraded_search_navigation_in_current_date > 0
    or sum_churned_search_navigation_in_current_date > 0 then 1
    when sum_search_navigation_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_search_navigation_to_experience_creation,
    case when increased_experience_creation_in_current_date = 1 then
    case when sum_downgraded_configured_commerce_subs_in_current_date > 0
    or sum_churned_configured_commerce_subs_in_current_date > 0 then 1
    when sum_configured_commerce_subs_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_configured_commerce_subs_to_experience_creation,
    case when increased_experience_creation_in_current_date = 1 then
    case when sum_downgraded_eol_community_api_in_current_date > 0
    or sum_churned_community_api_in_current_date > 0 then 1
    when sum_community_api_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_community_api_to_experience_creation,

    case when increased_experience_creation_in_current_date = 1 then
    case when sum_downgraded_commerce_connect_subs_in_current_date > 0
    or sum_churned_commerce_connect_subs_in_current_date > 0 then 1
    when sum_commerce_connect_subs_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_commerce_connect_subs_to_experience_creation,

    case when increased_experience_creation_in_current_date = 1 then
    case when sum_downgraded_content_mgmt_paas_subs_in_current_date > 0
    or sum_churned_content_mgmt_paas_subs_in_current_date > 0 then 1
    when sum_content_mgmt_paas_subs_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_content_mgmt_paas_subs_to_experience_creation,
    -- upsell migration segmentation to migration from products
    case when increased_segmentation_in_current_date = 1 then
    case when sum_downgraded_visitor_intelligence_in_current_date > 0
    or sum_churned_visitor_intelligence_in_current_date > 0 then 1
    when sum_visitor_intelligence_in_previous_date_with_arr > 0
    then 1 else 0 end
    else 0 end as upsell_migration_visitor_intelligence_to_segmentation
    from initial_table_3
  )
  , initial_table_5 as (
    select
    * ,
    (downgrade_migration_commerce_connect_li_to_other +
    downgrade_migration_commerce_connect_ms_to_other +
    downgrade_migration_configured_commerce_li_to_other +
    downgrade_migration_configured_commerce_ms_to_other +
    downgrade_migration_content_management_paas_li_to_other +
    downgrade_migration_content_management_paas_ms_to_other +
    downgrade_migration_eol_everweb_to_other +
    downgrade_migration_eol_legacy_ektron_to_other +
    downgrade_migration_search_navigation_to_other +
    downgrade_migration_configured_commerce_subs_to_other +
    downgrade_migration_eol_community_api_to_other +
    downgrade_migration_commerce_connect_subs_to_other +
    downgrade_migration_content_mgmt_paas_subs_to_other +


    downgrade_migration_commerce_connect_li_to_experience_creation +
    downgrade_migration_commerce_connect_ms_to_experience_creation +
    downgrade_migration_configured_commerce_li_to_experience_creation +
    downgrade_migration_configured_commerce_ms_to_experience_creation +
    downgrade_migration_content_management_paas_li_to_experience_creation +
    downgrade_migration_content_management_paas_ms_to_experience_creation +
    downgrade_migration_eol_everweb_to_experience_creation +
    downgrade_migration_eol_legacy_ektron_to_experience_creation +
    downgrade_migration_search_navigation_to_experience_creation +
    downgrade_migration_configured_commerce_subs_to_experience_creation +
    downgrade_migration_eol_community_api_to_experience_creation +
    downgrade_migration_commerce_connect_subs_to_experience_creation +
    downgrade_migration_content_mgmt_paas_subs_to_experience_creation +
    downgrade_migration_visitor_intelligence_to_segmentation) as downgrade_total,

    (downsell_migration_commerce_connect_li_to_other +
    downsell_migration_commerce_connect_ms_to_other +
    downsell_migration_configured_commerce_li_to_other +
    downsell_migration_configured_commerce_ms_to_other +
    downsell_migration_content_management_paas_li_to_other +
    downsell_migration_content_management_paas_ms_to_other +
    downsell_migration_eol_everweb_to_other +
    downsell_migration_eol_legacy_ektron_to_other +
    downsell_migration_search_navigation_to_other +
    downsell_migration_configured_commerce_subs_to_other +
    downsell_migration_community_api_to_other +
    downsell_migration_commerce_connect_subs_to_other +
    downsell_migration_content_mgmt_paas_subs_to_other +

    downsell_migration_commerce_connect_li_to_experience_creation+
    downsell_migration_commerce_connect_ms_to_experience_creation+
    downsell_migration_configured_commerce_li_to_experience_creation+
    downsell_migration_configured_commerce_ms_to_experience_creation+
    downsell_migration_content_management_paas_li_to_experience_creation+
    downsell_migration_content_management_paas_ms_to_experience_creation+
    downsell_migration_eol_everweb_to_experience_creation+
    downsell_migration_eol_legacy_ektron_to_experience_creation+
    downsell_migration_search_navigation_to_experience_creation+
    downsell_migration_configured_commerce_subs_to_experience_creation +
    downsell_migration_community_api_to_experience_creation +
    downsell_migration_commerce_connect_subs_to_experience_creation +
    downsell_migration_content_mgmt_paas_subs_to_experience_creation +



    downsell_migration_visitor_intelligence_to_segmentation ) as downsell_total,

    (cross_sell_migration_commerce_connect_li_to_other +
    cross_sell_migration_commerce_connect_ms_to_other +
    cross_sell_migration_configured_commerce_li_to_other +
    cross_sell_migration_configured_commerce_ms_to_other +
    cross_sell_migration_content_management_paas_li_to_other +
    cross_sell_migration_content_management_paas_ms_to_other +
    cross_sell_migration_eol_everweb_to_other +
    cross_sell_migration_eol_legacy_ektron_to_other +
    cross_sell_migration_search_navigation_to_other +
    cross_sell_migration_configured_commerce_subs_to_other +
    cross_sell_migration_eol_community_api_to_other +
    cross_sell_migration_commerce_connect_subs_to_other +
    cross_sell_migration_content_mgmt_paas_subs_to_other +

    cross_sell_migration_commerce_connect_li_to_experience_creation +
    cross_sell_migration_commerce_connect_ms_to_experience_creation +
    cross_sell_migration_configured_commerce_li_to_experience_creation +
    cross_sell_migration_configured_commerce_ms_to_experience_creation +
    cross_sell_migration_content_management_paas_li_to_experience_creation +
    cross_sell_migration_content_management_paas_ms_to_experience_creation +
    cross_sell_migration_eol_everweb_to_experience_creation +
    cross_sell_migration_eol_legacy_ektron_to_experience_creation +
    cross_sell_migration_search_navigation_to_experience_creation +
    cross_sell_migration_configured_commerce_subs_to_experience_creation +
    cross_sell_migration_eol_community_api_to_experience_creation +
    cross_sell_migration_commerce_connect_subs_to_experience_creation +
    cross_sell_migration_content_mgmt_paas_subs_to_experience_creation +

    cross_sell_migration_visitor_intelligence_to_segmentation ) as crossell_total,

    (upsell_migration_commerce_connect_li_to_other +
    upsell_migration_commerce_connect_ms_to_other +
    upsell_migration_configured_commerce_li_to_other +
    upsell_migration_configured_commerce_ms_to_other +
    upsell_migration_content_management_paas_li_to_other +
    upsell_migration_content_management_paas_ms_to_other +
    upsell_migration_eol_everweb_to_other +
    upsell_migration_eol_legacy_ektron_to_other +
    upsell_migration_search_navigation_to_other +
    upsell_migration_configured_commerce_subs_to_other +
    upsell_migration_community_api_to_other +
    upsell_migration_commerce_connect_subs_to_other +
    upsell_migration_content_mgmt_paas_subs_to_other +

    upsell_migration_commerce_connect_li_to_experience_creation +
    upsell_migration_commerce_connect_ms_to_experience_creation +
    upsell_migration_configured_commerce_li_to_experience_creation +
    upsell_migration_configured_commerce_ms_to_experience_creation +
    upsell_migration_content_management_paas_li_to_experience_creation +
    upsell_migration_content_management_paas_ms_to_experience_creation +
    upsell_migration_eol_everweb_to_experience_creation +
    upsell_migration_eol_legacy_ektron_to_experience_creation +
    upsell_migration_search_navigation_to_experience_creation +
    upsell_migration_configured_commerce_subs_to_experience_creation +
    upsell_migration_community_api_to_experience_creation +
    upsell_migration_commerce_connect_subs_to_experience_creation +
    upsell_migration_content_mgmt_paas_subs_to_experience_creation +

    upsell_migration_visitor_intelligence_to_segmentation) as upsell_total,
    concat(
    case when downgrade_migration_commerce_connect_li_to_other = 1 then
    ',downgrade - migration -- commerce_connect_li to other' else null end,
    case when downgrade_migration_commerce_connect_ms_to_other = 1 then
    ',downgrade - migration -- commerce_connect_ms to other' else null end,
    case when downgrade_migration_configured_commerce_li_to_other = 1 then
    ',downgrade - migration -- configured_commerce_li to other' else null end,
    case when downgrade_migration_configured_commerce_ms_to_other = 1 then
    ',downgrade - migration -- configured_commerce_ms to other' else null end,
    case when downgrade_migration_content_management_paas_li_to_other = 1 then
    ',downgrade - migration -- content_management_paas_li to other' else null end,
    case when downgrade_migration_content_management_paas_ms_to_other = 1 then
    ',downgrade - migration -- content_management_paas_ms to other' else null end,
    case when downgrade_migration_eol_everweb_to_other = 1 then
    ',downgrade - migration -- eol_everweb to other' else null end,
    case when downgrade_migration_eol_legacy_ektron_to_other = 1 then
    ',downgrade - migration -- eol_legacy_ektron to other' else null end,
    case when downgrade_migration_search_navigation_to_other = 1 then
    ',downgrade - migration -- search_navigation_standalone to other' else null end,
    case when downgrade_migration_configured_commerce_subs_to_other = 1 then
    ',downgrade - migration -- configured_commerce_subs to other' else null end,
    case when downgrade_migration_eol_community_api_to_other = 1 then
    ',downgrade - migration -- eol_community_api to other' else null end,

    case when downgrade_migration_commerce_connect_subs_to_other = 1 then
    ',downgrade - migration -- commerce_connect_subscriptions to other' else null end,
    case when downgrade_migration_content_mgmt_paas_subs_to_other = 1 then
    ',downgrade - migration -- content_management_paas_subscriptions to other' else null end,

    case when downgrade_migration_commerce_connect_li_to_experience_creation = 1 then
    ',downgrade - migration -- commerce_connect_li to experience_creation' else null end,
    case when downgrade_migration_commerce_connect_ms_to_experience_creation = 1 then
    ',downgrade - migration -- commerce_connect_ms to experience_creation' else null end,
    case when downgrade_migration_configured_commerce_li_to_experience_creation = 1 then
    ',downgrade - migration -- configured_commerce_li to experience_creation' else null end,
    case when downgrade_migration_configured_commerce_ms_to_experience_creation = 1 then
    ',downgrade - migration -- configured_commerce_ms to experience_creation' else null end,
    case when downgrade_migration_content_management_paas_li_to_experience_creation = 1 then
    ',downgrade - migration -- content_management_paas_li to experience_creation' else null end,
    case when downgrade_migration_content_management_paas_ms_to_experience_creation = 1 then
    ',downgrade - migration -- content_management_paas_ms to experience_creation' else null end,
    case when downgrade_migration_eol_everweb_to_experience_creation = 1 then
    ',downgrade - migration -- eol_everweb to experience_creation' else null end,
    case when downgrade_migration_eol_legacy_ektron_to_experience_creation = 1 then
    ',downgrade - migration -- eol_legacy_ektron to experience_creation' else null end,
    case when downgrade_migration_search_navigation_to_experience_creation = 1 then
    ',downgrade - migration -- search_navigation_standalone to experience_creation' else null end,
    case when downgrade_migration_configured_commerce_subs_to_experience_creation = 1 then
    ',downgrade - migration -- configured_commerce_subs to experience_creation' else null end,
    case when downgrade_migration_eol_community_api_to_experience_creation = 1 then
    ',downgrade - migration -- eol_community_api to experience_creation' else null end,

    case when downgrade_migration_commerce_connect_subs_to_experience_creation = 1 then
    ',downgrade - migration -- commerce_connect_subscriptions to experience_creation' else null end,
    case when downgrade_migration_content_mgmt_paas_subs_to_experience_creation = 1 then
    ',downgrade - migration -- content_management_paas_subscriptions to experience_creation' else null end,

    case when downgrade_migration_visitor_intelligence_to_segmentation = 1 then
    ',downgrade - migration -- eol_legacy_visitor_intelligence to segmentation' else null end ) as downgrade_fd ,



    concat(case when downsell_migration_commerce_connect_li_to_other = 1 then
    ',downsell - migration -- commerce_connect_li to other' else null end,
    case when downsell_migration_commerce_connect_ms_to_other = 1 then
    ',downsell - migration -- commerce_connect_ms to other' else null end,
    case when downsell_migration_configured_commerce_li_to_other = 1 then
    ',downsell - migration -- configured_commerce_li to other' else null end,
    case when downsell_migration_configured_commerce_ms_to_other = 1 then
    ',downsell - migration -- configured_commerce_ms to other' else null end,
    case when downsell_migration_content_management_paas_li_to_other = 1 then
    ',downsell - migration -- content_management_paas_li to other' else null end,
    case when downsell_migration_content_management_paas_ms_to_other = 1 then
    ',downsell - migration -- content_management_paas_ms to other' else null end,
    case when downsell_migration_eol_everweb_to_other = 1 then
    ',downsell - migration -- eol_everweb to other' else null end,
    case when downsell_migration_eol_legacy_ektron_to_other = 1 then
    ',downsell - migration -- eol_legacy_ektron to other' else null end,
    case when downsell_migration_search_navigation_to_other = 1 then
    ',downsell - migration -- search_navigation_standalone to other' else null end,
    case when downsell_migration_configured_commerce_subs_to_other = 1 then
    ',downsell - migration -- configured_commerce_subs to other' else null end,
    case when downsell_migration_community_api_to_other = 1 then
    ',downsell - migration -- eol_community_api to other' else null end,

    case when downsell_migration_commerce_connect_subs_to_other = 1 then
    ',downsell - migration -- commerce_connect_subscriptions to other' else null end,
    case when downsell_migration_content_mgmt_paas_subs_to_other = 1 then
    ',downsell - migration -- content_management_paas_subscriptions to other' else null end,

    case when downsell_migration_commerce_connect_li_to_experience_creation = 1 then
    ',downsell - migration -- commerce_connect_li to experience_creation' else null end,
    case when downsell_migration_commerce_connect_ms_to_experience_creation = 1 then
    ',downsell - migration -- commerce_connect_ms to experience_creation' else null end,
    case when downsell_migration_configured_commerce_li_to_experience_creation = 1 then
    ',downsell - migration -- configured_commerce_li to experience_creation' else null end,
    case when downsell_migration_configured_commerce_ms_to_experience_creation = 1 then
    ',downsell - migration -- configured_commerce_ms to experience_creation' else null end,
    case when downsell_migration_content_management_paas_li_to_experience_creation = 1 then
    ',downsell - migration -- content_management_paas_li to experience_creation' else null end,
    case when downsell_migration_content_management_paas_ms_to_experience_creation = 1 then
    ',downsell - migration -- content_management_paas_ms to experience_creation' else null end,
    case when downsell_migration_eol_everweb_to_experience_creation = 1 then
    ',downsell - migration -- eol_everweb to experience_creation' else null end,
    case when downsell_migration_eol_legacy_ektron_to_experience_creation = 1 then
    ',downsell - migration -- eol_legacy_ektron to experience_creation' else null end,
    case when downsell_migration_search_navigation_to_experience_creation = 1 then
    ',downsell - migration -- search_navigation_standalone to experience_creation' else null end,
    case when downsell_migration_configured_commerce_subs_to_experience_creation = 1 then
    ',downsell - migration -- configured_commerce_subs to experience_creation' else null end,
    case when downsell_migration_community_api_to_experience_creation = 1 then
    ',downsell - migration -- eol_community_api to experience_creation' else null end,

    case when downsell_migration_commerce_connect_subs_to_experience_creation = 1 then
    ',downsell - migration -- commerce_connect_subscriptions to experience_creation' else null end,
    case when downsell_migration_content_mgmt_paas_subs_to_experience_creation = 1 then
    ',downsell - migration -- content_management_paas_subscriptions to experience_creation' else null end,

    case when downsell_migration_visitor_intelligence_to_segmentation = 1 then
    ',downsell - migration -- eol_legacy_visitor_intelligence to segmentation' else null end) as downsell_fd,



    concat(case when cross_sell_migration_commerce_connect_li_to_other = 1 then
    ',cross_sell - migration -- commerce_connect_li to other' else null end,
    case when cross_sell_migration_commerce_connect_ms_to_other = 1 then
    ',cross_sell - migration -- commerce_connect_ms to other' else null end,
    case when cross_sell_migration_configured_commerce_li_to_other = 1 then
    ',cross_sell - migration -- configured_commerce_li to other' else null end,
    case when cross_sell_migration_configured_commerce_ms_to_other = 1 then
    ',cross_sell - migration -- configured_commerce_ms to other' else null end,
    case when cross_sell_migration_content_management_paas_li_to_other = 1 then
    ',cross_sell - migration -- content_management_paas_li to other' else null end,
    case when cross_sell_migration_content_management_paas_ms_to_other = 1 then
    ',cross_sell - migration -- content_management_paas_ms to other' else null end,
    case when cross_sell_migration_eol_everweb_to_other = 1 then
    ',cross_sell - migration -- eol_everweb to other' else null end,
    case when cross_sell_migration_eol_legacy_ektron_to_other = 1 then
    ',cross_sell - migration -- eol_legacy_ektron to other' else null end,
    case when cross_sell_migration_search_navigation_to_other = 1 then
    ',cross_sell - migration -- search_navigation_standalone to other' else null end,
    case when cross_sell_migration_configured_commerce_subs_to_other = 1 then
    ',cross_sell - migration -- configured_commerce_subs to other' else null end,
    case when cross_sell_migration_eol_community_api_to_other = 1 then
    ',cross_sell - migration -- eol_community_api to other' else null end,

    case when cross_sell_migration_commerce_connect_subs_to_other = 1 then
    ',cross_sell - migration -- commerce_connect_subscriptions to other' else null end,
    case when cross_sell_migration_content_mgmt_paas_subs_to_other = 1 then
    ',cross_sell - migration -- content_management_paas_subscriptions to other' else null end,

    case when cross_sell_migration_commerce_connect_li_to_experience_creation = 1 then
    ',cross_sell - migration -- commerce_connect_li to experience_creation' else null end,
    case when cross_sell_migration_commerce_connect_ms_to_experience_creation = 1 then
    ',cross_sell - migration -- commerce_connect_ms to experience_creation' else null end,
    case when cross_sell_migration_configured_commerce_li_to_experience_creation = 1 then
    ',cross_sell - migration -- configured_commerce_li to experience_creation' else null end,
    case when cross_sell_migration_configured_commerce_ms_to_experience_creation = 1 then
    ',cross_sell - migration -- configured_commerce_ms to experience_creation' else null end,
    case when cross_sell_migration_content_management_paas_li_to_experience_creation = 1 then
    ',cross_sell - migration -- content_management_paas_li to experience_creation' else null end,
    case when cross_sell_migration_content_management_paas_ms_to_experience_creation = 1 then
    ',cross_sell - migration -- content_management_paas_ms to experience_creation' else null end,
    case when cross_sell_migration_eol_everweb_to_experience_creation = 1 then
    ',cross_sell - migration -- eol_everweb to experience_creation' else null end,
    case when cross_sell_migration_eol_legacy_ektron_to_experience_creation = 1 then
    ',cross_sell - migration -- eol_legacy_ektron to experience_creation' else null end,
    case when cross_sell_migration_search_navigation_to_experience_creation = 1 then
    ',cross_sell - migration -- search_navigation_standalone to experience_creation' else null end,
    case when cross_sell_migration_configured_commerce_subs_to_experience_creation = 1 then
    ',cross_sell - migration -- configured_commerce_subs to experience_creation' else null end,
    case when cross_sell_migration_eol_community_api_to_experience_creation = 1 then
    ',cross_sell - migration -- eol_community_api to experience_creation' else null end,

    case when cross_sell_migration_commerce_connect_subs_to_experience_creation = 1 then
    ',cross_sell - migration -- commerce_connect_subscriptions to experience_creation' else null end,
    case when cross_sell_migration_content_mgmt_paas_subs_to_experience_creation = 1 then
    ',cross_sell - migration -- content_management_paas_subscriptions to experience_creation' else null end,

    case when cross_sell_migration_visitor_intelligence_to_segmentation = 1 then
    ',cross_sell - migration -- eol_legacy_visitor_intelligence to segmentation' else null end) as crossell_fd,

    concat(case when upsell_migration_commerce_connect_li_to_other = 1 then
    ',upsell - migration -- commerce_connect_li to other' else null end,
    case when upsell_migration_commerce_connect_ms_to_other = 1 then
    ',upsell - migration -- commerce_connect_ms to other' else null end,
    case when upsell_migration_configured_commerce_li_to_other = 1 then
    ',upsell - migration -- configured_commerce_li to other' else null end,
    case when upsell_migration_configured_commerce_ms_to_other = 1 then
    ',upsell - migration -- configured_commerce_ms to other' else null end,
    case when upsell_migration_content_management_paas_li_to_other = 1 then
    ',upsell - migration -- content_management_paas_li to other' else null end,
    case when upsell_migration_content_management_paas_ms_to_other = 1 then
    ',upsell - migration -- content_management_paas_ms to other' else null end,
    case when upsell_migration_eol_everweb_to_other = 1 then
    ',upsell - migration -- eol_everweb to other' else null end,
    case when upsell_migration_eol_legacy_ektron_to_other = 1 then
    ',upsell - migration -- eol_legacy_ektron to other' else null end,
    case when upsell_migration_search_navigation_to_other = 1 then
    ',upsell - migration -- search_navigation_standalone to other' else null end,
    case when upsell_migration_configured_commerce_subs_to_other = 1 then
    ',upsell - migration -- configured_commerce_subs to other' else null end,
    case when upsell_migration_community_api_to_other = 1 then
    ',upsell - migration -- eol_community_api to other' else null end,

    case when upsell_migration_commerce_connect_subs_to_other = 1 then
    ',upsell - migration -- commerce_connect_subscriptions to other' else null end,
    case when upsell_migration_content_mgmt_paas_subs_to_other = 1 then
    ',upsell - migration -- content_management_paas_subscriptions to other' else null end,


    case when upsell_migration_commerce_connect_li_to_experience_creation = 1 then
    ',upsell - migration -- commerce_connect_li to experience_creation' else null end,
    case when upsell_migration_commerce_connect_ms_to_experience_creation = 1 then
    ',upsell - migration -- commerce_connect_ms to experience_creation' else null end,
    case when upsell_migration_configured_commerce_li_to_experience_creation = 1 then
    ',upsell - migration -- configured_commerce_li to experience_creation' else null end,
    case when upsell_migration_configured_commerce_ms_to_experience_creation = 1 then
    ',upsell - migration -- configured_commerce_ms to experience_creation' else null end,
    case when upsell_migration_content_management_paas_li_to_experience_creation = 1 then
    ',upsell - migration -- content_management_paas_li to experience_creation' else null end,
    case when upsell_migration_content_management_paas_ms_to_experience_creation = 1 then
    ',upsell - migration -- content_management_paas_ms to experience_creation' else null end,
    case when upsell_migration_eol_everweb_to_experience_creation = 1 then
    ',upsell - migration -- eol_everweb to experience_creation' else null end,
    case when upsell_migration_eol_legacy_ektron_to_experience_creation = 1 then
    ',upsell - migration -- eol_legacy_ektron to experience_creation' else null end,
    case when upsell_migration_search_navigation_to_experience_creation = 1 then
    ',upsell - migration -- search_navigation_standalone to experience_creation' else null end,
    case when upsell_migration_configured_commerce_subs_to_experience_creation = 1 then
    ',upsell - migration -- configured_commerce_subs to experience_creation' else null end,
    case when upsell_migration_community_api_to_experience_creation = 1 then
    ',upsell - migration -- eol_community_api to experience_creation' else null end,

    case when upsell_migration_commerce_connect_subs_to_experience_creation = 1 then
    ',upsell - migration -- commerce_connect_subscriptions to experience_creation' else null end,
    case when upsell_migration_content_mgmt_paas_subs_to_experience_creation = 1 then
    ',upsell - migration -- content_management_paas_subscriptions to experience_creation' else null end,

    case when upsell_migration_visitor_intelligence_to_segmentation = 1 then
    ',upsell - migration -- eol_legacy_visitor_intelligence to segmentation' else null end
    ) as upsell_fd

    from initial_table_4
  )

--     select * from initial_table_5
--                 where mcid = '7183df70-0c08-e511-9afb-0050568d2da8'
-- and evaluation_period  = '2025M03';

    , initial_table_5_internal as (

    select
      evaluation_period  ,
      current_period ,
      prior_period ,
      mcid ,
      currency_code ,
      current_product_group,
      prior_product_group,
      current_product_solution,
      prior_product_solution,
      REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                     REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          REGEXP_REPLACE(current_pathways, '\s*-\s*', '_')
                           ,' & ', '_')
                        ,'\s*M&S\s*', '_MS')
                      , ' \(', '_')
                    ,'\)', '')
                   ,' ', '_')
            ,' ','_'
           ) AS current_pathways ,
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                     REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          REGEXP_REPLACE(prior_pathways, '\s*-\s*', '_')
                           ,' & ', '_')
                        ,'\s*M&S\s*', '_MS')
                      , ' \(', '_')
                    ,'\)', '')
                   ,' ', '_')
            ,' ','_'
           ) AS prior_pathways ,
      prior_period_product_arr_lcu,
      current_period_product_arr_lcu,
      prior_period_product_arr_usd_ccfx,
      current_period_product_arr_usd_ccfx,
      product_arr_change_ccfx,
      product_arr_change_lcu,
      (downgrade_total + downsell_total + crossell_total + upsell_total ) as active_flag_count ,
      concat(downgrade_fd , downsell_fd ,crossell_fd , upsell_fd ) as flag_descriptions
    from initial_table_5
  )
  select * from initial_table_5_internal
);




drop table if exists sandbox.churn_migration_classifiers_max_value_v2_2_split;
create table   sandbox.churn_migration_classifiers_max_value_v2_2_split as (
with base_with_id AS (
  SELECT
    *,
    ROW_NUMBER() OVER () as rn -- Add a unique identifier for each row
  FROM
     sandbox.churn_migration_classifiers_max_value_v2_int_split-- Replace with the name of the table containing the RAW data

),
unnested_flags AS (
  SELECT
    rn,
    trim(flag_part) as flag_part
  FROM
    base_with_id
  CROSS JOIN LATERAL
    unnest(string_to_array(trim(BOTH ',' FROM flag_descriptions), ',')) AS flag_part
  WHERE flag_descriptions IS NOT NULL AND flag_descriptions != ''
),
extracted_parts AS (
  SELECT
    rn,
    flag_part,
    (regexp_matches(
      flag_part,
      '^\s*(?:upsell|downsell|cross_sell|downgrade)\s*-\s*migration\s*--\s*(.*?)\s+to\s+(.*?)\s*$',
      'i'
    ))[1] AS from_part,
    (regexp_matches(
      flag_part,
      '^\s*(?:upsell|downsell|cross_sell|downgrade)\s*-\s*migration\s*--\s*(.*?)\s+to\s+(.*?)\s*$',
      'i'
    ))[2] AS to_part
  FROM
    unnested_flags
),
aggregated_parts AS (
  SELECT
    rn,
    string_agg(DISTINCT trim(from_part), ', ') FILTER (WHERE from_part IS NOT NULL) AS migration_from,
    string_agg(DISTINCT trim(to_part), ', ') FILTER (WHERE to_part IS NOT NULL) AS migration_to
  FROM
    extracted_parts
  GROUP BY
    rn
),
base_with_migrations AS (
  -- Base data including migrations and original row identifier 'rn'
  SELECT
    t.evaluation_period, t.current_period, t.prior_period, t.mcid, t.currency_code,
    t.current_product_group, t.prior_product_group, t.current_product_solution, t.prior_product_solution,
    t.current_pathways, t.prior_pathways, t.prior_period_product_arr_lcu, t.current_period_product_arr_lcu,
    t.prior_period_product_arr_usd_ccfx, t.current_period_product_arr_usd_ccfx, t.product_arr_change_ccfx,
    t.product_arr_change_lcu, t.active_flag_count, t.flag_descriptions,
    ap.migration_from, ap.migration_to, t.rn -- Include original columns and generated ones + rn
  FROM base_with_id t
  LEFT JOIN aggregated_parts ap ON t.rn = ap.rn
),
unnested_targets AS (
  -- Unnest the migration_to list for each original row (rn)
  -- Keep track of the original row (rn) and the target items it could migrate to
  SELECT
    bwm.rn,
    bwm.mcid,
    bwm.evaluation_period,
    bwm.currency_code,
    lower(trim(target_item)) as target_item -- lowercase target item from the list
  FROM
    base_with_migrations bwm
  CROSS JOIN LATERAL
    -- Split migration_to by ', ' - adjust delimiter if needed
    unnest(string_to_array(bwm.migration_to, ', ')) AS target_item
  WHERE bwm.migration_to IS NOT NULL AND bwm.migration_to != '' -- Only process rows with a migration_to list
),
target_arrs AS (
  -- Find the ARR associated with each potential target_item for a given original row (ut.rn)
  -- by looking up the ARR of rows in the same group where current_pathways matches the target_item
  SELECT
    ut.rn, -- original row identifier from unnested_targets
    ut.target_item,
    -- Use COALESCE to treat missing matches as 0 ARR for ranking purposes
    COALESCE(bwm_group.current_period_product_arr_usd_ccfx, 0) as arr
  FROM
    unnested_targets ut
  LEFT JOIN -- Use LEFT JOIN to include target_items even if they don't match a pathway
    base_with_migrations bwm_group ON ut.mcid = bwm_group.mcid
                                   AND ut.evaluation_period = bwm_group.evaluation_period
                                   AND ut.currency_code = bwm_group.currency_code
                                   -- Match the unnested target item with the pathway (case-insensitive)
                                   AND ut.target_item = lower(trim(bwm_group.current_pathways))
),
ranked_targets AS (
  -- For each original row (rn), rank its potential target_items based on the ARR found
  SELECT
    rn,
    target_item,
    arr,
    ROW_NUMBER() OVER (PARTITION BY rn ORDER BY arr DESC NULLS LAST) as rnk
  FROM
    target_arrs
),
selected_best_target AS (
  -- Select the top-ranked target_item (highest ARR) for each original row
  SELECT rn, target_item
  FROM ranked_targets
  WHERE rnk = 1
),
unnested_sources AS (
  SELECT
    bwm.rn,
    bwm.mcid,
    bwm.evaluation_period,
    bwm.currency_code,
    lower(trim(source_item)) as source_item
  FROM
    base_with_migrations bwm
  CROSS JOIN LATERAL
    unnest(string_to_array(bwm.migration_from, ', ')) AS source_item
  WHERE bwm.migration_from IS NOT NULL AND bwm.migration_from != ''
),
source_arrs AS (
  SELECT
    us.rn,
    us.source_item,
    COALESCE(bwm_group.prior_period_product_arr_usd_ccfx, 0) as prior_arr
  FROM
    unnested_sources us
  LEFT JOIN
    base_with_migrations bwm_group ON us.mcid = bwm_group.mcid
                                   AND us.evaluation_period = bwm_group.evaluation_period
                                   AND us.currency_code = bwm_group.currency_code
                                   AND us.source_item = lower(trim(bwm_group.prior_pathways))
),
ranked_sources AS (
  SELECT
    rn,
    source_item,
    prior_arr,
    ROW_NUMBER() OVER (PARTITION BY rn ORDER BY prior_arr DESC NULLS LAST) as rnk
  FROM
    source_arrs
),
selected_best_source AS (
  SELECT rn, source_item as selected_migration_from
  FROM ranked_sources
  WHERE rnk = 1
)
-- Final Select: Join the best target and source back to the full base data
,
final_select as (
SELECT
  bwm.evaluation_period,
  bwm.current_period,
  bwm.prior_period,
  bwm.mcid,
  bwm.currency_code,
  bwm.current_product_group,
  bwm.prior_product_group,
  bwm.current_product_solution,
  bwm.prior_product_solution,
  bwm.current_pathways,
  bwm.prior_pathways,
  bwm.prior_period_product_arr_lcu,
  bwm.current_period_product_arr_lcu,
  bwm.prior_period_product_arr_usd_ccfx,
  bwm.current_period_product_arr_usd_ccfx,
  bwm.product_arr_change_ccfx,
  bwm.product_arr_change_lcu,
  bwm.active_flag_count,
  bwm.flag_descriptions,
  bwm.migration_from,
  bwm.migration_to,
  sbt.target_item AS selected_migration_to,
  sbs.selected_migration_from,
  ep.flag_part AS "Movement Classification"
FROM
  base_with_migrations bwm
LEFT JOIN
  selected_best_target sbt ON bwm.rn = sbt.rn
LEFT JOIN
  selected_best_source sbs ON bwm.rn = sbs.rn
LEFT JOIN
  extracted_parts ep ON bwm.rn = ep.rn
                       AND lower(trim(ep.from_part)) = lower(trim(sbs.selected_migration_from))
                       AND lower(trim(ep.to_part)) = lower(trim(sbt.target_item))

ORDER BY
  bwm.mcid, bwm.evaluation_period, bwm.rn
    ),
--- reducing negative migrations
    ranking_negative_migrations as (
select
    evaluation_period,
    mcid,
    currency_code ,
    current_pathways ,
    prior_pathways ,
    selected_migration_to ,
    selected_migration_from ,
    product_arr_change_ccfx,
    "Movement Classification",
    row_number() over(partition by mcid , evaluation_period, currency_code order by product_arr_change_ccfx asc) as rnk_neg_mig
from final_select
where product_arr_change_ccfx < 0 and "Movement Classification" is not null
    ),
selected_negative_migration as (

       select
        *,
        case when rnk_neg_mig = 1 then "Movement Classification" else null end as mvnt_cls
       from ranking_negative_migrations
),
    joining_negative_migration as (
    select
          a.*,
          b.rnk_neg_mig,
          b.mvnt_cls
--           case when  a.product_arr_change_ccfx < 0
--               then b.mvnt_cls
--               else a."Movement Classification" end as "Movement Classification"
    from final_select as a
    left join selected_negative_migration as b
        on a.evaluation_period = b.evaluation_period
        and a.mcid = b.mcid
        and a.currency_code  = b.currency_code
        and a.product_arr_change_ccfx = b.product_arr_change_ccfx
        and a."Movement Classification" = b."Movement Classification"
)
, fixed_negatives as (
select
        a.evaluation_period,
          a.current_period,
          a.prior_period,
          a.mcid,
          a.currency_code,
          a.current_product_group,
          a.prior_product_group,
          a.current_product_solution,
          a.prior_product_solution,
          a.current_pathways,
          a.prior_pathways,
          a.prior_period_product_arr_lcu,
          a.current_period_product_arr_lcu,
          a.prior_period_product_arr_usd_ccfx,
          a.current_period_product_arr_usd_ccfx,
          a.product_arr_change_ccfx,
          a.product_arr_change_lcu,
          a.active_flag_count,
          a.flag_descriptions,
          a.migration_from,
          a.migration_to,
          a.selected_migration_to,
          a.selected_migration_from,
          case when product_arr_change_ccfx < 0 and "Movement Classification" is not null then
            mvnt_cls else "Movement Classification" end as "Movement Classification"
    from joining_negative_migration as a
)
, ranking_positive_migrations as (
    select
        evaluation_period,
        mcid,
        currency_code ,
        current_pathways ,
        prior_pathways ,
        selected_migration_to ,
        selected_migration_from ,
        product_arr_change_ccfx,
        "Movement Classification",
        row_number() over(partition by mcid , evaluation_period, currency_code order by product_arr_change_ccfx desc) as rnk_pos_mig,
        count("Movement Classification") over(partition by evaluation_period,mcid) as instance_count
    from fixed_negatives
    where product_arr_change_ccfx > 0 and "Movement Classification" is not null
)

, selecting_positive_migrations as (

       select
        *,
        case when rnk_pos_mig = 1 then "Movement Classification" else null end as mvnt_cls
       from ranking_positive_migrations
)
, joining_positive_migrations as (
    select
          a.*,
          b.rnk_pos_mig,
          b.mvnt_cls

    from fixed_negatives as a
    left join selecting_positive_migrations as b
        on a.evaluation_period = b.evaluation_period
        and a.mcid = b.mcid
        and a.currency_code  = b.currency_code
        and a.product_arr_change_ccfx = b.product_arr_change_ccfx
        and a."Movement Classification" = b."Movement Classification"
)
    SELECT
        a.evaluation_period,
          a.current_period,
          a.prior_period,
          a.mcid,
          a.currency_code,
          a.current_product_group,
          a.prior_product_group,
          a.current_product_solution,
          a.prior_product_solution,
          a.current_pathways,
          a.prior_pathways,
          a.prior_period_product_arr_lcu,
          a.current_period_product_arr_lcu,
          a.prior_period_product_arr_usd_ccfx,
          a.current_period_product_arr_usd_ccfx,
          a.product_arr_change_ccfx,
          a.product_arr_change_lcu,
          a.active_flag_count,
          a.flag_descriptions,
          a.migration_from,
          a.migration_to,
          a.selected_migration_to,
          a.selected_migration_from,
          case when product_arr_change_ccfx > 0 and "Movement Classification" is not null then
            mvnt_cls else "Movement Classification" end as "Movement Classification"
    from joining_positive_migrations as a

);