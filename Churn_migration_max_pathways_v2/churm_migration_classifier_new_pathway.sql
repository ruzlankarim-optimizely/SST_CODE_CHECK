-- ,cross_sell - migration -- commerce_connect_ms to monetize,
-- cross_sell - migration -- content_management_paas_ms to monetize,
-- cross_sell - migration -- eol_everweb to monetize,
-- cross_sell - migration -- search_navigation_standalone to monetize
-- REGEXP_REPLACE(
--     REGEXP_REPLACE(
--         REGEXP_REPLACE(column_name, ' \(', '_'),  -- Replace " (" with "_"
--         '\)', ''  -- Remove ")"
--     ),
--     ' ', '_'  -- Replace all remaining spaces with "_"
-- )
-- select * from (
-- SELECT a.*,
--     CASE
--     WHEN migration_from is not null THEN migration_from
--     WHEN migration_to is not null THEN migration_to
--     ELSE 'USUAL'
--   END AS pathways
-- FROM  ufdm_archive.sst_adhoc_lcoked_18032025_0244  as a) as a
-- where pathways ilike 'Commerce Connect%'
--
-- select
--     REGEXP_REPLACE(REGEXP_REPLACE(
--         REGEXP_REPLACE(
--           REGEXP_REPLACE(
--              REGEXP_REPLACE(
--                 REGEXP_REPLACE(
--                   REGEXP_REPLACE(current_pathways, '\s*-\s*', '_')
--                    ,' & ', '_')
--                 ,'\s*M&S\s*', '_MS')
--               , ' \(', '_')
--             ,'\)', '')
--            ,' ', '_')
--         ,' ','_'
--        ) AS current_pathways ,
--        REGEXP_REPLACE(
--         REGEXP_REPLACE(
--           REGEXP_REPLACE(
--              REGEXP_REPLACE(
--                 REGEXP_REPLACE(
--                   REGEXP_REPLACE(prior_pathways, '\s*-\s*', '_')
--                    ,' & ', '_')
--                 ,'\s*M&S\s*', '_MS')
--               , ' \(', '_')
--             ,'\)', '')
--            ,' ', '_'
--        ) AS prior_pathways
-- from ryzlan.sst_product_pathways_bridge2
--     group by 1,2
drop table if exists sandbox.churn_migration_classifiers_max_value_v2;
create table sandbox.churn_migration_classifiers_max_value_v2 as (
  with initial_table as (
    select *
    from ryzlan.sst_product_pathways_bridge2
    where mcid is not null
      and mcid <> '-' --     and mcid = '4e128cce-793a-e811-8124-70106faab5f1'
      --     and evaluation_period = '2024M10'
  ) -- 513810
  --     select count(*) from initial_table ;
,
  initial_table_2 as (
    select *,
      -- downgraded product
      case
        when current_pathways = 'Commerce Connect LI'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as downgraded_commerce_connect_li_in_current_date,
      case
        when current_pathways = 'Commerce Connect M&S'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as downgraded_commerce_connect_ms_in_current_date,
      case
        when current_pathways = 'Configured Commerce LI'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as downgraded_configured_commerce_li_in_current_date,
      case
        when current_pathways = 'Configured Commerce M&S'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as downgraded_configured_commerce_ms_in_current_date,
      case
        when current_pathways = 'Content Management PaaS LI'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as downgraded_content_management_paas_li_in_current_date,
      case
        when current_pathways = 'Content Management PaaS M&S'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as downgraded_content_management_paas_ms_in_current_date,
      case
        when current_pathways = 'EOL - Everweb'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as downgraded_eol_everweb_in_current_date,
      case
        when current_pathways = 'EOL - Legacy Visitor Intelligence'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as downgraded_visitor_intelligence_in_current_date,
      case
        when current_pathways = 'Legacy Ektron'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as downgraded_legacy_ektron_in_current_date,
      case
        when current_pathways = 'Legacy Ektron M&S'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as downgraded_legacy_ektron_ms_in_current_date,
      case
        when current_pathways = 'Search & Navigation - Standalone'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as downgraded_search_navigation_in_current_date,
      case
        when current_pathways = 'Search & Navigation - Standalone M&S'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as downgraded_search_navigation_ms_in_current_date,
      -- Churned product
      case
        when prior_pathways = 'Commerce Connect LI'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx = 0 then 1
        else 0
      end as churned_commerce_connect_li_in_current_date,
      case
        when prior_pathways = 'Commerce Connect M&S'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx = 0 then 1
        else 0
      end as churned_commerce_connect_ms_in_current_date,
      case
        when prior_pathways = 'Configured Commerce LI'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx = 0 then 1
        else 0
      end as churned_configured_commerce_li_in_current_date,
      case
        when prior_pathways = 'Configured Commerce M&S'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx = 0 then 1
        else 0
      end as churned_configured_commerce_ms_in_current_date,
      case
        when prior_pathways = 'Content Management PaaS LI'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx = 0 then 1
        else 0
      end as churned_content_management_paas_li_in_current_date,
      case
        when prior_pathways = 'Content Management PaaS M&S'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx = 0 then 1
        else 0
      end as churned_content_management_paas_ms_in_current_date,
      case
        when prior_pathways = 'EOL - Everweb'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx = 0 then 1
        else 0
      end as churned_eol_everweb_in_current_date,
      case
        when prior_pathways = 'EOL - Legacy Visitor Intelligence'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx = 0 then 1
        else 0
      end as churned_visitor_intelligence_in_current_date,
      case
        when prior_pathways = 'Legacy Ektron'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx = 0 then 1
        else 0
      end as churned_legacy_ektron_in_current_date,
      case
        when prior_pathways = 'Legacy Ektron M&S'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx = 0 then 1
        else 0
      end as churned_legacy_ektron_ms_in_current_date,
      case
        when prior_pathways = 'Search & Navigation - Standalone'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx = 0 then 1
        else 0
      end as churned_search_navigation_in_current_date,
      case
        when prior_pathways = 'Search & Navigation - Standalone M&S'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx = 0 then 1
        else 0
      end as churned_search_navigation_ms_in_current_date,
      -- added products 
      case
        when current_pathways = 'Monetize'
        and product_arr_change_ccfx > 0
        and prior_period_product_arr_usd_ccfx = 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as added_monetize_in_current_date,
      case
        when current_pathways = 'Content Management (CMS)'
        and product_arr_change_ccfx > 0
        and prior_period_product_arr_usd_ccfx = 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as added_cms_in_current_date,
      case
        when current_pathways = 'ODP'
        and product_arr_change_ccfx > 0
        and prior_period_product_arr_usd_ccfx = 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as added_odp_in_current_date,
      -- increased products 
      case
        when current_pathways = 'Monetize'
        and product_arr_change_ccfx > 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as increased_monetize_in_current_date,
      case
        when current_pathways = 'Content Management (CMS)'
        and product_arr_change_ccfx > 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as increased_cms_in_current_date,
      case
        when current_pathways = 'ODP'
        and product_arr_change_ccfx > 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as increased_odp_in_current_date,
      -- current_date with arr 
      case
        when current_pathways = 'Monetize'
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as monetize_in_current_date_with_arr,
      case
        when current_pathways = 'Content Management (CMS)'
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as content_management_cms_in_current_date_with_arr,
      case
        when current_pathways = 'ODP'
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as odp_in_current_date_with_arr,
      -- previous date with arr 
      case
        when prior_pathways = 'Commerce Connect LI'
        and prior_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as commerce_connect_li_in_previous_date_with_arr,
      case
        when prior_pathways = 'Commerce Connect M&S'
        and prior_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as commerce_connect_ms_in_previous_date_with_arr,
      case
        when prior_pathways = 'Configured Commerce LI'
        and prior_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as configured_commerce_li_in_previous_date_with_arr,
      case
        when prior_pathways = 'Configured Commerce M&S'
        and prior_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as configured_commerce_ms_in_previous_date_with_arr,
      case
        when prior_pathways = 'Content Management PaaS LI'
        and prior_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as content_management_paas_li_in_previous_date_with_arr,
      case
        when prior_pathways = 'Content Management PaaS M&S'
        and prior_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as content_management_paas_ms_in_previous_date_with_arr,
      case
        when prior_pathways = 'EOL - Everweb'
        and prior_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as eol_everweb_in_previous_date_with_arr,
      case
        when prior_pathways = 'EOL - Legacy Visitor Intelligence'
        and prior_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as eol_legacy_visitor_intelligence_in_previous_date_with_arr,
      case
        when prior_pathways = 'Legacy Ektron'
        and prior_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as legacy_ektron_in_previous_date_with_arr,
      case
        when prior_pathways = 'Legacy Ektron M&S'
        and prior_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as legacy_ektron_ms_in_previous_date_with_arr,
      case
        when prior_pathways = 'Search & Navigation - Standalone'
        and prior_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as search_navigation_standalone_in_previous_date_with_arr,
      case
        when prior_pathways = 'Search & Navigation - Standalone M&S'
        and prior_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as search_navigation_standalone_ms_in_previous_date_with_arr
    from initial_table
  ),
  initial_table_3 as (
    select *,
      -- SUM MIGRATION TO CURRENT DATE 
      sum(monetize_in_current_date_with_arr) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_monetize_in_current_date_with_arr,
      sum(content_management_cms_in_current_date_with_arr) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_cms_in_current_date_with_arr,
      sum(odp_in_current_date_with_arr) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_odp_in_current_date_with_arr,
      -- SUM DOWNGRADE
      sum(downgraded_commerce_connect_li_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_downgraded_commerce_connect_li_in_current_date,
      sum(downgraded_commerce_connect_ms_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_downgraded_commerce_connect_ms_in_current_date,
      sum(
        downgraded_configured_commerce_li_in_current_date
      ) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_downgraded_configured_commerce_li_in_current_date,
      sum(
        downgraded_configured_commerce_ms_in_current_date
      ) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_downgraded_configured_commerce_ms_in_current_date,
      sum(
        downgraded_content_management_paas_li_in_current_date
      ) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_downgraded_content_management_paas_li_in_current_date,
      sum(
        downgraded_content_management_paas_ms_in_current_date
      ) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_downgraded_content_management_paas_ms_in_current_date,
      sum(downgraded_eol_everweb_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_downgraded_eol_everweb_in_current_date,
      sum(downgraded_visitor_intelligence_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_downgraded_visitor_intelligence_in_current_date,
      sum(downgraded_legacy_ektron_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_downgraded_legacy_ektron_in_current_date,
      sum(downgraded_legacy_ektron_ms_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_downgraded_legacy_ektron_ms_in_current_date,
      sum(downgraded_search_navigation_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_downgraded_search_navigation_in_current_date,
      sum(downgraded_search_navigation_ms_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_downgraded_search_navigation_ms_in_current_date,
      -- SUM CHURNED
      sum(churned_commerce_connect_li_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_churned_commerce_connect_li_in_current_date,
      sum(churned_commerce_connect_ms_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_churned_commerce_connect_ms_in_current_date,
      sum(churned_configured_commerce_li_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_churned_configured_commerce_li_in_current_date,
      sum(churned_configured_commerce_ms_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_churned_configured_commerce_ms_in_current_date,
      sum(
        churned_content_management_paas_li_in_current_date
      ) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_churned_content_management_paas_li_in_current_date,
      sum(
        churned_content_management_paas_ms_in_current_date
      ) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_churned_content_management_paas_ms_in_current_date,
      sum(churned_eol_everweb_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_churned_eol_everweb_in_current_date,
      sum(churned_visitor_intelligence_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_churned_visitor_intelligence_in_current_date,
      sum(churned_legacy_ektron_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_churned_legacy_ektron_in_current_date,
      sum(churned_legacy_ektron_ms_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_churned_legacy_ektron_ms_in_current_date,
      sum(churned_search_navigation_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_churned_search_navigation_in_current_date,
      sum(churned_search_navigation_ms_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_churned_search_navigation_ms_in_current_date,
      -- SUM MIGRATION FROM PREVIOUS DATE
      sum(commerce_connect_li_in_previous_date_with_arr) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_commerce_connect_li_in_previous_date_with_arr,
      sum(commerce_connect_ms_in_previous_date_with_arr) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_commerce_connect_ms_in_previous_date_with_arr,
      sum(configured_commerce_li_in_previous_date_with_arr) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_configured_commerce_li_in_previous_date_with_arr,
      sum(configured_commerce_ms_in_previous_date_with_arr) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_configured_commerce_ms_in_previous_date_with_arr,
      sum(
        content_management_paas_li_in_previous_date_with_arr
      ) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_content_management_paas_li_in_previous_date_with_arr,
      sum(
        content_management_paas_ms_in_previous_date_with_arr
      ) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_content_management_paas_ms_in_previous_date_with_arr,
      sum(eol_everweb_in_previous_date_with_arr) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_eol_everweb_in_previous_date_with_arr,
      sum(
        eol_legacy_visitor_intelligence_in_previous_date_with_arr
      ) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_visitor_intelligence_in_previous_date_with_arr,
      sum(legacy_ektron_in_previous_date_with_arr) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_legacy_ektron_in_previous_date_with_arr,
      sum(legacy_ektron_ms_in_previous_date_with_arr) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_legacy_ektron_ms_in_previous_date_with_arr,
      sum(
        search_navigation_standalone_in_previous_date_with_arr
      ) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_search_navigation_in_previous_date_with_arr,
      sum(
        search_navigation_standalone_ms_in_previous_date_with_arr
      ) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_search_navigation_ms_in_previous_date_with_arr,
      -- SUM ADDED in current date
      sum(added_monetize_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_added_monetize_in_current_date,
      sum(added_cms_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_added_cms_in_current_date,
      sum(added_odp_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_added_odp_in_current_date,
      -- SUM INCREASED in current date
      sum(increased_monetize_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_increased_monetize_in_current_date,
      sum(increased_cms_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_increased_cms_in_current_date,
      sum(increased_odp_in_current_date) over(
        partition by mcid,
        evaluation_period,
        currency_code
      ) as sum_increased_odp_in_current_date
    from initial_table_2
  ),
  initial_table_4 as (
    select *,
      -- downgraded commerce connect li to to product
      case
        when downgraded_commerce_connect_li_in_current_date = 1 THEN case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_commerce_connect_li_to_monetize,
      case
        when downgraded_commerce_connect_li_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_commerce_connect_li_to_cms,
      -- downgrade commerce connect ms to product
      case
        when downgraded_commerce_connect_ms_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_commerce_connect_ms_to_monetize,
      case
        when downgraded_commerce_connect_ms_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_commerce_connect_ms_to_cms,
      -- downgrade configured commerce li to product
      case
        when downgraded_configured_commerce_li_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_configured_commerce_li_to_monetize,
      case
        when downgraded_configured_commerce_li_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_configured_commerce_li_to_cms,
      -- downgrade configured commerce ms to product
      case
        when downgraded_configured_commerce_ms_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_configured_commerce_ms_to_monetize,
      case
        when downgraded_configured_commerce_ms_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_configured_commerce_ms_to_cms,
      -- downgrade content management paas li to product
      case
        when downgraded_content_management_paas_li_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_content_management_paas_li_to_monetize,
      case
        when downgraded_content_management_paas_li_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_content_management_paas_li_to_cms,
      -- downgrade content management paas ms to product
      case
        when downgraded_content_management_paas_ms_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_content_management_paas_ms_to_monetize,
      case
        when downgraded_content_management_paas_ms_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_content_management_paas_ms_to_cms,
      -- downgrade eol everweb to product
      case
        when downgraded_eol_everweb_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_eol_everweb_to_monetize,
      case
        when downgraded_eol_everweb_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_eol_everweb_to_cms,
      -- downgrade eol legacy visitor intelligence to product
      case
        when downgraded_visitor_intelligence_in_current_date = 1 then case
          when sum_odp_in_current_date_with_arr > 0 then 1
          when sum_added_odp_in_current_date > 0
          or sum_increased_odp_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_visitor_intelligence_to_odp,
      -- downgrade legacy ektron to product
      case
        when downgraded_legacy_ektron_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_legacy_ektron_to_monetize,
      case
        when downgraded_legacy_ektron_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_legacy_ektron_to_cms,
      -- downgrade legacy ektron ms to product
      case
        when downgraded_legacy_ektron_ms_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_legacy_ektron_ms_to_monetize,
      case
        when downgraded_legacy_ektron_ms_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_legacy_ektron_ms_to_cms,
      -- downgrade search navigation standalone to product
      case
        when downgraded_search_navigation_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_search_navigation_to_monetize,
      case
        when downgraded_search_navigation_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_search_navigation_to_cms,
      -- downgrade search navigation standalone ms to product
      case
        when downgraded_search_navigation_ms_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_search_navigation_ms_to_monetize,
      case
        when downgraded_search_navigation_ms_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downgrade_migration_search_navigation_ms_to_cms,
      -- downsell commerce connect li to product
      case
        when churned_commerce_connect_li_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_commerce_connect_li_to_monetize,
      case
        when churned_commerce_connect_li_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_commerce_connect_li_to_cms,
      -- churned commerce connect ms to product
      case
        when churned_commerce_connect_ms_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_commerce_connect_ms_to_monetize,
      case
        when churned_commerce_connect_ms_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_commerce_connect_ms_to_cms,
      -- churned configured commerce li to product
      case
        when churned_configured_commerce_li_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_configured_commerce_li_to_monetize,
      case
        when churned_configured_commerce_li_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_configured_commerce_li_to_cms,
      -- churned configured commerce ms to product
      case
        when churned_configured_commerce_ms_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_configured_commerce_ms_to_monetize,
      case
        when churned_configured_commerce_ms_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_configured_commerce_ms_to_cms,
      -- churned content management paas li to product
      case
        when churned_content_management_paas_li_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_content_management_paas_li_to_monetize,
      case
        when churned_content_management_paas_li_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_content_management_paas_li_to_cms,
      -- churned content management paas ms to product
      case
        when churned_content_management_paas_ms_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_content_management_paas_ms_to_monetize,
      case
        when churned_content_management_paas_ms_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_content_management_paas_ms_to_cms,
      -- churned eol everweb to product
      case
        when churned_eol_everweb_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_eol_everweb_to_monetize,
      case
        when churned_eol_everweb_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_eol_everweb_to_cms,
      -- churned eol legacy visitor intelligence to product
      case
        when churned_visitor_intelligence_in_current_date = 1 then case
          when sum_odp_in_current_date_with_arr > 0 then 1
          when sum_added_odp_in_current_date > 0
          or sum_increased_odp_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_visitor_intelligence_to_odp,
      -- churned legacy ektron to product
      case
        when churned_legacy_ektron_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_legacy_ektron_to_monetize,
      case
        when churned_legacy_ektron_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_legacy_ektron_to_cms,
      -- churned legacy ektron ms to product
      case
        when churned_legacy_ektron_ms_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_legacy_ektron_ms_to_monetize,
      case
        when churned_legacy_ektron_ms_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_legacy_ektron_ms_to_cms,
      -- churned search navigation standalone to product
      case
        when churned_search_navigation_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_search_navigation_to_monetize,
      case
        when churned_search_navigation_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_search_navigation_to_cms,
      -- churned search navigation standalone ms to product
      case
        when churned_search_navigation_ms_in_current_date = 1 then case
          when sum_monetize_in_current_date_with_arr > 0 then 1
          when sum_added_monetize_in_current_date > 0
          or sum_increased_monetize_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_search_navigation_ms_to_monetize,
      case
        when churned_search_navigation_ms_in_current_date = 1 then case
          when sum_cms_in_current_date_with_arr > 0 then 1
          when sum_added_cms_in_current_date > 0
          or sum_increased_cms_in_current_date > 0 then 1
          else 0
        end
        else 0
      end as downsell_migration_search_navigation_ms_to_cms,
      -- CROSS SELL monetize to migration from products
      case
        when added_monetize_in_current_date = 1 then case
          when sum_downgraded_commerce_connect_li_in_current_date > 0
          or sum_churned_commerce_connect_li_in_current_date > 0 then 1
          when sum_commerce_connect_li_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_commerce_connect_li_to_monetize,
      case
        when added_monetize_in_current_date = 1 then case
          when sum_downgraded_commerce_connect_ms_in_current_date > 0
          or sum_churned_commerce_connect_ms_in_current_date > 0 then 1
          when sum_commerce_connect_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_commerce_connect_ms_to_monetize,
      case
        when added_monetize_in_current_date = 1 then case
          when sum_downgraded_configured_commerce_li_in_current_date > 0
          or sum_churned_configured_commerce_li_in_current_date > 0 then 1
          when sum_configured_commerce_li_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_configured_commerce_li_to_monetize,
      case
        when added_monetize_in_current_date = 1 then case
          when sum_downgraded_configured_commerce_ms_in_current_date > 0
          or sum_churned_configured_commerce_ms_in_current_date > 0 then 1
          when sum_configured_commerce_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_configured_commerce_ms_to_monetize,
      case
        when added_monetize_in_current_date = 1 then case
          when sum_downgraded_content_management_paas_li_in_current_date > 0
          or sum_churned_content_management_paas_li_in_current_date > 0 then 1
          when sum_content_management_paas_li_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_content_management_paas_li_to_monetize,
      case
        when added_monetize_in_current_date = 1 then case
          when sum_downgraded_content_management_paas_ms_in_current_date > 0
          or sum_churned_content_management_paas_ms_in_current_date > 0 then 1
          when sum_content_management_paas_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_content_management_paas_ms_to_monetize,
      case
        when added_monetize_in_current_date = 1 then case
          when sum_downgraded_eol_everweb_in_current_date > 0
          or sum_churned_eol_everweb_in_current_date > 0 then 1
          when sum_eol_everweb_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_eol_everweb_to_monetize,
      case
        when added_monetize_in_current_date = 1 then case
          when sum_downgraded_legacy_ektron_in_current_date > 0
          or sum_churned_legacy_ektron_in_current_date > 0 then 1
          when sum_legacy_ektron_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_legacy_ektron_to_monetize,
      case
        when added_monetize_in_current_date = 1 then case
          when sum_downgraded_legacy_ektron_ms_in_current_date > 0
          or sum_churned_legacy_ektron_ms_in_current_date > 0 then 1
          when sum_legacy_ektron_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_legacy_ektron_ms_to_monetize,
      case
        when added_monetize_in_current_date = 1 then case
          when sum_downgraded_search_navigation_in_current_date > 0
          or sum_churned_search_navigation_in_current_date > 0 then 1
          when sum_search_navigation_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_search_navigation_to_monetize,
      case
        when added_monetize_in_current_date = 1 then case
          when sum_downgraded_search_navigation_ms_in_current_date > 0
          or sum_churned_search_navigation_ms_in_current_date > 0 then 1
          when sum_search_navigation_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_search_navigation_ms_to_monetize,
      -- cross sell content management cms to migration from products
      case
        when added_cms_in_current_date = 1 then case
          when sum_downgraded_commerce_connect_li_in_current_date > 0
          or sum_churned_commerce_connect_li_in_current_date > 0 then 1
          when sum_commerce_connect_li_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_commerce_connect_li_to_cms,
      case
        when added_cms_in_current_date = 1 then case
          when sum_downgraded_commerce_connect_ms_in_current_date > 0
          or sum_churned_commerce_connect_ms_in_current_date > 0 then 1
          when sum_commerce_connect_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_commerce_connect_ms_to_cms,
      case
        when added_cms_in_current_date = 1 then case
          when sum_downgraded_configured_commerce_li_in_current_date > 0
          or sum_churned_configured_commerce_li_in_current_date > 0 then 1
          when sum_configured_commerce_li_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_configured_commerce_li_to_cms,
      case
        when added_cms_in_current_date = 1 then case
          when sum_downgraded_configured_commerce_ms_in_current_date > 0
          or sum_churned_configured_commerce_ms_in_current_date > 0 then 1
          when sum_configured_commerce_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_configured_commerce_ms_to_cms,
      case
        when added_cms_in_current_date = 1 then case
          when sum_downgraded_content_management_paas_li_in_current_date > 0
          or sum_churned_content_management_paas_li_in_current_date > 0 then 1
          when sum_content_management_paas_li_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_content_management_paas_li_to_cms,
      case
        when added_cms_in_current_date = 1 then case
          when sum_downgraded_content_management_paas_ms_in_current_date > 0
          or sum_churned_content_management_paas_ms_in_current_date > 0 then 1
          when sum_content_management_paas_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_content_management_paas_ms_to_cms,
      case
        when added_cms_in_current_date = 1 then case
          when sum_downgraded_eol_everweb_in_current_date > 0
          or sum_churned_eol_everweb_in_current_date > 0 then 1
          when sum_eol_everweb_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_eol_everweb_to_cms,
      case
        when added_cms_in_current_date = 1 then case
          when sum_downgraded_legacy_ektron_in_current_date > 0
          or sum_churned_legacy_ektron_in_current_date > 0 then 1
          when sum_legacy_ektron_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_legacy_ektron_to_cms,
      case
        when added_cms_in_current_date = 1 then case
          when sum_downgraded_legacy_ektron_ms_in_current_date > 0
          or sum_churned_legacy_ektron_ms_in_current_date > 0 then 1
          when sum_legacy_ektron_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_legacy_ektron_ms_to_cms,
      case
        when added_cms_in_current_date = 1 then case
          when sum_downgraded_search_navigation_in_current_date > 0
          or sum_churned_search_navigation_in_current_date > 0 then 1
          when sum_search_navigation_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_search_navigation_to_cms,
      case
        when added_cms_in_current_date = 1 then case
          when sum_downgraded_search_navigation_ms_in_current_date > 0
          or sum_churned_search_navigation_ms_in_current_date > 0 then 1
          when sum_search_navigation_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_search_navigation_ms_to_cms,
      -- cross sell odp to migration from products
      case
        when added_odp_in_current_date = 1 then case
          when sum_downgraded_visitor_intelligence_in_current_date > 0
          or sum_churned_visitor_intelligence_in_current_date > 0 then 1
          when sum_visitor_intelligence_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as cross_sell_migration_visitor_intelligence_to_odp,
      -- UPSELL migration from products to monetize
      -- upsell migration monetize to migration from products
      case
        when increased_monetize_in_current_date = 1 then case
          when sum_downgraded_commerce_connect_li_in_current_date > 0
          or sum_churned_commerce_connect_li_in_current_date > 0 then 1
          when sum_commerce_connect_li_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_commerce_connect_li_to_monetize,
      case
        when increased_monetize_in_current_date = 1 then case
          when sum_downgraded_commerce_connect_ms_in_current_date > 0
          or sum_churned_commerce_connect_ms_in_current_date > 0 then 1
          when sum_commerce_connect_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_commerce_connect_ms_to_monetize,
      case
        when increased_monetize_in_current_date = 1 then case
          when sum_downgraded_configured_commerce_li_in_current_date > 0
          or sum_churned_configured_commerce_li_in_current_date > 0 then 1
          when sum_configured_commerce_li_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_configured_commerce_li_to_monetize,
      case
        when increased_monetize_in_current_date = 1 then case
          when sum_downgraded_configured_commerce_ms_in_current_date > 0
          or sum_churned_configured_commerce_ms_in_current_date > 0 then 1
          when sum_configured_commerce_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_configured_commerce_ms_to_monetize,
      case
        when increased_monetize_in_current_date = 1 then case
          when sum_downgraded_content_management_paas_li_in_current_date > 0
          or sum_churned_content_management_paas_li_in_current_date > 0 then 1
          when sum_content_management_paas_li_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_content_management_paas_li_to_monetize,
      case
        when increased_monetize_in_current_date = 1 then case
          when sum_downgraded_content_management_paas_ms_in_current_date > 0
          or sum_churned_content_management_paas_ms_in_current_date > 0 then 1
          when sum_content_management_paas_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_content_management_paas_ms_to_monetize,
      case
        when increased_monetize_in_current_date = 1 then case
          when sum_downgraded_eol_everweb_in_current_date > 0
          or sum_churned_eol_everweb_in_current_date > 0 then 1
          when sum_eol_everweb_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_eol_everweb_to_monetize,
      case
        when increased_monetize_in_current_date = 1 then case
          when sum_downgraded_legacy_ektron_in_current_date > 0
          or sum_churned_legacy_ektron_in_current_date > 0 then 1
          when sum_legacy_ektron_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_legacy_ektron_to_monetize,
      case
        when increased_monetize_in_current_date = 1 then case
          when sum_downgraded_legacy_ektron_ms_in_current_date > 0
          or sum_churned_legacy_ektron_ms_in_current_date > 0 then 1
          when sum_legacy_ektron_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_legacy_ektron_ms_to_monetize,
      case
        when increased_monetize_in_current_date = 1 then case
          when sum_downgraded_search_navigation_in_current_date > 0
          or sum_churned_search_navigation_in_current_date > 0 then 1
          when sum_search_navigation_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_search_navigation_to_monetize,
      case
        when increased_monetize_in_current_date = 1 then case
          when sum_downgraded_search_navigation_ms_in_current_date > 0
          or sum_churned_search_navigation_ms_in_current_date > 0 then 1
          when sum_search_navigation_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_search_navigation_ms_to_monetize,
      -- upsell migration content management cms to migration from products
      case
        when increased_cms_in_current_date = 1 then case
          when sum_downgraded_commerce_connect_li_in_current_date > 0
          or sum_churned_commerce_connect_li_in_current_date > 0 then 1
          when sum_commerce_connect_li_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_commerce_connect_li_to_cms,
      case
        when increased_cms_in_current_date = 1 then case
          when sum_downgraded_commerce_connect_ms_in_current_date > 0
          or sum_churned_commerce_connect_ms_in_current_date > 0 then 1
          when sum_commerce_connect_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_commerce_connect_ms_to_cms,
      case
        when increased_cms_in_current_date = 1 then case
          when sum_downgraded_configured_commerce_li_in_current_date > 0
          or sum_churned_configured_commerce_li_in_current_date > 0 then 1
          when sum_configured_commerce_li_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_configured_commerce_li_to_cms,
      case
        when increased_cms_in_current_date = 1 then case
          when sum_downgraded_configured_commerce_ms_in_current_date > 0
          or sum_churned_configured_commerce_ms_in_current_date > 0 then 1
          when sum_configured_commerce_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_configured_commerce_ms_to_cms,
      case
        when increased_cms_in_current_date = 1 then case
          when sum_downgraded_content_management_paas_li_in_current_date > 0
          or sum_churned_content_management_paas_li_in_current_date > 0 then 1
          when sum_content_management_paas_li_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_content_management_paas_li_to_cms,
      case
        when increased_cms_in_current_date = 1 then case
          when sum_downgraded_content_management_paas_ms_in_current_date > 0
          or sum_churned_content_management_paas_ms_in_current_date > 0 then 1
          when sum_content_management_paas_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_content_management_paas_ms_to_cms,
      case
        when increased_cms_in_current_date = 1 then case
          when sum_downgraded_eol_everweb_in_current_date > 0
          or sum_churned_eol_everweb_in_current_date > 0 then 1
          when sum_eol_everweb_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_eol_everweb_to_cms,
      case
        when increased_cms_in_current_date = 1 then case
          when sum_downgraded_legacy_ektron_in_current_date > 0
          or sum_churned_legacy_ektron_in_current_date > 0 then 1
          when sum_legacy_ektron_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_legacy_ektron_to_cms,
      case
        when increased_cms_in_current_date = 1 then case
          when sum_downgraded_legacy_ektron_ms_in_current_date > 0
          or sum_churned_legacy_ektron_ms_in_current_date > 0 then 1
          when sum_legacy_ektron_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_legacy_ektron_ms_to_cms,
      case
        when increased_cms_in_current_date = 1 then case
          when sum_downgraded_search_navigation_in_current_date > 0
          or sum_churned_search_navigation_in_current_date > 0 then 1
          when sum_search_navigation_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_search_navigation_to_cms,
      case
        when increased_cms_in_current_date = 1 then case
          when sum_downgraded_search_navigation_ms_in_current_date > 0
          or sum_churned_search_navigation_ms_in_current_date > 0 then 1
          when sum_search_navigation_ms_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_search_navigation_ms_to_cms,
      -- upsell migration odp to migration from products
      case
        when increased_odp_in_current_date = 1 then case
          when sum_downgraded_visitor_intelligence_in_current_date > 0
          or sum_churned_visitor_intelligence_in_current_date > 0 then 1
          when sum_visitor_intelligence_in_previous_date_with_arr > 0 then 1
          else 0
        end
        else 0
      end as upsell_migration_visitor_intelligence_to_odp
    from initial_table_3
  ),
  initial_table_5 as (
    select *,
      (
        downgrade_migration_commerce_connect_li_to_monetize + downgrade_migration_commerce_connect_ms_to_monetize + downgrade_migration_configured_commerce_li_to_monetize + downgrade_migration_configured_commerce_ms_to_monetize + downgrade_migration_content_management_paas_li_to_monetize + downgrade_migration_content_management_paas_ms_to_monetize + downgrade_migration_eol_everweb_to_monetize + downgrade_migration_legacy_ektron_to_monetize + downgrade_migration_legacy_ektron_ms_to_monetize + downgrade_migration_search_navigation_to_monetize + downgrade_migration_search_navigation_ms_to_monetize + downgrade_migration_commerce_connect_li_to_cms + downgrade_migration_commerce_connect_ms_to_cms + downgrade_migration_configured_commerce_li_to_cms + downgrade_migration_configured_commerce_ms_to_cms + downgrade_migration_content_management_paas_li_to_cms + downgrade_migration_content_management_paas_ms_to_cms + downgrade_migration_eol_everweb_to_cms + downgrade_migration_legacy_ektron_to_cms + downgrade_migration_legacy_ektron_ms_to_cms + downgrade_migration_search_navigation_to_cms + downgrade_migration_search_navigation_ms_to_cms + downgrade_migration_visitor_intelligence_to_odp + downsell_migration_commerce_connect_li_to_monetize + downsell_migration_commerce_connect_ms_to_monetize + downsell_migration_configured_commerce_li_to_monetize + downsell_migration_configured_commerce_ms_to_monetize + downsell_migration_content_management_paas_li_to_monetize + downsell_migration_content_management_paas_ms_to_monetize + downsell_migration_eol_everweb_to_monetize + downsell_migration_legacy_ektron_to_monetize + downsell_migration_legacy_ektron_ms_to_monetize + downsell_migration_search_navigation_to_monetize + downsell_migration_search_navigation_ms_to_monetize + downsell_migration_commerce_connect_li_to_cms + downsell_migration_commerce_connect_ms_to_cms + downsell_migration_configured_commerce_li_to_cms + downsell_migration_configured_commerce_ms_to_cms + downsell_migration_content_management_paas_li_to_cms + downsell_migration_content_management_paas_ms_to_cms + downsell_migration_eol_everweb_to_cms + downsell_migration_legacy_ektron_to_cms + downsell_migration_legacy_ektron_ms_to_cms + downsell_migration_search_navigation_to_cms + downsell_migration_search_navigation_ms_to_cms + downsell_migration_visitor_intelligence_to_odp + cross_sell_migration_commerce_connect_li_to_monetize + cross_sell_migration_commerce_connect_ms_to_monetize + cross_sell_migration_configured_commerce_li_to_monetize + cross_sell_migration_configured_commerce_ms_to_monetize + cross_sell_migration_content_management_paas_li_to_monetize + cross_sell_migration_content_management_paas_ms_to_monetize + cross_sell_migration_eol_everweb_to_monetize + cross_sell_migration_legacy_ektron_to_monetize + cross_sell_migration_legacy_ektron_ms_to_monetize + cross_sell_migration_search_navigation_to_monetize + cross_sell_migration_search_navigation_ms_to_monetize + cross_sell_migration_commerce_connect_li_to_cms + cross_sell_migration_commerce_connect_ms_to_cms + cross_sell_migration_configured_commerce_li_to_cms + cross_sell_migration_configured_commerce_ms_to_cms + cross_sell_migration_content_management_paas_li_to_cms + cross_sell_migration_content_management_paas_ms_to_cms + cross_sell_migration_eol_everweb_to_cms + cross_sell_migration_legacy_ektron_to_cms + cross_sell_migration_legacy_ektron_ms_to_cms + cross_sell_migration_search_navigation_to_cms + cross_sell_migration_search_navigation_ms_to_cms + cross_sell_migration_visitor_intelligence_to_odp + upsell_migration_commerce_connect_li_to_monetize + upsell_migration_commerce_connect_ms_to_monetize + upsell_migration_configured_commerce_li_to_monetize + upsell_migration_configured_commerce_ms_to_monetize + upsell_migration_content_management_paas_li_to_monetize + upsell_migration_content_management_paas_ms_to_monetize + upsell_migration_eol_everweb_to_monetize + upsell_migration_legacy_ektron_to_monetize + upsell_migration_legacy_ektron_ms_to_monetize + upsell_migration_search_navigation_to_monetize + upsell_migration_search_navigation_ms_to_monetize + upsell_migration_commerce_connect_li_to_cms + upsell_migration_commerce_connect_ms_to_cms + upsell_migration_configured_commerce_li_to_cms + upsell_migration_configured_commerce_ms_to_cms + upsell_migration_content_management_paas_li_to_cms + upsell_migration_content_management_paas_ms_to_cms + upsell_migration_eol_everweb_to_cms + upsell_migration_legacy_ektron_to_cms + upsell_migration_legacy_ektron_ms_to_cms + upsell_migration_search_navigation_to_cms + upsell_migration_search_navigation_ms_to_cms + upsell_migration_visitor_intelligence_to_odp
      ) as active_flag_count,
      concat(
        case
          when downgrade_migration_commerce_connect_li_to_monetize = 1 then ',downgrade - migration -- commerce_connect_li to monetize'
          else null
        end,
        case
          when downgrade_migration_commerce_connect_ms_to_monetize = 1 then ',downgrade - migration -- commerce_connect_ms to monetize'
          else null
        end,
        case
          when downgrade_migration_configured_commerce_li_to_monetize = 1 then ',downgrade - migration -- configured_commerce_li to monetize'
          else null
        end,
        case
          when downgrade_migration_configured_commerce_ms_to_monetize = 1 then ',downgrade - migration -- configured_commerce_ms to monetize'
          else null
        end,
        case
          when downgrade_migration_content_management_paas_li_to_monetize = 1 then ',downgrade - migration -- content_management_paas_li to monetize'
          else null
        end,
        case
          when downgrade_migration_content_management_paas_ms_to_monetize = 1 then ',downgrade - migration -- content_management_paas_ms to monetize'
          else null
        end,
        case
          when downgrade_migration_eol_everweb_to_monetize = 1 then ',downgrade - migration -- eol_everweb to monetize'
          else null
        end,
        case
          when downgrade_migration_legacy_ektron_to_monetize = 1 then ',downgrade - migration -- legacy_ektron to monetize'
          else null
        end,
        case
          when downgrade_migration_legacy_ektron_ms_to_monetize = 1 then ',downgrade - migration -- legacy_ektron_ms to monetize'
          else null
        end,
        case
          when downgrade_migration_search_navigation_to_monetize = 1 then ',downgrade - migration -- search_navigation_standalone to monetize'
          else null
        end,
        case
          when downgrade_migration_search_navigation_ms_to_monetize = 1 then ',downgrade - migration -- search_navigation_standalone_ms to monetize'
          else null
        end,
        case
          when downgrade_migration_commerce_connect_li_to_cms = 1 then ',downgrade - migration -- commerce_connect_li to content_management_cms'
          else null
        end,
        case
          when downgrade_migration_commerce_connect_ms_to_cms = 1 then ',downgrade - migration -- commerce_connect_ms to content_management_cms'
          else null
        end,
        case
          when downgrade_migration_configured_commerce_li_to_cms = 1 then ',downgrade - migration -- configured_commerce_li to content_management_cms'
          else null
        end,
        case
          when downgrade_migration_configured_commerce_ms_to_cms = 1 then ',downgrade - migration -- configured_commerce_ms to content_management_cms'
          else null
        end,
        case
          when downgrade_migration_content_management_paas_li_to_cms = 1 then ',downgrade - migration -- content_management_paas_li to content_management_cms'
          else null
        end,
        case
          when downgrade_migration_content_management_paas_ms_to_cms = 1 then ',downgrade - migration -- content_management_paas_ms to content_management_cms'
          else null
        end,
        case
          when downgrade_migration_eol_everweb_to_cms = 1 then ',downgrade - migration -- eol_everweb to content_management_cms'
          else null
        end,
        case
          when downgrade_migration_legacy_ektron_to_cms = 1 then ',downgrade - migration -- legacy_ektron to content_management_cms'
          else null
        end,
        case
          when downgrade_migration_legacy_ektron_ms_to_cms = 1 then ',downgrade - migration -- legacy_ektron_ms to content_management_cms'
          else null
        end,
        case
          when downgrade_migration_search_navigation_to_cms = 1 then ',downgrade - migration -- search_navigation_standalone to content_management_cms'
          else null
        end,
        case
          when downgrade_migration_search_navigation_ms_to_cms = 1 then ',downgrade - migration -- search_navigation_standalone_ms to content_management_cms'
          else null
        end,
        case
          when downgrade_migration_visitor_intelligence_to_odp = 1 then ',downgrade - migration -- eol_legacy_visitor_intelligence to odp'
          else null
        end,
        case
          when downsell_migration_commerce_connect_li_to_monetize = 1 then ',downsell - migration -- commerce_connect_li to monetize'
          else null
        end,
        case
          when downsell_migration_commerce_connect_ms_to_monetize = 1 then ',downsell - migration -- commerce_connect_ms to monetize'
          else null
        end,
        case
          when downsell_migration_configured_commerce_li_to_monetize = 1 then ',downsell - migration -- configured_commerce_li to monetize'
          else null
        end,
        case
          when downsell_migration_configured_commerce_ms_to_monetize = 1 then ',downsell - migration -- configured_commerce_ms to monetize'
          else null
        end,
        case
          when downsell_migration_content_management_paas_li_to_monetize = 1 then ',downsell - migration -- content_management_paas_li to monetize'
          else null
        end,
        case
          when downsell_migration_content_management_paas_ms_to_monetize = 1 then ',downsell - migration -- content_management_paas_ms to monetize'
          else null
        end,
        case
          when downsell_migration_eol_everweb_to_monetize = 1 then ',downsell - migration -- eol_everweb to monetize'
          else null
        end,
        case
          when downsell_migration_legacy_ektron_to_monetize = 1 then ',downsell - migration -- legacy_ektron to monetize'
          else null
        end,
        case
          when downsell_migration_legacy_ektron_ms_to_monetize = 1 then ',downsell - migration -- legacy_ektron_ms to monetize'
          else null
        end,
        case
          when downsell_migration_search_navigation_to_monetize = 1 then ',downsell - migration -- search_navigation_standalone to monetize'
          else null
        end,
        case
          when downsell_migration_search_navigation_ms_to_monetize = 1 then ',downsell - migration -- search_navigation_standalone_ms to monetize'
          else null
        end,
        case
          when downsell_migration_commerce_connect_li_to_cms = 1 then ',downsell - migration -- commerce_connect_li to content_management_cms'
          else null
        end,
        case
          when downsell_migration_commerce_connect_ms_to_cms = 1 then ',downsell - migration -- commerce_connect_ms to content_management_cms'
          else null
        end,
        case
          when downsell_migration_configured_commerce_li_to_cms = 1 then ',downsell - migration -- configured_commerce_li to content_management_cms'
          else null
        end,
        case
          when downsell_migration_configured_commerce_ms_to_cms = 1 then ',downsell - migration -- configured_commerce_ms to content_management_cms'
          else null
        end,
        case
          when downsell_migration_content_management_paas_li_to_cms = 1 then ',downsell - migration -- content_management_paas_li to content_management_cms'
          else null
        end,
        case
          when downsell_migration_content_management_paas_ms_to_cms = 1 then ',downsell - migration -- content_management_paas_ms to content_management_cms'
          else null
        end,
        case
          when downsell_migration_eol_everweb_to_cms = 1 then ',downsell - migration -- eol_everweb to content_management_cms'
          else null
        end,
        case
          when downsell_migration_legacy_ektron_to_cms = 1 then ',downsell - migration -- legacy_ektron to content_management_cms'
          else null
        end,
        case
          when downsell_migration_legacy_ektron_ms_to_cms = 1 then ',downsell - migration -- legacy_ektron_ms to content_management_cms'
          else null
        end,
        case
          when downsell_migration_search_navigation_to_cms = 1 then ',downsell - migration -- search_navigation_standalone to content_management_cms'
          else null
        end,
        case
          when downsell_migration_search_navigation_ms_to_cms = 1 then ',downsell - migration -- search_navigation_standalone_ms to content_management_cms'
          else null
        end,
        case
          when downsell_migration_visitor_intelligence_to_odp = 1 then ',downsell - migration -- eol_legacy_visitor_intelligence to odp'
          else null
        end,
        case
          when cross_sell_migration_commerce_connect_li_to_monetize = 1 then ',cross_sell - migration -- commerce_connect_li to monetize'
          else null
        end,
        case
          when cross_sell_migration_commerce_connect_ms_to_monetize = 1 then ',cross_sell - migration -- commerce_connect_ms to monetize'
          else null
        end,
        case
          when cross_sell_migration_configured_commerce_li_to_monetize = 1 then ',cross_sell - migration -- configured_commerce_li to monetize'
          else null
        end,
        case
          when cross_sell_migration_configured_commerce_ms_to_monetize = 1 then ',cross_sell - migration -- configured_commerce_ms to monetize'
          else null
        end,
        case
          when cross_sell_migration_content_management_paas_li_to_monetize = 1 then ',cross_sell - migration -- content_management_paas_li to monetize'
          else null
        end,
        case
          when cross_sell_migration_content_management_paas_ms_to_monetize = 1 then ',cross_sell - migration -- content_management_paas_ms to monetize'
          else null
        end,
        case
          when cross_sell_migration_eol_everweb_to_monetize = 1 then ',cross_sell - migration -- eol_everweb to monetize'
          else null
        end,
        case
          when cross_sell_migration_legacy_ektron_to_monetize = 1 then ',cross_sell - migration -- legacy_ektron to monetize'
          else null
        end,
        case
          when cross_sell_migration_legacy_ektron_ms_to_monetize = 1 then ',cross_sell - migration -- legacy_ektron_ms to monetize'
          else null
        end,
        case
          when cross_sell_migration_search_navigation_to_monetize = 1 then ',cross_sell - migration -- search_navigation_standalone to monetize'
          else null
        end,
        case
          when cross_sell_migration_search_navigation_ms_to_monetize = 1 then ',cross_sell - migration -- search_navigation_standalone_ms to monetize'
          else null
        end,
        case
          when cross_sell_migration_commerce_connect_li_to_cms = 1 then ',cross_sell - migration -- commerce_connect_li to content_management_cms'
          else null
        end,
        case
          when cross_sell_migration_commerce_connect_ms_to_cms = 1 then ',cross_sell - migration -- commerce_connect_ms to content_management_cms'
          else null
        end,
        case
          when cross_sell_migration_configured_commerce_li_to_cms = 1 then ',cross_sell - migration -- configured_commerce_li to content_management_cms'
          else null
        end,
        case
          when cross_sell_migration_configured_commerce_ms_to_cms = 1 then ',cross_sell - migration -- configured_commerce_ms to content_management_cms'
          else null
        end,
        case
          when cross_sell_migration_content_management_paas_li_to_cms = 1 then ',cross_sell - migration -- content_management_paas_li to content_management_cms'
          else null
        end,
        case
          when cross_sell_migration_content_management_paas_ms_to_cms = 1 then ',cross_sell - migration -- content_management_paas_ms to content_management_cms'
          else null
        end,
        case
          when cross_sell_migration_eol_everweb_to_cms = 1 then ',cross_sell - migration -- eol_everweb to content_management_cms'
          else null
        end,
        case
          when cross_sell_migration_legacy_ektron_to_cms = 1 then ',cross_sell - migration -- legacy_ektron to content_management_cms'
          else null
        end,
        case
          when cross_sell_migration_legacy_ektron_ms_to_cms = 1 then ',cross_sell - migration -- legacy_ektron_ms to content_management_cms'
          else null
        end,
        case
          when cross_sell_migration_search_navigation_to_cms = 1 then ',cross_sell - migration -- search_navigation_standalone to content_management_cms'
          else null
        end,
        case
          when cross_sell_migration_search_navigation_ms_to_cms = 1 then ',cross_sell - migration -- search_navigation_standalone_ms to content_management_cms'
          else null
        end,
        case
          when cross_sell_migration_visitor_intelligence_to_odp = 1 then ',cross_sell - migration -- eol_legacy_visitor_intelligence to odp'
          else null
        end,
        case
          when upsell_migration_commerce_connect_li_to_monetize = 1 then ',upsell - migration -- commerce_connect_li to monetize'
          else null
        end,
        case
          when upsell_migration_commerce_connect_ms_to_monetize = 1 then ',upsell - migration -- commerce_connect_ms to monetize'
          else null
        end,
        case
          when upsell_migration_configured_commerce_li_to_monetize = 1 then ',upsell - migration -- configured_commerce_li to monetize'
          else null
        end,
        case
          when upsell_migration_configured_commerce_ms_to_monetize = 1 then ',upsell - migration -- configured_commerce_ms to monetize'
          else null
        end,
        case
          when upsell_migration_content_management_paas_li_to_monetize = 1 then ',upsell - migration -- content_management_paas_li to monetize'
          else null
        end,
        case
          when upsell_migration_content_management_paas_ms_to_monetize = 1 then ',upsell - migration -- content_management_paas_ms to monetize'
          else null
        end,
        case
          when upsell_migration_eol_everweb_to_monetize = 1 then ',upsell - migration -- eol_everweb to monetize'
          else null
        end,
        case
          when upsell_migration_legacy_ektron_to_monetize = 1 then ',upsell - migration -- legacy_ektron to monetize'
          else null
        end,
        case
          when upsell_migration_legacy_ektron_ms_to_monetize = 1 then ',upsell - migration -- legacy_ektron_ms to monetize'
          else null
        end,
        case
          when upsell_migration_search_navigation_to_monetize = 1 then ',upsell - migration -- search_navigation_standalone to monetize'
          else null
        end,
        case
          when upsell_migration_search_navigation_ms_to_monetize = 1 then ',upsell - migration -- search_navigation_standalone_ms to monetize'
          else null
        end,
        case
          when upsell_migration_commerce_connect_li_to_cms = 1 then ',upsell - migration -- commerce_connect_li to content_management_cms'
          else null
        end,
        case
          when upsell_migration_commerce_connect_ms_to_cms = 1 then ',upsell - migration -- commerce_connect_ms to content_management_cms'
          else null
        end,
        case
          when upsell_migration_configured_commerce_li_to_cms = 1 then ',upsell - migration -- configured_commerce_li to content_management_cms'
          else null
        end,
        case
          when upsell_migration_configured_commerce_ms_to_cms = 1 then ',upsell - migration -- configured_commerce_ms to content_management_cms'
          else null
        end,
        case
          when upsell_migration_content_management_paas_li_to_cms = 1 then ',upsell - migration -- content_management_paas_li to content_management_cms'
          else null
        end,
        case
          when upsell_migration_content_management_paas_ms_to_cms = 1 then ',upsell - migration -- content_management_paas_ms to content_management_cms'
          else null
        end,
        case
          when upsell_migration_eol_everweb_to_cms = 1 then ',upsell - migration -- eol_everweb to content_management_cms'
          else null
        end,
        case
          when upsell_migration_legacy_ektron_to_cms = 1 then ',upsell - migration -- legacy_ektron to content_management_cms'
          else null
        end,
        case
          when upsell_migration_legacy_ektron_ms_to_cms = 1 then ',upsell - migration -- legacy_ektron_ms to content_management_cms'
          else null
        end,
        case
          when upsell_migration_search_navigation_to_cms = 1 then ',upsell - migration -- search_navigation_standalone to content_management_cms'
          else null
        end,
        case
          when upsell_migration_search_navigation_ms_to_cms = 1 then ',upsell - migration -- search_navigation_standalone_ms to content_management_cms'
          else null
        end,
        case
          when upsell_migration_visitor_intelligence_to_odp = 1 then ',upsell - migration -- eol_legacy_visitor_intelligence to odp'
          else null
        end
      ) as flag_descriptions
    from initial_table_4
  ),
  initial_table_5_internal as (
    select evaluation_period,
      current_period,
      prior_period,
      mcid,
      currency_code,
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
                  REGEXP_REPLACE(current_pathways, '\s*-\s*', '_'),
                  ' & ',
                  '_'
                ),
                '\s*M&S\s*',
                '_MS'
              ),
              ' \(',
              '_'
            ),
            '\)',
            ''
          ),
          ' ',
          '_'
        ),
        ' ',
        '_'
      ) AS current_pathways,
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(prior_pathways, '\s*-\s*', '_'),
                  ' & ',
                  '_'
                ),
                '\s*M&S\s*',
                '_MS'
              ),
              ' \(',
              '_'
            ),
            '\)',
            ''
          ),
          ' ',
          '_'
        ),
        ' ',
        '_'
      ) AS prior_pathways,
      prior_period_product_arr_lcu,
      current_period_product_arr_lcu,
      prior_period_product_arr_usd_ccfx,
      current_period_product_arr_usd_ccfx,
      product_arr_change_ccfx,
      product_arr_change_lcu,
      active_flag_count,
      flag_descriptions
    from initial_table_5
  ),
  initial_table_6 as (
    with extracted as (
      SELECT evaluation_period,
        mcid,
        currency_code,
        string_agg(DISTINCT TRIM(SPLIT_PART(segment, 'to', 2)), ',') FILTER (
          WHERE segment ILIKE '%to%'
        ) AS extracted_targets
      FROM initial_table_5_internal t
        LEFT JOIN LATERAL (
          SELECT unnest(string_to_array(flag_descriptions, ',')) AS segment
        ) AS sub ON TRUE
      GROUP BY evaluation_period,
        mcid,
        currency_code
    ) --      select * from extracted ;
,
    base as (
      SELECT t.*,
        e.extracted_targets
      FROM initial_table_5_internal t
        LEFT JOIN extracted e ON t.evaluation_period = e.evaluation_period
        AND t.mcid = e.mcid
        AND t.currency_code = e.currency_code
    ) --      select * from base ;
,
    qualified AS (
      SELECT *,
        CASE
          WHEN current_pathways IN ('Content_Management_CMS', 'Monetize', 'ODP')
          AND EXISTS (
            SELECT 1
            FROM unnest(string_to_array(extracted_targets, ',')) AS target
            WHERE LOWER(current_pathways) LIKE '%' || LOWER(TRIM(target)) || '%'
          ) THEN current_period_product_arr_usd_ccfx
          ELSE NULL
        END AS qualifying_arr
      FROM base
    ) --      select * from qualified;
,
    max_value_mig_to as (
      SELECT *,
        MAX(qualifying_arr) OVER (
          PARTITION BY evaluation_period,
          mcid,
          currency_code
          ORDER BY current_period_product_arr_usd_ccfx DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS max_value_migration_to
      FROM qualified
    )
    select evaluation_period,
      current_period,
      prior_period,
      mcid,
      currency_code,
      current_product_group,
      prior_product_group,
      current_product_solution,
      prior_product_solution,
      current_pathways,
      prior_pathways,
      prior_period_product_arr_lcu,
      current_period_product_arr_lcu,
      prior_period_product_arr_usd_ccfx,
      current_period_product_arr_usd_ccfx,
      product_arr_change_ccfx,
      product_arr_change_lcu,
      active_flag_count,
      flag_descriptions,
      --               selected_mig_to
      upper(
        max(selected_mig_to) over(
          partition by mcid,
          evaluation_period,
          currency_code
        )
      ) as selected_mig_to
    from (
        select *,
          case
            when max_value_migration_to = current_period_product_arr_usd_ccfx
            and current_pathways IN ('Content_Management_CMS', 'Monetize', 'ODP') then current_pathways
            else null
          end as selected_mig_to
        from max_value_mig_to
      ) as a
  ) -- ,cross_sell - migration -- content_management_paas_ms to content_management_cms,
  -- cross_sell - migration -- search_navigation_standalone to content_management_cms
  -- ,downsell - migration -- content_management_paas_ms to monetize,
  -- downsell - migration -- content_management_paas_ms to content_management_cms
  -- select * from initial_table_6 ;
,
  initial_table_7_int as (
    with extracted as (
      SELECT evaluation_period,
        mcid,
        currency_code,
        string_agg(
          DISTINCT trim(
            split_part(split_part(segment, '--', 2), 'to', 1)
          ),
          ','
        ) FILTER (
          WHERE segment ILIKE '%--%'
            AND segment ILIKE '%to%'
        ) AS extracted_targets
      FROM initial_table_6
        LEFT JOIN LATERAL (
          SELECT unnest(string_to_array(flag_descriptions, ',')) AS segment
        ) AS sub ON TRUE
      GROUP BY 1,
        2,
        3
    ) --      select * from extracted;
,
    base as (
      select t.*,
        e.extracted_targets
      from initial_table_6 t
        left join extracted e on t.evaluation_period = e.evaluation_period
        and t.mcid = e.mcid
        and t.currency_code = e.currency_code
    ),
    qualified as (
      select *,
        case
          when lower(prior_pathways) <> lower(selected_mig_to)
          and (
            (
              selected_mig_to = 'ODP'
              AND prior_pathways = 'EOL_Legacy_Visitor_Intelligence'
            )
            OR (
              selected_mig_to <> 'ODP'
              AND prior_pathways NOT IN (
                'USUAL',
                'Content_Management_CMS',
                'Monetize',
                'ODP',
                'EOL_Legacy_Visitor_Intelligence'
              )
            )
          )
          and exists(
            select 1
            from unnest(string_to_array(extracted_targets, ',')) as target
            where lower(prior_pathways) like '%' || lower(trim(target)) || '%'
          ) then prior_period_product_arr_usd_ccfx
          else null
        end as qualifying_arr
      from base
    ) --         select * from qualified ;
,
    max_value_mig_from as (
      select *,
        max(qualifying_arr) over(
          partition by evaluation_period,
          mcid,
          currency_code
          order by prior_period_product_arr_usd_ccfx desc rows between unbounded preceding and unbounded following
        ) as max_value_migration_from
      from qualified
    )
    select evaluation_period,
      current_period,
      prior_period,
      mcid,
      currency_code,
      current_product_group,
      prior_product_group,
      current_product_solution,
      prior_product_solution,
      current_pathways,
      prior_pathways,
      prior_period_product_arr_lcu,
      current_period_product_arr_lcu,
      prior_period_product_arr_usd_ccfx,
      current_period_product_arr_usd_ccfx,
      product_arr_change_ccfx,
      product_arr_change_lcu,
      active_flag_count,
      flag_descriptions,
      selected_mig_to,
      --   selected_mig_from
      upper(
        max(selected_mig_from) over(
          partition by mcid,
          evaluation_period,
          currency_code
        )
      ) as selected_mig_from
    from (
        select *,
          case
            when max_value_migration_from = prior_period_product_arr_usd_ccfx then prior_pathways
            else null
          end as selected_mig_from
        from max_value_mig_from
      ) as a
  ),
  initial_table_7 as (
    select evaluation_period,
      current_period,
      prior_period,
      mcid,
      currency_code,
      current_product_group,
      prior_product_group,
      current_product_solution,
      prior_product_solution,
      current_pathways,
      prior_pathways,
      prior_period_product_arr_lcu,
      current_period_product_arr_lcu,
      prior_period_product_arr_usd_ccfx,
      current_period_product_arr_usd_ccfx,
      product_arr_change_ccfx,
      product_arr_change_lcu,
      active_flag_count,
      flag_descriptions,
      case
        when flag_descriptions <> ''
        and active_flag_count >= 1 then selected_mig_to
        else null
      end as selected_mig_to,
      case
        when flag_descriptions <> ''
        and active_flag_count >= 1 then selected_mig_from
        else null
      end as selected_mig_from
    from initial_table_7_int
  ) -- select * from initial_table_7;
  --   where mcid = '1c13e6c9-8ff8-c97f-5994-477b5c850dcf' and evaluation_period = '2021M12';
,
  final_table as (
    SELECT t.*,
      matched_flags.matched_flag_description,
      case
        when active_flag_count > 1 then case
          when flag_descriptions <> '' then matched_flags.matched_flag_description
          else null
        end
        else (string_to_array(flag_descriptions, ',')) [2]
      end as "Movement Classification" --     matched_flags.matched_flag_description
    FROM initial_table_7 t
      LEFT JOIN LATERAL (
        SELECT string_agg(flag, ', ') AS matched_flag_description
        FROM (
            SELECT trim(f) AS flag
            FROM unnest(string_to_array(t.flag_descriptions, ',')) AS f
            WHERE f ILIKE '%' || t.selected_mig_from || '%'
              AND f ILIKE '%' || t.selected_mig_to || '%'
          ) AS filtered
      ) AS matched_flags ON TRUE
  )
  select *
  from final_table --       where active_flag_count  >=  1 and matched_flag_description is null ;
);
--
--
--     , marker as (
-- select * from final_table
-- where flag_descriptions <> '' and ("Migration Classification" is  null or "Migration Classification" = '')
-- )
--
-- select * from marker ;
-- -- );
--
-- select
--     *,
--     row_number() over (partition by  mcid , evaluation_period, currency_code , active_flag_count) as rnk
-- from sandbox.churn_migration_classifiers_max_value_v2
-- where
--         mcid = '1c13e6c9-8ff8-c97f-5994-477b5c850dcf' and evaluation_period = '2021M12'
-- --     flag_descriptions <> '' and "Migration Classification" is not  null
-- and
--     active_flag_count > 1
-- limit 10
--
-- select *
-- from sandbox.churn_migration_classifiers_max_value_v2
-- where mcid = '1c13e6c9-8ff8-c97f-5994-477b5c850dcf' and evaluation_period = '2021M12'
--
--
-- select * from ufdm_archive.churn_migration_classifiers_lcoked_18032025_0244
-- where mcid = '1c13e6c9-8ff8-c97f-5994-477b5c850dcf' and evaluation_period = '2021M12'