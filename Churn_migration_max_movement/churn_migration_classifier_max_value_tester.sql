with initial_table as (
  select *
  from ryzlan.sst_product_pathways_bridge2
  where mcid is not null and mcid <> '-'
--   and  mcid = 'be8ab5f4-c33f-e511-9afb-0050568d2da8' and evaluation_period = '2021M09'
--     mcid = '5590ae29-b450-e211-9907-0050568d002c'
--   and evaluation_period = '2020M09'
--     mcid='1c51a6e6-2446-df11-a462-0018717a8c82' and evaluation_period='2025M01'
--     mcid = '5590ae29-b450-e211-9907-0050568d002c'
--   and evaluation_period = '2020M09'
),
initial_table_2 as (
  select *,
    case
      when current_pathways = 'Licenses'
      and product_arr_change_ccfx < 0
      and prior_period_product_arr_usd_ccfx > 0
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as downgraded_license_in_current_date,
    case
      when current_pathways IN ('Everweb')
      and product_arr_change_ccfx < 0
      and prior_period_product_arr_usd_ccfx > 0
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as downgraded_everweb_in_current_date,
    case
      when current_pathways IN ('Ektron')
      and product_arr_change_ccfx < 0
      and prior_period_product_arr_usd_ccfx > 0
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as downgraded_ektron_in_current_date,
    case
      when current_pathways = 'Find'
      and product_arr_change_ccfx < 0
      and prior_period_product_arr_usd_ccfx > 0
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as downgraded_find_in_current_date,
    case
      when current_pathways IN (
        'Visitor Intelligence',
        'Search & Navigation - Standalone'
      )
      and product_arr_change_ccfx < 0
      and prior_period_product_arr_usd_ccfx > 0
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as downgraded_vis_int_in_current_date,
    case
      when prior_pathways IN ('Licenses')
      and product_arr_change_ccfx < 0
      and prior_period_product_arr_usd_ccfx > 0
      and current_period_product_arr_usd_ccfx = 0 then 1
      else 0
    end as churned_licenses_in_current_date,
    case
      when prior_pathways IN ('Everweb')
      and product_arr_change_ccfx < 0
      and prior_period_product_arr_usd_ccfx > 0
      and current_period_product_arr_usd_ccfx = 0 then 1
      else 0
    end as churned_everweb_in_current_date,
    case
      when prior_pathways IN ('Ektron')
      and product_arr_change_ccfx < 0
      and prior_period_product_arr_usd_ccfx > 0
      and current_period_product_arr_usd_ccfx = 0 then 1
      else 0
    end as churned_ektron_in_current_date,
    case
      when prior_pathways IN ('Find')
      and product_arr_change_ccfx < 0
      and prior_period_product_arr_usd_ccfx > 0
      and current_period_product_arr_usd_ccfx = 0 then 1
      else 0
    end as churned_find_in_current_date,
    case
      when prior_pathways IN (
        'Visitor Intelligence',
        'Search & Navigation - Standalone'
      )
      and product_arr_change_ccfx < 0
      and prior_period_product_arr_usd_ccfx > 0
      and current_period_product_arr_usd_ccfx = 0 then 1
      else 0
    end as churned_vis_int_in_current_date,
    case
      when current_pathways IN ('Orchestrate')
      and product_arr_change_ccfx > 0
      and prior_period_product_arr_usd_ccfx = 0
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as added_orchestrate_in_current_date,
    case
      when current_pathways IN ('Monetize')
      and product_arr_change_ccfx > 0
      and prior_period_product_arr_usd_ccfx = 0
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as added_monetize_in_current_date,
    case
      when current_pathways IN ('CMP')
      and product_arr_change_ccfx > 0
      and prior_period_product_arr_usd_ccfx = 0
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as added_cmp_in_current_date,
    case
      when current_pathways IN ('CMS')
      and product_arr_change_ccfx > 0
      and prior_period_product_arr_usd_ccfx = 0
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as added_cms_in_current_date,
    case
      when current_pathways IN ('ODP')
      and product_arr_change_ccfx > 0
      and prior_period_product_arr_usd_ccfx = 0
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as added_odp_in_current_date,
    case
      when current_pathways IN ('Orchestrate')
      and product_arr_change_ccfx > 0
      and prior_period_product_arr_usd_ccfx > 0
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as increased_orchestrate_in_current_date,
    case
      when current_pathways IN ('CMS')
      and product_arr_change_ccfx > 0
      and prior_period_product_arr_usd_ccfx > 0
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as increased_cms_in_current_date,
    case
      when current_pathways IN ('Monetize')
      and product_arr_change_ccfx > 0
      and prior_period_product_arr_usd_ccfx > 0
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as increased_monetize_in_current_date,
    case
      when current_pathways IN ('CMP')
      and product_arr_change_ccfx > 0
      and prior_period_product_arr_usd_ccfx > 0
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as increased_cmp_in_current_date,
    case
      when current_pathways IN ('ODP')
      and product_arr_change_ccfx > 0
      and prior_period_product_arr_usd_ccfx > 0
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as increased_odp_in_current_date,
    case
      when current_pathways IN ('Orchestrate')
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as orchestrate_in_current_date_with_arr,
    case
      when current_pathways IN ('CMS')
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as cms_in_current_date_with_arr,
    case
      when current_pathways IN ('Monetize')
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as monetize_in_current_date_with_arr,
    case
      when current_pathways IN ('CMP')
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as cmp_in_current_date_with_arr,
    case
      when current_pathways IN ('ODP')
      and current_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as odp_in_current_date_with_arr,
    case
      when prior_pathways IN ('Licenses')
      and prior_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as licenses_in_previous_date_with_arr,
    case
      when prior_pathways IN ('Everweb')
      and prior_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as everweb_in_previous_date_with_arr,
    case
      when prior_pathways IN ('Ektron')
      and prior_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as ektron_in_previous_date_with_arr,
    case
      when prior_pathways IN ('Find')
      and prior_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as find_in_previous_date_with_arr,
    case
      when prior_pathways IN (
        'Visitor Intelligence',
        'Search & Navigation - Standalone'
      )
      and prior_period_product_arr_usd_ccfx > 0 then 1
      else 0
    end as vis_int_in_previous_date_with_arr
  from initial_table
)
, initial_table_3 as (
select
*,
sum(cmp_in_current_date_with_arr) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_cmp_in_current_date_with_arr,
sum(orchestrate_in_current_date_with_arr) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_orchestrate_in_current_date_with_arr,
sum(monetize_in_current_date_with_arr) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_monetize_in_current_date_with_arr,
sum(cms_in_current_date_with_arr) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_cms_in_current_date_with_arr,
sum(odp_in_current_date_with_arr) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_odp_in_current_date_with_arr,
sum(downgraded_license_in_current_date) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_downgraded_license_in_current_date,
sum(churned_licenses_in_current_date) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_churned_licenses_in_current_date,
sum(downgraded_everweb_in_current_date) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_downgraded_everweb_in_current_date,
sum(churned_everweb_in_current_date) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_churned_everweb_in_current_date,
sum(downgraded_find_in_current_date) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_downgraded_find_in_current_date,
sum(churned_find_in_current_date) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_churned_find_in_current_date,
sum(downgraded_ektron_in_current_date) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_downgraded_ektron_in_current_date,
sum(churned_ektron_in_current_date) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_churned_ektron_in_current_date,
sum(downgraded_vis_int_in_current_date) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_downgraded_vis_int_in_current_date,
sum(churned_vis_int_in_current_date) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_churned_vis_int_in_current_date,
sum(licenses_in_previous_date_with_arr) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_licenses_in_previous_date_with_arr,
sum(everweb_in_previous_date_with_arr) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_everweb_in_previous_date_with_arr,
sum(find_in_previous_date_with_arr) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_find_in_previous_date_with_arr,
sum(ektron_in_previous_date_with_arr) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_ektron_in_previous_date_with_arr,
sum(vis_int_in_previous_date_with_arr) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_vis_int_in_previous_date_with_arr,
sum(added_cmp_in_current_date) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_added_cmp_in_current_date,
sum(increased_cmp_in_current_date) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_increased_cmp_in_current_date,
sum(added_orchestrate_in_current_date) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_added_orchestrate_in_current_date,
sum(increased_orchestrate_in_current_date) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_increased_orchestrate_in_current_date,
sum(added_monetize_in_current_date) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_added_monetize_in_current_date,
sum(
  increased_monetize_in_current_date
) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_increased_monetize_in_current_date,
sum(added_cms_in_current_date) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_added_cms_in_current_date,
sum(increased_cms_in_current_date) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_increased_cms_in_current_date,
sum(added_odp_in_current_date) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_added_odp_in_current_date,
sum(increased_odp_in_current_date) over(
  partition by mcid,
  evaluation_period,
  currency_code
) as sum_increased_odp_in_current_date
from initial_table_2
)
, initial_table_4 as (
select
*,
CASE
  WHEN downgraded_license_in_current_date = 1 then
  case when sum_cmp_in_current_date_with_arr > 0 then 1
  when sum_added_cmp_in_current_date >0 or sum_increased_cmp_in_current_date > 0
  then 1 else 0 end
  else 0 end as downgrade_migration_licenses_to_cmp,
CASE
  WHEN downgraded_license_in_current_date = 1 then
  case when sum_orchestrate_in_current_date_with_arr > 0 then 1
  when sum_added_orchestrate_in_current_date > 0  or sum_increased_orchestrate_in_current_date > 0
  then 1 else 0 end
  else 0 end as downgrade_migration_licenses_to_orchestrate,
CASE
  WHEN downgraded_license_in_current_date = 1 then
  Case when sum_monetize_in_current_date_with_arr > 0 then 1
  when sum_added_monetize_in_current_date > 0 or sum_increased_monetize_in_current_date > 0
  then 1 else 0 end
  else 0 end as downgrade_migration_licenses_to_monetize ,

CASE
  WHEN downgraded_license_in_current_date = 1 then
  case when  sum_cms_in_current_date_with_arr> 0 then 1
  when sum_added_cms_in_current_date > 0 or sum_increased_cms_in_current_date > 0
  then 1 else 0 end
  else 0 end as downgrade_migration_licenses_to_cms,
CASE
  WHEN downgraded_everweb_in_current_date = 1 then
  case when sum_cmp_in_current_date_with_arr >0  then 1
  when sum_added_cmp_in_current_date > 0 or sum_increased_cmp_in_current_date > 0
  then 1 else 0 end
  else 0 end as downgrade_migration_everweb_to_cmp,
CASE
  WHEN downgraded_everweb_in_current_date = 1 then
  case when sum_orchestrate_in_current_date_with_arr > 0 then 1
  when sum_added_orchestrate_in_current_date > 0 or sum_increased_orchestrate_in_current_date > 0
  then 1 else 0 end
  else 0 end as downgrade_migration_everweb_to_orchestrate,
CASE
  WHEN downgraded_everweb_in_current_date = 1 then
  case when sum_monetize_in_current_date_with_arr > 0 then 1
  when sum_added_monetize_in_current_date > 0 and sum_increased_monetize_in_current_date > 0
  then 1 else 0 end
  else 0 end as downgrade_migration_everweb_to_monetize,
CASE
  WHEN downgraded_everweb_in_current_date = 1 then
  case when sum_cms_in_current_date_with_arr > 0 then 1
  when sum_added_cms_in_current_date > 0 or sum_increased_cms_in_current_date > 0
  then 1 else 0 end
  else 0 end as downgrade_migration_everweb_to_cms,
CASE
  WHEN downgraded_find_in_current_date = 1 then
  case when sum_cmp_in_current_date_with_arr > 0 then 1
  when sum_added_cmp_in_current_date > 0 or sum_increased_cmp_in_current_date > 0
  then 1 else 0 end
  else 0 end as downgrade_migration_personalised_find_to_cmp,
CASE
  WHEN downgraded_find_in_current_date = 1 then
  case when sum_orchestrate_in_current_date_with_arr > 0 then 1
  when sum_added_orchestrate_in_current_date > 0 or sum_increased_orchestrate_in_current_date > 0
  then 1 else 0 end
  else 0 end as downgrade_migration_personalised_find_to_orchestrate,
CASE
  WHEN downgraded_find_in_current_date = 1 then
  case when sum_monetize_in_current_date_with_arr > 0 then 1
  when sum_added_monetize_in_current_date > 0 or sum_increased_monetize_in_current_date > 0
  then 1 else 0 end
  else 0 end as downgrade_migration_personalised_find_to_monetize,
CASE
  WHEN downgraded_find_in_current_date = 1 then
  case when sum_cms_in_current_date_with_arr > 0 then 1
  when sum_added_cms_in_current_date > 0 or sum_increased_cms_in_current_date > 0
  then 1 else 0 end
  else 0 end as downgrade_migration_personalised_find_to_cms,
CASE
  WHEN downgraded_ektron_in_current_date = 1 then
  case when sum_cms_in_current_date_with_arr > 0 then 1
  when sum_added_cms_in_current_date > 0 or sum_increased_cms_in_current_date > 0
  then 1 else 0 end
  else 0 end as downgrade_migration_ektron_to_cms,
CASE
  WHEN downgraded_vis_int_in_current_date = 1 then
  case when sum_odp_in_current_date_with_arr > 0 then 1
  when sum_added_odp_in_current_date > 0 or sum_increased_odp_in_current_date > 0
  then 1 else 0 end
  else 0 end as downgrade_migration_visitorint_to_odp,
CASE
  WHEN churned_licenses_in_current_date = 1 then
  case when sum_cmp_in_current_date_with_arr > 0 then 1
  when sum_added_cmp_in_current_date > 0 or sum_increased_cmp_in_current_date > 0
  then 1 else 0 end
  else 0 end as downsell_migration_licenses_to_cmp,

CASE
  WHEN churned_licenses_in_current_date = 1 then
  case when sum_orchestrate_in_current_date_with_arr > 0 then 1
  when sum_added_orchestrate_in_current_date > 0 or sum_increased_orchestrate_in_current_date > 0
  then 1 else 0 end
  else 0 end as downsell_migration_licenses_to_orchestrate,
CASE
  WHEN churned_licenses_in_current_date = 1 then
  case when sum_monetize_in_current_date_with_arr > 0 then 1
  when sum_added_monetize_in_current_date > 0 or sum_increased_monetize_in_current_date > 0
  then 1 else 0 end
  else 0 end as downsell_migration_licenses_to_monetize,
CASE
  WHEN churned_licenses_in_current_date = 1 then
  case when sum_cms_in_current_date_with_arr > 0 then 1
  when sum_added_cms_in_current_date > 0 or sum_increased_cms_in_current_date > 0
  then 1 else 0 end
  else 0 end as downsell_migration_licenses_to_cms,
CASE
  WHEN churned_everweb_in_current_date = 1 then
  case when sum_cmp_in_current_date_with_arr > 0 then 1
  when sum_added_cmp_in_current_date > 0 or sum_increased_cmp_in_current_date > 0
  then 1 else 0 end
  else 0 end as downsell_migration_everweb_to_cmp,
CASE
  WHEN churned_everweb_in_current_date = 1 then
  case when sum_orchestrate_in_current_date_with_arr > 0 then 1
  when sum_added_orchestrate_in_current_date > 0 or sum_increased_orchestrate_in_current_date > 0
  then 1 else 0 end
  else 0 end as downsell_migration_everweb_to_orchestrate,
CASE
  WHEN churned_everweb_in_current_date = 1 then
  case when sum_monetize_in_current_date_with_arr > 0 then 1
  when sum_added_monetize_in_current_date > 0 or sum_increased_monetize_in_current_date > 0
  then 1 else 0 end
  else 0 end as downsell_migration_everweb_to_monetize,
CASE
  WHEN churned_everweb_in_current_date = 1 then
  case when sum_cms_in_current_date_with_arr > 0 then 1
  when sum_added_cms_in_current_date > 0 or sum_increased_cms_in_current_date > 0
  then 1 else 0 end
  else 0 end as downsell_migration_everweb_to_cms,
CASE
  WHEN churned_find_in_current_date = 1 then
  case when sum_cmp_in_current_date_with_arr > 0 then 1
  when sum_added_cmp_in_current_date > 0 or sum_increased_cmp_in_current_date > 0
  then 1 else 0 end
  else 0 end as downsell_migration_personalised_find_to_cmp,
CASE
  WHEN churned_find_in_current_date = 1 then
  case when sum_orchestrate_in_current_date_with_arr > 0 then 1
  when sum_added_orchestrate_in_current_date > 0 or sum_increased_orchestrate_in_current_date > 0
  then 1 else 0 end
  else 0 end as downsell_migration_personalised_find_to_orchestrate,
CASE
  WHEN churned_find_in_current_date = 1 then
  case when sum_monetize_in_current_date_with_arr > 0 then 1
  when sum_added_monetize_in_current_date > 0 or sum_increased_monetize_in_current_date > 0
  then 1 else 0 end
  else 0 end as downsell_migration_personalised_find_to_monetize,
CASE
  WHEN churned_find_in_current_date = 1 then
  case when sum_cms_in_current_date_with_arr > 0 then 1
  when sum_added_cms_in_current_date > 0 or sum_increased_cms_in_current_date > 0
  then 1 else 0 end
  else 0 end as downsell_migration_personalised_find_to_cms,
CASE
  WHEN churned_ektron_in_current_date = 1 then
  case when sum_cms_in_current_date_with_arr > 0 then 1
  when sum_added_cms_in_current_date > 0 or sum_increased_cms_in_current_date > 0
  then 1 else 0 end
  else 0 end as downsell_migration_ektron_to_cms,
CASE
  WHEN churned_vis_int_in_current_date = 1 then
  case when sum_odp_in_current_date_with_arr > 0 then 1
  when sum_added_odp_in_current_date > 0 or sum_increased_odp_in_current_date > 0
  then 1 else 0 end
  else 0 end as downsell_migration_visitorint_to_odp,
-- CROSS SELL
CASE
  WHEN added_cmp_in_current_date = 1 then
  case when sum_downgraded_license_in_current_date > 0 or sum_churned_licenses_in_current_date > 0 then 1
  when sum_licenses_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as cross_sell_migration_licenses_to_cmp,
CASE
  WHEN added_orchestrate_in_current_date = 1 then
  case when sum_downgraded_license_in_current_date > 0 or sum_churned_licenses_in_current_date > 0 then 1
  when sum_licenses_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as cross_sell_migration_licenses_to_orchestrate,
CASE
  WHEN added_monetize_in_current_date = 1 then
  case when sum_downgraded_license_in_current_date >0 or sum_churned_licenses_in_current_date > 0 then 1
  when sum_licenses_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as cross_sell_migration_licenses_to_monetize,
CASE
  WHEN added_cms_in_current_date = 1 then
  case when sum_downgraded_license_in_current_date > 0 or sum_churned_licenses_in_current_date >0 then 1
  when sum_licenses_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as cross_sell_migration_licenses_to_cms ,
CASE
  WHEN added_cmp_in_current_date = 1 then
  case when sum_downgraded_everweb_in_current_date > 0 or sum_churned_everweb_in_current_date > 0 then 1
  when sum_everweb_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as cross_sell_migration_everweb_to_cmp,
CASE
  WHEN added_orchestrate_in_current_date = 1 then
  case when sum_downgraded_everweb_in_current_date > 0 or sum_churned_everweb_in_current_date > 0 then 1
  when sum_everweb_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as cross_sell_migration_everweb_to_orchestrate,
CASE
  WHEN added_monetize_in_current_date = 1 then
  case when sum_downgraded_everweb_in_current_date > 0 or sum_churned_everweb_in_current_date > 0 then 1
  when sum_everweb_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as cross_sell_migration_everweb_to_monetize,
CASE
  WHEN added_cms_in_current_date = 1 then
  case when sum_downgraded_everweb_in_current_date > 0 or sum_churned_everweb_in_current_date > 0 then 1
  when sum_everweb_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as cross_sell_migration_everweb_to_cms,
CASE
  WHEN added_cmp_in_current_date = 1 then
  case when sum_downgraded_find_in_current_date > 0 or sum_churned_find_in_current_date > 0 then 1
  when sum_find_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as cross_sell_migration_personalised_find_to_cmp,
CASE
  WHEN added_orchestrate_in_current_date = 1 then
  case when sum_downgraded_find_in_current_date > 0 or sum_churned_find_in_current_date > 0 then 1
  when sum_find_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as cross_sell_migration_personalised_find_to_orchestrate ,
CASE
  WHEN added_monetize_in_current_date = 1 then
  case when sum_downgraded_find_in_current_date > 0 or sum_churned_find_in_current_date >0 then 1
  when sum_find_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as cross_sell_migration_personalised_find_to_monetize,
CASE
  WHEN added_cms_in_current_date = 1 then
  case when sum_downgraded_find_in_current_date > 0 or sum_churned_find_in_current_date > 0 then 1
  when sum_find_in_previous_date_with_arr >0
  then 1 else 0 end
  else 0 end as cross_sell_migration_personalised_find_to_cms,
CASE
  WHEN added_cms_in_current_date = 1 then
  case when sum_downgraded_ektron_in_current_date > 0 or sum_churned_ektron_in_current_date > 0 then 1
  when sum_ektron_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end cross_sell_migration_ektron_to_cms,
CASE
  WHEN added_odp_in_current_date = 1 then
  case when sum_downgraded_vis_int_in_current_date > 0 or sum_churned_vis_int_in_current_date > 0 then 1
  when sum_vis_int_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as cross_sell_migration_visitorint_to_odp,
-- UPSELL

CASE
  WHEN increased_cmp_in_current_date = 1 then
  case when sum_downgraded_license_in_current_date > 0 or sum_churned_licenses_in_current_date > 0 then 1
  when sum_licenses_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as upsell_migration_licenses_to_cmp,
CASE
  WHEN increased_orchestrate_in_current_date = 1 then
  case when sum_downgraded_license_in_current_date > 0 or sum_churned_licenses_in_current_date > 0 then 1
  when sum_licenses_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as upsell_migration_licenses_to_orchestrate,
CASE
  WHEN increased_monetize_in_current_date = 1 then
  case when sum_downgraded_license_in_current_date >0 or sum_churned_licenses_in_current_date > 0 then 1
  when sum_licenses_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as upsell_migration_licenses_to_monetize,
CASE
  WHEN increased_cms_in_current_date = 1 then
  case when sum_downgraded_license_in_current_date > 0 or sum_churned_licenses_in_current_date >0 then 1
  when sum_licenses_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as upsell_migration_licenses_to_cms ,

CASE
  WHEN increased_cmp_in_current_date = 1 then
  case when sum_downgraded_everweb_in_current_date > 0 or sum_churned_everweb_in_current_date > 0 then 1
  when sum_everweb_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as upsell_migration_everweb_to_cmp,
CASE
  WHEN increased_orchestrate_in_current_date = 1 then
  case when sum_downgraded_everweb_in_current_date > 0 or sum_churned_everweb_in_current_date > 0 then 1
  when sum_everweb_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as upsell_migration_everweb_to_orchestrate,
CASE
  WHEN increased_monetize_in_current_date = 1 then
  case when sum_downgraded_everweb_in_current_date > 0 or sum_churned_everweb_in_current_date > 0 then 1
  when sum_everweb_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as upsell_migration_everweb_to_monetize,
CASE
  WHEN increased_cms_in_current_date = 1 then
  case when sum_downgraded_everweb_in_current_date > 0 or sum_churned_everweb_in_current_date > 0 then 1
  when sum_everweb_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as upsell_migration_everweb_to_cms,
CASE
  WHEN increased_cmp_in_current_date = 1 then
  case when sum_downgraded_find_in_current_date > 0 or sum_churned_find_in_current_date > 0 then 1
  when sum_find_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as upsell_migration_personalised_find_to_cmp,
CASE
  WHEN increased_orchestrate_in_current_date = 1 then
  case when sum_downgraded_find_in_current_date > 0 or sum_churned_find_in_current_date > 0 then 1
  when sum_find_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as upsell_migration_personalised_find_to_orchestrate ,
CASE
  WHEN increased_monetize_in_current_date = 1 then
  case when sum_downgraded_find_in_current_date > 0 or sum_churned_find_in_current_date >0 then 1
  when sum_find_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as upsell_migration_personalised_find_to_monetize,
CASE
  WHEN increased_cms_in_current_date = 1 then
  case when sum_downgraded_find_in_current_date > 0 or sum_churned_find_in_current_date > 0 then 1
  when sum_find_in_previous_date_with_arr >0
  then 1 else 0 end
  else 0 end as upsell_migration_personalised_find_to_cms,
CASE
  WHEN increased_cms_in_current_date = 1 then
  case when sum_downgraded_ektron_in_current_date > 0 or sum_churned_ektron_in_current_date > 0 then 1
  when sum_ektron_in_previous_date_with_arr >0
  then 1 else 0 end
  else 0 end as upsell_migration_ektron_to_cms ,
CASE
  WHEN increased_odp_in_current_date = 1 then
  case when sum_downgraded_vis_int_in_current_date > 0 or sum_churned_vis_int_in_current_date > 0 then 1
  when sum_vis_int_in_previous_date_with_arr > 0
  then 1 else 0 end
  else 0 end as upsell_migration_visitorint_to_odp
from initial_table_3
)
  ,initial_table_5 as (
select
*,
(downgrade_migration_licenses_to_cmp+
downgrade_migration_licenses_to_orchestrate+
downgrade_migration_licenses_to_monetize +
downgrade_migration_licenses_to_cms+
downgrade_migration_everweb_to_cmp+
downgrade_migration_everweb_to_orchestrate+
downgrade_migration_everweb_to_monetize+
downgrade_migration_everweb_to_cms+
downgrade_migration_personalised_find_to_cmp+
downgrade_migration_personalised_find_to_orchestrate+
downgrade_migration_personalised_find_to_monetize+
downgrade_migration_personalised_find_to_cms+
downgrade_migration_ektron_to_cms+
downgrade_migration_visitorint_to_odp+
downsell_migration_licenses_to_cmp+
downsell_migration_licenses_to_orchestrate+
downsell_migration_licenses_to_monetize+
downsell_migration_licenses_to_cms+
downsell_migration_everweb_to_cmp+
downsell_migration_everweb_to_orchestrate+
downsell_migration_everweb_to_monetize+
downsell_migration_everweb_to_cms+
downsell_migration_personalised_find_to_cmp+
downsell_migration_personalised_find_to_orchestrate+
downsell_migration_personalised_find_to_monetize+
downsell_migration_personalised_find_to_cms+
downsell_migration_ektron_to_cms+
downsell_migration_visitorint_to_odp+
cross_sell_migration_licenses_to_cmp+
cross_sell_migration_licenses_to_orchestrate+
cross_sell_migration_licenses_to_monetize+
cross_sell_migration_licenses_to_cms +
cross_sell_migration_everweb_to_cmp+
cross_sell_migration_everweb_to_orchestrate+
cross_sell_migration_everweb_to_monetize+
cross_sell_migration_everweb_to_cms+
cross_sell_migration_personalised_find_to_cmp+
cross_sell_migration_personalised_find_to_orchestrate +
cross_sell_migration_personalised_find_to_monetize+
cross_sell_migration_personalised_find_to_cms+
cross_sell_migration_ektron_to_cms+
cross_sell_migration_visitorint_to_odp+
upsell_migration_licenses_to_cmp+
upsell_migration_licenses_to_orchestrate+
upsell_migration_licenses_to_monetize+
upsell_migration_licenses_to_cms +
upsell_migration_everweb_to_cmp+
upsell_migration_everweb_to_orchestrate+
upsell_migration_everweb_to_monetize+
upsell_migration_everweb_to_cms+
upsell_migration_personalised_find_to_cmp+
upsell_migration_personalised_find_to_orchestrate +
upsell_migration_personalised_find_to_monetize+
upsell_migration_personalised_find_to_cms+
upsell_migration_ektron_to_cms +
upsell_migration_visitorint_to_odp) as active_flag_count,
CONCAT(
  CASE WHEN downgrade_migration_licenses_to_cmp = 1 THEN ',downgrade - migration -- Licenses to CMP' ELSE NULL END,
  CASE WHEN downgrade_migration_licenses_to_orchestrate = 1 THEN ',downgrade - migration -- Licenses to ORCHESTRATE' ELSE NULL END,
  CASE WHEN downgrade_migration_licenses_to_monetize = 1 THEN ',downgrade - migration -- Licenses to MONETIZE' ELSE NULL END,
  CASE WHEN downgrade_migration_licenses_to_cms = 1 THEN ',downgrade - migration -- Licenses to CMS' ELSE NULL END,
  CASE WHEN downgrade_migration_everweb_to_cmp = 1 THEN ',downgrade - migration -- Everweb to CMP' ELSE NULL END,
  CASE WHEN downgrade_migration_everweb_to_orchestrate = 1 THEN ',downgrade - migration -- Everweb to ORCHESTRATE' ELSE NULL END,
  CASE WHEN downgrade_migration_everweb_to_monetize = 1 THEN ',downgrade - migration -- Everweb to MONETIZE' ELSE NULL END,
  CASE WHEN downgrade_migration_everweb_to_cms = 1 THEN ',downgrade - migration -- Everweb to CMS' ELSE NULL END,
  CASE WHEN downgrade_migration_personalised_find_to_cmp = 1 THEN ',downgrade - migration -- Personalised Find to CMP' ELSE NULL END,
  CASE WHEN downgrade_migration_personalised_find_to_orchestrate = 1 THEN ',downgrade - migration -- Personalised Find to ORCHESTRATE' ELSE NULL END,
  CASE WHEN downgrade_migration_personalised_find_to_monetize = 1 THEN ',downgrade - migration -- Personalised Find to MONETIZE' ELSE NULL END,
  CASE WHEN downgrade_migration_personalised_find_to_cms = 1 THEN ',downgrade - migration -- Personalised Find to CMS' ELSE NULL END,
  CASE WHEN downgrade_migration_ektron_to_cms = 1 THEN ',downgrade - migration -- Ektron to CMS' ELSE NULL END,
  CASE WHEN downgrade_migration_visitorint_to_odp = 1 THEN ',downgrade - migration -- Visitorint to ODP' ELSE NULL END,
  CASE WHEN downsell_migration_licenses_to_cmp = 1 THEN ',downsell - migration -- Licenses to CMP' ELSE NULL END,
  CASE WHEN downsell_migration_licenses_to_orchestrate = 1 THEN ',downsell - migration -- Licenses to ORCHESTRATE' ELSE NULL END,
  CASE WHEN downsell_migration_licenses_to_monetize = 1 THEN ',downsell - migration -- Licenses to MONETIZE' ELSE NULL END,
  CASE WHEN downsell_migration_licenses_to_cms = 1 THEN ',downsell - migration -- Licenses to CMS' ELSE NULL END,
  CASE WHEN downsell_migration_everweb_to_cmp = 1 THEN ',downsell - migration -- Everweb to CMP' ELSE NULL END,
  CASE WHEN downsell_migration_everweb_to_orchestrate = 1 THEN ',downsell - migration -- Everweb to ORCHESTRATE' ELSE NULL END,
  CASE WHEN downsell_migration_everweb_to_monetize = 1 THEN ',downsell - migration -- Everweb to MONETIZE' ELSE NULL END,
  CASE WHEN downsell_migration_everweb_to_cms = 1 THEN ',downsell - migration -- Everweb to CMS' ELSE NULL END,
  CASE WHEN downsell_migration_personalised_find_to_cmp = 1 THEN ',downsell - migration -- Personalised Find to CMP' ELSE NULL END,
  CASE WHEN downsell_migration_personalised_find_to_orchestrate = 1 THEN ',downsell - migration -- Personalised Find to ORCHESTRATE' ELSE NULL END,
  CASE WHEN downsell_migration_personalised_find_to_monetize = 1 THEN ',downsell - migration -- Personalised Find to MONETIZE' ELSE NULL END,
  CASE WHEN downsell_migration_personalised_find_to_cms = 1 THEN ',downsell - migration -- Personalised Find to CMS' ELSE NULL END,
  CASE WHEN downsell_migration_ektron_to_cms = 1 THEN ',downsell - migration -- Ektron to CMS' ELSE NULL END,
  CASE WHEN downsell_migration_visitorint_to_odp = 1 THEN ',downsell - migration -- Visitorint to ODP' ELSE NULL END,
  CASE WHEN cross_sell_migration_licenses_to_cmp = 1 THEN ',cross sell - migration -- Licenses to CMP' ELSE NULL END,
  CASE WHEN cross_sell_migration_licenses_to_orchestrate = 1 THEN ',cross sell - migration -- Licenses to ORCHESTRATE' ELSE NULL END,
  CASE WHEN cross_sell_migration_licenses_to_monetize = 1 THEN ',cross sell - migration -- Licenses to MONETIZE' ELSE NULL END,
  CASE WHEN cross_sell_migration_licenses_to_cms = 1 THEN ',cross sell - migration -- Licenses to CMS' ELSE NULL END,
  CASE WHEN cross_sell_migration_everweb_to_cmp = 1 THEN ',cross sell - migration -- Everweb to CMP' ELSE NULL END,
  CASE WHEN cross_sell_migration_everweb_to_orchestrate = 1 THEN ',cross sell - migration -- Everweb to ORCHESTRATE' ELSE NULL END,
  CASE WHEN cross_sell_migration_everweb_to_monetize = 1 THEN ',cross sell - migration -- Everweb to MONETIZE' ELSE NULL END,
  CASE WHEN cross_sell_migration_everweb_to_cms = 1 THEN ',cross sell - migration -- Everweb to CMS' ELSE NULL END,
  CASE WHEN cross_sell_migration_personalised_find_to_cmp = 1 THEN ',cross sell - migration -- Personalised Find to CMP' ELSE NULL END,
  CASE WHEN cross_sell_migration_personalised_find_to_orchestrate = 1 THEN ',cross sell - migration -- Personalised Find to ORCHESTRATE' ELSE NULL END,
  CASE WHEN cross_sell_migration_personalised_find_to_monetize = 1 THEN ',cross sell - migration -- Personalised Find to MONETIZE' ELSE NULL END,
  CASE WHEN cross_sell_migration_personalised_find_to_cms = 1 THEN ',cross sell - migration -- Personalised Find to CMS' ELSE NULL END,
  CASE WHEN cross_sell_migration_ektron_to_cms = 1 THEN ',cross sell - migration -- Ektron to CMS' ELSE NULL END,
  CASE WHEN cross_sell_migration_visitorint_to_odp = 1 THEN ',cross sell - migration -- Visitorint to ODP' ELSE NULL END,
  CASE WHEN upsell_migration_licenses_to_cmp = 1 THEN ',upsell - migration -- Licenses to CMP' ELSE NULL END,
  CASE WHEN upsell_migration_licenses_to_orchestrate = 1 THEN ',upsell - migration -- Licenses to ORCHESTRATE' ELSE NULL END,
  CASE WHEN upsell_migration_licenses_to_monetize = 1 THEN ',upsell - migration -- Licenses to MONETIZE' ELSE NULL END,
  CASE WHEN upsell_migration_licenses_to_cms = 1 THEN ',upsell - migration -- Licenses to CMS' ELSE NULL END,
  CASE WHEN upsell_migration_everweb_to_cmp = 1 THEN ',upsell - migration -- Everweb to CMP' ELSE NULL END,
  CASE WHEN upsell_migration_everweb_to_orchestrate = 1 THEN ',upsell - migration -- Everweb to ORCHESTRATE' ELSE NULL END,
  CASE WHEN upsell_migration_everweb_to_monetize = 1 THEN ',upsell - migration -- Everweb to MONETIZE' ELSE NULL END,
  CASE WHEN upsell_migration_everweb_to_cms = 1 THEN ',upsell - migration -- Everweb to CMS' ELSE NULL END,
  CASE WHEN upsell_migration_personalised_find_to_cmp = 1 THEN ',upsell - migration -- Personalised Find to CMP' ELSE NULL END,
  CASE WHEN upsell_migration_personalised_find_to_orchestrate = 1 THEN ',upsell - migration -- Personalised Find to ORCHESTRATE' ELSE NULL END,
  CASE WHEN upsell_migration_personalised_find_to_monetize = 1 THEN ',upsell - migration -- Personalised Find to MONETIZE' ELSE NULL END,
  CASE WHEN upsell_migration_personalised_find_to_cms = 1 THEN ',upsell - migration -- Personalised Find to CMS' ELSE NULL END,
  CASE WHEN upsell_migration_ektron_to_cms = 1 THEN ',upsell - migration -- Ektron to CMS' ELSE NULL END,
  CASE WHEN upsell_migration_visitorint_to_odp = 1 THEN ',upsell - migration -- Visitorint to ODP' ELSE NULL END
) AS flag_descriptions

from initial_table_4)

-- select * from initial_table_5;
, initial_table_6 as (
  select
      evaluation_period  ,
      mcid ,
      currency_code ,
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
      active_flag_count ,
      flag_descriptions,
      max(current_period_product_arr_usd_ccfx)
      over(
          partition by evaluation_period , mcid , currency_code
          order by current_period_product_arr_usd_ccfx desc
          ) as max_value_migration_to
--       max(prior_period_product_arr_usd_ccfx)
--           over(
--           partition by evaluation_period , mcid , currency_code
--           order by prior_period_product_arr_usd_ccfx desc
--           ) as max_value_migration_from
  from initial_table_5
--   where active_flag_count > 0
)
--    select * from initial_table_6;
, initial_table_7 as (
select
  evaluation_period  ,
  mcid ,
  currency_code ,
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
  active_flag_count ,
  flag_descriptions,
  selected_mig_to,
  upper(max(selected_mig_from)over(partition by mcid, evaluation_period, currency_code)) as selected_mig_from
from (
    select
        *,case when
        max(case when lower(prior_pathways) <> lower(selected_mig_to) then prior_period_product_arr_usd_ccfx end) over(partition by evaluation_period , mcid , currency_code order by prior_period_product_arr_usd_ccfx desc
      ) = prior_period_product_arr_usd_ccfx then prior_pathways else null end as selected_mig_from
    FROM (
        select
              evaluation_period  ,
              mcid ,
              currency_code ,
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
              active_flag_count ,
              flag_descriptions,
              upper(max(selected_mig_to) over(partition by mcid,evaluation_period,currency_code )) as selected_mig_to
        from (
           select
               *,
               case when max_value_migration_to = current_period_product_arr_usd_ccfx then current_pathways else null end as selected_mig_to
           from initial_table_6
        ) as a
    ) as a
     ) as a
)

--    select active_flag_count , count(*) from initial_table_7 group by 1 ;

-- , initial_table_7 as (
-- select
--   evaluation_period  ,
--   mcid ,
--   currency_code ,
--   current_product_group,
--   prior_product_group,
--   current_product_solution,
--   prior_product_solution,
--   current_pathways,
--   prior_pathways,
--   prior_period_product_arr_lcu,
--   current_period_product_arr_lcu,
--   prior_period_product_arr_usd_ccfx,
--   current_period_product_arr_usd_ccfx,
--   product_arr_change_ccfx,
--   product_arr_change_lcu,
--   active_flag_count ,
--   flag_descriptions,
-- --   max_value ,
--   upper(max(selected_mig_to) over(partition by mcid,evaluation_period,currency_code )) as selected_mig_to,
--   upper(max(selected_mig_from)over(partition by mcid, evaluation_period, currency_code)) as selected_mig_from
-- from (
--     select
--          *,
--         case when max_value_migration_to = current_period_product_arr_usd_ccfx then current_pathways else null end as selected_mig_to,
--         case when max_value_migration_from = prior_period_product_arr_usd_ccfx
--             and current_pathways <> prior_pathways
--             then prior_pathways else null end  as selected_mig_from
--
--     from initial_table_6
--     ) as a
-- )
--    select * from initial_table_7;
, base as (
select * from (
    select
    mcid ,
    evaluation_period,
    currency_code,
    trim(unnest(string_to_array(flag_descriptions, ','))) AS individual_value
    from initial_table_7
    where active_flag_count >1) as a
where individual_value is not null and individual_value <> ''
group by 1,2,3,4
)

-- select * from base;

, final_table as (
select
  evaluation_period  ,
  mcid ,
  currency_code ,
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
  active_flag_count ,
  flag_descriptions,
  case when active_flag_count > 1 then
      case when flag_descriptions <> '' then individual_value else null end
      else (string_to_array(flag_descriptions, ','))[2] end
      as "Migration Classification"
from (
    select
        a.*,
        b.individual_value
    from initial_table_7 as a
    left join base as b
    on a.mcid = b.mcid
    and a.evaluation_period = b.evaluation_period
    and a.currency_code = b.currency_code
    and lower(individual_value) LIKE '%' || lower(selected_mig_from) || '%'
    AND lower(individual_value) LIKE '%to ' || lower(selected_mig_to) || '%'
    AND individual_value != ''
    and a.active_flag_count > 1
     )as a
)
-- select * from final_table;
-- where "Migration Classification" is not null;
, exp_a as(
    select
        mcid ,
        evaluation_period,
        count(*) as table_a_num
    from initial_table_6
    group by 1,2
)
, exp_b as (
    select
        mcid ,
        evaluation_period ,
        count(*) as table_b_num
    from final_table
    group by mcid, evaluation_period
)
select
    *
from (
select
    coalesce(a.mcid , b.mcid ) as mcid ,
    coalesce(a.evaluation_period , b.evaluation_period ) as evaluation_period ,
    table_a_num ,
    table_b_num,
    abs(table_a_num - table_b_num) as diff
from exp_a as a
full join exp_b as b
on a.mcid = b.mcid
and a.evaluation_period = b.evaluation_period
     ) as a
where diff > 0
;
-- and a.active_flag_count > 0