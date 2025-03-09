DROP TABLE IF EXISTS sandbox.churn_migration_classifiers4;
create table sandbox.churn_migration_classifiers4 as (
  with initial_table as (
    select *
    from ryzlan.sst_product_pathways_bridge2
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
  ),
  initial_table_3 as (
    select *,
        CASE WHEN downgraded_license_in_current_date = 1
        and (
          sum(cmp_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downgrade_migration_licenses_to_cmp,
        CASE WHEN downgraded_license_in_current_date = 1
        and (
          sum(orchestrate_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downgrade_migration_licenses_to_orchestrate,
        CASE WHEN downgraded_license_in_current_date = 1
        and (
          sum(monetize_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downgrade_migration_licenses_to_monetize,
        CASE WHEN downgraded_license_in_current_date = 1
        and (
          sum(cms_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downgrade_migration_licenses_to_cms,
        CASE WHEN downgraded_everweb_in_current_date = 1
        and (
          sum(cmp_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downgrade_migration_everweb_to_cmp,
        CASE WHEN downgraded_everweb_in_current_date = 1
        and (
          sum(orchestrate_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downgrade_migration_everweb_to_orchestrate,
        CASE WHEN downgraded_everweb_in_current_date = 1
        and (
          sum(monetize_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downgrade_migration_everweb_to_monetize,
        CASE WHEN downgraded_everweb_in_current_date = 1
        and (
          sum(cms_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downgrade_migration_everweb_to_cms,
        CASE WHEN downgraded_find_in_current_date = 1
        and (
          sum(cmp_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downgrade_migration_personalised_find_to_cmp,
        CASE WHEN downgraded_find_in_current_date = 1
        and (
          sum(orchestrate_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downgrade_migration_personalised_find_to_orchestrate,
        CASE WHEN downgraded_find_in_current_date = 1
        and (
          sum(monetize_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downgrade_migration_personalised_find_to_monetize,
        CASE WHEN downgraded_find_in_current_date = 1
        and (
          sum(cms_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downgrade_migration_personalised_find_to_cms,
        CASE WHEN downgraded_ektron_in_current_date = 1
        and (
          sum(cms_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downgrade_migration_ektron_to_cms,
        CASE WHEN downgraded_vis_int_in_current_date = 1
        and (
          sum(odp_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downgrade_migration_visitorint_to_odp,
        CASE WHEN churned_licenses_in_current_date = 1
        and (
          sum(cmp_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downsell_migration_licenses_to_cmp,
        CASE WHEN churned_licenses_in_current_date = 1
        and (
          sum(orchestrate_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downsell_migration_licenses_to_orchestrate,
        CASE WHEN churned_licenses_in_current_date = 1
        and (
          sum(monetize_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downsell_migration_licenses_to_monetize,
        CASE WHEN churned_licenses_in_current_date = 1
        and (
          sum(cms_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downsell_migration_licenses_to_cms,
        CASE WHEN churned_everweb_in_current_date = 1
        and (
          sum(cmp_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downsell_migration_everweb_to_cmp,
        CASE WHEN churned_everweb_in_current_date = 1
        and (
          sum(orchestrate_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downsell_migration_everweb_to_orchestrate,
        CASE WHEN churned_everweb_in_current_date = 1
        and (
          sum(monetize_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downsell_migration_everweb_to_monetize,
        CASE WHEN churned_everweb_in_current_date = 1
        and (
          sum(cms_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downsell_migration_everweb_to_cms,
        CASE WHEN churned_find_in_current_date = 1
        and (
          sum(cmp_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downsell_migration_personalised_find_to_cmp,
        CASE WHEN churned_find_in_current_date = 1
        and (
          sum(orchestrate_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downsell_migration_personalised_find_to_orchestrate,
        CASE WHEN churned_find_in_current_date = 1
        and (
          sum(monetize_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downsell_migration_personalised_find_to_monetize,
        CASE WHEN churned_find_in_current_date = 1
        and (
          sum(cms_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downsell_migration_personalised_find_to_cms,
        CASE WHEN churned_ektron_in_current_date = 1
        and (
          sum(cms_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downsell_migration_ektron_to_cms,
        CASE WHEN churned_vis_int_in_current_date = 1
        and (
          sum(odp_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as downsell_migration_visitorint_to_odp,
        CASE WHEN added_cmp_in_current_date = 1
        and (
          (
            sum(downgraded_license_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(churned_licenses_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as cross_sell_migration_licenses_to_cmp,
        CASE WHEN added_orchestrate_in_current_date = 1
        and (
          (
            sum(downgraded_license_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(churned_licenses_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as cross_sell_migration_licenses_to_orchestrate,
        CASE WHEN added_monetize_in_current_date = 1
        and (
          (
            sum(downgraded_license_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(churned_licenses_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as cross_sell_migration_licenses_to_monetize,
        CASE WHEN added_cms_in_current_date = 1
        and (
          (
            sum(downgraded_license_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(churned_licenses_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as cross_sell_migration_licenses_to_cms,
        CASE WHEN added_cmp_in_current_date = 1
        and (
          (
            sum(downgraded_everweb_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(churned_everweb_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as cross_sell_migration_everweb_to_cmp,
        CASE WHEN added_orchestrate_in_current_date = 1
        and (
          (
            sum(downgraded_everweb_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(churned_everweb_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as cross_sell_migration_everweb_to_orchestrate,
        CASE WHEN added_monetize_in_current_date = 1
        and (
          (
            sum(downgraded_everweb_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(churned_everweb_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as cross_sell_migration_everweb_to_monetize,
        CASE WHEN added_cms_in_current_date = 1
        and (
          (
            sum(downgraded_everweb_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(churned_everweb_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as cross_sell_migration_everweb_to_cms,
        CASE WHEN added_cmp_in_current_date = 1
        and (
          (
            sum(
              downgraded_find_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              churned_find_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as cross_sell_migration_personalised_find_to_cmp,
        CASE WHEN added_orchestrate_in_current_date = 1
        and (
          (
            sum(
              downgraded_find_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              churned_find_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as cross_sell_migration_personalised_find_to_orchestrate,
        CASE WHEN added_monetize_in_current_date = 1
        and (
          (
            sum(
              downgraded_find_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              churned_find_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as cross_sell_migration_personalised_find_to_monetize,
        CASE WHEN added_cms_in_current_date = 1
        and (
          (
            sum(
              downgraded_find_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              churned_find_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as cross_sell_migration_personalised_find_to_cms,
        CASE WHEN added_cms_in_current_date = 1
        and (
          (
            sum(
              downgraded_ektron_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              churned_ektron_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as cross_sell_migration_ektron_to_cms,
        CASE WHEN added_odp_in_current_date = 1
        and (
          (
            sum(
              downgraded_vis_int_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(churned_vis_int_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as cross_sell_migration_visitorint_to_odp,
        CASE WHEN increased_cmp_in_current_date = 1
        and (
          (
            sum(downgraded_license_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(churned_licenses_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as upsell_migration_licenses_to_cmp,
        CASE WHEN increased_orchestrate_in_current_date = 1
        and (
          (
            sum(downgraded_license_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(churned_licenses_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as upsell_migration_licenses_to_orchestrate,
        CASE WHEN increased_monetize_in_current_date = 1
        and (
          (
            sum(downgraded_license_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(churned_licenses_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as upsell_migration_licenses_to_monetize,
        CASE WHEN increased_cms_in_current_date = 1
        and (
          (
            sum(downgraded_license_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(churned_licenses_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as upsell_migration_licenses_to_cms,
        CASE WHEN increased_cmp_in_current_date = 1
        and (
          (
            sum(downgraded_everweb_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(churned_everweb_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as upsell_migration_everweb_to_cmp,
        CASE WHEN increased_orchestrate_in_current_date = 1
        and (
          (
            sum(downgraded_everweb_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(churned_everweb_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as upsell_migration_everweb_to_orchestrate,
        CASE WHEN increased_monetize_in_current_date = 1
        and (
          (
            sum(downgraded_everweb_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(churned_everweb_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as upsell_migration_everweb_to_monetize,
        CASE WHEN increased_cms_in_current_date = 1
        and (
          (
            sum(downgraded_everweb_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(churned_everweb_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as upsell_migration_everweb_to_cms,
        CASE WHEN increased_cmp_in_current_date = 1
        and (
          (
            sum(
              downgraded_find_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              churned_find_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as upsell_migration_personalised_find_to_cmp,
        CASE WHEN increased_orchestrate_in_current_date = 1
        and (
          (
            sum(
              downgraded_find_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              churned_find_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as upsell_migration_personalised_find_to_orchestrate,
        CASE WHEN increased_monetize_in_current_date = 1
        and (
          (
            sum(
              downgraded_find_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              churned_find_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as upsell_migration_personalised_find_to_monetize,
        CASE WHEN increased_cms_in_current_date = 1
        and (
          (
            sum(
              downgraded_find_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              churned_find_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as upsell_migration_personalised_find_to_cms,
        CASE WHEN increased_cms_in_current_date = 1
        and (
          (
            sum(
              downgraded_ektron_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              churned_ektron_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as upsell_migration_ektron_to_cms,
        CASE WHEN increased_odp_in_current_date = 1
        and (
          (
            sum(
              downgraded_vis_int_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(churned_vis_int_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as upsell_migration_visitorint_to_odp,
        CASE WHEN added_cmp_in_current_date = 1
        and (
          sum(licenses_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as cross_sell_migration_licenses_to_cmp,
        CASE WHEN added_orchestrate_in_current_date = 1
        and (
          sum(licenses_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as cross_sell_migration_licenses_to_orchestrate,
        CASE WHEN added_monetize_in_current_date = 1
        and (
          sum(licenses_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as cross_sell_migration_licenses_to_monetize,
        CASE WHEN added_cms_in_current_date = 1
        and (
          sum(licenses_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as cross_sell_migration_licenses_to_cms,
        CASE WHEN added_cmp_in_current_date = 1
        and (
          sum(everweb_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as cross_sell_migration_everweb_to_cmp,
        CASE WHEN added_orchestrate_in_current_date = 1
        and (
          sum(everweb_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as cross_sell_migration_everweb_to_orchestrate,
        CASE WHEN added_monetize_in_current_date = 1
        and (
          sum(everweb_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as cross_sell_migration_everweb_to_monetize,
        CASE WHEN added_cms_in_current_date = 1
        and (
          sum(everweb_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as cross_sell_migration_everweb_to_cms,
        CASE WHEN added_cmp_in_current_date = 1
        and (
          sum(find_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as cross_sell_migration_personalised_find_to_cmp,
        CASE WHEN added_orchestrate_in_current_date = 1
        and (
          sum(find_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as cross_sell_migration_personalised_find_to_orchestrate,
        CASE WHEN added_monetize_in_current_date = 1
        and (
          sum(find_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as cross_sell_migration_personalised_find_to_monetize,
        CASE WHEN added_cms_in_current_date = 1
        and (
          sum(find_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as cross_sell_migration_personalised_find_to_cms,
        CASE WHEN added_cms_in_current_date = 1
        and (
          sum(
            ektron_in_previous_date_with_arr
          ) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as cross_sell_migration_ektron_to_cms,
        CASE WHEN added_odp_in_current_date = 1
        and (
          sum(vis_int_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as cross_sell_migration_visitorint_to_odp,
        CASE WHEN increased_cmp_in_current_date = 1
        and (
          sum(licenses_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as upsell_migration_licenses_to_cmp,
        CASE WHEN increased_orchestrate_in_current_date = 1
        and (
          sum(licenses_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as upsell_migration_licenses_to_orchestrate,
        CASE WHEN increased_monetize_in_current_date = 1
        and (
          sum(licenses_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as upsell_migration_licenses_to_monetize,
        CASE WHEN increased_cms_in_current_date = 1
        and (
          sum(licenses_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as upsell_migration_licenses_to_cms,
        CASE WHEN increased_cmp_in_current_date = 1
        and (
          sum(everweb_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as upsell_migration_everweb_to_cmp,
        CASE WHEN increased_orchestrate_in_current_date = 1
        and (
          sum(everweb_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as upsell_migration_everweb_to_orchestrate,
        CASE WHEN increased_monetize_in_current_date = 1
        and (
          sum(everweb_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as upsell_migration_everweb_to_monetize,
        CASE WHEN increased_cms_in_current_date = 1
        and (
          sum(everweb_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as upsell_migration_everweb_to_cms,
        CASE WHEN increased_cmp_in_current_date = 1
        and (
          sum(find_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as upsell_migration_personalised_find_to_cmp,
        CASE WHEN increased_orchestrate_in_current_date = 1
        and (
          sum(find_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as upsell_migration_personalised_find_to_orchestrate,
        CASE WHEN increased_monetize_in_current_date = 1
        and (
          sum(find_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as upsell_migration_personalised_find_to_monetize,
        CASE WHEN increased_cms_in_current_date = 1
        and (
          sum(find_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as upsell_migration_personalised_find_to_cms,
        CASE WHEN increased_cms_in_current_date = 1
        and (
          sum(
            ektron_in_previous_date_with_arr
          ) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as upsell_migration_ektron_to_cms,
        CASE WHEN increased_odp_in_current_date = 1
        and (
          sum(vis_int_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 THEN 1 ELSE 0 END as upsell_migration_visitorint_to_odp,
        CASE WHEN churned_licenses_in_current_date = 1
        and (
          (
            sum(added_cmp_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(increased_cmp_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downsell_migration_licenses_to_cmp,
        CASE WHEN churned_licenses_in_current_date = 1
        and (
          (
            sum(added_orchestrate_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(increased_orchestrate_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downsell_migration_licenses_to_orchestrate,
        CASE WHEN churned_licenses_in_current_date = 1
        and (
          (
            sum(added_monetize_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              increased_monetize_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downsell_migration_licenses_to_monetize,
        CASE WHEN churned_licenses_in_current_date = 1
        and (
          (
            sum(added_cms_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(increased_cms_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downsell_migration_licenses_to_cms,
        CASE WHEN churned_everweb_in_current_date = 1
        and (
          (
            sum(added_cmp_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              increased_cmp_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downsell_migration_everweb_to_cmp,
        CASE WHEN churned_everweb_in_current_date = 1
        and (
          (
            sum(added_orchestrate_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              increased_orchestrate_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downsell_migration_everweb_to_orchestrate,
        CASE WHEN churned_everweb_in_current_date = 1
        and (
          (
            sum(added_monetize_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              increased_monetize_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downsell_migration_everweb_to_monetize,
        CASE WHEN churned_everweb_in_current_date = 1
        and (
          (
            sum(added_cms_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              increased_cms_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downsell_migration_everweb_to_cms,
        CASE WHEN churned_find_in_current_date = 1
        and (
          (
            sum(added_cmp_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              increased_cmp_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downsell_migration_personalised_find_to_cmp,
        CASE WHEN churned_find_in_current_date = 1
        and (
          (
            sum(added_orchestrate_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              increased_orchestrate_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downsell_migration_personalised_find_to_orchestrate,
        CASE WHEN churned_find_in_current_date = 1
        and (
          (
            sum(added_monetize_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              increased_monetize_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downsell_migration_personalised_find_to_monetize,
        CASE WHEN churned_find_in_current_date = 1
        and (
          (
            sum(added_cms_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              increased_cms_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downsell_migration_personalised_find_to_cms,
        CASE WHEN churned_ektron_in_current_date = 1
        and (
          (
            sum(added_cms_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(increased_cms_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downsell_migration_ektron_to_cms,
        CASE WHEN churned_vis_int_in_current_date = 1
        and (
          (
            sum(added_odp_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(increased_odp_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downsell_migration_visitorint_to_odp,
        CASE WHEN downgraded_license_in_current_date = 1
        and (
          (
            sum(added_cmp_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(increased_cmp_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downgrade_migration_licenses_to_cmp,
        CASE WHEN downgraded_license_in_current_date = 1
        and (
          (
            sum(added_orchestrate_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(increased_orchestrate_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downgrade_migration_licenses_to_orchestrate,
        CASE WHEN downgraded_license_in_current_date = 1
        and (
          (
            sum(added_monetize_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              increased_monetize_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downgrade_migration_licenses_to_monetize,
        CASE WHEN downgraded_license_in_current_date = 1
        and (
          (
            sum(added_cms_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              increased_cms_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downgrade_migration_licenses_to_cms,
        CASE WHEN downgraded_everweb_in_current_date = 1
        and (
          (
            sum(added_cmp_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              increased_cmp_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downgrade_migration_everweb_to_cmp,
        CASE WHEN downgraded_everweb_in_current_date = 1
        and (
          (
            sum(added_orchestrate_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              increased_orchestrate_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downgrade_migration_everweb_to_orchestrate,
        CASE WHEN downgraded_everweb_in_current_date = 1
        and (
          (
            sum(added_monetize_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              increased_monetize_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downgrade_migration_everweb_to_monetize,
        CASE WHEN downgraded_everweb_in_current_date = 1
        and (
          (
            sum(added_cms_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              increased_cms_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downgrade_migration_everweb_to_cms,
        CASE WHEN downgraded_find_in_current_date = 1
        and (
          (
            sum(added_cmp_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              increased_cmp_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downgrade_migration_personalised_find_to_cmp,
        CASE WHEN downgraded_find_in_current_date = 1
        and (
          (
            sum(added_orchestrate_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              increased_orchestrate_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downgrade_migration_personalised_find_to_orchestrate,
        CASE WHEN downgraded_find_in_current_date = 1
        and (
          (
            sum(added_monetize_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              increased_monetize_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downgrade_migration_personalised_find_to_monetize,
        CASE WHEN downgraded_find_in_current_date = 1
        and (
          (
            sum(added_cms_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              increased_cms_in_current_date
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downgrade_migration_personalised_find_to_cms,
        CASE WHEN downgraded_ektron_in_current_date = 1
        and (
          (
            sum(added_cms_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(increased_cms_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downgrade_migration_ektron_to_cms,
        CASE WHEN downgraded_vis_int_in_current_date = 1
        and (
          (
            sum(added_odp_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(increased_odp_in_current_date) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) THEN 1 ELSE 0 END as downgrade_migration_visitorint_to_odp 
      from initial_table_2 
    )
  SELECT *
  FROM initial_table_3
);
select distinct "Movement Classification"
from sandbox.churn_migration_classifiers4;