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
      case
        when downgraded_license_in_current_date = 1
        and (
          sum(cmp_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'downgrade - migration -- Licenses to CMP'
        when downgraded_license_in_current_date = 1
        and (
          sum(orchestrate_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'downgrade - migration -- Licenses to Orchestrate'
        when downgraded_license_in_current_date = 1
        and (
          sum(monetize_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'downgrade - migration -- Licenses to Monetize'
        when downgraded_license_in_current_date = 1
        and (
          sum(cms_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'downgrade - migration -- Licenses to CMS'
        when downgraded_everweb_in_current_date = 1
        and (
          sum(cmp_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'downgrade - migration -- Everweb to CMP'
        when downgraded_everweb_in_current_date = 1
        and (
          sum(orchestrate_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'downgrade - migration -- Everweb to Orchestrate'
        when downgraded_everweb_in_current_date = 1
        and (
          sum(monetize_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'downgrade - migration -- Everweb to Monetize'
        when downgraded_everweb_in_current_date = 1
        and (
          sum(cms_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'downgrade - migration -- Everweb to CMS'
        when downgraded_find_in_current_date = 1
        and (
          sum(cmp_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'downgrade - migration -- Personalised Find to CMP'
        when downgraded_find_in_current_date = 1
        and (
          sum(orchestrate_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'downgrade - migration -- Personalised Find to Orchestrate'
        when downgraded_find_in_current_date = 1
        and (
          sum(monetize_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'downgrade - migration -- Personalised Find to Monetize'
        when downgraded_find_in_current_date = 1
        and (
          sum(cms_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'downgrade - migration -- Personalised Find to CMS'
        when downgraded_ektron_in_current_date = 1
        and (
          sum(cms_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'downgrade - migration -- Ektron to CMS'
        when downgraded_vis_int_in_current_date = 1
        and (
          sum(odp_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'downgrade - migration -- VisitorInt to ODP'
        when churned_licenses_in_current_date = 1
        and (
          sum(cmp_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Downsell - migration -- Licenses to CMP'
        when churned_licenses_in_current_date = 1
        and (
          sum(orchestrate_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Downsell - migration -- Licenses to Orchestrate'
        when churned_licenses_in_current_date = 1
        and (
          sum(monetize_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Downsell - migration -- Licenses to Monetize'
        when churned_licenses_in_current_date = 1
        and (
          sum(cms_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Downsell - migration -- Licenses to CMS'
        when churned_everweb_in_current_date = 1
        and (
          sum(cmp_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Downsell - migration -- Everweb to CMP'
        when churned_everweb_in_current_date = 1
        and (
          sum(orchestrate_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Downsell - migration -- Everweb to Orchestrate'
        when churned_everweb_in_current_date = 1
        and (
          sum(monetize_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Downsell - migration -- Everweb to Monetize'
        when churned_everweb_in_current_date = 1
        and (
          sum(cms_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Downsell - migration -- Everweb to CMS'
        when churned_find_in_current_date = 1
        and (
          sum(cmp_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Downsell - migration -- Personalised Find to CMP'
        when churned_find_in_current_date = 1
        and (
          sum(orchestrate_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Downsell - migration -- Personalised Find to Orchestrate'
        when churned_find_in_current_date = 1
        and (
          sum(monetize_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Downsell - migration -- Personalised Find to Monetize'
        when churned_find_in_current_date = 1
        and (
          sum(cms_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Downsell - migration -- Personalised Find to CMS'
        when churned_ektron_in_current_date = 1
        and (
          sum(cms_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Downsell - migration -- Ektron to CMS'
        when churned_vis_int_in_current_date = 1
        and (
          sum(odp_in_current_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Downsell - migration -- VisitorInt to ODP'
        when added_cmp_in_current_date = 1
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
        ) then 'Cross-sell - migration -- Licenses to CMP'
        when added_orchestrate_in_current_date = 1
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
        ) then 'Cross-sell - migration -- Licenses to Orchestrate'
        when added_monetize_in_current_date = 1
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
        ) then 'Cross-sell - migration -- Licenses to Monetize'
        when added_cms_in_current_date = 1
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
        ) then 'Cross-sell - migration -- Licenses to CMS'
        when added_cmp_in_current_date = 1
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
        ) then 'Cross-sell - migration -- Everweb to CMP'
        when added_orchestrate_in_current_date = 1
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
        ) then 'Cross-sell - migration -- Everweb to Orchestrate'
        when added_monetize_in_current_date = 1
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
        ) then 'Cross-sell - migration -- Everweb to Monetize'
        when added_cms_in_current_date = 1
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
        ) then 'Cross-sell - migration -- Everweb to CMS'
        when added_cmp_in_current_date = 1
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
        ) then 'Cross-sell - migration -- Personalised Find to CMP'
        when added_orchestrate_in_current_date = 1
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
        ) then 'Cross-sell - migration -- Personalised Find to Orchestrate'
        when added_monetize_in_current_date = 1
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
        ) then 'Cross-sell - migration -- Personalised Find to Monetize'
        when added_cms_in_current_date = 1
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
        ) then 'Cross-sell - migration -- Personalised Find to CMS'
        when added_cms_in_current_date = 1
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
        ) then 'Cross-sell - migration -- Ektron to CMS'
        when added_odp_in_current_date = 1
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
        ) then 'Cross-sell - migration -- VisitorInt to ODP'
        when increased_cmp_in_current_date = 1
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
        ) then 'upsell - migration -- Licenses to CMP'
        when increased_orchestrate_in_current_date = 1
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
        ) then 'upsell - migration -- Licenses to Orchestrate'
        when increased_monetize_in_current_date = 1
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
        ) then 'upsell - migration -- Licenses to Monetize'
        when increased_cms_in_current_date = 1
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
        ) then 'upsell - migration -- Licenses to CMS'
        when increased_cmp_in_current_date = 1
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
        ) then 'upsell - migration -- Everweb to CMP'
        when increased_orchestrate_in_current_date = 1
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
        ) then 'upsell - migration -- Everweb to Orchestrate'
        when increased_monetize_in_current_date = 1
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
        ) then 'upsell - migration -- Everweb to Monetize'
        when increased_cms_in_current_date = 1
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
        ) then 'upsell - migration -- Everweb to CMS'
        when increased_cmp_in_current_date = 1
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
        ) then 'upsell - migration -- Personalised Find to CMP'
        when increased_orchestrate_in_current_date = 1
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
        ) then 'upsell - migration -- Personalised Find to Orchestrate'
        when increased_monetize_in_current_date = 1
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
        ) then 'upsell - migration -- Personalised Find to Monetize'
        when increased_cms_in_current_date = 1
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
        ) then 'upsell - migration -- Personalised Find to CMS'
        when increased_cms_in_current_date = 1
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
        ) then 'upsell - migration -- Ektron to CMS'
        when increased_odp_in_current_date = 1
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
        ) then 'upsell - migration -- VisitorInt to ODP'
        when added_cmp_in_current_date = 1
        and (
          sum(licenses_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Cross-sell - migration -- Licenses to CMP'
        when added_orchestrate_in_current_date = 1
        and (
          sum(licenses_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Cross-sell - migration -- Licenses to Orchestrate'
        when added_monetize_in_current_date = 1
        and (
          sum(licenses_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Cross-sell - migration -- Licenses to Monetize'
        when added_cms_in_current_date = 1
        and (
          sum(licenses_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Cross-sell - migration -- Licenses to CMS'
        when added_cmp_in_current_date = 1
        and (
          sum(everweb_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Cross-sell - migration -- Everweb to CMP'
        when added_orchestrate_in_current_date = 1
        and (
          sum(everweb_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Cross-sell - migration -- Everweb to Orchestrate'
        when added_monetize_in_current_date = 1
        and (
          sum(everweb_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Cross-sell - migration -- Everweb to Monetize'
        when added_cms_in_current_date = 1
        and (
          sum(everweb_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Cross-sell - migration -- Everweb to CMS'
        when added_cmp_in_current_date = 1
        and (
          sum(find_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Cross-sell - migration -- Personalised Find to CMP'
        when added_orchestrate_in_current_date = 1
        and (
          sum(find_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Cross-sell - migration -- Personalised Find to Orchestrate'
        when added_monetize_in_current_date = 1
        and (
          sum(find_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Cross-sell - migration -- Personalised Find to Monetize'
        when added_cms_in_current_date = 1
        and (
          sum(find_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Cross-sell - migration -- Personalised Find to CMS'
        when added_cms_in_current_date = 1
        and (
          sum(
            ektron_in_previous_date_with_arr
          ) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Cross-sell - migration -- Ektron to CMS'
        when added_odp_in_current_date = 1
        and (
          sum(vis_int_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'Cross-sell - migration -- VisitorInt to ODP'
        when increased_cmp_in_current_date = 1
        and (
          sum(licenses_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'upsell - migration -- Licenses to CMP'
        when increased_orchestrate_in_current_date = 1
        and (
          sum(licenses_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'upsell - migration -- Licenses to Orchestrate'
        when increased_monetize_in_current_date = 1
        and (
          sum(licenses_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'upsell - migration -- Licenses to Monetize'
        when increased_cms_in_current_date = 1
        and (
          sum(licenses_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'upsell - migration -- Licenses to CMS'
        when increased_cmp_in_current_date = 1
        and (
          sum(everweb_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'upsell - migration -- Everweb to CMP'
        when increased_orchestrate_in_current_date = 1
        and (
          sum(everweb_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'upsell - migration -- Everweb to Orchestrate'
        when increased_monetize_in_current_date = 1
        and (
          sum(everweb_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'upsell - migration -- Everweb to Monetize'
        when increased_cms_in_current_date = 1
        and (
          sum(everweb_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'upsell - migration -- Everweb to CMS'
        when increased_cmp_in_current_date = 1
        and (
          sum(find_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'upsell - migration -- Personalised Find to CMP'
        when increased_orchestrate_in_current_date = 1
        and (
          sum(find_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'upsell - migration -- Personalised Find to Orchestrate'
        when increased_monetize_in_current_date = 1
        and (
          sum(find_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'upsell - migration -- Personalised Find to Monetize'
        when increased_cms_in_current_date = 1
        and (
          sum(find_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'upsell - migration -- Personalised Find to CMS'
        when increased_cms_in_current_date = 1
        and (
          sum(
            ektron_in_previous_date_with_arr
          ) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'upsell - migration -- Ektron to CMS'
        when increased_odp_in_current_date = 1
        and (
          sum(vis_int_in_previous_date_with_arr) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 then 'upsell - migration -- VisitorInt to ODP'
        when churned_licenses_in_current_date = 1
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
        ) then 'Downsell - migration -- Licenses to CMP'
        when churned_licenses_in_current_date = 1
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
        ) then 'Downsell - migration -- Licenses to Orchestrate'
        when churned_licenses_in_current_date = 1
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
        ) then 'Downsell - migration -- Licenses to Monetize'
        when churned_licenses_in_current_date = 1
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
        ) then 'Downsell - migration -- Licenses to CMS'
        when churned_everweb_in_current_date = 1
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
        ) then 'Downsell - migration -- Everweb to CMP'
        when churned_everweb_in_current_date = 1
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
        ) then 'Downsell - migration -- Everweb to Orchestrate'
        when churned_everweb_in_current_date = 1
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
        ) then 'Downsell - migration -- Everweb to Monetize'
        when churned_everweb_in_current_date = 1
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
        ) then 'Downsell - migration -- Everweb to CMS'
        when churned_find_in_current_date = 1
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
        ) then 'Downsell - migration -- Personalised Find to CMP'
        when churned_find_in_current_date = 1
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
        ) then 'Downsell - migration -- Personalised Find to Orchestrate'
        when churned_find_in_current_date = 1
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
        ) then 'Downsell - migration -- Personalised Find to Monetize'
        when churned_find_in_current_date = 1
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
        ) then 'Downsell - migration -- Personalised Find to CMS'
        when churned_ektron_in_current_date = 1
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
        ) then 'Downsell - migration -- Ektron to CMS'
        when churned_vis_int_in_current_date = 1
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
        ) then 'Downsell - migration -- VisitorInt to ODP'
        when downgraded_license_in_current_date = 1
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
        ) then 'downgrade - migration -- Licenses to CMP'
        when downgraded_license_in_current_date = 1
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
        ) then 'downgrade - migration -- Licenses to Orchestrate'
        when downgraded_license_in_current_date = 1
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
        ) then 'downgrade - migration -- Licenses to Monetize'
        when downgraded_license_in_current_date = 1
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
        ) then 'downgrade - migration -- Licenses to CMS'
        when downgraded_everweb_in_current_date = 1
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
        ) then 'downgrade - migration -- Everweb to CMP'
        when downgraded_everweb_in_current_date = 1
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
        ) then 'downgrade - migration -- Everweb to Orchestrate'
        when downgraded_everweb_in_current_date = 1
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
        ) then 'downgrade - migration -- Everweb to Monetize'
        when downgraded_everweb_in_current_date = 1
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
        ) then 'downgrade - migration -- Everweb to CMS'
        when downgraded_find_in_current_date = 1
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
        ) then 'downgrade - migration -- Personalised Find to CMP'
        when downgraded_find_in_current_date = 1
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
        ) then 'downgrade - migration -- Personalised Find to Orchestrate'
        when downgraded_find_in_current_date = 1
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
        ) then 'downgrade - migration -- Personalised Find to Monetize'
        when downgraded_find_in_current_date = 1
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
        ) then 'downgrade - migration -- Personalised Find to CMS'
        when downgraded_ektron_in_current_date = 1
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
        ) then 'downgrade - migration -- Ektron to CMS'
        when downgraded_vis_int_in_current_date = 1
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
        ) then 'downgrade - migration -- VisitorInt to ODP'
      end as "Movement Classification"
    from initial_table_2
  )
  SELECT *
  FROM initial_table_3
);
select distinct "Movement Classification"
from sandbox.churn_migration_classifiers4;