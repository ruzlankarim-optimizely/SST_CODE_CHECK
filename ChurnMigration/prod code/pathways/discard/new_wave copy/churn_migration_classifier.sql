DROP TABLE IF EXISTS sandbox.churn_migration_classifiers;
create table sandbox.churn_migration_classifiers as (
  with initial_table as (
    select *,
      current_pathways as current_product_family_class,
      prior_pathways as prior_product_family_class
    from ryzlan.sst_product_pathways_bridge
  ),
  initial_table_2 as (
    select *,
      --Did a customer downgrade a legacy product family in the current snapshot date
      case
        when current_product_family_class = 'Licenses'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Downgraded a Licenses  Product in Current Date",
      case
        when current_product_family_class IN ('Everweb', 'Ektron')
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Downgraded a Everweb-Ektron  Product in Current Date",
      case
        when current_product_family_class = 'Personalized Find'
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Downgraded a Personalized Find  Product in Current Date",
      case
        when current_product_family_class IN (
          'Visitor Intelligence',
          'Search & Navigation - Standalone'
        )
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Downgraded a Visitor Int  Product in Current Date",
      --Did a customer churn a legacy product family in the current snapshot date
      case
        when prior_product_family_class IN ('Licenses')
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx = 0 then 1
        else 0
      end as "Churned a Licenses Product in Current Date",
      case
        when prior_product_family_class IN ('Everweb', 'Ektron')
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx = 0 then 1
        else 0
      end as "Churned a Everweb-Ektron Product in Current Date",
      case
        when prior_product_family_class IN ('Personalized Find')
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx = 0 then 1
        else 0
      end as "Churned a Personalized Find Product in Current Date",
      case
        when prior_product_family_class IN (
          'Visitor Intelligence',
          'Search & Navigation - Standalone'
        )
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx = 0 then 1
        else 0
      end as "Churned a Visitor Int Product in Current Date",
      --Did the customer add a named product in the current snapshot date
      case
        when current_product_family_class IN ('Cloud')
        and product_arr_change_ccfx > 0
        and prior_period_product_arr_usd_ccfx = 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Added a Cloud Product in Current Date",
      case
        when current_product_family_class IN (
          'Content Managemen System (CMS)',
          'Content Management System (CMS)'
        )
        and product_arr_change_ccfx > 0
        and prior_period_product_arr_usd_ccfx = 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Added a CMS Product in Current Date",
      case
        when current_product_family_class IN ('Content Graph')
        and product_arr_change_ccfx > 0
        and prior_period_product_arr_usd_ccfx = 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Added a Content Graph Product in Current Date",
      case
        when current_product_family_class IN ('Data Platform (ODP)')
        and product_arr_change_ccfx > 0
        and prior_period_product_arr_usd_ccfx = 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Added a ODP Product in Current Date",
      --Did the customer increase a named product in the current snapshot date
      case
        when current_product_family_class IN ('Cloud')
        and product_arr_change_ccfx > 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Increased a Cloud Product in Current Date",
      case
        when current_product_family_class IN (
          'Content Managemen System (CMS)',
          'Content Management System (CMS)'
        )
        and product_arr_change_ccfx > 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Increased a CMS Product in Current Date",
      case
        when current_product_family_class IN ('Content Graph')
        and product_arr_change_ccfx > 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Increased a Content Graph Product in Current Date",
      case
        when current_product_family_class IN ('Data Platform (ODP)')
        and product_arr_change_ccfx > 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Increased a ODP Product in Current Date",
      --Do they have a named product in current snapshot period with ARR > 0
      case
        when current_product_family_class IN ('Cloud')
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Cloud Product in Current Date with ARR",
      case
        when current_product_family_class IN (
          'Content Managemen System (CMS)',
          'Content Management System (CMS)'
        )
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "CMS  Product in Current Date with ARR",
      case
        when current_product_family_class IN ('Content Graph')
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Content Graph Product in Current Date with ARR",
      case
        when current_product_family_class IN ('Data Platform (ODP)')
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "ODP Product in Current Date with ARR",
      --Did they have a legacy product in the prior snapshot period
      case
        when prior_product_family_class IN ('Licenses')
        and prior_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Licenses Product in Previous Date with ARR",
      case
        when prior_product_family_class IN ('Everweb', 'Ektron')
        and prior_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Everweb-Ektron Product in Previous Date with ARR",
      case
        when prior_product_family_class IN ('Personalized Find')
        and prior_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Personalized Find Product in Previous Date with ARR",
      case
        when prior_product_family_class IN (
          'Visitor Intelligence',
          'Search & Navigation - Standalone'
        )
        and prior_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Visitor Int Product in Previous Date with ARR"
    from initial_table
  ),
  initial_table_3 as (
    select *,
      --If a customer churned or downgraded a legacy product family & they had a named product in the current snapshot period >0 ARR we would classify the churn / downgrade as migration
      case
        --ai)
        when "Downgraded a Licenses  Product in Current Date" = 1 --Downgraded a legacy product
        and (
          sum("Cloud Product in Current Date with ARR") over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- have named product in current date with ARR > 0
        then 'downgrade - migration -- Licenses to Cloud' --ai)
        --ai)
        when "Downgraded a Everweb-Ektron  Product in Current Date" = 1 --Downgraded a legacy product
        and (
          sum("CMS  Product in Current Date with ARR") over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- have named product in current date with ARR > 0
        then 'downgrade - migration -- EverwebEktron to CMS' --ai)
        --ai)
        when "Downgraded a Personalized Find  Product in Current Date" = 1 --Downgraded a legacy product
        and (
          sum("Content Graph Product in Current Date with ARR") over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- have named product in current date with ARR > 0
        then 'downgrade - migration -- Personalised Find to Content Graph' --ai)
        --ai)
        when "Downgraded a Visitor Int  Product in Current Date" = 1 --Downgraded a legacy product
        and (
          sum("ODP Product in Current Date with ARR") over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- have named product in current date with ARR > 0
        then 'downgrade - migration -- VisitorInt to ODP' --ai)
        when "Churned a Licenses Product in Current Date" = 1 --Churned a legacy product
        and (
          sum("Cloud Product in Current Date with ARR") over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- have named product in current date with ARR > 0
        then 'churn - migration -- Licenses to Cloud' --bi)
        when "Churned a Everweb-Ektron Product in Current Date" = 1 --Churned a legacy product
        and (
          sum("CMS  Product in Current Date with ARR") over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- have named product in current date with ARR > 0
        then 'churn - migration -- EverwebEktron to CMS' --bi)
        when "Churned a Personalized Find Product in Current Date" = 1 --Churned a legacy product
        and (
          sum("Content Graph Product in Current Date with ARR") over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- have named product in current date with ARR > 0
        then 'churn - migration -- Personalised Find to Content Graph' --bi)
        when "Churned a Visitor Int Product in Current Date" = 1 --Churned a legacy product
        and (
          sum("ODP Product in Current Date with ARR") over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- have named product in current date with ARR > 0
        then 'churn - migration -- VisitorInt to ODP' --bi)
        when "Added a Cloud Product in Current Date" = 1 --Added a named product in the current snapshot date
        and (
          (
            sum("Downgraded a Licenses  Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum("Churned a Licenses Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either churned or downgraded a legacy product in the current snapshot date
        then 'crosssell - migration -- Licenses to Cloud' --bii)
        when "Added a CMS Product in Current Date" = 1 --Added a named product in the current snapshot date
        and (
          (
            sum(
              "Downgraded a Everweb-Ektron  Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              "Churned a Everweb-Ektron Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either churned or downgraded a legacy product in the current snapshot date
        then 'crosssell - migration -- EverwebEktron to CMS' --bii)
        when "Added a Content Graph Product in Current Date" = 1 --Added a named product in the current snapshot date
        and (
          (
            sum(
              "Downgraded a Personalized Find  Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              "Churned a Personalized Find Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either churned or downgraded a legacy product in the current snapshot date
        then 'crosssell - migration -- Personalised Find to Content Graph' --bii)
        when "Added a ODP Product in Current Date" = 1 --Added a named product in the current snapshot date
        and (
          (
            sum(
              "Downgraded a Visitor Int  Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum("Churned a Visitor Int Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either churned or downgraded a legacy product in the current snapshot date
        then 'crosssell - migration -- VisitorInt to ODP' --bii)
        when "Increased a Cloud Product in Current Date" = 1 --Increased a named product in the current snapshot date
        and (
          (
            sum("Downgraded a Licenses  Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum("Churned a Licenses Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either churned or downgraded a legacy product in the current snapshot date
        then 'upsell - migration -- Licenses to Cloud' --If a customer increased or added a named product & they had a legacy product in the prior snapshot period > 0 ARR we would classify the movement as migration
        when "Increased a CMS Product in Current Date" = 1 --Increased a named product in the current snapshot date
        and (
          (
            sum(
              "Downgraded a Everweb-Ektron  Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              "Churned a Everweb-Ektron Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either churned or downgraded a legacy product in the current snapshot date
        then 'upsell - migration -- EverwebEktron to CMS' --If a customer increased or added a named product & they had a legacy product in the prior snapshot period > 0 ARR we would classify the movement as migration
        when "Increased a Content Graph Product in Current Date" = 1 --Increased a named product in the current snapshot date
        and (
          (
            sum(
              "Downgraded a Personalized Find  Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              "Churned a Personalized Find Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either churned or downgraded a legacy product in the current snapshot date
        then 'upsell - migration -- Personalised Find to Content Graph' --If a customer increased or added a named product & they had a legacy product in the prior snapshot period > 0 ARR we would classify the movement as migration
        when "Increased a ODP Product in Current Date" = 1 --Increased a named product in the current snapshot date
        and (
          (
            sum(
              "Downgraded a Visitor Int  Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum("Churned a Visitor Int Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either churned or downgraded a legacy product in the current snapshot date
        then 'upsell - migration -- VisitorInt to ODP' --If a customer increased or added a named product & they had a legacy product in the prior snapshot period > 0 ARR we would classify the movement as migration
        when "Added a Cloud Product in Current Date" = 1 --Added a named product in the current snapshot date
        and (
          sum("Licenses Product in Previous Date with ARR") over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- had a legacy product in previous snapshot date with ARR > 0
        then 'crosssell - migration -- Licenses to Cloud' --cii)
        when "Added a CMS Product in Current Date" = 1 --Added a named product in the current snapshot date
        and (
          sum(
            "Everweb-Ektron Product in Previous Date with ARR"
          ) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- had a legacy product in previous snapshot date with ARR > 0
        then 'crosssell - migration -- EverwebEktron to CMS' --cii)
        when "Added a Content Graph Product in Current Date" = 1 --Added a named product in the current snapshot date
        and (
          sum(
            "Personalized Find Product in Previous Date with ARR"
          ) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- had a legacy product in previous snapshot date with ARR > 0
        then 'crosssell - migration -- Personalised Find to Content Graph' --cii)
        when "Added a ODP Product in Current Date" = 1 --Added a named product in the current snapshot date
        and (
          sum("Visitor Int Product in Previous Date with ARR") over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- had a legacy product in previous snapshot date with ARR > 0
        then 'crosssell - migration -- VisitorInt to ODP' --cii)
        when "Increased a Cloud Product in Current Date" = 1 --Added a named product in the current snapshot date
        and (
          sum("Licenses Product in Previous Date with ARR") over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- had a legacy product in previous snapshot date with ARR > 0
        then 'upsell - migration -- Licenses to Cloud' --di)
        when "Increased a CMS Product in Current Date" = 1 --Added a named product in the current snapshot date
        and (
          sum(
            "Everweb-Ektron Product in Previous Date with ARR"
          ) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- had a legacy product in previous snapshot date with ARR > 0
        then 'upsell - migration -- EverwebEktron to CMS' --di)
        when "Increased a Content Graph Product in Current Date" = 1 --Added a named product in the current snapshot date
        and (
          sum(
            "Personalized Find Product in Previous Date with ARR"
          ) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- had a legacy product in previous snapshot date with ARR > 0
        then 'upsell - migration -- Personalised Find to Content Graph' --di)
        when "Increased a ODP Product in Current Date" = 1 --Added a named product in the current snapshot date
        and (
          sum("Visitor Int Product in Previous Date with ARR") over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- had a legacy product in previous snapshot date with ARR > 0
        then 'upsell - migration -- VisitorInt to ODP' --di)
        when "Churned a Licenses Product in Current Date" = 1 --Churned a legacy product from prior snapshot date
        and (
          (
            sum("Added a Cloud Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum("Increased a Cloud Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either Added or Increased a named product in the current snapshot date
        then 'churn - migration -- Licenses to Cloud' --dii)
        when "Churned a Everweb-Ektron Product in Current Date" = 1 --Churned a legacy product from prior snapshot date
        and (
          (
            sum("Added a CMS Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum("Increased a CMS Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either Added or Increased a named product in the current snapshot date
        then 'churn - migration -- EverwebEktron to CMS' --dii)
        when "Churned a Personalized Find Product in Current Date" = 1 --Churned a legacy product from prior snapshot date
        and (
          (
            sum("Added a Content Graph Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              "Increased a Content Graph Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either Added or Increased a named product in the current snapshot date
        then 'churn - migration -- Personalised Find to Content Graph' --dii)
        when "Churned a Visitor Int Product in Current Date" = 1 --Churned a legacy product from prior snapshot date
        and (
          (
            sum("Added a ODP Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum("Increased a ODP Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either Added or Increased a named product in the current snapshot date
        then 'churn - migration -- VisitorInt to ODP' --dii)
        when "Downgraded a Licenses  Product in Current Date" = 1 --Downgraded a legacy product from prior snapshot date
        and (
          (
            sum("Added a Cloud Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum("Increased a Cloud Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either Added or Increased a named product in the current snapshot date
        then 'downgrade - migration -- Licenses to Cloud'
        when "Downgraded a Everweb-Ektron  Product in Current Date" = 1 --Downgraded a legacy product from prior snapshot date
        and (
          (
            sum("Added a CMS Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum("Increased a CMS Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either Added or Increased a named product in the current snapshot date
        then 'downgrade - migration -- EverwebEktron to CMS'
        when "Downgraded a Personalized Find  Product in Current Date" = 1 --Downgraded a legacy product from prior snapshot date
        and (
          (
            sum("Added a Content Graph Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              "Increased a Content Graph Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either Added or Increased a named product in the current snapshot date
        then 'downgrade - migration -- Personalised Find to Content Graph'
        when "Downgraded a Visitor Int  Product in Current Date" = 1 --Downgraded a legacy product from prior snapshot date
        and (
          (
            sum("Added a ODP Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum("Increased a ODP Product in Current Date") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either Added or Increased a named product in the current snapshot date
        then 'downgrade - migration -- VisitorInt to ODP'
      end as "Movement Classification"
    from initial_table_2
  )
  select it3.evaluation_period,
    it3.prior_period,
    it3.current_period,
    it3.current_end_customer,
    it3.prior_end_customer,
    it3.mcid,
    it3.current_master_customer_id,
    it3.prior_master_customer_id,
    it3.current_product_family,
    it3.prior_product_family,
    it3.currency_code,
    it3.prior_period_product_arr_usd_ccfx,
    it3.current_period_product_arr_usd_ccfx,
    it3.product_arr_change_ccfx,
    it3.prior_period_product_arr_lcu,
    it3.current_period_product_arr_lcu,
    it3.product_arr_change_lcu,
    it3.product_bridge,
    it3.prior_product_group,
    it3.current_product_group,
    it3.current_product_family_class,
    it3.prior_product_family_class,
    it3."Downgraded a Licenses  Product in Current Date",
    it3."Downgraded a Everweb-Ektron  Product in Current Date",
    it3."Downgraded a Personalized Find  Product in Current Date",
    it3."Downgraded a Visitor Int  Product in Current Date",
    it3."Churned a Licenses Product in Current Date",
    it3."Churned a Everweb-Ektron Product in Current Date",
    it3."Churned a Personalized Find Product in Current Date",
    it3."Churned a Visitor Int Product in Current Date",
    it3."Added a Cloud Product in Current Date",
    it3."Added a CMS Product in Current Date",
    it3."Added a Content Graph Product in Current Date",
    it3."Added a ODP Product in Current Date",
    it3."Increased a Cloud Product in Current Date",
    it3."Increased a CMS Product in Current Date",
    it3."Increased a Content Graph Product in Current Date",
    it3."Increased a ODP Product in Current Date",
    it3."Cloud Product in Current Date with ARR",
    it3."CMS  Product in Current Date with ARR",
    it3."Content Graph Product in Current Date with ARR",
    it3."ODP Product in Current Date with ARR",
    it3."Licenses Product in Previous Date with ARR",
    it3."Everweb-Ektron Product in Previous Date with ARR",
    it3."Personalized Find Product in Previous Date with ARR",
    it3."Visitor Int Product in Previous Date with ARR",
    it3."Movement Classification"
  from initial_table_3 it3
);