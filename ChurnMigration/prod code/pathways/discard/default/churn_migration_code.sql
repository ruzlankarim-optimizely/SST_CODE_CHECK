DROP TABLE IF EXISTS product_family_product_group_bridge;
CREATE TEMP TABLE product_family_product_group_bridge AS
select *
from ryzlan.sst_product_bridge_product_family_pathways_CM;
DROP TABLE IF EXISTS product_group_product_solution_bridge;
CREATE TEMP TABLE product_group_product_solution_bridge AS
select *
from ryzlan.sst_product_bridge_product_group_cm
WHERE product_bridge IN (
    'Churn',
    'Cross-sell',
    'Downgrade',
    'Downsell',
    'New',
    'Up Sell',
    'Winback LT'
  );
DROP TABLE IF EXISTS product_solution_bridge;
CREATE TEMP TABLE product_solution_bridge AS
select *
from ufdm.sst_product_bridge_product_solution
WHERE product_bridge IN (
    'Churn',
    'Cross-sell',
    'Downgrade',
    'Downsell',
    'New',
    'Up Sell'
  );
DROP TABLE IF EXISTS customer_bridge;
CREATE TEMP TABLE customer_bridge AS
select *
from ufdm.sst_customer_bridge
where customer_bridge IN ('Churn', 'Downgrade', 'New', 'Up Sell');
DROP TABLE IF EXISTS sandbox.churn_migration_main_pathways;
create table sandbox.churn_migration_main_pathways as (
  with initial_table as (
    select *,
      --            case
      --                when current_tag = 'Legacy' then 'Legacy'
      --                when current_tag = 'Named' then 'Named'
      --            end 
      current_pathways as current_product_family_class,
      --            case
      --                when prior_tag = 'Legacy' then 'Legacy'
      --                when prior_tag = 'Named' then 'Named'
      --            end 
      prior_pathways as prior_product_family_class --    case
      --      when current_product_family in ('Non-Recurring: Perpetual License','Recurring: Cloud: Other Bookings: Other Bookings') then 'Legacy'
      --      when current_product_family in ('Recurring: Cloud: Commerce Cloud: B2C Commerce (incl. Headless)','Recurring: Cloud: Commerce Cloud: B2B Commerce (incl. Headless)','Recurring: Cloud: Content Cloud: Content PaaS') then 'Named'
      --  end as current_product_family_class,
      --  case
      --      when prior_product_family in ('Non-Recurring: Perpetual License','Recurring: Cloud: Other Bookings: Other Bookings') then 'Legacy'
      --      when prior_product_family in ('Recurring: Cloud: Commerce Cloud: B2C Commerce (incl. Headless)','Recurring: Cloud: Commerce Cloud: B2B Commerce (incl. Headless)','Recurring: Cloud: Content Cloud: Content PaaS') then 'Named'
      --  end as prior_product_family_class
    from product_family_product_group_bridge --ryzlan.sst_pb_temp --Product family Bridge ryzlan.sst_product_bridge_product_family_cloud_cm
  ),
  -- legacy == ('Everweb', 'Ektron', 'Personalized Find', 'Visitor Intelligence','Search & Navigation - Standalone' , 'Licenses' )
  -- named  == ('Content Managemen System (CMS)', 'Content Management System (CMS)', 'Content Graph', 'Data Platform (ODP)' , 'Cloud') 
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
  ),
  initial_table_4 as (
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
      it3.winback_period_days,
      it3.wip_flag,
      it3.price_increase_amount,
      it3.subsidiary_entity_name,
      it3.churn_period,
      it3.customer_bridge,
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
      it3."Movement Classification",
      case
        when it3."Movement Classification" is not null
        and it3.product_arr_change_ccfx > 0 then '+'
        when it3."Movement Classification" is not null
        and it3.product_arr_change_ccfx < 0 then '-'
        else null
      end as "Movement Type-PF",
      --type of migration movement
      --PG Information
      rt.product_arr_change_ccfx as pg_arr_change,
      rt.product_arr_change_lcu as pg_arr_change_lcu,
      rt.product_bridge as pg_bridge,
      --Bring in the product solution columns as well -- to roll it up on the PS level
      rt.current_product_solution,
      rt.prior_product_solution
    from initial_table_3 it3
      left join product_group_product_solution_bridge rt --ryzlan.sst_pb_pg_temp rt --Product group Bridge ryzlan.sst_product_bridge_product_group_cloud_cm
      on it3.evaluation_period = rt.evaluation_period
      and coalesce(it3.prior_product_group,it3.current_product_group) = 
      coalesce(rt.current_product_group, rt.prior_product_group)
      and it3.mcid = rt.mcid
      and it3.currency_code = rt.currency_code
  ),
  initial_table_5 as (
    select evaluation_period,
      prior_period,
      current_period,
      current_end_customer,
      prior_end_customer,
      mcid,
      current_master_customer_id,
      prior_master_customer_id,
      current_product_family,
      prior_product_family,
      currency_code,
      prior_period_product_arr_usd_ccfx,
      current_period_product_arr_usd_ccfx,
      product_arr_change_ccfx,
      prior_period_product_arr_lcu,
      current_period_product_arr_lcu,
      product_arr_change_lcu,
      product_bridge,
      winback_period_days,
      wip_flag,
      price_increase_amount,
      subsidiary_entity_name,
      churn_period,
      customer_bridge,
      prior_product_group,
      current_product_group,
      current_product_family_class,
      prior_product_family_class,
      "Downgraded a Licenses  Product in Current Date",
      "Downgraded a Everweb-Ektron  Product in Current Date",
      "Downgraded a Personalized Find  Product in Current Date",
      "Downgraded a Visitor Int  Product in Current Date",
      "Churned a Licenses Product in Current Date",
      "Churned a Everweb-Ektron Product in Current Date",
      "Churned a Personalized Find Product in Current Date",
      "Churned a Visitor Int Product in Current Date",
      "Added a Cloud Product in Current Date",
      "Added a CMS Product in Current Date",
      "Added a Content Graph Product in Current Date",
      "Added a ODP Product in Current Date",
      "Increased a Cloud Product in Current Date",
      "Increased a CMS Product in Current Date",
      "Increased a Content Graph Product in Current Date",
      "Increased a ODP Product in Current Date",
      "Cloud Product in Current Date with ARR",
      "CMS  Product in Current Date with ARR",
      "Content Graph Product in Current Date with ARR",
      "ODP Product in Current Date with ARR",
      "Licenses Product in Previous Date with ARR",
      "Everweb-Ektron Product in Previous Date with ARR",
      "Personalized Find Product in Previous Date with ARR",
      "Visitor Int Product in Previous Date with ARR",
      "Movement Classification",
      "Movement Type-PF",
      pg_arr_change,
      pg_arr_change_lcu,
      pg_bridge,
      current_product_solution,
      prior_product_solution,
      --
      case
        when pg_arr_change > 0 then '+'
        when pg_arr_change < 0 then '-'
        else null
      end as "Movement Type-PG"
    from initial_table_4
  ),
  initial_table_6 as (
    select evaluation_period,
      prior_period,
      current_period,
      current_end_customer,
      prior_end_customer,
      mcid,
      current_master_customer_id,
      prior_master_customer_id,
      current_product_family,
      prior_product_family,
      currency_code,
      prior_period_product_arr_usd_ccfx,
      current_period_product_arr_usd_ccfx,
      product_arr_change_ccfx,
      prior_period_product_arr_lcu,
      current_period_product_arr_lcu,
      product_arr_change_lcu,
      product_bridge,
      winback_period_days,
      wip_flag,
      price_increase_amount,
      subsidiary_entity_name,
      churn_period,
      customer_bridge,
      prior_product_group,
      current_product_group,
      current_product_family_class,
      prior_product_family_class,
      "Downgraded a Licenses  Product in Current Date",
      "Downgraded a Everweb-Ektron  Product in Current Date",
      "Downgraded a Personalized Find  Product in Current Date",
      "Downgraded a Visitor Int  Product in Current Date",
      "Churned a Licenses Product in Current Date",
      "Churned a Everweb-Ektron Product in Current Date",
      "Churned a Personalized Find Product in Current Date",
      "Churned a Visitor Int Product in Current Date",
      "Added a Cloud Product in Current Date",
      "Added a CMS Product in Current Date",
      "Added a Content Graph Product in Current Date",
      "Added a ODP Product in Current Date",
      "Increased a Cloud Product in Current Date",
      "Increased a CMS Product in Current Date",
      "Increased a Content Graph Product in Current Date",
      "Increased a ODP Product in Current Date",
      "Cloud Product in Current Date with ARR",
      "CMS  Product in Current Date with ARR",
      "Content Graph Product in Current Date with ARR",
      "ODP Product in Current Date with ARR",
      "Licenses Product in Previous Date with ARR",
      "Everweb-Ektron Product in Previous Date with ARR",
      "Personalized Find Product in Previous Date with ARR",
      "Visitor Int Product in Previous Date with ARR",
      "Movement Classification",
      "Movement Type-PF",
      "Movement Type-PG",
      --What is the PG movement? + or is it -
      --if neg, look back at pf movements and sum all - migration movements
      case
        when "Movement Type-PG" = '-'
        and "Movement Type-PF" = '-' then sum(product_arr_change_ccfx) filter(
          where "Movement Type-PF" = '-'
        ) over(
          partition by mcid,
          evaluation_period,
          currency_code,
          coalesce(current_product_group, prior_product_group)
        )
        when "Movement Type-PG" = '+'
        and "Movement Type-PF" = '+' then sum(product_arr_change_ccfx) filter(
          where "Movement Type-PF" = '+'
        ) over(
          partition by mcid,
          evaluation_period,
          currency_code,
          coalesce(current_product_group, prior_product_group)
        )
        else null
      end as "Sum of Positive or Negative Movements-PG",
      pg_arr_change,
      pg_arr_change_lcu,
      pg_bridge,
      current_product_solution,
      prior_product_solution
    from initial_table_5
  ),
  initial_table_7 as (
    select evaluation_period,
      prior_period,
      current_period,
      current_end_customer,
      prior_end_customer,
      mcid,
      current_master_customer_id,
      prior_master_customer_id,
      current_product_family,
      prior_product_family,
      currency_code,
      prior_period_product_arr_usd_ccfx,
      current_period_product_arr_usd_ccfx,
      product_arr_change_ccfx,
      prior_period_product_arr_lcu,
      current_period_product_arr_lcu,
      product_arr_change_lcu,
      product_bridge,
      winback_period_days,
      wip_flag,
      price_increase_amount,
      subsidiary_entity_name,
      churn_period,
      customer_bridge,
      prior_product_group,
      current_product_group,
      current_product_family_class,
      prior_product_family_class,
      "Downgraded a Licenses  Product in Current Date",
      "Downgraded a Everweb-Ektron  Product in Current Date",
      "Downgraded a Personalized Find  Product in Current Date",
      "Downgraded a Visitor Int  Product in Current Date",
      "Churned a Licenses Product in Current Date",
      "Churned a Everweb-Ektron Product in Current Date",
      "Churned a Personalized Find Product in Current Date",
      "Churned a Visitor Int Product in Current Date",
      "Added a Cloud Product in Current Date",
      "Added a CMS Product in Current Date",
      "Added a Content Graph Product in Current Date",
      "Added a ODP Product in Current Date",
      "Increased a Cloud Product in Current Date",
      "Increased a CMS Product in Current Date",
      "Increased a Content Graph Product in Current Date",
      "Increased a ODP Product in Current Date",
      "Cloud Product in Current Date with ARR",
      "CMS  Product in Current Date with ARR",
      "Content Graph Product in Current Date with ARR",
      "ODP Product in Current Date with ARR",
      "Licenses Product in Previous Date with ARR",
      "Everweb-Ektron Product in Previous Date with ARR",
      "Personalized Find Product in Previous Date with ARR",
      "Visitor Int Product in Previous Date with ARR",
      "Movement Classification",
      "Movement Type-PF",
      "Movement Type-PG",
      "Sum of Positive or Negative Movements-PG",
      pg_arr_change,
      pg_arr_change_lcu,
      pg_bridge,
      --if neg take the max (if pos take min) between the two PG movement and sum and tag the PG bridge movement as migration
      case
        when "Sum of Positive or Negative Movements-PG" is not null
        and "Movement Type-PG" = '-' then greatest(
          pg_arr_change,
          "Sum of Positive or Negative Movements-PG"
        )
        when "Sum of Positive or Negative Movements-PG" is not null
        and "Movement Type-PG" = '+' then least(
          pg_arr_change,
          "Sum of Positive or Negative Movements-PG"
        )
        else null
      end as "Min/Max PF Level movement",
      --Bring in the product solution columns as well -- to roll it up on the PS level
      current_product_solution,
      prior_product_solution
    from initial_table_6
  ),
  initial_table_8 as (
    select evaluation_period,
      prior_period,
      current_period,
      current_end_customer,
      prior_end_customer,
      mcid,
      current_master_customer_id,
      prior_master_customer_id,
      current_product_family,
      prior_product_family,
      currency_code,
      prior_period_product_arr_usd_ccfx,
      current_period_product_arr_usd_ccfx,
      product_arr_change_ccfx,
      prior_period_product_arr_lcu,
      current_period_product_arr_lcu,
      product_arr_change_lcu,
      product_bridge,
      winback_period_days,
      wip_flag,
      price_increase_amount,
      subsidiary_entity_name,
      churn_period,
      customer_bridge,
      prior_product_group,
      current_product_group,
      current_product_family_class,
      prior_product_family_class,
      "Downgraded a Licenses  Product in Current Date",
      "Downgraded a Everweb-Ektron  Product in Current Date",
      "Downgraded a Personalized Find  Product in Current Date",
      "Downgraded a Visitor Int  Product in Current Date",
      "Churned a Licenses Product in Current Date",
      "Churned a Everweb-Ektron Product in Current Date",
      "Churned a Personalized Find Product in Current Date",
      "Churned a Visitor Int Product in Current Date",
      "Added a Cloud Product in Current Date",
      "Added a CMS Product in Current Date",
      "Added a Content Graph Product in Current Date",
      "Added a ODP Product in Current Date",
      "Increased a Cloud Product in Current Date",
      "Increased a CMS Product in Current Date",
      "Increased a Content Graph Product in Current Date",
      "Increased a ODP Product in Current Date",
      "Cloud Product in Current Date with ARR",
      "CMS  Product in Current Date with ARR",
      "Content Graph Product in Current Date with ARR",
      "ODP Product in Current Date with ARR",
      "Licenses Product in Previous Date with ARR",
      "Everweb-Ektron Product in Previous Date with ARR",
      "Personalized Find Product in Previous Date with ARR",
      "Visitor Int Product in Previous Date with ARR",
      "Movement Classification",
      "Movement Type-PF",
      "Movement Type-PG",
      "Sum of Positive or Negative Movements-PG",
      pg_arr_change,
      pg_arr_change_lcu,
      pg_bridge,
      --if neg take the max (if pos take min) between the two PG movement and sum and tag the PG bridge movement as migration
      "Min/Max PF Level movement",
      case
        --Positive
        when "Movement Type-PG" = '+'
        and "Movement Type-PF" is not null
        and "Min/Max PF Level movement" >= pg_arr_change then "Min/Max PF Level movement"
        when "Movement Type-PG" = '+'
        and "Movement Type-PF" is not null
        and "Min/Max PF Level movement" < pg_arr_change then "Min/Max PF Level movement"
        when "Movement Type-PG" = '-'
        and "Movement Type-PF" is not null
        and "Min/Max PF Level movement" <= pg_arr_change then "Min/Max PF Level movement"
        when "Movement Type-PG" = '-'
        and "Movement Type-PF" is not null
        and "Min/Max PF Level movement" > pg_arr_change then "Min/Max PF Level movement"
      end as "PG Migration: Rolled Up Amount",
      case
        when "Movement Type-PG" = '+'
        and "Movement Type-PF" is not null
        and "Min/Max PF Level movement" < pg_arr_change then pg_arr_change - "Min/Max PF Level movement"
        when "Movement Type-PG" = '-'
        and "Movement Type-PF" is not null
        and "Min/Max PF Level movement" > pg_arr_change then pg_arr_change - "Min/Max PF Level movement"
        else null
      end as "PG Leftover: Rolled Up Amount",
      --Bring in the product solution columns as well -- to roll it up on the PS level
      current_product_solution,
      prior_product_solution
    from initial_table_7
  ),
  initial_table_9 as (
    select evaluation_period,
      prior_period,
      current_period,
      current_end_customer,
      prior_end_customer,
      mcid,
      current_master_customer_id,
      prior_master_customer_id,
      current_product_family,
      prior_product_family,
      currency_code,
      prior_period_product_arr_usd_ccfx,
      current_period_product_arr_usd_ccfx,
      product_arr_change_ccfx,
      prior_period_product_arr_lcu,
      current_period_product_arr_lcu,
      product_arr_change_lcu,
      product_bridge,
      winback_period_days,
      wip_flag,
      price_increase_amount,
      subsidiary_entity_name,
      churn_period,
      customer_bridge,
      prior_product_group,
      current_product_group,
      current_product_family_class,
      prior_product_family_class,
      "Downgraded a Licenses  Product in Current Date",
      "Downgraded a Everweb-Ektron  Product in Current Date",
      "Downgraded a Personalized Find  Product in Current Date",
      "Downgraded a Visitor Int  Product in Current Date",
      "Churned a Licenses Product in Current Date",
      "Churned a Everweb-Ektron Product in Current Date",
      "Churned a Personalized Find Product in Current Date",
      "Churned a Visitor Int Product in Current Date",
      "Added a Cloud Product in Current Date",
      "Added a CMS Product in Current Date",
      "Added a Content Graph Product in Current Date",
      "Added a ODP Product in Current Date",
      "Increased a Cloud Product in Current Date",
      "Increased a CMS Product in Current Date",
      "Increased a Content Graph Product in Current Date",
      "Increased a ODP Product in Current Date",
      "Cloud Product in Current Date with ARR",
      "CMS  Product in Current Date with ARR",
      "Content Graph Product in Current Date with ARR",
      "ODP Product in Current Date with ARR",
      "Licenses Product in Previous Date with ARR",
      "Everweb-Ektron Product in Previous Date with ARR",
      "Personalized Find Product in Previous Date with ARR",
      "Visitor Int Product in Previous Date with ARR",
      "Movement Classification",
      "Movement Type-PF",
      "Movement Type-PG",
      "Sum of Positive or Negative Movements-PG",
      pg_arr_change,
      pg_arr_change_lcu,
      pg_bridge,
      --if neg take the max (if pos take min) between the two PG movement and sum and tag the PG bridge movement as migration
      "Min/Max PF Level movement",
      --Positive
      "PG Migration: Rolled Up Amount",
      "PG Leftover: Rolled Up Amount",
      (
        pg_arr_change_lcu *(
          "PG Migration: Rolled Up Amount" /case
            when pg_arr_change = 0
            or pg_arr_change is null then 1
            else pg_arr_change
          end
        )
      ) as "PG Migration: Rolled Up Amount LCU",
      (
        pg_arr_change_lcu * (
          "PG Leftover: Rolled Up Amount" / case
            when pg_arr_change = 0
            or pg_arr_change is null then 1
            else pg_arr_change
          end
        )
      ) as "PG Leftover: Rolled Up Amount LCU",
      case
        when "PG Migration: Rolled Up Amount" is not null then "Movement Classification"
        else null
      end as "PG Migration: Classification",
      case
        when "PG Leftover: Rolled Up Amount" is not null then pg_bridge
        else null
      end as "PG Leftover: Classification",
      --Bring in the product solution columns as well -- to roll it up on the PS level
      current_product_solution,
      prior_product_solution
    from initial_table_8
  ) --Now join to product solution bridge
,
  initial_table_10 as (
    select cmp.evaluation_period,
      cmp.prior_period,
      cmp.current_period,
      cmp.current_end_customer,
      cmp.prior_end_customer,
      cmp.mcid,
      cmp.current_master_customer_id,
      cmp.prior_master_customer_id,
      cmp.current_product_family,
      cmp.prior_product_family,
      cmp.currency_code,
      cmp.prior_period_product_arr_usd_ccfx,
      cmp.current_period_product_arr_usd_ccfx,
      cmp.product_arr_change_ccfx,
      cmp.prior_period_product_arr_lcu,
      cmp.current_period_product_arr_lcu,
      cmp.product_arr_change_lcu,
      cmp.product_bridge,
      cmp.winback_period_days,
      cmp.wip_flag,
      cmp.price_increase_amount,
      cmp.subsidiary_entity_name,
      cmp.churn_period,
      cmp.customer_bridge,
      cmp.prior_product_group,
      cmp.current_product_group,
      cmp.current_product_family_class,
      cmp.prior_product_family_class,
      cmp."Downgraded a Licenses  Product in Current Date",
      cmp."Downgraded a Everweb-Ektron  Product in Current Date",
      cmp."Downgraded a Personalized Find  Product in Current Date",
      cmp."Downgraded a Visitor Int  Product in Current Date",
      cmp."Churned a Licenses Product in Current Date",
      cmp."Churned a Everweb-Ektron Product in Current Date",
      cmp."Churned a Personalized Find Product in Current Date",
      cmp."Churned a Visitor Int Product in Current Date",
      cmp."Added a Cloud Product in Current Date",
      cmp."Added a CMS Product in Current Date",
      cmp."Added a Content Graph Product in Current Date",
      cmp."Added a ODP Product in Current Date",
      cmp."Increased a Cloud Product in Current Date",
      cmp."Increased a CMS Product in Current Date",
      cmp."Increased a Content Graph Product in Current Date",
      cmp."Increased a ODP Product in Current Date",
      cmp."Cloud Product in Current Date with ARR",
      cmp."CMS  Product in Current Date with ARR",
      cmp."Content Graph Product in Current Date with ARR",
      cmp."ODP Product in Current Date with ARR",
      cmp."Licenses Product in Previous Date with ARR",
      cmp."Everweb-Ektron Product in Previous Date with ARR",
      cmp."Personalized Find Product in Previous Date with ARR",
      cmp."Visitor Int Product in Previous Date with ARR",
      cmp."Movement Classification",
      cmp."Movement Type-PF",
      cmp."Movement Type-PG",
      cmp."Sum of Positive or Negative Movements-PG",
      cmp.pg_arr_change,
      cmp.pg_arr_change_lcu,
      cmp.pg_bridge,
      cmp."Min/Max PF Level movement",
      cmp."PG Migration: Rolled Up Amount",
      cmp."PG Migration: Rolled Up Amount LCU",
      cmp."PG Migration: Classification",
      cmp."PG Leftover: Rolled Up Amount",
      cmp."PG Leftover: Rolled Up Amount LCU",
      cmp."PG Leftover: Classification",
      cmp.current_product_solution,
      cmp.prior_product_solution,
      rst.product_arr_change_ccfx as product_arr_change_ccfx_ps,
      rst.product_arr_change_lcu as product_arr_change_lcu_ps,
      rst.product_bridge as ps_bridge,
      --Product Solution Bridge Labelling
      --Label PG Migration: Only Migration
      case
        when "PG Migration: Classification" in ('downgrade - migration%', 'churn - migration%') then '-'
        when "PG Migration: Classification" in ('crosssell - migration%', 'upsell - migration%') then '+'
        else null
      end as "PG Migration Movement Final Classification",
      --Classify the PS Level Movements
      case
        when rst.product_arr_change_ccfx > 0 then '+'
        when rst.product_arr_change_ccfx < 0 then '-'
        else null
      end as "Movement Type-PS"
    from initial_table_9 cmp
      left join product_solution_bridge rst --ryzlan.sst_ps_temp rst --Product Solution Bridge
      on cmp.evaluation_period = rst.evaluation_period
      and cmp.mcid = rst.mcid
      and cmp.currency_code = rst.currency_code
      and coalesce(
        cmp.current_product_solution,
        cmp.prior_product_solution
      ) = coalesce(
        rst.current_product_solution,
        rst.prior_product_solution
      )
  ) --Take distinct PG Migration Movements for distinct product groups or else it duplicates
  --Good examples:    mcid = '035c17f2-7b31-e411-9f63-0050568d2da8'and evaluation_period = '2022M01' --Really good example of why we need to take the distinct
,
  distinct_table_1 as (
    select distinct mcid,
      evaluation_period,
      currency_code,
      current_product_group,
      prior_product_group,
      current_product_solution,
      prior_product_solution,
      "Movement Type-PS",
      "PG Migration Movement Final Classification",
      "PG Migration: Rolled Up Amount"
    from initial_table_10
  ),
  distinct_table_2 as (
    select mcid,
      evaluation_period,
      currency_code,
      current_product_solution,
      prior_product_solution,
      "PG Migration Movement Final Classification",
      "PG Migration: Rolled Up Amount",
      case
        when "Movement Type-PS" = '-'
        and "PG Migration Movement Final Classification" = '-' then sum("PG Migration: Rolled Up Amount") filter(
          where "PG Migration Movement Final Classification" = '-'
        ) over(
          partition by mcid,
          evaluation_period,
          currency_code,
          coalesce(current_product_solution, prior_product_solution)
        )
        when "Movement Type-PS" = '+'
        and "PG Migration Movement Final Classification" = '+' then sum("PG Migration: Rolled Up Amount") filter(
          where "PG Migration Movement Final Classification" = '+'
        ) over(
          partition by mcid,
          evaluation_period,
          currency_code,
          coalesce(current_product_solution, prior_product_solution)
        )
        else null
      end as "Sum of Positive or Negative Movements-PS"
    from distinct_table_1
  ),
  initial_table_11 as (
    select it10.evaluation_period,
      it10.prior_period,
      it10.current_period,
      it10.current_end_customer,
      it10.prior_end_customer,
      it10.mcid,
      it10.current_master_customer_id,
      it10.prior_master_customer_id,
      it10.current_product_family,
      it10.prior_product_family,
      it10.currency_code,
      it10.prior_period_product_arr_usd_ccfx,
      it10.current_period_product_arr_usd_ccfx,
      it10.product_arr_change_ccfx,
      it10.prior_period_product_arr_lcu,
      it10.current_period_product_arr_lcu,
      it10.product_arr_change_lcu,
      it10.product_bridge,
      it10.winback_period_days,
      it10.wip_flag,
      it10.price_increase_amount,
      it10.subsidiary_entity_name,
      it10.churn_period,
      it10.customer_bridge,
      it10.prior_product_group,
      it10.current_product_group,
      it10.current_product_family_class,
      it10.prior_product_family_class,
      it10."Downgraded a Licenses  Product in Current Date",
      it10."Downgraded a Everweb-Ektron  Product in Current Date",
      it10."Downgraded a Personalized Find  Product in Current Date",
      it10."Downgraded a Visitor Int  Product in Current Date",
      it10."Churned a Licenses Product in Current Date",
      it10."Churned a Everweb-Ektron Product in Current Date",
      it10."Churned a Personalized Find Product in Current Date",
      it10."Churned a Visitor Int Product in Current Date",
      it10."Added a Cloud Product in Current Date",
      it10."Added a CMS Product in Current Date",
      it10."Added a Content Graph Product in Current Date",
      it10."Added a ODP Product in Current Date",
      it10."Increased a Cloud Product in Current Date",
      it10."Increased a CMS Product in Current Date",
      it10."Increased a Content Graph Product in Current Date",
      it10."Increased a ODP Product in Current Date",
      it10."Cloud Product in Current Date with ARR",
      it10."CMS  Product in Current Date with ARR",
      it10."Content Graph Product in Current Date with ARR",
      it10."ODP Product in Current Date with ARR",
      it10."Licenses Product in Previous Date with ARR",
      it10."Everweb-Ektron Product in Previous Date with ARR",
      it10."Personalized Find Product in Previous Date with ARR",
      it10."Visitor Int Product in Previous Date with ARR",
      it10."Movement Classification",
      it10."Movement Type-PF",
      it10."Movement Type-PG",
      it10."Sum of Positive or Negative Movements-PG",
      it10.pg_arr_change,
      it10.pg_arr_change_lcu,
      it10.pg_bridge,
      it10."Min/Max PF Level movement",
      it10."PG Migration: Rolled Up Amount",
      it10."PG Migration: Rolled Up Amount LCU",
      it10."PG Migration: Classification",
      it10."PG Leftover: Rolled Up Amount",
      it10."PG Leftover: Rolled Up Amount LCU",
      it10."PG Leftover: Classification",
      it10."PG Migration Movement Final Classification",
      it10.current_product_solution,
      it10.prior_product_solution,
      it10.product_arr_change_ccfx_ps,
      it10.product_arr_change_lcu_ps,
      it10.ps_bridge,
      it10."Movement Type-PS",
      dt."Sum of Positive or Negative Movements-PS"
    from initial_table_10 it10
      left join distinct_table_2 dt on it10.mcid = dt.mcid
      and it10.evaluation_period = dt.evaluation_period
      and it10.currency_code = dt.currency_code
      and coalesce(
        it10.current_product_solution,
        it10.prior_product_solution
      ) = coalesce(
        dt.current_product_solution,
        dt.prior_product_solution
      )
      and it10."PG Migration Movement Final Classification" = dt."PG Migration Movement Final Classification"
      and it10."PG Migration: Rolled Up Amount" = dt."PG Migration: Rolled Up Amount"
  ) --select
  --  *
  --from
  --  initial_table_11
  --where
  --  mcid = '035c17f2-7b31-e411-9f63-0050568d2da8'
  --and
  --  evaluation_period = '2022M01'
,
  initial_table_12 as (
    select evaluation_period,
      prior_period,
      current_period,
      current_end_customer,
      prior_end_customer,
      mcid,
      current_master_customer_id,
      prior_master_customer_id,
      current_product_family,
      prior_product_family,
      currency_code,
      prior_period_product_arr_usd_ccfx,
      current_period_product_arr_usd_ccfx,
      product_arr_change_ccfx,
      prior_period_product_arr_lcu,
      current_period_product_arr_lcu,
      product_arr_change_lcu,
      product_bridge,
      winback_period_days,
      wip_flag,
      price_increase_amount,
      subsidiary_entity_name,
      churn_period,
      customer_bridge,
      prior_product_group,
      current_product_group,
      current_product_family_class,
      prior_product_family_class,
      "Downgraded a Licenses  Product in Current Date",
      "Downgraded a Everweb-Ektron  Product in Current Date",
      "Downgraded a Personalized Find  Product in Current Date",
      "Downgraded a Visitor Int  Product in Current Date",
      "Churned a Licenses Product in Current Date",
      "Churned a Everweb-Ektron Product in Current Date",
      "Churned a Personalized Find Product in Current Date",
      "Churned a Visitor Int Product in Current Date",
      "Added a Cloud Product in Current Date",
      "Added a CMS Product in Current Date",
      "Added a Content Graph Product in Current Date",
      "Added a ODP Product in Current Date",
      "Increased a Cloud Product in Current Date",
      "Increased a CMS Product in Current Date",
      "Increased a Content Graph Product in Current Date",
      "Increased a ODP Product in Current Date",
      "Cloud Product in Current Date with ARR",
      "CMS  Product in Current Date with ARR",
      "Content Graph Product in Current Date with ARR",
      "ODP Product in Current Date with ARR",
      "Licenses Product in Previous Date with ARR",
      "Everweb-Ektron Product in Previous Date with ARR",
      "Personalized Find Product in Previous Date with ARR",
      "Visitor Int Product in Previous Date with ARR",
      "Movement Classification",
      "Movement Type-PF",
      "Movement Type-PG",
      "Sum of Positive or Negative Movements-PG",
      pg_arr_change,
      pg_arr_change_lcu,
      pg_bridge,
      "Min/Max PF Level movement",
      "PG Migration: Rolled Up Amount",
      "PG Migration: Rolled Up Amount LCU",
      "PG Migration: Classification",
      "PG Leftover: Rolled Up Amount",
      "PG Leftover: Rolled Up Amount LCU",
      "PG Leftover: Classification",
      "PG Migration Movement Final Classification",
      current_product_solution,
      prior_product_solution,
      product_arr_change_ccfx_ps,
      product_arr_change_lcu_ps,
      ps_bridge,
      "Movement Type-PS",
      "Sum of Positive or Negative Movements-PS",
      --if neg take the max (if pos take min) between the two PG movement and sum and tag the PS bridge movement as migration
      case
        when "Sum of Positive or Negative Movements-PS" is not null
        and "Movement Type-PS" = '-' then greatest(
          product_arr_change_ccfx_ps,
          "Sum of Positive or Negative Movements-PS"
        )
        when "Sum of Positive or Negative Movements-PS" is not null
        and "Movement Type-PS" = '+' then least(
          product_arr_change_ccfx_ps,
          "Sum of Positive or Negative Movements-PS"
        )
        else null
      end as "Min/Max PG Level movement"
    from initial_table_11
  ),
  initial_table_13 as (
    select evaluation_period,
      prior_period,
      current_period,
      current_end_customer,
      prior_end_customer,
      mcid,
      current_master_customer_id,
      prior_master_customer_id,
      current_product_family,
      prior_product_family,
      currency_code,
      prior_period_product_arr_usd_ccfx,
      current_period_product_arr_usd_ccfx,
      product_arr_change_ccfx,
      prior_period_product_arr_lcu,
      current_period_product_arr_lcu,
      product_arr_change_lcu,
      product_bridge,
      winback_period_days,
      wip_flag,
      price_increase_amount,
      subsidiary_entity_name,
      churn_period,
      customer_bridge,
      prior_product_group,
      current_product_group,
      current_product_family_class,
      prior_product_family_class,
      "Downgraded a Licenses  Product in Current Date",
      "Downgraded a Everweb-Ektron  Product in Current Date",
      "Downgraded a Personalized Find  Product in Current Date",
      "Downgraded a Visitor Int  Product in Current Date",
      "Churned a Licenses Product in Current Date",
      "Churned a Everweb-Ektron Product in Current Date",
      "Churned a Personalized Find Product in Current Date",
      "Churned a Visitor Int Product in Current Date",
      "Added a Cloud Product in Current Date",
      "Added a CMS Product in Current Date",
      "Added a Content Graph Product in Current Date",
      "Added a ODP Product in Current Date",
      "Increased a Cloud Product in Current Date",
      "Increased a CMS Product in Current Date",
      "Increased a Content Graph Product in Current Date",
      "Increased a ODP Product in Current Date",
      "Cloud Product in Current Date with ARR",
      "CMS  Product in Current Date with ARR",
      "Content Graph Product in Current Date with ARR",
      "ODP Product in Current Date with ARR",
      "Licenses Product in Previous Date with ARR",
      "Everweb-Ektron Product in Previous Date with ARR",
      "Personalized Find Product in Previous Date with ARR",
      "Visitor Int Product in Previous Date with ARR",
      "Movement Classification",
      "Movement Type-PF",
      "Movement Type-PG",
      "Sum of Positive or Negative Movements-PG",
      pg_arr_change,
      pg_arr_change_lcu,
      pg_bridge,
      "Min/Max PF Level movement",
      "PG Migration: Rolled Up Amount",
      "PG Migration: Rolled Up Amount LCU",
      "PG Migration: Classification",
      "PG Leftover: Rolled Up Amount",
      "PG Leftover: Rolled Up Amount LCU",
      "PG Leftover: Classification",
      "PG Migration Movement Final Classification",
      current_product_solution,
      prior_product_solution,
      product_arr_change_ccfx_ps,
      product_arr_change_lcu_ps,
      ps_bridge,
      "Movement Type-PS",
      "Sum of Positive or Negative Movements-PS",
      --if neg take the max (if pos take min) between the two PG movement and sum and tag the PS bridge movement as migration
      "Min/Max PG Level movement",
      case
        --Positive
        when "Movement Type-PS" = '+'
        and "PG Migration Movement Final Classification" is not null
        and "Min/Max PG Level movement" >= product_arr_change_ccfx_ps then "Min/Max PG Level movement"
        when "Movement Type-PS" = '+'
        and "PG Migration Movement Final Classification" is not null
        and "Min/Max PG Level movement" < product_arr_change_ccfx_ps then "Min/Max PG Level movement"
        when "Movement Type-PS" = '-'
        and "PG Migration Movement Final Classification" is not null
        and "Min/Max PG Level movement" <= product_arr_change_ccfx_ps then "Min/Max PG Level movement"
        when "Movement Type-PS" = '-'
        and "PG Migration Movement Final Classification" is not null
        and "Min/Max PG Level movement" > product_arr_change_ccfx_ps then "Min/Max PG Level movement"
      end as "PS Migration: Rolled Up Amount",
      case
        when "Movement Type-PS" = '+'
        and "PG Migration Movement Final Classification" is not null
        and "Min/Max PG Level movement" < product_arr_change_ccfx_ps then product_arr_change_ccfx_ps - "Min/Max PG Level movement"
        when "Movement Type-PS" = '-'
        and "PG Migration Movement Final Classification" is not null
        and "Min/Max PG Level movement" > product_arr_change_ccfx_ps then product_arr_change_ccfx_ps - "Min/Max PG Level movement"
        else null
      end as "PS Leftover: Rolled Up Amount"
    from initial_table_12
  ),
  initial_table_14 as (
    select evaluation_period,
      prior_period,
      current_period,
      current_end_customer,
      prior_end_customer,
      mcid,
      current_master_customer_id,
      prior_master_customer_id,
      current_product_family,
      prior_product_family,
      currency_code,
      prior_period_product_arr_usd_ccfx,
      current_period_product_arr_usd_ccfx,
      product_arr_change_ccfx,
      prior_period_product_arr_lcu,
      current_period_product_arr_lcu,
      product_arr_change_lcu,
      product_bridge,
      winback_period_days,
      wip_flag,
      price_increase_amount,
      subsidiary_entity_name,
      churn_period,
      customer_bridge,
      prior_product_group,
      current_product_group,
      current_product_family_class,
      prior_product_family_class,
      "Downgraded a Licenses  Product in Current Date",
      "Downgraded a Everweb-Ektron  Product in Current Date",
      "Downgraded a Personalized Find  Product in Current Date",
      "Downgraded a Visitor Int  Product in Current Date",
      "Churned a Licenses Product in Current Date",
      "Churned a Everweb-Ektron Product in Current Date",
      "Churned a Personalized Find Product in Current Date",
      "Churned a Visitor Int Product in Current Date",
      "Added a Cloud Product in Current Date",
      "Added a CMS Product in Current Date",
      "Added a Content Graph Product in Current Date",
      "Added a ODP Product in Current Date",
      "Increased a Cloud Product in Current Date",
      "Increased a CMS Product in Current Date",
      "Increased a Content Graph Product in Current Date",
      "Increased a ODP Product in Current Date",
      "Cloud Product in Current Date with ARR",
      "CMS  Product in Current Date with ARR",
      "Content Graph Product in Current Date with ARR",
      "ODP Product in Current Date with ARR",
      "Licenses Product in Previous Date with ARR",
      "Everweb-Ektron Product in Previous Date with ARR",
      "Personalized Find Product in Previous Date with ARR",
      "Visitor Int Product in Previous Date with ARR",
      "Movement Classification",
      "Movement Type-PF",
      "Movement Type-PG",
      "Sum of Positive or Negative Movements-PG",
      pg_arr_change,
      pg_arr_change_lcu,
      pg_bridge,
      "Min/Max PF Level movement",
      "PG Migration: Rolled Up Amount",
      "PG Migration: Rolled Up Amount LCU",
      "PG Migration: Classification",
      "PG Leftover: Rolled Up Amount",
      "PG Leftover: Rolled Up Amount LCU",
      "PG Leftover: Classification",
      "PG Migration Movement Final Classification",
      current_product_solution,
      prior_product_solution,
      product_arr_change_ccfx_ps,
      product_arr_change_lcu_ps,
      ps_bridge,
      "Movement Type-PS",
      "Sum of Positive or Negative Movements-PS",
      --if neg take the max (if pos take min) between the two PG movement and sum and tag the PS bridge movement as migration
      "Min/Max PG Level movement",
      "PS Migration: Rolled Up Amount",
      (
        product_arr_change_lcu_ps * (
          "PS Migration: Rolled Up Amount" / case
            when product_arr_change_ccfx_ps = 0
            or product_arr_change_ccfx_ps is null then 1
            else product_arr_change_ccfx_ps
          end
        )
      ) as "PS Migration: Rolled Up Amount LCU",
      "PS Leftover: Rolled Up Amount",
      (
        product_arr_change_lcu_ps * (
          "PS Leftover: Rolled Up Amount" / case
            when product_arr_change_ccfx_ps = 0
            or product_arr_change_ccfx_ps is null then 1
            else product_arr_change_ccfx_ps
          end
        )
      ) as "PS Leftover: Rolled Up Amount LCU",
      case
        when "PS Migration: Rolled Up Amount" is not null then "PG Migration: Classification"
        else null
      end as "PS Migration: Classification",
      case
        when "PS Leftover: Rolled Up Amount" is not null then ps_bridge
        else null
      end as "PS Leftover: Classification"
    from initial_table_13
  ),
  cust_bridge as (
    select distinct cb.evaluation_period,
      cb.mcid,
      cb.baseline_currency,
      cb.customer_arr_change_lcu,
      cb.customer_arr_change_ccfx,
      cb.customer_bridge
    from customer_bridge cb
    where customer_bridge IN ('Churn', 'Downgrade', 'New', 'Up Sell') --ufdm_archive.sst_customer_bridge_lcoked_09102023_1412 cb --Customer Bridge
  ),
  initial_table_15 as (
    select sc.evaluation_period,
      sc.prior_period,
      sc.current_period,
      sc.current_end_customer,
      sc.prior_end_customer,
      sc.mcid,
      sc.current_master_customer_id,
      sc.prior_master_customer_id,
      sc.current_product_family,
      sc.prior_product_family,
      sc.currency_code,
      sc.prior_period_product_arr_usd_ccfx,
      sc.current_period_product_arr_usd_ccfx,
      sc.product_arr_change_ccfx,
      sc.prior_period_product_arr_lcu,
      sc.current_period_product_arr_lcu,
      sc.product_arr_change_lcu,
      sc.product_bridge,
      sc.winback_period_days,
      sc.wip_flag,
      sc.price_increase_amount,
      sc.subsidiary_entity_name,
      sc.churn_period,
      --  sc.customer_bridge  ,
      sc.prior_product_group,
      sc.current_product_group,
      sc.current_product_family_class,
      sc.prior_product_family_class,
      sc."Downgraded a Licenses  Product in Current Date",
      sc."Downgraded a Everweb-Ektron  Product in Current Date",
      sc."Downgraded a Personalized Find  Product in Current Date",
      sc."Downgraded a Visitor Int  Product in Current Date",
      sc."Churned a Licenses Product in Current Date",
      sc."Churned a Everweb-Ektron Product in Current Date",
      sc."Churned a Personalized Find Product in Current Date",
      sc."Churned a Visitor Int Product in Current Date",
      sc."Added a Cloud Product in Current Date",
      sc."Added a CMS Product in Current Date",
      sc."Added a Content Graph Product in Current Date",
      sc."Added a ODP Product in Current Date",
      sc."Increased a Cloud Product in Current Date",
      sc."Increased a CMS Product in Current Date",
      sc."Increased a Content Graph Product in Current Date",
      sc."Increased a ODP Product in Current Date",
      sc."Cloud Product in Current Date with ARR",
      sc."CMS  Product in Current Date with ARR",
      sc."Content Graph Product in Current Date with ARR",
      sc."ODP Product in Current Date with ARR",
      sc."Licenses Product in Previous Date with ARR",
      sc."Everweb-Ektron Product in Previous Date with ARR",
      sc."Personalized Find Product in Previous Date with ARR",
      sc."Visitor Int Product in Previous Date with ARR",
      sc."Movement Classification",
      sc."Movement Type-PF",
      sc."Movement Type-PG",
      sc."Sum of Positive or Negative Movements-PG",
      sc.pg_arr_change,
      sc.pg_arr_change_lcu,
      sc.pg_bridge,
      sc."Min/Max PF Level movement",
      sc."PG Migration: Rolled Up Amount",
      sc."PG Migration: Rolled Up Amount LCU",
      sc."PG Migration: Classification",
      sc."PG Leftover: Rolled Up Amount",
      sc."PG Leftover: Rolled Up Amount LCU",
      sc."PG Leftover: Classification",
      sc."PG Migration Movement Final Classification",
      sc.current_product_solution,
      sc.prior_product_solution,
      sc.product_arr_change_ccfx_ps,
      sc.product_arr_change_lcu_ps,
      sc.ps_bridge,
      sc."Movement Type-PS",
      sc."Sum of Positive or Negative Movements-PS",
      sc."Min/Max PG Level movement",
      sc."PS Migration: Rolled Up Amount",
      sc."PS Migration: Rolled Up Amount LCU",
      sc."PS Migration: Classification",
      sc."PS Leftover: Rolled Up Amount",
      sc."PS Leftover: Rolled Up Amount LCU",
      sc."PS Leftover: Classification",
      --Add Customer Level Bridge Movements
      cb.customer_arr_change_ccfx,
      cb.customer_arr_change_lcu,
      cb.customer_bridge,
      --Label PS Migration: Only Migration
      case
        when sc."PS Migration: Classification" in ('downgrade - migration', 'churn - migration') then '-'
        when sc."PS Migration: Classification" in ('crosssell - migration', 'upsell - migration') then '+'
        else null
      end as "PS Migration Movement Final Classification",
      --Classify the CB Level Movements
      case
        when cb.customer_arr_change_ccfx > 0 then '+'
        when cb.customer_arr_change_ccfx < 0 then '-'
        else null
      end as "Movement Type-CB"
    from initial_table_14 sc
      left join cust_bridge cb on cb.evaluation_period = sc.evaluation_period
      and cb.mcid = sc.mcid
      and cb.baseline_currency = sc.currency_code
  ),
  distinct_table_3 as (
    select distinct mcid,
      evaluation_period,
      currency_code,
      current_product_solution,
      prior_product_solution,
      customer_arr_change_ccfx,
      customer_bridge,
      "Movement Type-CB",
      "PS Migration Movement Final Classification",
      "PS Migration: Rolled Up Amount"
    from initial_table_15
  ),
  distinct_table_4 as (
    select mcid,
      evaluation_period,
      currency_code,
      customer_arr_change_ccfx,
      customer_bridge,
      "Movement Type-CB",
      "PS Migration Movement Final Classification",
      "PS Migration: Rolled Up Amount",
      1 as join_id,
      --Sum Up the PS Movements at The Customer Level
      case
        when "Movement Type-CB" = '-'
        and "PS Migration Movement Final Classification" = '-' then sum("PS Migration: Rolled Up Amount") filter(
          where "PS Migration Movement Final Classification" = '-'
        ) over(
          partition by mcid,
          evaluation_period,
          currency_code
        )
        when "Movement Type-CB" = '+'
        and "PS Migration Movement Final Classification" = '+' then sum("PS Migration: Rolled Up Amount") filter(
          where "PS Migration Movement Final Classification" = '+'
        ) over(
          partition by mcid,
          evaluation_period,
          currency_code
        )
        else null
      end as "Sum of Positive or Negative Movements-CB"
    from distinct_table_3
  ) --Rejoin it back to the old table
  --Given that this is on the customer level only join it to one row -- rank the row with the movements
,
  initial_table_16 as (
    select sc.evaluation_period,
      sc.prior_period,
      sc.current_period,
      sc.current_end_customer,
      sc.prior_end_customer,
      sc.mcid,
      sc.current_master_customer_id,
      sc.prior_master_customer_id,
      sc.current_product_family,
      sc.prior_product_family,
      sc.currency_code,
      sc.prior_period_product_arr_usd_ccfx,
      sc.current_period_product_arr_usd_ccfx,
      sc.product_arr_change_ccfx,
      sc.prior_period_product_arr_lcu,
      sc.current_period_product_arr_lcu,
      sc.product_arr_change_lcu,
      sc.product_bridge,
      sc.winback_period_days,
      sc.wip_flag,
      sc.price_increase_amount,
      sc.subsidiary_entity_name,
      sc.churn_period,
      --  sc.customer_bridge  ,
      sc.prior_product_group,
      sc.current_product_group,
      sc.current_product_family_class,
      sc.prior_product_family_class,
      sc."Downgraded a Licenses  Product in Current Date",
      sc."Downgraded a Everweb-Ektron  Product in Current Date",
      sc."Downgraded a Personalized Find  Product in Current Date",
      sc."Downgraded a Visitor Int  Product in Current Date",
      sc."Churned a Licenses Product in Current Date",
      sc."Churned a Everweb-Ektron Product in Current Date",
      sc."Churned a Personalized Find Product in Current Date",
      sc."Churned a Visitor Int Product in Current Date",
      sc."Added a Cloud Product in Current Date",
      sc."Added a CMS Product in Current Date",
      sc."Added a Content Graph Product in Current Date",
      sc."Added a ODP Product in Current Date",
      sc."Increased a Cloud Product in Current Date",
      sc."Increased a CMS Product in Current Date",
      sc."Increased a Content Graph Product in Current Date",
      sc."Increased a ODP Product in Current Date",
      sc."Cloud Product in Current Date with ARR",
      sc."CMS  Product in Current Date with ARR",
      sc."Content Graph Product in Current Date with ARR",
      sc."ODP Product in Current Date with ARR",
      sc."Licenses Product in Previous Date with ARR",
      sc."Everweb-Ektron Product in Previous Date with ARR",
      sc."Personalized Find Product in Previous Date with ARR",
      sc."Visitor Int Product in Previous Date with ARR",
      sc."Movement Classification",
      sc."Movement Type-PF",
      sc."Movement Type-PG",
      sc."Sum of Positive or Negative Movements-PG",
      sc.pg_arr_change,
      sc.pg_arr_change_lcu,
      sc.pg_bridge,
      sc."Min/Max PF Level movement",
      sc."PG Migration: Rolled Up Amount",
      sc."PG Migration: Rolled Up Amount LCU",
      sc."PG Migration: Classification",
      sc."PG Leftover: Rolled Up Amount",
      sc."PG Leftover: Rolled Up Amount LCU",
      sc."PG Leftover: Classification",
      sc."PG Migration Movement Final Classification",
      sc.current_product_solution,
      sc.prior_product_solution,
      sc.product_arr_change_ccfx_ps,
      sc.product_arr_change_lcu_ps,
      sc.ps_bridge,
      sc."Movement Type-PS",
      sc."Sum of Positive or Negative Movements-PS",
      sc."Min/Max PG Level movement",
      sc."PS Migration: Rolled Up Amount",
      sc."PS Migration: Rolled Up Amount LCU",
      sc."PS Migration: Classification",
      sc."PS Leftover: Rolled Up Amount",
      sc."PS Leftover: Rolled Up Amount LCU",
      sc."PS Leftover: Classification",
      --Add Customer Level Bridge Movements
      sc.customer_arr_change_ccfx,
      sc.customer_arr_change_lcu,
      sc.customer_bridge,
      --Label PS Migration: Only Migration
      sc."PS Migration Movement Final Classification",
      --Classify the CB Level Movements
      sc."Movement Type-CB",
      dt4."Sum of Positive or Negative Movements-CB"
    from initial_table_15 sc
      left join distinct_table_4 dt4 on sc.mcid = dt4.mcid
      and sc.evaluation_period = dt4.evaluation_period
      and sc.currency_code = dt4.currency_code
      and sc."PS Migration Movement Final Classification" = dt4."PS Migration Movement Final Classification"
      and sc."PS Migration: Rolled Up Amount" = dt4."PS Migration: Rolled Up Amount"
  ),
  initial_table_17 as (
    select it16.evaluation_period,
      it16.prior_period,
      it16.current_period,
      it16.current_end_customer,
      it16.prior_end_customer,
      it16.mcid,
      it16.current_master_customer_id,
      it16.prior_master_customer_id,
      it16.current_product_family,
      it16.prior_product_family,
      it16.currency_code,
      it16.prior_period_product_arr_usd_ccfx,
      it16.current_period_product_arr_usd_ccfx,
      it16.product_arr_change_ccfx,
      it16.prior_period_product_arr_lcu,
      it16.current_period_product_arr_lcu,
      it16.product_arr_change_lcu,
      it16.product_bridge,
      it16.winback_period_days,
      it16.wip_flag,
      it16.price_increase_amount,
      it16.subsidiary_entity_name,
      it16.churn_period,
      --  it16.customer_bridge    ,
      it16.prior_product_group,
      it16.current_product_group,
      it16.current_product_family_class,
      it16.prior_product_family_class,
      it16."Downgraded a Licenses  Product in Current Date",
      it16."Downgraded a Everweb-Ektron  Product in Current Date",
      it16."Downgraded a Personalized Find  Product in Current Date",
      it16."Downgraded a Visitor Int  Product in Current Date",
      it16."Churned a Licenses Product in Current Date",
      it16."Churned a Everweb-Ektron Product in Current Date",
      it16."Churned a Personalized Find Product in Current Date",
      it16."Churned a Visitor Int Product in Current Date",
      it16."Added a Cloud Product in Current Date",
      it16."Added a CMS Product in Current Date",
      it16."Added a Content Graph Product in Current Date",
      it16."Added a ODP Product in Current Date",
      it16."Increased a Cloud Product in Current Date",
      it16."Increased a CMS Product in Current Date",
      it16."Increased a Content Graph Product in Current Date",
      it16."Increased a ODP Product in Current Date",
      it16."Cloud Product in Current Date with ARR",
      it16."CMS  Product in Current Date with ARR",
      it16."Content Graph Product in Current Date with ARR",
      it16."ODP Product in Current Date with ARR",
      it16."Licenses Product in Previous Date with ARR",
      it16."Everweb-Ektron Product in Previous Date with ARR",
      it16."Personalized Find Product in Previous Date with ARR",
      it16."Visitor Int Product in Previous Date with ARR",
      it16."Movement Classification",
      it16."Movement Type-PF",
      it16."Movement Type-PG",
      it16."Sum of Positive or Negative Movements-PG",
      it16.pg_arr_change,
      it16.pg_arr_change_lcu,
      it16.pg_bridge,
      it16."Min/Max PF Level movement",
      it16."PG Migration: Rolled Up Amount",
      it16."PG Migration: Rolled Up Amount LCU",
      it16."PG Migration: Classification",
      it16."PG Leftover: Rolled Up Amount",
      it16."PG Leftover: Rolled Up Amount LCU",
      it16."PG Leftover: Classification",
      it16."PG Migration Movement Final Classification",
      it16.current_product_solution,
      it16.prior_product_solution,
      it16.product_arr_change_ccfx_ps,
      it16.product_arr_change_lcu_ps,
      it16.ps_bridge,
      it16."Movement Type-PS",
      it16."Sum of Positive or Negative Movements-PS",
      it16."Min/Max PG Level movement",
      it16."PS Migration: Rolled Up Amount",
      it16."PS Migration: Rolled Up Amount LCU",
      it16."PS Migration: Classification",
      it16."PS Leftover: Rolled Up Amount",
      it16."PS Leftover: Rolled Up Amount LCU",
      it16."PS Leftover: Classification",
      it16.customer_arr_change_ccfx,
      it16.customer_arr_change_lcu,
      it16.customer_bridge,
      --Label PS Migration: Only Migration
      it16."PS Migration Movement Final Classification",
      --Classify the CB Level Movements
      it16."Movement Type-CB",
      it16."Sum of Positive or Negative Movements-CB",
      --if neg take the max (if pos take min) between the two PG movement and sum and tag the PS bridge movement as migration
      case
        when it16."Sum of Positive or Negative Movements-CB" is not null
        and it16."Movement Type-CB" = '-' then greatest(
          it16.customer_arr_change_ccfx,
          it16."Sum of Positive or Negative Movements-CB"
        )
        when "Sum of Positive or Negative Movements-CB" is not null
        and "Movement Type-CB" = '+' then least(
          it16.customer_arr_change_ccfx,
          it16."Sum of Positive or Negative Movements-CB"
        )
        else null
      end as "Min/Max PS Level movement"
    from initial_table_16 it16
  ),
  initial_table_18 as (
    select it17.evaluation_period,
      it17.prior_period,
      it17.current_period,
      it17.current_end_customer,
      it17.prior_end_customer,
      it17.mcid,
      it17.current_master_customer_id,
      it17.prior_master_customer_id,
      it17.current_product_family,
      it17.prior_product_family,
      it17.currency_code,
      it17.prior_period_product_arr_usd_ccfx,
      it17.current_period_product_arr_usd_ccfx,
      it17.product_arr_change_ccfx,
      it17.prior_period_product_arr_lcu,
      it17.current_period_product_arr_lcu,
      it17.product_arr_change_lcu,
      it17.product_bridge,
      it17.winback_period_days,
      it17.wip_flag,
      it17.price_increase_amount,
      it17.subsidiary_entity_name,
      it17.churn_period,
      --  it17.customer_bridge    ,
      it17.prior_product_group,
      it17.current_product_group,
      it17.current_product_family_class,
      it17.prior_product_family_class,
      it17."Downgraded a Licenses  Product in Current Date",
      it17."Downgraded a Everweb-Ektron  Product in Current Date",
      it17."Downgraded a Personalized Find  Product in Current Date",
      it17."Downgraded a Visitor Int  Product in Current Date",
      it17."Churned a Licenses Product in Current Date",
      it17."Churned a Everweb-Ektron Product in Current Date",
      it17."Churned a Personalized Find Product in Current Date",
      it17."Churned a Visitor Int Product in Current Date",
      it17."Added a Cloud Product in Current Date",
      it17."Added a CMS Product in Current Date",
      it17."Added a Content Graph Product in Current Date",
      it17."Added a ODP Product in Current Date",
      it17."Increased a Cloud Product in Current Date",
      it17."Increased a CMS Product in Current Date",
      it17."Increased a Content Graph Product in Current Date",
      it17."Increased a ODP Product in Current Date",
      it17."Cloud Product in Current Date with ARR",
      it17."CMS  Product in Current Date with ARR",
      it17."Content Graph Product in Current Date with ARR",
      it17."ODP Product in Current Date with ARR",
      it17."Licenses Product in Previous Date with ARR",
      it17."Everweb-Ektron Product in Previous Date with ARR",
      it17."Personalized Find Product in Previous Date with ARR",
      it17."Visitor Int Product in Previous Date with ARR",
      it17."Movement Classification",
      it17."Movement Type-PF",
      it17."Movement Type-PG",
      it17."Sum of Positive or Negative Movements-PG",
      it17.pg_arr_change,
      it17.pg_arr_change_lcu,
      it17.pg_bridge,
      it17."Min/Max PF Level movement",
      it17."PG Migration: Rolled Up Amount",
      it17."PG Migration: Rolled Up Amount LCU",
      it17."PG Migration: Classification",
      it17."PG Leftover: Rolled Up Amount",
      it17."PG Leftover: Rolled Up Amount LCU",
      it17."PG Leftover: Classification",
      it17."PG Migration Movement Final Classification",
      it17.current_product_solution,
      it17.prior_product_solution,
      it17.product_arr_change_ccfx_ps,
      it17.product_arr_change_lcu_ps,
      it17.ps_bridge,
      it17."Movement Type-PS",
      it17."Sum of Positive or Negative Movements-PS",
      it17."Min/Max PG Level movement",
      it17."PS Migration: Rolled Up Amount",
      it17."PS Migration: Rolled Up Amount LCU",
      it17."PS Migration: Classification",
      it17."PS Leftover: Rolled Up Amount",
      it17."PS Leftover: Rolled Up Amount LCU",
      it17."PS Leftover: Classification",
      it17.customer_arr_change_ccfx,
      it17.customer_arr_change_lcu,
      it17.customer_bridge,
      --Label PS Migration: Only Migration
      it17."PS Migration Movement Final Classification",
      --Classify the CB Level Movements
      it17."Movement Type-CB",
      it17."Sum of Positive or Negative Movements-CB",
      it17."Min/Max PS Level movement",
      case
        --Positive
        when it17."Movement Type-CB" = '+'
        and it17."PS Migration Movement Final Classification" is not null
        and it17."Min/Max PS Level movement" >= it17.customer_arr_change_ccfx then it17."Min/Max PS Level movement"
        when it17."Movement Type-CB" = '+'
        and it17."PS Migration Movement Final Classification" is not null
        and it17."Min/Max PS Level movement" < it17.customer_arr_change_ccfx then it17."Min/Max PS Level movement"
        when it17."Movement Type-CB" = '-'
        and it17."PS Migration Movement Final Classification" is not null
        and it17."Min/Max PS Level movement" <= it17.customer_arr_change_ccfx then it17."Min/Max PS Level movement"
        when it17."Movement Type-CB" = '-'
        and it17."PS Migration Movement Final Classification" is not null
        and it17."Min/Max PS Level movement" > it17.customer_arr_change_ccfx then it17."Min/Max PS Level movement"
      end as "CB Migration: Rolled Up Amount",
      case
        when it17."Movement Type-CB" = '+'
        and it17."PS Migration Movement Final Classification" is not null
        and it17."Min/Max PS Level movement" < it17.customer_arr_change_ccfx then it17.customer_arr_change_ccfx - it17."Min/Max PS Level movement"
        when it17."Movement Type-CB" = '-'
        and it17."PS Migration Movement Final Classification" is not null
        and it17."Min/Max PS Level movement" > it17.customer_arr_change_ccfx then it17.customer_arr_change_ccfx - it17."Min/Max PS Level movement"
        else null
      end as "CB Leftover: Rolled Up Amount"
    from initial_table_17 it17
  )
  select left(it18.evaluation_period, 4) as "Year",
    it18.evaluation_period,
    it18.prior_period,
    it18.current_period,
    it18.current_end_customer,
    it18.prior_end_customer,
    it18.mcid,
    it18.current_master_customer_id,
    it18.prior_master_customer_id,
    it18.current_product_family,
    it18.prior_product_family,
    it18.currency_code,
    it18.prior_period_product_arr_usd_ccfx,
    it18.current_period_product_arr_usd_ccfx,
    it18.product_arr_change_ccfx,
    it18.prior_period_product_arr_lcu,
    it18.current_period_product_arr_lcu,
    it18.product_arr_change_lcu,
    it18.product_bridge,
    it18.winback_period_days,
    it18.wip_flag,
    it18.price_increase_amount,
    it18.subsidiary_entity_name,
    it18.churn_period,
    --  it18.customer_bridge    ,
    it18.prior_product_group,
    it18.current_product_group,
    it18.current_product_family_class,
    it18.prior_product_family_class,
    it18."Downgraded a Licenses  Product in Current Date",
    it18."Downgraded a Everweb-Ektron  Product in Current Date",
    it18."Downgraded a Personalized Find  Product in Current Date",
    it18."Downgraded a Visitor Int  Product in Current Date",
    it18."Churned a Licenses Product in Current Date",
    it18."Churned a Everweb-Ektron Product in Current Date",
    it18."Churned a Personalized Find Product in Current Date",
    it18."Churned a Visitor Int Product in Current Date",
    it18."Added a Cloud Product in Current Date",
    it18."Added a CMS Product in Current Date",
    it18."Added a Content Graph Product in Current Date",
    it18."Added a ODP Product in Current Date",
    it18."Increased a Cloud Product in Current Date",
    it18."Increased a CMS Product in Current Date",
    it18."Increased a Content Graph Product in Current Date",
    it18."Increased a ODP Product in Current Date",
    it18."Cloud Product in Current Date with ARR",
    it18."CMS  Product in Current Date with ARR",
    it18."Content Graph Product in Current Date with ARR",
    it18."ODP Product in Current Date with ARR",
    it18."Licenses Product in Previous Date with ARR",
    it18."Everweb-Ektron Product in Previous Date with ARR",
    it18."Personalized Find Product in Previous Date with ARR",
    it18."Visitor Int Product in Previous Date with ARR",
    it18."Movement Classification",
    it18."Movement Type-PF",
    it18."Movement Type-PG",
    it18."Sum of Positive or Negative Movements-PG",
    it18.pg_arr_change,
    it18.pg_arr_change_lcu,
    it18.pg_bridge,
    it18."Min/Max PF Level movement",
    it18."PG Migration: Rolled Up Amount",
    it18."PG Migration: Rolled Up Amount LCU",
    it18."PG Migration: Classification",
    it18."PG Leftover: Rolled Up Amount",
    it18."PG Leftover: Rolled Up Amount LCU",
    it18."PG Leftover: Classification",
    it18."PG Migration Movement Final Classification",
    it18.current_product_solution,
    it18.prior_product_solution,
    it18.product_arr_change_ccfx_ps,
    it18.product_arr_change_lcu_ps,
    it18.ps_bridge,
    it18."Movement Type-PS",
    it18."Sum of Positive or Negative Movements-PS",
    it18."Min/Max PG Level movement",
    it18."PS Migration: Rolled Up Amount",
    it18."PS Migration: Rolled Up Amount LCU",
    it18."PS Migration: Classification",
    it18."PS Leftover: Rolled Up Amount",
    it18."PS Leftover: Rolled Up Amount LCU",
    it18."PS Leftover: Classification",
    it18.customer_arr_change_ccfx,
    it18.customer_arr_change_lcu,
    it18.customer_bridge,
    --Label PS Migration: Only Migration
    it18."PS Migration Movement Final Classification",
    --Classify the CB Level Movements
    it18."Movement Type-CB",
    it18."Sum of Positive or Negative Movements-CB",
    it18."Min/Max PS Level movement",
    it18."CB Migration: Rolled Up Amount",
    (
      it18.customer_arr_change_lcu * (
        it18."CB Migration: Rolled Up Amount" / case
          when it18.customer_arr_change_ccfx = 0
          or it18.customer_arr_change_ccfx is null then 1
          else it18.customer_arr_change_ccfx
        end
      )
    ) as "CB Migration: Rolled Up Amount LCU",
    it18."CB Leftover: Rolled Up Amount",
    (
      it18.customer_arr_change_lcu * (
        it18."CB Leftover: Rolled Up Amount" / case
          when it18.customer_arr_change_ccfx = 0
          or it18.customer_arr_change_ccfx is null then 1
          else it18.customer_arr_change_ccfx
        end
      )
    ) as "CB Leftover: Rolled Up Amount LCU",
    case
      when it18."CB Migration: Rolled Up Amount" is not null then it18."PS Migration: Classification"
      else null
    end as "CB Migration: Classification",
    case
      when it18."CB Leftover: Rolled Up Amount" is not null then it18.customer_bridge
      else null
    end as "CB Leftover: Classification"
  from initial_table_18 as it18
);