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
        when current_product_family_class IN ('Everweb')
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Downgraded a Everweb  Product in Current Date",
      case
        when current_product_family_class IN ('Ektron')
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Downgraded a Ektron  Product in Current Date",
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
        when prior_product_family_class IN ('Everweb')
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx = 0 then 1
        else 0
      end as "Churned a Everweb Product in Current Date",
      case
        when prior_product_family_class IN ('Ektron')
        and product_arr_change_ccfx < 0
        and prior_period_product_arr_usd_ccfx > 0
        and current_period_product_arr_usd_ccfx = 0 then 1
        else 0
      end as "Churned a Ektron Product in Current Date",
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
        when prior_product_family_class IN ('Everweb')
        and prior_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Everweb Product in Previous Date with ARR",
      case
        when prior_product_family_class IN ('Ektron')
        and prior_period_product_arr_usd_ccfx > 0 then 1
        else 0
      end as "Ektron Product in Previous Date with ARR",
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
        when "Downgraded a Everweb  Product in Current Date" = 1 --Downgraded a legacy product
        and (
          sum("CMS  Product in Current Date with ARR") over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- have named product in current date with ARR > 0
        then 'downgrade - migration -- Everweb to CMS' --ai)
        when "Downgraded a Ektron  Product in Current Date" = 1 --Downgraded a legacy product
          and (
            sum("CMS  Product in Current Date with ARR") over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0 -- have named product in current date with ARR > 0
          then 'downgrade - migration -- Ektron to CMS' --ai)
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
        then 'Downsell - migration -- Licenses to Cloud' --bi)
        when "Churned a Everweb Product in Current Date" = 1 --Churned a legacy product
        and (
          sum("CMS  Product in Current Date with ARR") over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- have named product in current date with ARR > 0
        then 'Downsell - migration -- Everweb to CMS' --bi)
        when "Churned a Ektron Product in Current Date" = 1 --Churned a legacy product
        and (
          sum("CMS  Product in Current Date with ARR") over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- have named product in current date with ARR > 0
        then 'Downsell - migration -- Ektron to CMS'
        when "Churned a Personalized Find Product in Current Date" = 1 --Churned a legacy product
        and (
          sum("Content Graph Product in Current Date with ARR") over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- have named product in current date with ARR > 0
        then 'Downsell - migration -- Personalised Find to Content Graph' --bi)
        when "Churned a Visitor Int Product in Current Date" = 1 --Churned a legacy product
        and (
          sum("ODP Product in Current Date with ARR") over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- have named product in current date with ARR > 0
        then 'Downsell - migration -- VisitorInt to ODP' --bi)
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
        then 'Cross-sell - migration -- Licenses to Cloud' --bii)
        when "Added a CMS Product in Current Date" = 1 --Added a named product in the current snapshot date
        and (
          (
            sum(
              "Downgraded a Everweb  Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              "Churned a Everweb Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either churned or downgraded a legacy product in the current snapshot date
        then 'Cross-sell - migration -- Everweb to CMS' 
        when "Added a CMS Product in Current Date" = 1 --Added a named product in the current snapshot date
        and (
          (
            sum(
              "Downgraded a Ektron  Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              "Churned a Ektron Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either churned or downgraded a legacy product in the current snapshot date
        then 'Cross-sell - migration -- Ektron to CMS'--bii)
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
        then 'Cross-sell - migration -- Personalised Find to Content Graph' --bii)
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
        then 'Cross-sell - migration -- VisitorInt to ODP' --bii)
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
              "Downgraded a Everweb  Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              "Churned a Everweb Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either churned or downgraded a legacy product in the current snapshot date
        then 'upsell - migration -- Everweb to CMS' 
        when "Increased a CMS Product in Current Date" = 1 --Increased a named product in the current snapshot date
        and (
          (
            sum(
              "Downgraded a Ektron  Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
          or (
            sum(
              "Churned a Ektron Product in Current Date"
            ) over(
              partition by mcid,
              evaluation_period,
              currency_code
            )
          ) > 0
        ) -- Either churned or downgraded a legacy product in the current snapshot date
        then 'upsell - migration -- Ektron to CMS'
        --If a customer increased or added a named product & they had a legacy product in the prior snapshot period > 0 ARR we would classify the movement as migration
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
        then 'Cross-sell - migration -- Licenses to Cloud' --cii)
        when "Added a CMS Product in Current Date" = 1 --Added a named product in the current snapshot date
        and (
          sum(
            "Everweb Product in Previous Date with ARR"
          ) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- had a legacy product in previous snapshot date with ARR > 0
        then 'Cross-sell - migration -- Everweb to CMS' 
        when "Added a CMS Product in Current Date" = 1 --Added a named product in the current snapshot date
        and (
          sum(
            "Ektron Product in Previous Date with ARR"
          ) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- had a legacy product in previous snapshot date with ARR > 0
        then 'Cross-sell - migration -- Ektron to CMS'--cii)
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
        then 'Cross-sell - migration -- Personalised Find to Content Graph' --cii)
        when "Added a ODP Product in Current Date" = 1 --Added a named product in the current snapshot date
        and (
          sum("Visitor Int Product in Previous Date with ARR") over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- had a legacy product in previous snapshot date with ARR > 0
        then 'Cross-sell - migration -- VisitorInt to ODP' --cii)
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
            "Everweb Product in Previous Date with ARR"
          ) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- had a legacy product in previous snapshot date with ARR > 0
        then 'upsell - migration -- Everweb to CMS' 
        when "Increased a CMS Product in Current Date" = 1 --Added a named product in the current snapshot date
        and (
          sum(
            "Ektron Product in Previous Date with ARR"
          ) over(
            partition by mcid,
            evaluation_period,
            currency_code
          )
        ) > 0 -- had a legacy product in previous snapshot date with ARR > 0
        then 'upsell - migration -- Ektron to CMS'--di)
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
        then 'Downsell - migration -- Licenses to Cloud' --dii)
        when "Churned a Everweb Product in Current Date" = 1 --Churned a legacy product from prior snapshot date
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
        then 'Downsell - migration -- Everweb to CMS'
        when "Churned a Ektron Product in Current Date" = 1 --Churned a legacy product from prior snapshot date
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
        then 'Downsell - migration -- Ektron to CMS' --dii)
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
        then 'Downsell - migration -- Personalised Find to Content Graph' --dii)
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
        then 'Downsell - migration -- VisitorInt to ODP' --dii)
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
        when "Downgraded a Everweb  Product in Current Date" = 1 --Downgraded a legacy product from prior snapshot date
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
        then 'downgrade - migration -- Everweb to CMS'
        when "Downgraded a Ektron  Product in Current Date" = 1 --Downgraded a legacy product from prior snapshot date
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
        then 'downgrade - migration -- Ektron to CMS'
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
    it3.current_product_solution,
    it3.prior_product_solution,
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
    it3."Downgraded a Everweb  Product in Current Date",
    it3."Downgraded a Ektron  Product in Current Date",
    it3."Downgraded a Personalized Find  Product in Current Date",
    it3."Downgraded a Visitor Int  Product in Current Date",
    it3."Churned a Licenses Product in Current Date",
    it3."Churned a Everweb Product in Current Date",
    it3."Churned a Ektron Product in Current Date",
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
    it3."Everweb Product in Previous Date with ARR",
    it3."Ektron Product in Previous Date with ARR",
    it3."Personalized Find Product in Previous Date with ARR",
    it3."Visitor Int Product in Previous Date with ARR",
    it3."Movement Classification"
  from initial_table_3 it3
);
Drop table if exists sandbox.churn_migration_classifiers_pg;
Create table sandbox.churn_migration_classifiers_pg as (
  WITH initial_table_4 as (
    SELECT it3.*,
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
    from sandbox.churn_migration_classifiers it3
      left join product_group_product_solution_bridge rt --ryzlan.sst_pb_pg_temp rt --Product group Bridge ryzlan.sst_product_bridge_product_group_cloud_cm
      on it3.evaluation_period = rt.evaluation_period
      and coalesce(
        it3.prior_product_group,
        it3.current_product_group
      ) = coalesce(rt.current_product_group, rt.prior_product_group)
      and it3.mcid = rt.mcid
      and it3.currency_code = rt.currency_code
  ),
  initial_table_5 as (
    select *,
      --
      case
        when pg_arr_change > 0 then '+'
        when pg_arr_change < 0 then '-'
        else null
      end as "Movement Type-PG"
    from initial_table_4
  ),
  initial_table_6 as (
    select *,
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
      end as "Sum of Positive or Negative Movements-PG"
    from initial_table_5
  ),
  initial_table_7 as (
    select * --if neg take the max (if pos take min) between the two PG movement and sum and tag the PG bridge movement as migration
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
      end as "Min/Max PF Level movement" --Bring in the product solution columns as well -- to roll it up on the PS level
    from initial_table_6
  ),
  initial_table_8 as (
    select *,
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
      end as "PG Leftover: Rolled Up Amount" --Bring in the product solution columns as well -- to roll it up on the PS level
    from initial_table_7
  ),
  initial_table_9 as (
    select --if neg take the max (if pos take min) between the two PG movement and sum and tag the PG bridge movement as migration
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
      end as "PG Leftover: Classification"
    from initial_table_8
  )
  select *
  from initial_table_9
);
Drop table if exists sandbox.churn_migration_classifiers_ps;
Create table sandbox.churn_migration_classifiers_ps as (
  with initial_table_10 as (
    select cmp.*,
      rst.product_arr_change_ccfx as product_arr_change_ccfx_ps,
      rst.product_arr_change_lcu as product_arr_change_lcu_ps,
      rst.product_bridge as ps_bridge,
      --Product Solution Bridge Labelling
      --Label PG Migration: Only Migration
      case
        when "PG Migration: Classification" in ('downgrade - migration%', 'Downsell - migration%') then '-'
        when "PG Migration: Classification" in ('Cross-sell - migration%', 'upsell - migration%') then '+'
        else null
      end as "PG Migration Movement Final Classification",
      --Classify the PS Level Movements
      case
        when rst.product_arr_change_ccfx > 0 then '+'
        when rst.product_arr_change_ccfx < 0 then '-'
        else null
      end as "Movement Type-PS"
    from sandbox.churn_migration_classifiers_pg cmp
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
  ),
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
    select it10.*,
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
  ),
  initial_table_12 as (
    select *,
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
    select *,
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
    select *,
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
  )
  select *
  from initial_table_14
);
Drop table if exists sandbox.churn_migration_classifiers_cb;
Create table sandbox.churn_migration_classifiers_cb as (
  With initial_table_15 as (
    select sc.*,
      --Add Customer Level Bridge Movements
      cb.customer_arr_change_ccfx,
      cb.customer_arr_change_lcu,
      cb.customer_bridge,
      --Label PS Migration: Only Migration
      case
        when sc."PS Migration: Classification" in ('downgrade - migration', 'Downsell - migration') then '-'
        when sc."PS Migration: Classification" in ('Cross-sell - migration', 'upsell - migration') then '+'
        else null
      end as "PS Migration Movement Final Classification",
      --Classify the CB Level Movements
      case
        when cb.customer_arr_change_ccfx > 0 then '+'
        when cb.customer_arr_change_ccfx < 0 then '-'
        else null
      end as "Movement Type-CB"
    from sandbox.churn_migration_classifiers_ps sc
      left join customer_bridge cb on cb.evaluation_period = sc.evaluation_period
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
  ),
  initial_table_16 as (
    select sc.*,
      dt4."Sum of Positive or Negative Movements-CB"
    from initial_table_15 sc
      left join distinct_table_4 dt4 on sc.mcid = dt4.mcid
      and sc.evaluation_period = dt4.evaluation_period
      and sc.currency_code = dt4.currency_code
      and sc."PS Migration Movement Final Classification" = dt4."PS Migration Movement Final Classification"
      and sc."PS Migration: Rolled Up Amount" = dt4."PS Migration: Rolled Up Amount"
  ),
  initial_table_17 as (
    select it16.*,
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
    select it17.*,
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
    it18.*,
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