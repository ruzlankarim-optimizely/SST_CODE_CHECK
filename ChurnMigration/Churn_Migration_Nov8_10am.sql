drop table if exists sandbox.churn_migration_fk;
create table sandbox.churn_migration_fk as 
(
with initial_table as 
(
select 
	*, 
	case 
		when current_product_family in ('Non-Recurring: Perpetual License','Recurring: Cloud: Other Bookings: Other Bookings') then 'Legacy'
		when current_product_family in ('Recurring: Cloud: Commerce Cloud: B2C Commerce (incl. Headless)','Recurring: Cloud: Commerce Cloud: B2B Commerce (incl. Headless)','Recurring: Cloud: Content Cloud: Content PaaS') then 'Named'
	end as current_product_family_class, 
	case 
		when prior_product_family in ('Non-Recurring: Perpetual License','Recurring: Cloud: Other Bookings: Other Bookings') then 'Legacy'
		when prior_product_family in ('Recurring: Cloud: Commerce Cloud: B2C Commerce (incl. Headless)','Recurring: Cloud: Commerce Cloud: B2B Commerce (incl. Headless)','Recurring: Cloud: Content Cloud: Content PaaS') then 'Named'
	end as prior_product_family_class
from 
	ryzlan.sst_pb_temp
)

, 	initial_table_2 as 
(
select 
	*, 
	--Did a customer downgrade a legacy product family in the current snapshot date 
 	case 
		when current_product_family_class = 'Legacy' and product_arr_change_ccfx < 0 and prior_period_product_arr_usd_ccfx > 0 and current_period_product_arr_usd_ccfx > 0 
			then 1 else 0 
		end as "Downgraded a Legacy Product in Current Date", 
	--Did a customer churn a legacy product family in the current snapshot date 
	case 
		when prior_product_family_class = 'Legacy' and product_arr_change_ccfx< 0 and prior_period_product_arr_usd_ccfx > 0 and current_period_product_arr_usd_ccfx = 0 
			then 1 else 0 
		end as "Churned a Legacy Product in Current Date", 
	--Did the customer add a named product in the current snapshot date 
	case 
		when current_product_family_class = 'Named' and product_arr_change_ccfx > 0 and prior_period_product_arr_usd_ccfx = 0 and current_period_product_arr_usd_ccfx > 0 
			then 1 else 0 
		end as "Added a Named Product in Current Date", 
	--Did the customer increase a named product in the current snapshot date 
	case 
		when current_product_family_class = 'Named' and product_arr_change_ccfx> 0 and prior_period_product_arr_usd_ccfx > 0 and current_period_product_arr_usd_ccfx > 0 
			then 1 else 0 
		end as "Increased a Named Product in Current Date", 
	--Do they have a named product in current snapshot period with ARR > 0 
	case 
		when current_product_family_class = 'Named' and current_period_product_arr_usd_ccfx > 0 
			then 1 else 0 
		end as "Named Product in Current Date with ARR", 
	--Did they have a legacy product in the prior snapshot period 
	case 
		when prior_product_family_class = 'Legacy' and prior_period_product_arr_usd_ccfx > 0 
			then 1 else 0 
		end as "Legacy Product in Previous Date with ARR"
	from 
		initial_table 
)

,	initial_table_3 as
(
select 
	*, 
--If a customer churned or downgraded a legacy product family & they had a named product in the current snapshot period >0 ARR we would classify the churn / downgrade as migration
case 
	--ai) 
	when 
		"Downgraded a Legacy Product in Current Date" = 1 --Downgraded a legacy product 
		and 
		(sum("Named Product in Current Date with ARR") over(partition by mcid, evaluation_period)) > 0 -- have named product in current date with ARR > 0 
	then 'downgrade - migration'
	--ai) 
	when 
		"Churned a Legacy Product in Current Date" = 1 --Churned a legacy product 
		and 
		(sum("Named Product in Current Date with ARR") over(partition by mcid, evaluation_period)) > 0 -- have named product in current date with ARR > 0 
	then 'churn - migration'
	--bi) 
	when 
		"Added a Named Product in Current Date" = 1 --Added a named product in the current snapshot date 
		and 
		(
		(sum("Downgraded a Legacy Product in Current Date") over(partition by mcid, evaluation_period)) > 0
		or 
		(sum("Churned a Legacy Product in Current Date") over(partition by mcid, evaluation_period)) > 0
		) -- Either churned or downgraded a legacy product in the current snapshot date 
	then 'crosssell - migration'
	--bii) 
	when 
		"Increased a Named Product in Current Date" = 1 --Increased a named product in the current snapshot date 
		and 
		(
		(sum("Downgraded a Legacy Product in Current Date") over(partition by mcid, evaluation_period)) > 0
		or 
		(sum("Churned a Legacy Product in Current Date") over(partition by mcid, evaluation_period)) > 0
		) -- Either churned or downgraded a legacy product in the current snapshot date 
	then 'upsell - migration'
--If a customer increased or added a named product & they had a legacy product in the prior snapshot period > 0 ARR we would classify the movement as migration
	--ci)
	when 
		"Added a Named Product in Current Date" = 1 --Added a named product in the current snapshot date
		and 
		(sum("Legacy Product in Previous Date with ARR") over(partition by mcid, evaluation_period)) > 0 -- had a legacy product in previous snapshot date with ARR > 0 
	then 'crosssell - migration'
	--cii) 
	when 
		"Increased a Named Product in Current Date"  = 1 --Added a named product in the current snapshot date
		and 
		(sum("Legacy Product in Previous Date with ARR") over(partition by mcid, evaluation_period)) > 0 -- had a legacy product in previous snapshot date with ARR > 0 
	then 'upsell - migration'
	--di) 
	when 
		"Churned a Legacy Product in Current Date" = 1 --Churned a legacy product from prior snapshot date 
		and 
		(
		(sum("Added a Named Product in Current Date" ) over(partition by mcid, evaluation_period)) > 0
		or 
		(sum("Increased a Named Product in Current Date") over(partition by mcid, evaluation_period)) > 0
		) -- Either Added or Increased a named product in the current snapshot date 
	then 'churn - migration'
	--dii) 
	when 
		"Downgraded a Legacy Product in Current Date" = 1 --Downgraded a legacy product from prior snapshot date 
		and 
		(
		(sum("Added a Named Product in Current Date" ) over(partition by mcid, evaluation_period)) > 0
		or 
		(sum("Increased a Named Product in Current Date") over(partition by mcid, evaluation_period)) > 0
		) -- Either Added or Increased a named product in the current snapshot date 
	then 'downgrade - migration'
end as "Movement Classification"
from 
	initial_table_2 
)

--Join PG Bridge here to get PG movements and PS information 

,	initial_table_4 as 
(
select 
	it3.evaluation_period,	
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
	it3."Downgraded a Legacy Product in Current Date",	
	it3."Churned a Legacy Product in Current Date",	
	it3."Added a Named Product in Current Date",	
	it3."Increased a Named Product in Current Date",
	it3."Named Product in Current Date with ARR",	
	it3."Legacy Product in Previous Date with ARR",
	it3."Movement Classification",
	case 
		when it3."Movement Classification" is not null and it3.product_arr_change_ccfx> 0 then '+'
		when it3."Movement Classification" is not null and it3.product_arr_change_ccfx< 0 then '-'
		else null 
	end as "Movement Type-PF", 
	--PG Information 
	rt.product_arr_change_ccfx as pg_arr_change, 
	rt.product_bridge as pg_bridge, 
	--Bring in the product solution columns as well -- to roll it up on the PS level 
	rt.current_product_solution,
	rt.prior_product_solution
from 
	initial_table_3 it3
left join 
	ryzlan.sst_pb_pg_temp rt 
		on 
			it3.evaluation_period = rt.evaluation_period
			and 
			coalesce(it3.prior_product_group, it3.current_product_group) = coalesce(rt.current_product_group, rt.prior_product_group)
			and 
			it3.mcid = rt.mcid
)

,	initial_table_5 as 
(
select 
	evaluation_period,	
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
	"Downgraded a Legacy Product in Current Date",	
	"Churned a Legacy Product in Current Date",	
	"Added a Named Product in Current Date",
	"Increased a Named Product in Current Date",	
	"Named Product in Current Date with ARR",	
	"Legacy Product in Previous Date with ARR",	
	"Movement Classification",	
	"Movement Type-PF",	
	pg_arr_change,	
	pg_bridge,	
	current_product_solution,	
	prior_product_solution, 
	--
	case 
		when pg_arr_change > 0 then '+'
		when pg_arr_change < 0 then '-'
		else null 
	end as "Movement Type-PG"
from 
	initial_table_4
)

	
,	initial_table_6 as 
(
select 
	evaluation_period,	
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
	"Downgraded a Legacy Product in Current Date",	
	"Churned a Legacy Product in Current Date",	
	"Added a Named Product in Current Date",
	"Increased a Named Product in Current Date",	
	"Named Product in Current Date with ARR",	
	"Legacy Product in Previous Date with ARR",	
	"Movement Classification",	
	"Movement Type-PF",	
	"Movement Type-PG",
	--What is the PG movement? + or is it -
	--if neg, look back at pf movements and sum all - migration movements
	case 
		when "Movement Type-PG" = '-' then 
			sum(product_arr_change_ccfx) filter(where "Movement Type-PF" = '-') over(partition by mcid, evaluation_period, coalesce(current_product_group,prior_product_group))
		when "Movement Type-PG" = '+' then 
			sum(product_arr_change_ccfx) filter(where "Movement Type-PF" = '+') over(partition by mcid, evaluation_period, coalesce(current_product_group,prior_product_group))
		else null  
	end as "Sum of Positive or Negative Movements-PG", 
	pg_arr_change,	
	pg_bridge,	
	current_product_solution,	
	prior_product_solution
from 	
	initial_table_5
)

,	initial_table_7 as 
(
select 
	evaluation_period,	
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
	"Downgraded a Legacy Product in Current Date",	
	"Churned a Legacy Product in Current Date",	
	"Added a Named Product in Current Date",
	"Increased a Named Product in Current Date",	
	"Named Product in Current Date with ARR",	
	"Legacy Product in Previous Date with ARR",	
	"Movement Classification",	
	"Movement Type-PF",	
	"Movement Type-PG",
	"Sum of Positive or Negative Movements-PG", 
	pg_arr_change, 
	pg_bridge, 
	--if neg take the max (if pos take min) between the two PG movement and sum and tag the PG bridge movement as migration 
	case 
		when "Sum of Positive or Negative Movements-PG" is not null and "Movement Type-PG" = '-' then 
			greatest(pg_arr_change, "Sum of Positive or Negative Movements-PG") 
		when "Sum of Positive or Negative Movements-PG" is not null and "Movement Type-PG" = '+' then 
			least(pg_arr_change, "Sum of Positive or Negative Movements-PG")
		else null 
	end as "Min/Max PF Level movement", 
	--Bring in the product solution columns as well -- to roll it up on the PS level 
	current_product_solution,
	prior_product_solution
from 
	initial_table_6
)

,	initial_table_8 as 
(
select 
	evaluation_period,	
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
	"Downgraded a Legacy Product in Current Date",	
	"Churned a Legacy Product in Current Date",	
	"Added a Named Product in Current Date",
	"Increased a Named Product in Current Date",	
	"Named Product in Current Date with ARR",	
	"Legacy Product in Previous Date with ARR",	
	"Movement Classification",	
	"Movement Type-PF",	
	"Movement Type-PG",
	"Sum of Positive or Negative Movements-PG", 
	pg_arr_change, 
	pg_bridge, 
	--if neg take the max (if pos take min) between the two PG movement and sum and tag the PG bridge movement as migration 
	"Min/Max PF Level movement", 
	case 
	--Positive 
		when "Movement Type-PG" = '+' and "Movement Type-PF" is not null and "Min/Max PF Level movement" >= pg_arr_change then "Min/Max PF Level movement"
		when "Movement Type-PG" = '+' and "Movement Type-PF" is not null and "Min/Max PF Level movement" < pg_arr_change then "Min/Max PF Level movement"
		when "Movement Type-PG" = '-' and "Movement Type-PF" is not null and "Min/Max PF Level movement" <= pg_arr_change then "Min/Max PF Level movement"
		when "Movement Type-PG" = '-' and "Movement Type-PF" is not null and "Min/Max PF Level movement" > pg_arr_change then "Min/Max PF Level movement"
	end as "PG Migration: Rolled Up Amount", 
	case 
		when "Movement Type-PG" = '+' and "Movement Type-PF" is not null and "Min/Max PF Level movement" < pg_arr_change then pg_arr_change-"Min/Max PF Level movement"
		when "Movement Type-PG" = '-' and "Movement Type-PF" is not null and "Min/Max PF Level movement" > pg_arr_change then pg_arr_change-"Min/Max PF Level movement"
		else null 
	end as "PG Leftover: Rolled Up Amount", 
	--Bring in the product solution columns as well -- to roll it up on the PS level 
	current_product_solution,
	prior_product_solution
from 
	initial_table_7 
)

,	initial_table_9 as 
(
select 
	evaluation_period,	
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
	"Downgraded a Legacy Product in Current Date",	
	"Churned a Legacy Product in Current Date",	
	"Added a Named Product in Current Date",
	"Increased a Named Product in Current Date",	
	"Named Product in Current Date with ARR",	
	"Legacy Product in Previous Date with ARR",	
	"Movement Classification",	
	"Movement Type-PF",	
	"Movement Type-PG",
	"Sum of Positive or Negative Movements-PG", 
	pg_arr_change, 
	pg_bridge, 
	--if neg take the max (if pos take min) between the two PG movement and sum and tag the PG bridge movement as migration 
	"Min/Max PF Level movement", 
	--Positive 
	"PG Migration: Rolled Up Amount", 
	"PG Leftover: Rolled Up Amount", 
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
from 
	initial_table_8
)

--Now join to product solution bridge 
, 	initial_table_10 as 
(
select 
	cmp.evaluation_period,	
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
	cmp."Downgraded a Legacy Product in Current Date",	
	cmp."Churned a Legacy Product in Current Date",	
	cmp."Added a Named Product in Current Date",
	cmp."Increased a Named Product in Current Date",	
	cmp."Named Product in Current Date with ARR",	
	cmp."Legacy Product in Previous Date with ARR",	
	cmp."Movement Classification",	
	cmp."Movement Type-PF",	
	cmp."Movement Type-PG",
	cmp."Sum of Positive or Negative Movements-PG", 
	cmp.pg_arr_change, 
	cmp.pg_bridge, 
	cmp."Min/Max PF Level movement",	
	cmp."PG Migration: Rolled Up Amount",	
	cmp."PG Migration: Classification",	
	cmp."PG Leftover: Rolled Up Amount",	
	cmp."PG Leftover: Classification", 
	cmp.current_product_solution,
	cmp.prior_product_solution, 
	rst.product_arr_change_ccfx as product_arr_change_ccfx_ps, 
	--Label PG Migration: Only Migration 
	case 
		when "PG Migration: Classification" in ('downgrade - migration','churn - migration') then '-'
		when "PG Migration: Classification" in ('crosssell - migration','upsell - migration') then '+'
		else null 
	end as "PG Migration Movement Final Classification", 
	--Classify the PS Level Movements 
	case 
		when rst.product_arr_change_ccfx > 0 then '+'
		when rst.product_arr_change_ccfx < 0 then '-'
		else null 
	end as "Movement Type-PS"
from 
	initial_table_9 cmp 
left join 
	ryzlan.sst_ps_temp rst 
		on 
			cmp.evaluation_period = rst.evaluation_period 
			and 
			cmp.mcid = rst.mcid 
			and 
			coalesce(cmp.current_product_solution, cmp.prior_product_solution) = coalesce(rst.current_product_solution, rst.prior_product_solution)
)

,	initial_table_11 as 
(
select 
	evaluation_period,	
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
	"Downgraded a Legacy Product in Current Date",	
	"Churned a Legacy Product in Current Date",	
	"Added a Named Product in Current Date",	
	"Increased a Named Product in Current Date",
	"Named Product in Current Date with ARR",	
	"Legacy Product in Previous Date with ARR",	
	"Movement Classification",	
	"Movement Type-PF",	
	"Movement Type-PG",	
	"Sum of Positive or Negative Movements-PG",	
	pg_arr_change,	
	pg_bridge,	
	"Min/Max PF Level movement",
	"PG Migration: Rolled Up Amount",	
	"PG Migration: Classification",	
	"PG Leftover: Rolled Up Amount",	
	"PG Leftover: Classification",	
	"PG Migration Movement Final Classification", 
	current_product_solution,	
	prior_product_solution,	
	product_arr_change_ccfx_ps,	
	"Movement Type-PS", 
	case 
		when "Movement Type-PS" = '-' then 
			sum(PG Migration: Rolled Up Amount) filter(where "PG Migration Movement Final Classification" = '-') over(partition by mcid, evaluation_period, coalesce(current_product_solution,prior_product_solution))
		when "Movement Type-PS" = '+' then 
			sum(PG Migration: Rolled Up Amount) filter(where "PG Migration Movement Final Classification" = '+') over(partition by mcid, evaluation_period, coalesce(current_product_solution,prior_product_solution))
else null  
end as "Sum of Positive or Negative Movements-PS"
from 
	initial_table_10 
)

select 
	evaluation_period,	
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
	"Downgraded a Legacy Product in Current Date",	
	"Churned a Legacy Product in Current Date",	
	"Added a Named Product in Current Date",	
	"Increased a Named Product in Current Date",
	"Named Product in Current Date with ARR",	
	"Legacy Product in Previous Date with ARR",	
	"Movement Classification",	
	"Movement Type-PF",	
	"Movement Type-PG",	
	"Sum of Positive or Negative Movements-PG",	
	pg_arr_change,	
	pg_bridge,	
	"Min/Max PF Level movement",
	"PG Migration: Rolled Up Amount",	
	"PG Migration: Classification",	
	"PG Leftover: Rolled Up Amount",	
	"PG Leftover: Classification",	
	"PG Migration Movement Final Classification", 
	current_product_solution,	
	prior_product_solution,	
	product_arr_change_ccfx_ps,	
	"Movement Type-PS", 
	"Sum of Positive or Negative Movements-PS", 
	--if neg take the max (if pos take min) between the two PG movement and sum and tag the PS bridge movement as migration 
	case 
		when "Sum of Positive or Negative Movements-PS" is not null and "Movement Type-PS" = '-' then 
			greatest(product_arr_change_ccfx_ps, "Sum of Positive or Negative Movements-PS") 
		when "Sum of Positive or Negative Movements-PS" is not null and "Movement Type-PS" = '+' then 
			least(product_arr_change_ccfx_ps, "Sum of Positive or Negative Movements-PS")
		else null 
end as "Min/Max PG Level movement"
from 
	initial_table_11 
)


select 
	evaluation_period,	
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
	"Downgraded a Legacy Product in Current Date",	
	"Churned a Legacy Product in Current Date",	
	"Added a Named Product in Current Date",	
	"Increased a Named Product in Current Date",
	"Named Product in Current Date with ARR",	
	"Legacy Product in Previous Date with ARR",	
	"Movement Classification",	
	"Movement Type-PF",	
	"Movement Type-PG",	
	"Sum of Positive or Negative Movements-PG",	
	pg_arr_change,	
	pg_bridge,	
	"Min/Max PF Level movement",
	"PG Migration: Rolled Up Amount",	
	"PG Migration: Classification",	
	"PG Leftover: Rolled Up Amount",	
	"PG Leftover: Classification",	
	"PG Migration Movement Final Classification", 
	current_product_solution,	
	prior_product_solution,	
	product_arr_change_ccfx_ps,	
	"Movement Type-PS", 
	"Sum of Positive or Negative Movements-PS", 
	--if neg take the max (if pos take min) between the two PG movement and sum and tag the PS bridge movement as migration 
 	"Min/Max PG Level movement", 
 	case 
	--Positive 
		when "Movement Type-PS" = '+' and "PG Migration Movement Final Classification" is not null and "Min/Max PG Level movement" >= product_arr_change_ccfx_ps then "Min/Max PG Level movement"
		when "Movement Type-PS" = '+' and "PG Migration Movement Final Classification" is not null and "Min/Max PG Level movement" < product_arr_change_ccfx_ps then "Min/Max PG Level movement"
		when "Movement Type-PS" = '-' and "PG Migration Movement Final Classification" is not null and "Min/Max PG Level movement" <= product_arr_change_ccfx_ps then "Min/Max PG Level movement"
		when "Movement Type-PS" = '-' and "PG Migration Movement Final Classification" is not null and "Min/Max PG Level movement" > product_arr_change_ccfx_ps then "Min/Max PG Level movement"
	end as "PS Migration: Rolled Up Amount", 
	case 
		when "Movement Type-PS" = '+' and "PG Migration Movement Final Classification" is not null and "Min/Max PG Level movement" < product_arr_change_ccfx_ps then product_arr_change_ccfx_ps-"Min/Max PG Level movement"
		when "Movement Type-PS" = '-' and "PG Migration Movement Final Classification" is not null and "Min/Max PG Level movement" > product_arr_change_ccfx_ps then product_arr_change_ccfx_ps-"Min/Max PG Level movement"
		else null 
	end as "PS Leftover: Rolled Up Amount"
from 
	sandbox.churn_migration_fk 
where 
	mcid = '351340c9-b2e4-e411-9afb-0050568d2da8'
and 
	evaluation_period = '2019M06'
	
	
	
	
select 
	cmp.evaluation_period,	
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
	cmp."Downgraded a Legacy Product in Current Date",	
	cmp."Churned a Legacy Product in Current Date",	
	cmp."Added a Named Product in Current Date",
	cmp."Increased a Named Product in Current Date",	
	cmp."Named Product in Current Date with ARR",	
	cmp."Legacy Product in Previous Date with ARR",	
	cmp."Movement Classification",	
	cmp."Movement Type-PF",	
	cmp."Movement Type-PG",
	cmp."Sum of Positive or Negative Movements-PG", 
	cmp.pg_arr_change, 
	cmp.pg_bridge, 
	cmp."Min/Max PF Level movement",	
	cmp."PG Migration: Rolled Up Amount",	
	cmp."PG Migration: Classification",	
	cmp."PG Leftover: Rolled Up Amount",	
	cmp."PG Leftover: Classification", 
	cmp.current_product_solution,
	cmp.prior_product_solution, 
	rst.product_arr_change_ccfx as product_arr_change_ccfx_ps
from 
	sandbox.churn_migration_fk cmp 
left join 
	ryzlan.sst_ps_temp rst 
		on 
			cmp.evaluation_period = rst.evaluation_period 
			and 
			cmp.mcid = rst.mcid 
			and 
			coalesce(cmp.current_product_solution, cmp.prior_product_solution) = coalesce(rst.current_product_solution, rst.prior_product_solution)



--Now join this to ruzlan's table 
with joined_table as 
(
select 
	coalesce(rff.mcid, cmp.mcid) as "Formatted MCID", 
	coalesce(rff.evaluation_period, cmp.evaluation_period) as "Formatted Evaluation Period", 
	rff.mcid as ruz_mcid, 
	rff.evaluation_period as ruz_evaluation_period,  
	rff.current_product_group as ruz_current_product_group, 
	rff.prior_product_group as ruz_prior_product_group, 
	rff.pg_bridge as ruz_pg_bridge,  
	rff.pg_num as ruz_pg_num, 
	rff.pg_num_lcu as ruz_pg_num_lcu,
	rff.pf_pos_num as ruz_pf_pos_num,
	rff.pf_pos_lcu_num as ruz_pf_pos_lcu_num, 
	rff.pf_neg_num as ruz_pf_neg_num, 
	rff.pf_neg_lcu_num as ruz_pf_neg_lcu_num, 
	rff.just_migration_flag as ruz_just_migration_flag, 
	rff.maximun_negative_arr as ruz_maximun_negative_arr, 
	rff.maximun_negative_arr_lcu as ruz_maximun_negative_arr_lcu, 
	rff.minimum_positive_arr as ruz_minimum_positive_arr, 
	rff.minimum_positive_arr_lcu as ruz_minimum_positive_arr_lcu, 	
	rff.migration_marker as ruz_migration_marker,
	rff.default_value as ruz_default_value,
	rff.default_value_lcu as ruz_default_value_lcu,
	rff.migrated_value as ruz_migrated_value,
	rff.migrated_value_lcu as ruz_migrated_value_lcu,
	cmp.evaluation_period,	
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
	cmp."Downgraded a Legacy Product in Current Date",	
	cmp."Churned a Legacy Product in Current Date",	
	cmp."Added a Named Product in Current Date",
	cmp."Increased a Named Product in Current Date",	
	cmp."Named Product in Current Date with ARR",	
	cmp."Legacy Product in Previous Date with ARR",	
	cmp."Movement Classification",	
	cmp."Movement Type-PF",	
	cmp."Movement Type-PG",
	cmp."Sum of Positive or Negative Movements-PG", 
	cmp.pg_arr_change, 
	cmp.pg_bridge, 
	cmp."Min/Max PF Level movement",	
	cmp."PG Migration: Rolled Up Amount",	
	cmp."PG Migration: Classification",	
	cmp."PG Leftover: Rolled Up Amount",	
	cmp."PG Leftover: Classification", 
	cmp.current_product_solution,
	cmp.prior_product_solution

from 
	ryzlan.mig_main_filter_final rff 
full join 
	sandbox.churn_migration_fk cmp 
		on 
			rff.mcid = cmp.mcid 
			and 
			rff.evaluation_period = cmp.evaluation_period 
			and 
			coalesce(rff.current_product_group,	rff.prior_product_group) = coalesce(cmp.current_product_group, cmp.prior_product_group) 
) 


,	joined_table_2 as 
(
select 
	*, 
	--Do we agree on Spliting Migration 
	case 
		when sum(case when "PG Leftover: Classification" is not null and ruz_migration_marker = 'Just Migration' then 1 else 0 end) 
				over(partition by "Formatted MCID", "Formatted Evaluation Period") > 1 then 1 else 0 
		end as "Disagreement in Split Migration (1 if yes)", 
	--Do we agree on product groups that should be classified as migration? 
	case 
		when sum(case 
					when 
						(coalesce("PG Leftover: Classification","PG Migration: Classification") is not null and ruz_migration_marker is null)
					or 
						(coalesce("PG Leftover: Classification","PG Migration: Classification") is null and ruz_migration_marker is not null)
				then 1 else 0 end) 
				over(partition by "Formatted MCID", "Formatted Evaluation Period") > 1 then 1 else 0 
		end as "Disagreement Migration Classification (1 if yes)"
from 
	joined_table 
--where 
--	"Formatted MCID" = '03c56cf3-c42c-e411-9f63-0050568d2da8'
--and 
--	"Formatted Evaluation Period" = '2020M04'
)

select 
	*
from 
	joined_table_2
where 
	"Formatted MCID" = '035c17f2-7b31-e411-9f63-0050568d2da8'
and 
	"Formatted Evaluation Period" = '2022M01'
--	"Disagreement Migration Classification (1 if yes)" = 1 
--	or 
--	"Disagreement in Split Migration (1 if yes)" = 1

















with test as 
(
select 
	* 
from 
	ufdm_archive.sst_lcoked_09102023_1412
where 
	new_product is null 
--	and 
--	new_product_solution not ilike '%Exp%'
) 

select 
	distinct snapshot_date, 
	sum(arr) over(partition by snapshot_date) as sum_null 
from 
	test 
order by 
	snapshot_date 