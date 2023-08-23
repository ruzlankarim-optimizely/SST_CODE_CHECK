with tat_dates as 
(
select 
	distinct mcid,
	max(date_trunc('MONTH',snapshot_date)) over(partition by mcid, record_source) as "MAX Snapshot Date of TAT"
from 
--	ufdm.sst
--I use sst backup which has the record sources without sensitivity analysis or manual changes 
	sandbox.control_sst_before_manual_changes
where 
	record_source = 'sst_tat'
)

--select 
--	distinct mcid, 
--	snapshot_date, 
--	record_source 
--from 
--	sandbox.sst_recreate_backup
--where 
--	mcid = '10d0858f-9f42-dd11-93be-0018717a8c82'

--Start prepping UFDM for Join 
,	ufdm_arr_1a as 
(
select 
	*,
	case 
		when strpos(reverse(product_family), ':') = 0 then length(product_family)
		else strpos(reverse(product_family), ':')-1
	end as num_charac
from 
	sandbox_pd.arr
)

,	ufdm_arr_1b as 
(
select 
	mcid,
	snapshot_date,
	arr_usd_ccfx, 
	product_family, 
	trim(right(product_family, num_charac)) as product_family_ufdm
from 
	ufdm_arr_1a
--Take only Non-Fopti Data from UFDM ARR 
where 
--not 
--(
--date_trunc('month', snapshot_date) = '2022-01-01'::DATE
--                  AND product_family = 'Recurring: Cloud: Other Bookings: Campaign'
--)
	line_type not ILIKE '%Fopti%'
and arr_source not ilike '%GMBH overages%'

)

--select 
--	distinct arr_source 
--from 
--	sandbox_pd.arr
--where 
--	mcid = '5e479b1a-2251-e811-813c-70106fa51d21'
--and 
--	snapshot_date in ('2022-01-31')


,	ufdm_arr_1c as 
(
select 
	mcid,
	snapshot_date,
	arr_usd_ccfx, 
	case 
		when product_family = 'Recurring: Cloud: Content Cloud: Content PaaS' then 'Recurring: Cloud: Content Cloud: Content SaaS'
		else product_family 
	end as product_family 
from 
	ufdm_arr_1b
)  

--select 
--	*
--from 
--	ufdm_arr_1c
--where 
--	mcid = 	'50661331-d24e-e811-813c-70106fa6f451'
	
--This is the ufdm file with product family and arr data 
,	ufdm_arr_1 as 
(
select 
	distinct mcid as mcid_arr, 
	product_family as product_family_arr,
	snapshot_date, 
	date_trunc('MONTH',snapshot_date) as snapshot_date_arr, 
	sum(arr_usd_ccfx) over(partition by mcid, product_family, snapshot_date) as "Sum by Product Family and Date - UFDM ARR",
	sum(arr_usd_ccfx) over(partition by mcid, snapshot_date) as "Sum by MCID & Date - UFDM ARR"
from 	
	ufdm_arr_1c
)

--select 
--	*
--from 
--	ufdm_arr_1 
--where 
--	mcid_arr = '00855ad7-1ba5-45bc-b744-2c60ae82b5e1'


--We need min. date when the MCID starts
,	ufdm_arr_11 as 
(
select 
	mcid_arr,
	product_family_arr,
	snapshot_date_arr, 
	MIN(snapshot_date) filter(where "Sum by MCID & Date - UFDM ARR" > 0) over(partition by mcid_arr) as "Start Date in UFDM ARR",
	"Sum by Product Family and Date - UFDM ARR",
	"Sum by MCID & Date - UFDM ARR"
from 
	ufdm_arr_1
)


--select 
--	distinct product_family_arr 
--from 
--	ufdm_arr_11 
--where 
--	mcid_arr = '003463de-d300-df11-b498-0018717a8c82'

--Prepare the TAT data 
--Change Content PaaS to Content SaaS 
,	tat_0 as 
(
select 
	mcid, 
	date_trunc('MONTH',snapshot_date) as snapshot_date, 
	arr, 
	case 
		when product_family = 'Recurring: Cloud: Content Cloud: Content PaaS' then 'Recurring: Cloud: Content Cloud: Content SaaS'
		else product_family 
	end as product_family_tat 
from 
--I use sst backup which has the record sources without sensitivity analysis or manual changes 
	sandbox.control_sst_before_manual_changes
where 
	record_source = 'sst_tat'
and 
not 
(
date_trunc('month', snapshot_date) = '2021-12-01'::DATE
                  AND product_family = 'Recurring: Cloud: Other Bookings: Campaign'
)
and 
	overage_flag is distinct from 'Y'
)


--select 
--	*
--from 
--	ufdm.sst 
--where 
--	mcid = '5e479b1a-2251-e811-813c-70106fa51d21'
--and 
--	date_trunc('MONTH',snapshot_date) = '2021-12-01'
	

--select 
--	*
--from 
--	sandbox.control_sst_before_manual_changes
--where 
--	record_source = 'sst_tat'
--and
--	mcid = '5e479b1a-2251-e811-813c-70106fa51d21'

,	tat_1 as 
(
select 
	distinct mcid as mcid_tat, 
	product_family_tat as product_family_tat, 
	snapshot_date as snapshot_date_tat,
	sum(arr) over(partition by mcid, product_family_tat, snapshot_date) as "Sum by Product Family and Date - TAT",
	sum(arr) over(partition by mcid, snapshot_date) as "Sum by MICD & Date - TAT"
from 
	tat_0 
)

--select 
--	*
--from 
--	tat_1 
--where 
--	mcid_tat = '95a4a5fa-47f8-8e25-9b73-57e19bd1e791'

--Filter the TAT data only to have snapshot dates which are the last snapshot dates for TAT in SST 

,	tat_2 as 
(
select 
	t1.mcid_tat, 
	t1.product_family_tat, 
	t1.snapshot_date_tat,
	td."MAX Snapshot Date of TAT",
	t1."Sum by Product Family and Date - TAT"
from 
	tat_1 t1 
inner join 
	tat_dates td 
		on 
			t1.mcid_tat = td.mcid
			and 
			date_trunc('MONTH',t1.snapshot_date_tat) = date_trunc('MONTH',td."MAX Snapshot Date of TAT")
where 
	t1."Sum by Product Family and Date - TAT" > 0 or t1.product_family_tat is not null 
)

--select 
--	distinct mcid
--from 
--	tat_dates

--select 
--	*
--from 
--	tat_2
--where 
--	mcid_tat = '00ec9665-e386-18a4-2d14-40b5803bac2c'
	
--take the UFDM ARR table and only take the MCIDs that are present in TAT at transition date and take only the snapshot dates that are Transition Date + 1 month 
--This is the ufdm_arr_2 -- that needs to be joined to 
--We need to do an inner join on mcid and snapshot date.  

,	ufdm_arr_2 as 
(
select 
	ua1.mcid_arr, 
	ua1.product_family_arr,
	ua1.snapshot_date_arr, 
	ua1."Sum by Product Family and Date - UFDM ARR",
	ua1."Start Date in UFDM ARR"
from 
	ufdm_arr_11 ua1
inner join 
	tat_dates td
		on 
			ua1.mcid_arr = td.mcid 
			and 
			(ua1.snapshot_date_arr) = td."MAX Snapshot Date of TAT"+ interval '1 Month'
)

--select 
--	*
----	"MAX Snapshot Date of TAT"+ interval '1 Month'
--from 
--	ufdm_arr_2
--where 
--	mcid_arr = 'cccfcefe-1eaa-db11-8952-0018717a8c82'

---Now take the mcids which are only in ufdm arr for tat 

,	tat_3 as 
(
select 
	t2.mcid_tat, 
	t2.product_family_tat, 
	t2.snapshot_date_tat,
	t2."MAX Snapshot Date of TAT",
	t2."Sum by Product Family and Date - TAT"
from 
	tat_2 t2
where 
	t2.mcid_tat in 
	(
	select 
		distinct mcid_arr
	from 
		ufdm_arr_2 
	)
--Get Rid of Any PF that has 0 data 
and
	t2."Sum by Product Family and Date - TAT" > 0 
)

--select 
--	distinct mcid_tat
--from 
--	tat_2
--where 
--	mcid_tat = 'cccfcefe-1eaa-db11-8952-0018717a8c82'
	

,	combined_table_1 as 
(
select 
	coalesce(t2.mcid_tat, u1.mcid_arr) as "Combined MCID",
	coalesce(t2.product_family_tat, u1.product_family_arr) as "Combined Product Family",
	t2.mcid_tat, 
	t2.product_family_tat, 
	t2.snapshot_date_tat,
	t2."MAX Snapshot Date of TAT",
	t2."Sum by Product Family and Date - TAT",
	u1.mcid_arr, 
	u1.product_family_arr,
	u1.snapshot_date_arr, 
	u1."Sum by Product Family and Date - UFDM ARR",
	u1."Start Date in UFDM ARR"
from 
	tat_3 t2
full join 
	ufdm_arr_2 u1
		on 
			t2.mcid_tat = u1.mcid_arr
			and 
			t2.product_family_tat  = u1.product_family_arr
)

--select 
--	distinct "Combined MCID"
--from 
--	combined_table_1

,	combined_table_2 as 
(
select 
	"Combined MCID",
	"Combined Product Family",
	case 
		when "Combined Product Family" = product_family_tat and "Combined Product Family" = product_family_arr then 'Present in Both TAT and UFDM ARR'
		when "Combined Product Family" = product_family_tat and  product_family_arr is null then 'Present in TAT only - Loss of PF in Bridge'
		when product_family_tat is null and  "Combined Product Family" = product_family_arr then 'Present in UFDM ARR only - New PF in Bridge'
	end as "Presence of PF in 2 Tables", 
	coalesce("Sum by Product Family and Date - UFDM ARR",0)-coalesce("Sum by Product Family and Date - TAT",0) as "Difference between UFDM and TAT -- ARR", 
	mcid_tat, 
	product_family_tat, 
	snapshot_date_tat,
	"MAX Snapshot Date of TAT",
	"Sum by Product Family and Date - TAT",
	mcid_arr, 
	product_family_arr,
	snapshot_date_arr, 
	"Sum by Product Family and Date - UFDM ARR",
	"Start Date in UFDM ARR"
from 
	combined_table_1
)

,	combined_table_3 as 
(
select 
	"Combined MCID",
	"Combined Product Family",
	coalesce(snapshot_date_arr, (snapshot_date_tat + interval '1 Month'))::DATE as "Combined Date -- ARR", 
	"Difference between UFDM and TAT -- ARR", 
	"Presence of PF in 2 Tables", 
	case 
		when "Presence of PF in 2 Tables" = 'Present in Both TAT and UFDM ARR' and "Sum by Product Family and Date - TAT" > 1 and "Sum by Product Family and Date - UFDM ARR" > 1 and "Difference between UFDM and TAT -- ARR" >= 1 then 'Upsell'
		when "Presence of PF in 2 Tables" = 'Present in Both TAT and UFDM ARR' and "Sum by Product Family and Date - TAT" > 1 and "Sum by Product Family and Date - UFDM ARR" > 1 and "Difference between UFDM and TAT -- ARR" <= -1 then 'Partial Churn'
		when "Presence of PF in 2 Tables" = 'Present in Both TAT and UFDM ARR' and ABS("Difference between UFDM and TAT -- ARR") < 1 then 'Flat'
		when 
		--Churn case 1
			("Presence of PF in 2 Tables" = 'Present in TAT only - Loss of PF in Bridge' and "Difference between UFDM and TAT -- ARR" <= -1) 
			then 'Churn'
		--Churn case 2 
		when "Presence of PF in 2 Tables" = 'Present in Both TAT and UFDM ARR' and "Sum by Product Family and Date - TAT" > 1 and "Sum by Product Family and Date - UFDM ARR" < 1 
			then 'Churn' 
		--New case 1	
		when 
			("Presence of PF in 2 Tables" = 'Present in UFDM ARR only - New PF in Bridge' and "Difference between UFDM and TAT -- ARR" >= 1 )
			then 'New'
		--New case 2 
		when "Presence of PF in 2 Tables" = 'Present in Both TAT and UFDM ARR' and "Sum by Product Family and Date - TAT" < 1 and "Sum by Product Family and Date - UFDM ARR" > 1 
			then 'New' 
	end as "Product Bridge", 
	mcid_tat, 
	product_family_tat, 
	snapshot_date_tat,
	"MAX Snapshot Date of TAT",
	"Sum by Product Family and Date - TAT",
	mcid_arr, 
	product_family_arr,
	snapshot_date_arr, 
	"Sum by Product Family and Date - UFDM ARR",
	"Start Date in UFDM ARR"
from 
	combined_table_2 
)

,	combined_table_4 as 
(
select 
	"Combined MCID",
	"Combined Product Family",
	"Combined Date -- ARR", 
	"Presence of PF in 2 Tables", 
	"Difference between UFDM and TAT -- ARR", 
	"Product Bridge", 
	--Flag only flat customers
	case 
		when sum(case when "Product Bridge" = 'Flat' then 0 else 1 end) over(partition by "Combined MCID") = 0 then 1
		else 0 
	end as "Flat Customers Only (1 if yes)", 
	--Flag only Upsell/Partial Churn Customers 
	case 
		when sum(case when "Product Bridge" in ('Upsell','Partial Churn') then 0 else 1 end) over(partition by "Combined MCID") = 0 then 1
		else 0 
	end as "Upsell/Partial Customers Only (1 if yes)", 
	--Flag only New/Churn Customers 
	case 
		when sum(case when "Product Bridge" in ('New','Churn') then 0 else 1 end) over(partition by "Combined MCID") = 0 then 1
		else 0 
	end as "New/Churn Customers Only (1 if yes)", 
	mcid_tat, 
	product_family_tat, 
	snapshot_date_tat,
	"MAX Snapshot Date of TAT",
	"Sum by Product Family and Date - TAT",
	mcid_arr, 
	product_family_arr,
	snapshot_date_arr, 
	"Sum by Product Family and Date - UFDM ARR",
	"Start Date in UFDM ARR"
from 
	combined_table_3 
)

--Find out customers whose total ARR does not change but the ratio changes -- this can be taken care of by dragging the ratio
,	combined_table_5 as 
(
select 
	"Combined MCID",
	"Combined Product Family",
	"Combined Date -- ARR", 
	"Presence of PF in 2 Tables", 
	"Difference between UFDM and TAT -- ARR", 
	"Product Bridge", 
	--Flag only flat customers
	"Flat Customers Only (1 if yes)", 
	--Flag only Upsell/Partial Churn Customers 
	"Upsell/Partial Customers Only (1 if yes)", 
	--Flag only New/Churn Customers 
	"New/Churn Customers Only (1 if yes)", 
	--Flag customers who have -- ARR between the 2 tables is the same, PF Makeup is the same but Ratio is Different
	case 
		when 
			--same ARR 
			ABS(coalesce(sum("Sum by Product Family and Date - TAT") over(partition by "Combined MCID"),0) - coalesce(sum("Sum by Product Family and Date - UFDM ARR") over(partition by "Combined MCID"),0)) < 5
			and 
			--not a flat customer 
			"Flat Customers Only (1 if yes)" != 1 
			and 
			--product families present in both 
			sum(case when "Presence of PF in 2 Tables" in ('Present in Both TAT and UFDM ARR') then 0 else 1 end) over(partition by "Combined MCID") = 0
		then 1 
		else 0 
	end as "Same ARR & PF But Different Ratio (1 if yes)", 
	--Flag customers who have -- ARR between the 2 tables is the same, PF Makeup is the Different & Ratio is Different 
	case 
		when 
			--same ARR 
			ABS(coalesce(sum("Sum by Product Family and Date - TAT") over(partition by "Combined MCID"),0) - coalesce(sum("Sum by Product Family and Date - UFDM ARR") over(partition by "Combined MCID"),0)) < 5
			and 
			--not a flat customer 
			"Flat Customers Only (1 if yes)" != 1 
			and 
			--Different product families in both 
			sum(case when "Presence of PF in 2 Tables" in ('Present in Both TAT and UFDM ARR') then 0 else 1 end) over(partition by "Combined MCID") != 0
		then 1 
		else 0 
	end as "Same ARR & But Different PF and Ratio (1 if yes)", 
	--Identify Customers who have Different ARR and Different Makeup
	case 
		when 
			--Different ARR 
			ABS(coalesce(sum("Sum by Product Family and Date - TAT") over(partition by "Combined MCID"),0) - coalesce(sum("Sum by Product Family and Date - UFDM ARR") over(partition by "Combined MCID"),0)) > 5
			and 
			--not a flat customer 
			"Flat Customers Only (1 if yes)" != 1 
			and 
			--Different product families in both 
			sum(case when "Presence of PF in 2 Tables" in ('Present in Both TAT and UFDM ARR') then 0 else 1 end) over(partition by "Combined MCID") != 0
		then 1 
		else 0 
	end as "Different ARR & PF in Both Tables(1 if yes)", 
	--Identify Customers who have Different ARR and Same Makeup 
	case 
		when 
			--different ARR 
			ABS(coalesce(sum("Sum by Product Family and Date - TAT") over(partition by "Combined MCID"),0) - coalesce(sum("Sum by Product Family and Date - UFDM ARR") over(partition by "Combined MCID"),0)) > 5
			and 
			--not a flat customer 
			"Flat Customers Only (1 if yes)" != 1 
			and 
			--Same product families in both 
			sum(case when "Presence of PF in 2 Tables" in ('Present in Both TAT and UFDM ARR') then 0 else 1 end) over(partition by "Combined MCID") = 0
		then 1 
		else 0 
	end as "Different ARR But Same PF in Both Tables(1 if yes)", 
	mcid_tat, 
	product_family_tat, 
	snapshot_date_tat,
	"MAX Snapshot Date of TAT",
	"Sum by Product Family and Date - TAT",
	mcid_arr, 
	product_family_arr,
	snapshot_date_arr, 
	"Sum by Product Family and Date - UFDM ARR",
	"Start Date in UFDM ARR"
from 
	combined_table_4 
--where 
--	"Combined MCID" = '50661331-d24e-e811-813c-70106fa6f451'
)

--Find out if the churn was true churn or churn due to different source. Join it to previous month's data 

--Make a sub-table of UFDM ARR with previous month's data 

,	ufdm_arr_2b as 
(
select 
	ua1.mcid_arr as mcid_prev_month, 
	ua1.product_family_arr as pf_arr_prev_month,
	ua1.snapshot_date_arr as snapshot_date_arr_pmonth, 
	ua1."Sum by Product Family and Date - UFDM ARR" as "Sum by Product Family and Date - UFDM ARR Prev. Month"
from 
	ufdm_arr_1 ua1
inner join 
	tat_dates td
		on 
			ua1.mcid_arr = td.mcid 
			and 
			--use same snapshot date 
			(ua1.snapshot_date_arr) = td."MAX Snapshot Date of TAT"
)

--Join this to the combined table above on TAT mcid, date and product family 

,	combined_table_6 as 
(
select 
	ct5."Combined MCID" as "Combined MCID after Transition",
	coalesce(ct5.mcid_tat, u2b.mcid_prev_month) as "Combined MCID Before Transition",
	ct5."Combined Date -- ARR" as "Combined Date After Transition", 
	coalesce(ct5.snapshot_date_tat, u2b.snapshot_date_arr_pmonth) as "Combine Date Before Transition", 
	ct5."Presence of PF in 2 Tables", 
	ct5."Difference between UFDM and TAT -- ARR", 
	ct5."Product Bridge", 
	--Flag only flat customers
	ct5."Flat Customers Only (1 if yes)", 
	--Flag only Upsell/Partial Churn Customers 
	ct5."Upsell/Partial Customers Only (1 if yes)", 
	--Flag only New/Churn Customers 
	ct5."New/Churn Customers Only (1 if yes)", 
	--Flag Customers who have Same ARR and PF but Ratio is Different
	ct5."Same ARR & PF But Different Ratio (1 if yes)", 
	--Flag Customers who have Same ARR but different PF and Ratio 
	"Same ARR & But Different PF and Ratio (1 if yes)",
	--Flag Customers who have Different ARR and PF in Both Tables 
	ct5."Different ARR & PF in Both Tables(1 if yes)",
	--Flag Customers who have Different ARR but Same PF Makeup in both Tables 
	ct5."Different ARR But Same PF in Both Tables(1 if yes)", 
	ct5.mcid_tat, 
	ct5.product_family_tat, 
	ct5.snapshot_date_tat,
	ct5."MAX Snapshot Date of TAT",
	ct5."Sum by Product Family and Date - TAT",
	ct5.mcid_arr, 
	ct5.product_family_arr,
	ct5.snapshot_date_arr, 
	ct5."Sum by Product Family and Date - UFDM ARR",
	ct5."Start Date in UFDM ARR",
	u2b.mcid_prev_month, 
	u2b.pf_arr_prev_month,
	u2b.snapshot_date_arr_pmonth, 
	u2b."Sum by Product Family and Date - UFDM ARR Prev. Month"
from 
	combined_table_5 ct5
full join 
	ufdm_arr_2b u2b
		on 
			ct5.mcid_tat = u2b.mcid_prev_month
			and 
			ct5.product_family_tat  = u2b.pf_arr_prev_month
			and 
			ct5.snapshot_date_tat	= u2b.snapshot_date_arr_pmonth
)

--Start calculating true churn 

,	combined_table_7 as 
(
select 
	"Combined MCID after Transition",
	"Combined MCID Before Transition",
	coalesce("Combined MCID after Transition", "Combined MCID Before Transition") as "Combined MCID Before and After Transition", 
	"Combined Date After Transition", 
	"Combine Date Before Transition", 
	"Presence of PF in 2 Tables", 
	"Difference between UFDM and TAT -- ARR", 
	ABS("Difference between UFDM and TAT -- ARR") as "Absolute Diff. between UFDM and TAT--ARR", 
	"Product Bridge", 
	--Flag only flat customers
	"Flat Customers Only (1 if yes)", 
	--Flag only Upsell/Partial Churn Customers 
	"Upsell/Partial Customers Only (1 if yes)", 
	--Flag only New/Churn Customers 
	"New/Churn Customers Only (1 if yes)", 
	--Flag Customers who have Same ARR and PF but Ratio is Different
	"Same ARR & PF But Different Ratio (1 if yes)", 
	--Flag Customers who have Same ARR but different PF and Ratio 
	"Same ARR & But Different PF and Ratio (1 if yes)",
	--Flag Customers who have Different ARR and PF in Both Tables 
	"Different ARR & PF in Both Tables(1 if yes)",
	--Flag Customers who have Different ARR but Same PF Makeup in both Tables 
	"Different ARR But Same PF in Both Tables(1 if yes)", 
	mcid_tat, 
	product_family_tat, 
	snapshot_date_tat,
	"MAX Snapshot Date of TAT",
	"Sum by Product Family and Date - TAT",
	mcid_arr, 
	product_family_arr,
	snapshot_date_arr, 
	"Sum by Product Family and Date - UFDM ARR",
	"Start Date in UFDM ARR",
	mcid_prev_month, 
	pf_arr_prev_month,
	snapshot_date_arr_pmonth, 
	"Sum by Product Family and Date - UFDM ARR Prev. Month",
	--Identify True Bridge Customers 
	--If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match 	
	case 
		when 
		sum 
			(
			case 
			when pf_arr_prev_month = product_family_tat --Same Product family in [re]
				and 
			abs("Sum by Product Family and Date - UFDM ARR Prev. Month"-"Sum by Product Family and Date - TAT") < 1 --same ARR 
			then 0 else 1 
			end
			)
		over(partition by "Combined MCID after Transition", "Combined Date After Transition") = 0 then 1
		else 0 
		end as "No Diff between UFDM and TAT Prev. Month (1 if yes)"
from 
	combined_table_6
)
	
--Join it to Campaign Welcome and Unbundling 


,	campaign AS (
        SELECT
            distinct mcid as mcid_camp, date_trunc('MONTH',snapshot_date) as snapshot_date_camp, '1' AS campaign_overages
        FROM
            ufdm.sst
        WHERE
            record_source = 'ufdm_campaigns_dec2021'
            AND overage_flag = 'Y'
        ORDER BY 
            1,2
    )
    
    
,   welcome AS (
        SELECT 
            distinct mcid as mcid_welc, date_trunc('MONTH',snapshot_date) as snapshot_date_welc, '1' AS welcome_historicals
        FROM
            ufdm.arr
        WHERE 
            reference_number = 'Welcome Historicals'
        ORDER BY 
            1,2
    )
    
    
,    unbundling AS (
        SELECT
            distinct mcid as mcid_unbund, date_trunc('MONTH',snapshot_date) as snapshot_date_unbund, '1' AS unbundling
        FROM
            ufdm_blue.monthly_metrics
        WHERE
            modified_comments ILIKE '%unbundling%'
        ORDER BY
            1,2
    )
	
---Now left join all the flags to combined_table_7
    
,	combined_table_8 as 
(
select 
	ct7."Combined MCID after Transition",
	ct7."Combined MCID Before Transition",
	ct7."Combined MCID Before and After Transition", 
	ct7."Combined Date After Transition", 
	ct7."Combine Date Before Transition", 
	ct7."Presence of PF in 2 Tables", 
	ct7."Difference between UFDM and TAT -- ARR", 
	ct7."Absolute Diff. between UFDM and TAT--ARR", 
	ct7."Product Bridge", 
	--Flag only flat customers
	ct7."Flat Customers Only (1 if yes)", 
	--Flag only Upsell/Partial Churn Customers 
	ct7."Upsell/Partial Customers Only (1 if yes)", 
	--Flag only New/Churn Customers 
	ct7."New/Churn Customers Only (1 if yes)", 
	--Flag Customers who have Same ARR and PF but Ratio is Different
	ct7."Same ARR & PF But Different Ratio (1 if yes)", 
	--Flag Customers who have Same ARR but different PF and Ratio 
	ct7."Same ARR & But Different PF and Ratio (1 if yes)",
	--Flag Customers who have Different ARR and PF in Both Tables 
	ct7."Different ARR & PF in Both Tables(1 if yes)",
	--Flag Customers who have Different ARR but Same PF Makeup in both Tables 
	ct7."Different ARR But Same PF in Both Tables(1 if yes)", 
	ct7.mcid_tat, 
	ct7.product_family_tat, 
	ct7.snapshot_date_tat,
	ct7."MAX Snapshot Date of TAT",
	ct7."Sum by Product Family and Date - TAT",
	ct7.mcid_arr, 
	ct7.product_family_arr,
	ct7.snapshot_date_arr, 
	ct7."Sum by Product Family and Date - UFDM ARR",
	ct7."Start Date in UFDM ARR",
	ct7.mcid_prev_month, 
	ct7.pf_arr_prev_month,
	ct7.snapshot_date_arr_pmonth, 
	ct7."Sum by Product Family and Date - UFDM ARR Prev. Month",
	--Identify True Bridge Customers 
	--If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match 	
	ct7."No Diff between UFDM and TAT Prev. Month (1 if yes)",
	cap.campaign_overages as "Campaign Flag (1 if yes)",
	welc.welcome_historicals as "Wecome Flag (1 if yes)", 
	ubund.unbundling as "Unbundling (1 if yes)"
from 
	combined_table_7 ct7
left join 
	campaign cap 
		on 
			ct7.mcid_arr = cap.mcid_camp
			and 
			ct7.snapshot_date_arr = cap.snapshot_date_camp
left join 
	 welcome welc 
	 	on 
	 		ct7.mcid_arr = welc.mcid_welc 
			and 
			ct7.snapshot_date_arr = welc.snapshot_date_welc 
left join 
	unbundling ubund 
		on 
	 		ct7.mcid_arr = ubund.mcid_unbund
			and 
			ct7.snapshot_date_arr = ubund.snapshot_date_unbund
)

--Take customers who are not flat or true movement customers 
--End of Initial Bucket Analysis 

, 	table_invest_1 as 
(
select 
	ct8."Combined MCID after Transition",
	ct8."Combined MCID Before Transition",
	ct8."Combined MCID Before and After Transition", 
	ct8."Combined Date After Transition", 
	ct8."Combine Date Before Transition", 
	ct8."Presence of PF in 2 Tables", 
	ct8."Difference between UFDM and TAT -- ARR", 
	ct8."Absolute Diff. between UFDM and TAT--ARR", 
	ct8."Product Bridge", 
	--Flag only flat customers
	ct8."Flat Customers Only (1 if yes)", 
	--Flag only Upsell/Partial Churn Customers 
	ct8."Upsell/Partial Customers Only (1 if yes)", 
	--Flag only New/Churn Customers 
	ct8."New/Churn Customers Only (1 if yes)", 
	--Flag Customers who have Same ARR and PF but Ratio is Different
	ct8."Same ARR & PF But Different Ratio (1 if yes)", 
	--Flag Customers who have Same ARR but different PF and Ratio 
	ct8."Same ARR & But Different PF and Ratio (1 if yes)",
	--Flag Customers who have Different ARR and PF in Both Tables 
	ct8."Different ARR & PF in Both Tables(1 if yes)",
	--Flag Customers who have Different ARR but Same PF Makeup in both Tables 
	ct8."Different ARR But Same PF in Both Tables(1 if yes)", 
	ct8.mcid_tat, 
	ct8.product_family_tat, 
	ct8.snapshot_date_tat,
	ct8."MAX Snapshot Date of TAT",
	ct8."Sum by Product Family and Date - TAT",
	ct8.mcid_arr, 
	ct8.product_family_arr,
	ct8.snapshot_date_arr, 
	ct8."Sum by Product Family and Date - UFDM ARR",
	ct8."Start Date in UFDM ARR",
	ct8.mcid_prev_month, 
	ct8.pf_arr_prev_month,
	ct8.snapshot_date_arr_pmonth, 
	ct8."Sum by Product Family and Date - UFDM ARR Prev. Month",
	--Identify True Bridge Customers 
	--If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match 	
	ct8."No Diff between UFDM and TAT Prev. Month (1 if yes)",
	ct8."Campaign Flag (1 if yes)",
	ct8."Wecome Flag (1 if yes)", 
	ct8."Unbundling (1 if yes)"
from 
	combined_table_8 ct8
where 
	"Flat Customers Only (1 if yes)" = 0
    AND "No Diff between UFDM and TAT Prev. Month (1 if yes)" = 0
)

--select 
--	count(distinct "Combined MCID Before and After Transition")
--from 
--	table_invest_1 

--Take TAT data for those mcids only 
, 	table_invest_2 as 
(
select 	
	distinct ti1.mcid_tat,
	ti1.snapshot_date_tat as "Max Snapshot Date of TAT"
from 
	table_invest_1 ti1
)

--Take data from TAT for only these customers 
,	table_invest_3 as 
(
select 
	ti2.mcid_tat as "MCID TAT", 
	ti2."Max Snapshot Date of TAT", 
	t1.mcid_tat, 
	t1.product_family_tat, 
	t1.snapshot_date_tat,
	t1."Sum by Product Family and Date - TAT",
	t1."Sum by MICD & Date - TAT"
from 
	tat_1 t1 
inner join 
	table_invest_2 ti2
	 on 
	 	ti2.mcid_tat = t1.mcid_tat
)

--select 
--	count(distinct "MCID TAT")
--from 
--	table_invest_3

--Now only keep data in tat where snapshot_date <= Max Snapshot Date of TAT

,	table_invest_4 as 
(
select 
	ti3."MCID TAT", 
	ti3."Max Snapshot Date of TAT", 
	ti3.product_family_tat, 
	ti3.snapshot_date_tat,
	min(ti3.snapshot_date_tat) over(partition by ti3."MCID TAT") as "Start Date of TAT", 
	ti3."Sum by Product Family and Date - TAT",
	ti3."Sum by MICD & Date - TAT"
from 
	table_invest_3 ti3
where 
	ti3.snapshot_date_tat <= ti3."Max Snapshot Date of TAT"
and 
----Only take non null product families where ARR is greater than zero
	ti3."Sum by Product Family and Date - TAT" > 0
)

--select 
--	*
--from 
--	table_invest_4 
--where 
--	"MCID TAT" = '001ea07d-2184-df11-8804-0018717a8c82'

--The count will drop as we get rid of all customers who do not have values greater than zero 
--select 
--	count(distinct "MCID TAT")
--from 
--	table_invest_4

--Now join current month's data to previous months -- using MCID and PF. Do a Full Join
, table_invest_5 as 
(
select 
	ti4."MCID TAT", 
	ti4a."MCID TAT" as "Prev. Month MCID: TAT",
	coalesce(ti4."MCID TAT", ti4a."MCID TAT") as "Combined MCID: TAT", 
	coalesce(ti4.snapshot_date_tat, ti4a.snapshot_date_tat + interval '1 month') as "Combined Date TAT", 
	ti4."Max Snapshot Date of TAT", 
	ti4."Start Date of TAT",
	ti4.product_family_tat, 
	ti4a.product_family_tat as "Prev. Month PF: TAT", 
	ti4.snapshot_date_tat,
	ti4a.snapshot_date_tat as "Prev Month Date: TAT", 
	ti4."Sum by Product Family and Date - TAT",
	ti4."Sum by MICD & Date - TAT"
from 
	table_invest_4 ti4 
full join 
	table_invest_4 ti4a 
		on 
			ti4."MCID TAT" = ti4a."MCID TAT"
			and 
			ti4.snapshot_date_tat = ti4a.snapshot_date_tat + interval '1 month'
			and 
			ti4.product_family_tat = ti4a.product_family_tat
--stop taking data from the previous month where prev. month date in TAT < Max Date in TAT 
--where 
--	ti4a.snapshot_date_tat < ti4."Max Snapshot Date of TAT"
)

, table_invest_5a as 
(
select 
	ti5."Combined MCID: TAT", 
	ti5."Combined Date TAT", 
	ti5."MCID TAT", 
	ti5."Prev. Month MCID: TAT",
	max(ti5."Max Snapshot Date of TAT") over(partition by ti5."Combined MCID: TAT") as "Max Snapshot Date of TAT", 
	min(ti5."Start Date of TAT") over(partition by ti5."Combined MCID: TAT") as "Start Date of TAT", 
	ti5.product_family_tat, 
	ti5."Prev. Month PF: TAT", 
	ti5.snapshot_date_tat,
	ti5."Prev Month Date: TAT", 
	ti5."Sum by Product Family and Date - TAT",
	ti5."Sum by MICD & Date - TAT"
from 
	table_invest_5 ti5
)

,	table_invest_5b as 
(
select 
	ti5."Combined MCID: TAT", 
	ti5."Combined Date TAT", 
	ti5."MCID TAT", 
	ti5."Prev. Month MCID: TAT",
	ti5."Max Snapshot Date of TAT", 
	ti5."Start Date of TAT",
	ti5.product_family_tat, 
	ti5."Prev. Month PF: TAT", 
	ti5.snapshot_date_tat,
	ti5."Prev Month Date: TAT", 
	ti5."Sum by Product Family and Date - TAT",
	ti5."Sum by MICD & Date - TAT"
from 
	table_invest_5a ti5
where 
	ti5."Combined Date TAT" <= ti5."Max Snapshot Date of TAT"
)

--select 
--	*
--from 
--	table_invest_5b 
--where 
--	"Combined MCID: TAT" = '025c17f2-7b31-e411-9f63-0050568d2da8'

,	table_invest_6 as 
(
select 
	ti5."Combined MCID: TAT", 
	ti5."Combined Date TAT", 
	ti5."MCID TAT", 
	ti5."Prev. Month MCID: TAT",
	ti5."Max Snapshot Date of TAT", 
	ti5."Start Date of TAT",
	ti5.product_family_tat, 
	ti5."Prev. Month PF: TAT", 
	ti5.snapshot_date_tat,
	ti5."Prev Month Date: TAT", 
	ti5."Sum by Product Family and Date - TAT",
	ti5."Sum by MICD & Date - TAT",
	case 
		when ti5.snapshot_date_tat = ti5."Start Date of TAT" then 0 
		when ti5.snapshot_date_tat > ti5."Start Date of TAT" and ti5.product_family_tat is not null and ti5."Prev. Month PF: TAT" is not null then 0 
		else 1 
	end as "Prev Month and Current Month PF Matches (1 if no)"
from 
	table_invest_5b ti5 
)

--select 
--	*
--from 
--	table_invest_6
--where 
--	"Combined MCID: TAT" = '150b3417-2400-bed2-9fbb-0468b547aad4'

,	table_invest_7 as 
(
select 
	ti6."Combined MCID: TAT", 
	ti6."Combined Date TAT", 
	ti6."MCID TAT", 
	ti6."Prev. Month MCID: TAT",
	ti6."Max Snapshot Date of TAT", 
	ti6."Start Date of TAT",
	ti6.product_family_tat, 
	ti6."Prev. Month PF: TAT", 
	ti6.snapshot_date_tat,
	ti6."Prev Month Date: TAT", 
	ti6."Sum by Product Family and Date - TAT",
	ti6."Sum by MICD & Date - TAT",
	ti6."Prev Month and Current Month PF Matches (1 if no)",
	max(ti6."Combined Date TAT") filter(where ti6."Prev Month and Current Month PF Matches (1 if no)" =1) over(partition by ti6."Combined MCID: TAT") as "Last Date of History Change TAT", 
	case 
		when sum(ti6."Prev Month and Current Month PF Matches (1 if no)") over(partition by ti6."Combined MCID: TAT") > 0 then 1
		else 0 
	end as "Mismatch between PF in TAT History (1 if yes)"
from 
	table_invest_6 ti6
)

--select 
--	*
--from 
--	table_invest_7
--where 
--	"Combined MCID: TAT" = '046135a0-ebe5-e411-9afb-0050568d2da8'

--End of Adding Flags for TAT History 

,	table_invest_7a as 
(
select 
	distinct "Combined MCID: TAT",
	"Mismatch between PF in TAT History (1 if yes)",
	"Last Date of History Change TAT", 
	"Start Date of TAT"
from 
	table_invest_7 
)

--select 
--	*
--from 
--	table_invest_7a
--where 
--	"Combined MCID: TAT" = '046135a0-ebe5-e411-9afb-0050568d2da8'

--Add them back to the original analysis 

,	combined_table_9 as 
(
select 
	ct8."Combined MCID after Transition",
	ct8."Combined MCID Before Transition",
	ct8."Combined MCID Before and After Transition", 
	ct8."Combined Date After Transition", 
	ct8."Combine Date Before Transition", 
	ct8."Presence of PF in 2 Tables", 
	ct8."Difference between UFDM and TAT -- ARR", 
	ct8."Absolute Diff. between UFDM and TAT--ARR", 
	ct8."Product Bridge", 
	--Flag only flat customers
	ct8."Flat Customers Only (1 if yes)", 
	--Flag only Upsell/Partial Churn Customers 
	ct8."Upsell/Partial Customers Only (1 if yes)", 
	--Flag only New/Churn Customers 
	ct8."New/Churn Customers Only (1 if yes)", 
	--Flag Customers who have Same ARR and PF but Ratio is Different
	ct8."Same ARR & PF But Different Ratio (1 if yes)", 
	--Flag Customers who have Same ARR but different PF and Ratio 
	ct8."Same ARR & But Different PF and Ratio (1 if yes)",
	--Flag Customers who have Different ARR and PF in Both Tables 
	ct8."Different ARR & PF in Both Tables(1 if yes)",
	--Flag Customers who have Different ARR but Same PF Makeup in both Tables 
	ct8."Different ARR But Same PF in Both Tables(1 if yes)", 
	ct8.mcid_tat, 
	ct8.product_family_tat, 
	ct8.snapshot_date_tat,
	ct8."MAX Snapshot Date of TAT",
	ct8."Sum by Product Family and Date - TAT",
	ct8.mcid_arr, 
	ct8.product_family_arr,
	ct8.snapshot_date_arr, 
	ct8."Sum by Product Family and Date - UFDM ARR",
	ct8."Start Date in UFDM ARR",
	ct8.mcid_prev_month, 
	ct8.pf_arr_prev_month,
	ct8.snapshot_date_arr_pmonth, 
	ct8."Sum by Product Family and Date - UFDM ARR Prev. Month",
	--Identify True Bridge Customers 
	--If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match 	
	ct8."No Diff between UFDM and TAT Prev. Month (1 if yes)",
	ct8."Campaign Flag (1 if yes)",
	ct8."Wecome Flag (1 if yes)", 
	ct8."Unbundling (1 if yes)",
	--Add Flags if it has the same history in TAT or it changes. Also add flags that 
	ti7."Combined MCID: TAT", 
	ti7."Mismatch between PF in TAT History (1 if yes)",
	ti7."Last Date of History Change TAT",
	ti7."Start Date of TAT"
from 
	combined_table_8 ct8 
left join 
	table_invest_7a ti7 
		on 
			ct8.mcid_tat = ti7."Combined MCID: TAT"
)

--select 
--	*
--from 
--	combined_table_9
--where 
--	"Combined MCID Before and After Transition" = '046135a0-ebe5-e411-9afb-0050568d2da8'

--Coalesnce Mismatch between PF in TAT History to have either 1 (if there is mismatch) or 0 (no mistmatch )
--Also start using dense_rank for each of the product families in UFDM ARR
--Also coalesce the max snapshot date, the start date and last date of change over all rows for a customer 

,	combined_table_10 as 
(
select 
	ct9."Combined MCID after Transition",
	ct9."Combined MCID Before Transition",
	ct9."Combined MCID Before and After Transition", 
	ct9."Combined Date After Transition", 
	ct9."Combine Date Before Transition", 
	ct9."Presence of PF in 2 Tables", 
	ct9."Difference between UFDM and TAT -- ARR", 
	ct9."Absolute Diff. between UFDM and TAT--ARR", 
	ct9."Product Bridge", 
	--Flag only flat customers
	ct9."Flat Customers Only (1 if yes)", 
	--Flag only Upsell/Partial Churn Customers 
	ct9."Upsell/Partial Customers Only (1 if yes)", 
	--Flag only New/Churn Customers 
	ct9."New/Churn Customers Only (1 if yes)", 
	--Flag Customers who have Same ARR and PF but Ratio is Different
	ct9."Same ARR & PF But Different Ratio (1 if yes)", 
	--Flag Customers who have Same ARR but different PF and Ratio 
	ct9."Same ARR & But Different PF and Ratio (1 if yes)",
	--Flag Customers who have Different ARR and PF in Both Tables 
	ct9."Different ARR & PF in Both Tables(1 if yes)",
	--Flag Customers who have Different ARR but Same PF Makeup in both Tables 
	ct9."Different ARR But Same PF in Both Tables(1 if yes)", 
	ct9.mcid_tat, 
	ct9.product_family_tat, 
	ct9.snapshot_date_tat,
	case 
		when ct9.product_family_tat is null then 0 
		else dense_rank() over(partition by ct9.mcid_tat, ct9.snapshot_date_tat order by ct9.product_family_tat)
	end	as "Dense Rank of PF TAT", 
	max(ct9."MAX Snapshot Date of TAT") over(partition by ct9."Combined MCID Before and After Transition") as "MAX Snapshot Date of TAT", 
	ct9."Sum by Product Family and Date - TAT",
	ct9.mcid_arr, 
	ct9.product_family_arr,
	ct9.snapshot_date_arr, 
	case 
		when ct9.product_family_arr is null then 0 
		else dense_rank() over(partition by ct9.mcid_arr, ct9.snapshot_date_arr order by ct9.product_family_arr)
	end  as "Dense Rank of PF UFDM ARR",
	ct9."Sum by Product Family and Date - UFDM ARR",
	ct9."Start Date in UFDM ARR",
	ct9.mcid_prev_month, 
	ct9.pf_arr_prev_month,
	ct9.snapshot_date_arr_pmonth, 
	ct9."Sum by Product Family and Date - UFDM ARR Prev. Month",
	--Identify True Bridge Customers 
	--If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match 	
	ct9."No Diff between UFDM and TAT Prev. Month (1 if yes)",
	ct9."Campaign Flag (1 if yes)",
	ct9."Wecome Flag (1 if yes)", 
	ct9."Unbundling (1 if yes)",
	--Take the max of history in TAT 
	max(ct9."Mismatch between PF in TAT History (1 if yes)") over(partition by ct9."Combined MCID Before and After Transition") as "Mismatch between PF in TAT History (1 if yes)",
	max(ct9."Last Date of History Change TAT") over(partition by ct9."Combined MCID Before and After Transition") as "Last Date of History Change TAT", 
	max(ct9."Start Date of TAT") over(partition by ct9."Combined MCID Before and After Transition") as "Start Date of TAT"
from 
	combined_table_9 ct9
)

--select 
--	*
--from 
--	combined_table_10
--where 
--	"Combined MCID Before and After Transition" = '046135a0-ebe5-e411-9afb-0050568d2da8'

--Now take the max of each dense rank to find the number of product families in TAT and UFDM ARR during Transition 
--Also take max of last date of change and start date of tat -- to fill up null rows where we have no TAT data 
,	combined_table_11 as 
(
select 
	ct10."Combined MCID after Transition",
	ct10."Combined MCID Before Transition",
	ct10."Combined MCID Before and After Transition", 
	ct10."Combined Date After Transition", 
	ct10."Combine Date Before Transition", 
	ct10."Presence of PF in 2 Tables", 
	ct10."Difference between UFDM and TAT -- ARR", 
	ct10."Absolute Diff. between UFDM and TAT--ARR", 
	ct10."Product Bridge", 
	--Flag only flat customers
	ct10."Flat Customers Only (1 if yes)", 
	--Flag only Upsell/Partial Churn Customers 
	ct10."Upsell/Partial Customers Only (1 if yes)", 
	--Flag only New/Churn Customers 
	ct10."New/Churn Customers Only (1 if yes)", 
	--Flag Customers who have Same ARR and PF but Ratio is Different
	ct10."Same ARR & PF But Different Ratio (1 if yes)", 
	--Flag Customers who have Same ARR but different PF and Ratio 
	ct10."Same ARR & But Different PF and Ratio (1 if yes)",
	--Flag Customers who have Different ARR and PF in Both Tables 
	ct10."Different ARR & PF in Both Tables(1 if yes)",
	--Flag Customers who have Different ARR but Same PF Makeup in both Tables 
	ct10."Different ARR But Same PF in Both Tables(1 if yes)", 
	ct10.mcid_tat, 
	ct10.product_family_tat, 
	ct10.snapshot_date_tat,
	max(ct10."Dense Rank of PF TAT") over(partition by ct10.mcid_tat, ct10.snapshot_date_tat) as "No of PF TAT", 
	ct10."MAX Snapshot Date of TAT",
	ct10."Sum by Product Family and Date - TAT",
	ct10.mcid_arr, 
	ct10.product_family_arr,
	ct10.snapshot_date_arr, 
	ct10."Dense Rank of PF UFDM ARR",
	max(ct10."Dense Rank of PF UFDM ARR") over(partition by ct10.mcid_arr, ct10.snapshot_date_arr) as "No of PF ARR", 
	ct10."Sum by Product Family and Date - UFDM ARR",
	ct10."Start Date in UFDM ARR",
	ct10.mcid_prev_month, 
	ct10.pf_arr_prev_month,
	ct10.snapshot_date_arr_pmonth, 
	ct10."Sum by Product Family and Date - UFDM ARR Prev. Month",
	--Identify True Bridge Customers 
	--If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match 	
	ct10."No Diff between UFDM and TAT Prev. Month (1 if yes)",
	ct10."Campaign Flag (1 if yes)",
	ct10."Wecome Flag (1 if yes)", 
	ct10."Unbundling (1 if yes)",
	--Add Flags if it has the same history in TAT or it changes 
	coalesce(ct10."Mismatch between PF in TAT History (1 if yes)",0) as "Mismatch between PF in TAT History (1 if yes)",
	ct10."Last Date of History Change TAT",
	ct10."Start Date of TAT"
from 
	combined_table_10 ct10
order by 
	"Combined MCID Before and After Transition"
)

--select 
--	*
--from 
--	combined_table_11
--where 
--	"Combined MCID Before and After Transition" = '001ea07d-2184-df11-8804-0018717a8c82'

--Take Max of each product family to makesure it is the same throughout the MCID 

,	combined_table_12 as 
(
select 
	ct11."Combined MCID after Transition",
	ct11."Combined MCID Before Transition",
	ct11."Combined MCID Before and After Transition", 
	ct11."Combined Date After Transition", 
	ct11."Combine Date Before Transition", 
	ct11."Presence of PF in 2 Tables", 
	ct11."Difference between UFDM and TAT -- ARR", 
	ct11."Absolute Diff. between UFDM and TAT--ARR", 
	ct11."Product Bridge", 
	--Flag only flat customers
	ct11."Flat Customers Only (1 if yes)", 
	--Flag only Upsell/Partial Churn Customers 
	ct11."Upsell/Partial Customers Only (1 if yes)", 
	--Flag only New/Churn Customers 
	ct11."New/Churn Customers Only (1 if yes)", 
	--Flag Customers who have Same ARR and PF but Ratio is Different
	ct11."Same ARR & PF But Different Ratio (1 if yes)", 
	--Flag Customers who have Same ARR but different PF and Ratio 
	ct11."Same ARR & But Different PF and Ratio (1 if yes)",
	--Flag Customers who have Different ARR and PF in Both Tables 
	ct11."Different ARR & PF in Both Tables(1 if yes)",
	--Flag Customers who have Different ARR but Same PF Makeup in both Tables 
	ct11."Different ARR But Same PF in Both Tables(1 if yes)", 
	ct11.mcid_tat, 
	ct11.product_family_tat, 
	ct11.snapshot_date_tat,
	max(ct11."No of PF TAT") over(partition by ct11."Combined MCID Before and After Transition") as "No of PF TAT", 
	ct11."MAX Snapshot Date of TAT",
	ct11."Sum by Product Family and Date - TAT",
	ct11.mcid_arr, 
	ct11.product_family_arr,
	ct11.snapshot_date_arr, 
	ct11."Dense Rank of PF UFDM ARR",
	max(ct11."No of PF ARR") over(partition by ct11."Combined MCID Before and After Transition") as "No of PF ARR",
	ct11."Sum by Product Family and Date - UFDM ARR",
	ct11."Start Date in UFDM ARR",
	ct11.mcid_prev_month, 
	ct11.pf_arr_prev_month,
	ct11.snapshot_date_arr_pmonth, 
	ct11."Sum by Product Family and Date - UFDM ARR Prev. Month",
	--Identify True Bridge Customers 
	--If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match 	
	ct11."No Diff between UFDM and TAT Prev. Month (1 if yes)",
	ct11."Campaign Flag (1 if yes)",
	ct11."Wecome Flag (1 if yes)", 
	ct11."Unbundling (1 if yes)",
	--Add Flags if it has the same history in TAT or it changes 
	ct11."Mismatch between PF in TAT History (1 if yes)",
	ct11."Last Date of History Change TAT",
	ct11."Start Date of TAT"
from 
	combined_table_11 ct11
order by 
	"Combined MCID Before and After Transition"
)

--Start Looking at product family transition. Also look at ratios allocated to UFDM PF During Point of Transition 
,	combined_table_13 as 
(
select 
	ct12."Combined MCID after Transition",
	ct12."Combined MCID Before Transition",
	ct12."Combined MCID Before and After Transition", 
	ct12."Combined Date After Transition", 
	coalesce(ct12."Combined Date After Transition", ct12."Combine Date Before Transition"+interval '1 month') as "Combined Date Overall", 
	ct12."Combine Date Before Transition", 
	ct12."Presence of PF in 2 Tables", 
	ct12."Difference between UFDM and TAT -- ARR", 
	ct12."Absolute Diff. between UFDM and TAT--ARR", 
	ct12."Product Bridge", 
	--Flag only flat customers
	ct12."Flat Customers Only (1 if yes)", 
	--Flag only Upsell/Partial Churn Customers 
	ct12."Upsell/Partial Customers Only (1 if yes)", 
	--Flag only New/Churn Customers 
	ct12."New/Churn Customers Only (1 if yes)", 
	--Flag Customers who have Same ARR and PF but Ratio is Different
	ct12."Same ARR & PF But Different Ratio (1 if yes)", 
	--Flag Customers who have Same ARR but different PF and Ratio 
	ct12."Same ARR & But Different PF and Ratio (1 if yes)",
	--Flag Customers who have Different ARR and PF in Both Tables 
	ct12."Different ARR & PF in Both Tables(1 if yes)",
	--Flag Customers who have Different ARR but Same PF Makeup in both Tables 
	ct12."Different ARR But Same PF in Both Tables(1 if yes)", 
	ct12.mcid_tat, 
	ct12.product_family_tat, 
	ct12.snapshot_date_tat,
	ct12."No of PF TAT", 
	ct12."MAX Snapshot Date of TAT",
	ct12."Sum by Product Family and Date - TAT",
	ct12.mcid_arr, 
	ct12.product_family_arr,
	ct12.snapshot_date_arr, 
	ct12."Dense Rank of PF UFDM ARR",
	ct12."No of PF ARR",
	ct12."Sum by Product Family and Date - UFDM ARR",
	ct12."Sum by Product Family and Date - UFDM ARR"/nullif((sum(ct12."Sum by Product Family and Date - UFDM ARR") over(partition by ct12.mcid_arr, ct12.snapshot_date_arr)),0) as "Ratio of ARR Allocated to PF UFDM ARR",
	ct12."Start Date in UFDM ARR",
	ct12.mcid_prev_month, 
	ct12.pf_arr_prev_month,
	ct12.snapshot_date_arr_pmonth, 
	ct12."Sum by Product Family and Date - UFDM ARR Prev. Month",
	--Identify True Bridge Customers 
	--If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match 	
	ct12."No Diff between UFDM and TAT Prev. Month (1 if yes)",
	ct12."Campaign Flag (1 if yes)",
	ct12."Wecome Flag (1 if yes)", 
	ct12."Unbundling (1 if yes)",
	--Add Flags if it has the same history in TAT or it changes 
	ct12."Mismatch between PF in TAT History (1 if yes)",
	ct12."Last Date of History Change TAT",
	ct12."Start Date of TAT",
	--Add Flags for Number of PF from UFDM ARR to TAT 
	--1) Single PF -> Single PF 
	case 
		when ct12."No of PF TAT" = 1 and ct12."No of PF ARR" = 1 then 1 
		else 0 
	end as "Single PF to Single PF (1 if yes)", 
	--2) Multi PF --> Single PF 
	case 
		when ct12."No of PF TAT" > 1 and ct12."No of PF ARR" = 1 then 1 
		else 0 
	end as "Multi PF to Single PF (1 if yes)", 
	--3) Single to Multi PF 
	case 
		when ct12."No of PF TAT" = 1 and ct12."No of PF ARR" > 1 then 1 
		else 0 
	end as "Single PF to Multi PF (1 if yes)", 
	--4) Multi to Multi PF 
	case 
		when ct12."No of PF TAT" > 1 and ct12."No of PF ARR" > 1 then 1 
		else 0 
	end as "Multi PF to Multi PF (1 if yes)"
from 
	combined_table_12 ct12
--Get rid of any customers who don't have any data in TAT
where 
	ct12."No of PF TAT" > 0 
)

--select 
--	*
--from 
--	combined_table_13 
--where 
--	"Combined MCID Before and After Transition" = '046135a0-ebe5-e411-9afb-0050568d2da8'

--Product Family Transition 

,	combined_table_14 as 
(
select 
	ct13."Combined MCID after Transition",
	ct13."Combined MCID Before Transition",
	ct13."Combined MCID Before and After Transition", 
	ct13."Combined Date After Transition", 
	ct13."Combined Date Overall", 
	date_trunc('YEAR', ct13."Combined Date Overall") as "Year", 
	ct13."Combine Date Before Transition", 
	ct13."Presence of PF in 2 Tables", 
	ct13."Difference between UFDM and TAT -- ARR", 
	ct13."Absolute Diff. between UFDM and TAT--ARR", 
	ct13."Product Bridge", 
	--Flag only flat customers
	ct13."Flat Customers Only (1 if yes)", 
	--Flag only Upsell/Partial Churn Customers 
	ct13."Upsell/Partial Customers Only (1 if yes)", 
	--Flag only New/Churn Customers 
	ct13."New/Churn Customers Only (1 if yes)", 
	--Flag Customers who have Same ARR and PF but Ratio is Different
	ct13."Same ARR & PF But Different Ratio (1 if yes)", 
	--Flag Customers who have Same ARR but different PF and Ratio 
	ct13."Same ARR & But Different PF and Ratio (1 if yes)",
	--Flag Customers who have Different ARR and PF in Both Tables 
	ct13."Different ARR & PF in Both Tables(1 if yes)",
	--Flag Customers who have Different ARR but Same PF Makeup in both Tables 
	ct13."Different ARR But Same PF in Both Tables(1 if yes)", 
	ct13.mcid_tat, 
	ct13.product_family_tat, 
	ct13.snapshot_date_tat,
	ct13."No of PF TAT", 
	ct13."MAX Snapshot Date of TAT",
	ct13."Sum by Product Family and Date - TAT",
	ct13.mcid_arr, 
	ct13.product_family_arr,
	ct13.snapshot_date_arr, 
	ct13."Dense Rank of PF UFDM ARR",
	ct13."No of PF ARR",
	ct13."Sum by Product Family and Date - UFDM ARR",
	ct13."Ratio of ARR Allocated to PF UFDM ARR",
	ct13."Start Date in UFDM ARR",
	ct13.mcid_prev_month, 
	ct13.pf_arr_prev_month,
	ct13.snapshot_date_arr_pmonth, 
	ct13."Sum by Product Family and Date - UFDM ARR Prev. Month",
	--Identify True Bridge Customers 
	--If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match 	
	ct13."No Diff between UFDM and TAT Prev. Month (1 if yes)",
	ct13."Campaign Flag (1 if yes)",
	ct13."Wecome Flag (1 if yes)", 
	ct13."Unbundling (1 if yes)",
	--Add Flags if it has the same history in TAT or it changes 
	ct13."Mismatch between PF in TAT History (1 if yes)",
	ct13."Last Date of History Change TAT",
	ct13."Start Date of TAT",
	--Add Flags for Number of PF from UFDM ARR to TAT 
	--1) Single PF -> Single PF 
	case 
		when ct13."Single PF to Single PF (1 if yes)" = 1 then 'Single PF to Single PF' 
		when ct13."Multi PF to Single PF (1 if yes)" = 1 then 'Multi PF to Single PF' 
		when ct13."Single PF to Multi PF (1 if yes)" = 1 then 'Single PF to Multi PF' 
		when ct13."Multi PF to Multi PF (1 if yes)" = 1 then 'Multi PF to Multi PF' 
	else null 
	end as "Product Family Transition"
from 
	combined_table_13 ct13
)

--Full Export 
, combined_table_15 as
(
select 
	ct14."Combined MCID after Transition",
	ct14."Combined MCID Before Transition",
	ct14."Combined MCID Before and After Transition", 
	ct14."Combined Date After Transition", 
	ct14."Combined Date Overall", 
	ct14."Year", 
	ct14."Combine Date Before Transition", 
	ct14."Presence of PF in 2 Tables", 
	ct14."Difference between UFDM and TAT -- ARR", 
	ct14."Absolute Diff. between UFDM and TAT--ARR", 
	ct14."Product Bridge", 
	--Flag only flat customers
	ct14."Flat Customers Only (1 if yes)", 
	--Identify True Bridge Customers 
	--If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match 	
	ct14."No Diff between UFDM and TAT Prev. Month (1 if yes)",
	--Flag only Upsell/Partial Churn Customers 
	ct14."Upsell/Partial Customers Only (1 if yes)", 
	--Flag only New/Churn Customers 
	ct14."New/Churn Customers Only (1 if yes)", 
	--Flag Customers who have Same ARR and PF but Ratio is Different
	ct14."Same ARR & PF But Different Ratio (1 if yes)", 
	--Flag Customers who have Same ARR but different PF and Ratio 
	ct14."Same ARR & But Different PF and Ratio (1 if yes)",
	--Flag Customers who have Different ARR and PF in Both Tables 
	ct14."Different ARR & PF in Both Tables(1 if yes)",
	--Flag Customers who have Different ARR but Same PF Makeup in both Tables 
	ct14."Different ARR But Same PF in Both Tables(1 if yes)", 
	ct14.mcid_tat, 
	ct14.product_family_tat, 
	ct14.snapshot_date_tat,
	ct14."No of PF TAT", 
	ct14."MAX Snapshot Date of TAT",
	ct14."Sum by Product Family and Date - TAT",
	ct14.mcid_arr, 
	ct14.product_family_arr,
	ct14.snapshot_date_arr, 
	ct14."Dense Rank of PF UFDM ARR",
	ct14."No of PF ARR",
	ct14."Sum by Product Family and Date - UFDM ARR",
	ct14."Ratio of ARR Allocated to PF UFDM ARR",
	ct14."Start Date in UFDM ARR",
	ct14.mcid_prev_month, 
	ct14.pf_arr_prev_month,
	ct14.snapshot_date_arr_pmonth, 
	ct14."Sum by Product Family and Date - UFDM ARR Prev. Month",
	ct14."Campaign Flag (1 if yes)",
	ct14."Wecome Flag (1 if yes)", 
	ct14."Unbundling (1 if yes)",
	--Add Flags if it has the same history in TAT or it changes 
	ct14."Mismatch between PF in TAT History (1 if yes)",
	ct14."Last Date of History Change TAT",
	ct14."Start Date of TAT",
	--Add Flags for Number of PF from UFDM ARR to TAT 
	--1) Single PF -> Single PF 
	ct14."Product Family Transition"
from 
	combined_table_14 ct14
)

--select 
--	*
--from 
--	combined_table_15 
--where 
--	mcid_arr = '00c43d23-1651-e811-8143-70106fa67261'

--Prepare ratios for Scenario 1 & 2 

,	sc1_sc2 as 
(
select 
	mcid_arr, 
	"MAX Snapshot Date of TAT",
	product_family_arr,
	"Ratio of ARR Allocated to PF UFDM ARR",
	"Last Date of History Change TAT",
	"Start Date of TAT", 
	"Mismatch between PF in TAT History (1 if yes)", 
	"Start Date of TAT" as "Date to Drag to Under Scenario 1", 
	case 
		when "Mismatch between PF in TAT History (1 if yes)" = 1 then "Last Date of History Change TAT"
		else "Start Date of TAT"
	end as "Date to Drag Under Scenario 2", 
	"Product Family Transition"
from 
	combined_table_15 
where 
	"Flat Customers Only (1 if yes)" = 0 
	and 	
	"No Diff between UFDM and TAT Prev. Month (1 if yes)" = 0 
	and 
	mcid_arr is not null 
order by 
	"Combined MCID Before and After Transition"
)

--New code for drag ratio 
,	drag_ratio as 
(
select 
	mcid_arr, 
	"MAX Snapshot Date of TAT",
	product_family_arr,
	"Ratio of ARR Allocated to PF UFDM ARR",
	"Date to Drag to Under Scenario 1", 
	"Date to Drag Under Scenario 2", 
	"Product Family Transition"
from 
	sc1_sc2
where 
	"Ratio of ARR Allocated to PF UFDM ARR" is not null 
)

--Rough Tests 

--select 
--	*
--from 
--	drag_ratio 
--where 
--	mcid_arr = '00c43d23-1651-e811-8143-70106fa67261'


select 
	distinct dr.mcid_arr 
from 
	drag_ratio dr 
inner join 
	sandbox.cohort2_drag_ratio cdr  
		on 
			dr.mcid_arr = cdr.mcid
			
			
select 
	*
from 
	sandbox.cohort2_drag_ratio cdr  
where 
	mcid = '00c43d23-1651-e811-8143-70106fa67261'
			

