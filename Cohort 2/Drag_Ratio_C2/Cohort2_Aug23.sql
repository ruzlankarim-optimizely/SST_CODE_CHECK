--Find Customers who churn out from TAT but are present in TAT later or before 

--Customers who fully churn don't have a transition date anyways 

--Create a cartesian join 
DROP TABLE IF EXISTS sandbox.cohort2_drag_ratio;
CREATE TABLE sandbox.cohort2_drag_ratio AS (
with cartersian_table as 
	(
	select 
		a.mcid,
		b.snapshot_date 
	from 
		(
		select 
			distinct mcid
		from 
			ufdm.tat_upload_data tud
		where 
			is_deleted IS DISTINCT FROM 1
		and 
   		"Overage Y/N" is distinct from 'Y'
		and not 
		(
		date_trunc('month', snapshot_date) = '2021-12-01'::DATE
		                  AND product_family ilike '%Campaign%'
		)	
		) as a, 
			(
		select 
			distinct snapshot_date
		from 
			ufdm.tat_upload_data tud
				where 
			is_deleted IS DISTINCT FROM 1
		and 
   		"Overage Y/N" is distinct from 'Y'
		and not 
		(
		date_trunc('month', snapshot_date) = '2021-12-01'::DATE
		                  AND product_family ilike '%Campaign%'
		)	
		) as b 
	)

,	initial_tat as 
(
select 
	distinct mcid,
	snapshot_date, 
	sum(arr_usd_ccfx) over(partition by mcid, snapshot_date) as sum_mcid_date 
from 
	ufdm.tat_upload_data tud 
where 
			is_deleted IS DISTINCT FROM 1
		and 
   		"Overage Y/N" is distinct from 'Y'
		and not 
		(
		date_trunc('month', snapshot_date) = '2021-12-01'::DATE
		                  AND product_family ilike '%Campaign%'
		)	
)

--Now do a left join to absorb the data into the cartersian table 

,	tat_0 as 
(
select 
	ct.mcid, 
	ct.snapshot_date, 
	coalesce(it.sum_mcid_date,0) as sum_mcid_date
from 
	cartersian_table ct
left join 
	initial_tat it 
		on 
			ct.mcid = it.mcid 
			and 
			ct.snapshot_date = it.snapshot_date 
)


--Put a Limit on Date and Start Summing ARR From Now Till End and ARR Over the Entire Period 

,	tat_1 as 
(
select 
	mcid, 
	snapshot_date, 
	sum_mcid_date, 
	sum(sum_mcid_date) over(partition by mcid order by snapshot_date rows between current row and unbounded following) as "ARR from Now Till End",
	sum(sum_mcid_date) over(partition by mcid) as "ARR Over the Entire Time Period",
	min(snapshot_date) filter(where sum_mcid_date > 0) over(partition by mcid) as "Start of Data in TAT"
from 
	tat_0
--Put a limit on date 
where 
	snapshot_date < '2022-01-01'
)

--Get rid of customers who've never had ARR 
--Take the first date when ARR goes to zero for the rest of time 

,	tat_2 as 
(
select 
	mcid,
	snapshot_date, 
	sum_mcid_date, 
	"Start of Data in TAT", 
	"ARR from Now Till End",
	"ARR Over the Entire Time Period", 
	min(snapshot_date) filter(where "ARR from Now Till End" = 0) over(partition by mcid) as "Date of Churn"
from 
	tat_1 
where 
	"ARR Over the Entire Time Period" > 0 
)

--Now find only churn customers 

,	churn_cust as 
(
select 
	distinct mcid, 
	"Start of Data in TAT", 
	"Date of Churn"
from 
	tat_2 
where 
	"Date of Churn" is not null 
)  

--Test to make sure they are not in the TAT customers being changed 

--select 
--	cc.mcid 
--from 
--	churn_cust cc 
--inner join 
--	sandbox.drag_ration dr 
--		on 
--			dr.mcid = cc.mcid 

--There are 2 scenarios with churn customers 1) They churned out from TAT and never appeared in UFDM 2) They churned out in TAT and were found later in UFDM 

--Find Non-Fopti Customers in UFDM and find the minimum date they start 

,	non_fopti_1 as 
(
select 
	mcid,
	snapshot_date, 
	arr, 
	min(snapshot_date) filter(where arr > 0) over(partition by mcid) as "Start of SST Data from UFDM"
from 
	sandbox.control_sst_before_manual_changes
where 
	record_source in ('ufdm_2022') 
and 
	product_family not in ('Full Stack', 'Web', 'Recurring: Cloud: Intelligence Cloud: Web Experimentation and Personalization')
)

,	non_fopti_2 as 
(
select 
	distinct mcid, 
	"Start of SST Data from UFDM"
from 
	non_fopti_1 
where 
	"Start of SST Data from UFDM" is not null 
order by 
	mcid
)

--Now join the non-Fopti data to the TAT. This is the final table that shows TAT MCID, Date of Churn and When they Start in UFDM ARR 
	
,	tat_final as 
(
select 
	cc.mcid,
	cc."Date of Churn", 
	cc."Start of Data in TAT", 
	date_trunc('MONTH',nfp2."Start of SST Data from UFDM")::DATE as "Start of SST Data from UFDM"
from 
	churn_cust cc 
left join 
	non_fopti_2 nfp2 
		on 
			cc.mcid = nfp2.mcid 
)		


--Now look at UFDM ARR: Check the Non-Fopti Data in UFDM ARR 
,  ufdm_arr_0 as 
(
select 
	distinct mcid, 
	snapshot_date, 
	product_family, 
	sum(arr_usd_ccfx) over(partition by mcid, snapshot_date, product_family) as sum_arr_pf, 
	sum(arr_usd_ccfx) over(partition by mcid, snapshot_date, product_family)/nullif(sum(arr_usd_ccfx) over(partition by mcid, snapshot_date),0) as "Ratio to Each PF", 
	sum(arr_usd_ccfx) over(partition by mcid, snapshot_date) as sum_ufdm_arr 
from 
	sandbox_pd.arr
where 
	product_family not in ('Full Stack', 'Web', 'Recurring: Cloud: Intelligence Cloud: Web Experimentation and Personalization')
)

--Put a limit on the table to select only customers who have greater than arr > 0 
,	ufdm_arr_1 as 
(
select 
	mcid, 
	date_trunc('MONTH',snapshot_date)::DATE as date_ufdm_arr, 
	product_family, 
	sum_arr_pf, 
	"Ratio to Each PF", 
	sum_ufdm_arr 
from 
	ufdm_arr_0 
where 
	sum_ufdm_arr > 0 
)

--This is the final UFDM ARR that shows the Non-Fopti Data with ARR 
,	ufdm_arr_2 as 
(
select 
	distinct mcid, 
	date_ufdm_arr, 
	sum_ufdm_arr 
from 
	ufdm_arr_1
)

--select 
--	*
--from
--	ufdm_arr_1
--where 
--	mcid = '01052ec2-dae5-e411-9afb-0050568d2da8'
	
--Now Join TAT to UFDM ARR on 2 things: +/-3 months and +/-6 months 

, 	combined_table_1 as 
(
select 
	tf.mcid,
	tf."Date of Churn", 
	tf."Start of Data in TAT", 
	ua2.date_ufdm_arr as "UFDM ARR Dates in +/- 6 Month Range: with ARR", 
	abs(tf."Date of Churn"-ua2.date_ufdm_arr) as "Abs. Difference between Churn Date and 6 Month Range", 
	tf."Start of SST Data from UFDM"
from 
	tat_final tf 
left join 
	ufdm_arr_2 ua2 
		on 
			ua2.mcid = tf.mcid 
			and 
			(
				(ua2.date_ufdm_arr  >= tf."Date of Churn" - interval '6 month') and (ua2.date_ufdm_arr  <= tf."Date of Churn")
				or 
				(ua2.date_ufdm_arr  <= tf."Date of Churn" + interval '6 month') and (ua2.date_ufdm_arr  >= tf."Date of Churn")
			)
order by 
	tf.mcid
)

--Only take the closest date from the +6 month range 

--Rank the least difference first 

--Rank the latest dates first 

,	combined_table_2 as 
(
select 
	ct1.mcid,
	ct1."Date of Churn", 
	ct1."Start of Data in TAT", 
	ct1."UFDM ARR Dates in +/- 6 Month Range: with ARR", 
	ct1."Abs. Difference between Churn Date and 6 Month Range", 
	ct1."Start of SST Data from UFDM", 
	rank() over(partition by mcid order by ct1."Abs. Difference between Churn Date and 6 Month Range", ct1."UFDM ARR Dates in +/- 6 Month Range: with ARR" desc) as ranking_6_month
from 
	combined_table_1 ct1
)

--Now only keep the rows where ranking = 1 

,	combined_table_3 as 
(
select 
	ct2.mcid,
	ct2."Date of Churn", 
	ct2."Start of Data in TAT", 
	ct2."UFDM ARR Dates in +/- 6 Month Range: with ARR", 
	ct2."Abs. Difference between Churn Date and 6 Month Range", 
	ct2."Start of SST Data from UFDM", 
	ct2.ranking_6_month
from 
	combined_table_2 ct2
where 
	ranking_6_month = 1
)


--Test to make sure there are no duplicates 
--select
--	distinct mcid, 
--	count(*) as no_of_obs
--from 
--	combined_table_3 
--group by 
--	mcid 
--having 
--	count(*) > 1

--Now repeat this process for 2-month period 

, 	combined_table_4 as 
(
select 
	ct3.mcid,
	ct3."Date of Churn", 
	ct3."Start of Data in TAT", 
	ct3."UFDM ARR Dates in +/- 6 Month Range: with ARR", 
	ct3."Start of SST Data from UFDM", 
	ua3.date_ufdm_arr as "UFDM ARR Dates in +/- 2 Month Range: with ARR", 
	abs(ct3."Date of Churn"-ua3.date_ufdm_arr) as "Abs. Difference between Churn Date and 2 Month Range"
from 
	combined_table_3 ct3
left join 
	ufdm_arr_2 ua3 
		on 
			ua3.mcid = ct3.mcid 
			and 
			(
				(ua3.date_ufdm_arr  >= ct3."Date of Churn" - interval '2 month') and (ua3.date_ufdm_arr  <= ct3."Date of Churn")
				or 
				(ua3.date_ufdm_arr  <= ct3."Date of Churn" + interval '2 month') and (ua3.date_ufdm_arr  >= ct3."Date of Churn")
			)
order by 
	ct3.mcid
)

--Now start ranking the dates 

--Rank the least difference first 

--Rank the latest dates first 

,	combined_table_5 as 
(
select 
	ct4.mcid,
	ct4."Date of Churn", 
	ct4."Start of Data in TAT", 
	ct4."UFDM ARR Dates in +/- 6 Month Range: with ARR", 
	ct4."Start of SST Data from UFDM", 
	ct4."UFDM ARR Dates in +/- 2 Month Range: with ARR", 
	ct4."Abs. Difference between Churn Date and 2 Month Range", 
	rank() over(partition by mcid order by ct4."Abs. Difference between Churn Date and 2 Month Range", ct4."UFDM ARR Dates in +/- 2 Month Range: with ARR" desc) as ranking_2_month
from 
	combined_table_4 ct4
)

--Now only take the dates which are ranked 1 
,	final_table_1 as 
(
select 
	ct5.mcid,
	ct5."Start of SST Data from UFDM",
	ct5."Start of Data in TAT", 
	ct5."Date of Churn", 
	ct5."UFDM ARR Dates in +/- 6 Month Range: with ARR", 
	ct5."UFDM ARR Dates in +/- 2 Month Range: with ARR", 
	ct5."Abs. Difference between Churn Date and 2 Month Range"
from 
	combined_table_5 ct5
where 
	ranking_2_month = 1
)

--Joining to to UFDM ARR with PF Makeup and Ratio. 
, final_table_2  as 
(
select 
	ft1.mcid,
	ft1."Start of SST Data from UFDM",
	ft1."Start of Data in TAT", 
	ft1."Date of Churn", 
	ft1."UFDM ARR Dates in +/- 6 Month Range: with ARR",
	ua1.product_family,
	ua1."Ratio to Each PF"
from 
	final_table_1 ft1
left join 
	ufdm_arr_1 ua1 
		on 
			ft1.mcid = ua1.mcid 
			and 
			ft1."UFDM ARR Dates in +/- 6 Month Range: with ARR" = ua1.date_ufdm_arr
--Filter on Dates
where 
	ft1."UFDM ARR Dates in +/- 6 Month Range: with ARR" is not null 
)

--We will only we be using dates within the 6 month range as it covers the 2 month range as well and casts a wider net 
--Produce a final table for the DEs with mcid, Date in UFDM ARR, Start of Data in TAT, Product Family Makeup and Ratio 
--This is the drag ratio table

,	 drag_ratio_c2 as 
(
select 
	ft2.mcid, 
	ft2."Start of Data in TAT" as "Start of Drag Ratio in TAT", 
	ft2."Date of Churn"-interval '1 month' as "End of Drag Ratio in TAT", --end it before the churn date, 
	ft2."Date of Churn" as "Churn Date in TAT", 
	((ft2."UFDM ARR Dates in +/- 6 Month Range: with ARR"+interval '1 month')-interval '1 day') as "Date in UFDM ARR",
	ft2.product_family as "Product Family in UFDM ARR", 
	ft2."Ratio to Each PF" as "Ratio of ARR for Each PF in UFDM ARR"
from 
	final_table_2 ft2
)

----End of Code-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--Export this for the drag ratio table 

--Test: Make sure that sum of drag ratios by mcids do not cross 1 

, test_1 as 
(
select 
	*, 
	sum("Ratio of ARR for Each PF in UFDM ARR") over(partition by mcid, "Date in UFDM ARR") as sum_of_ratios 
from 
	drag_ratio_c2 
) 

select 
	*
from 
	test_1
);



-- where 
-- 	sum_of_ratios > 1.1
	
-- where 
-- 	mcid = '0f944d75-a26a-e611-80e5-c4346bad92d0'
			

-- --Test: Make sure they don't have duplicates 

--select 
--	distinct mcid, 
--	count(*)
--from 
--	final_table_1 
--group by 
--	mcid 
--having 
--	count(*) > 1



	
	