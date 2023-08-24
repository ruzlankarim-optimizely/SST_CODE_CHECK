with new_sst as 
(
SELECT 
	distinct mcid as mcid_new,
	snapshot_date as snapshot_date_new,
	sum(arr_usd_ccfx) as sum_arr_usd_new
FROM 
	sandbox.cohort2_tat_v1
--where 
--	overage_flag = 'N'
group by 
	1,2

)

,	old_sst as 
(
SELECT 
	distinct mcid,
	snapshot_date,
	sum(arr_usd_ccfx) as sum_arr_usd
FROM 
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
group by 
	1,2

)
	
, 	test_1 as 
(
select 
	ot.mcid,
	ot.snapshot_date,
	ot.sum_arr_usd,
	nt.sum_arr_usd_new,
	abs(coalesce(nt.sum_arr_usd_new,0)-coalesce(ot.sum_arr_usd,0)) as diff
from 
	old_sst ot 
full join 
	new_sst nt 
		on 
			ot.mcid = nt.mcid_new
			and 
			ot.snapshot_date = nt.snapshot_date_new
where 
	abs(coalesce(nt.sum_arr_usd_new,0)-coalesce(ot.sum_arr_usd,0))  > 1
order by 
	abs(nt.sum_arr_usd_new-ot.sum_arr_usd) desc 
) 

,	mismatch_mcid as 
(
select 
	distinct mcid 
from 
	test_1 
) 

	
select 
	distinct mcid 
from 
	sandbox.cohort2_drag_ration
	
--Compare TAT tables 
