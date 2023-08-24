--Comparing SST After Solution 1, Cohort 2

with new_sst as 
(
SELECT 
	distinct mcid as mcid_new,
	snapshot_date as snapshot_date_new,
	sum(arr) as sum_arr_usd_new
FROM 
	sandbox.solution2_sst_after_senti
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
	sum(arr) as sum_arr_usd
FROM 
	ufdm.sst 
where 
	overage_flag = 'N'
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
	abs(nt.sum_arr_usd_new-ot.sum_arr_usd) > 1
order by 
	abs(nt.sum_arr_usd_new-ot.sum_arr_usd) desc 
) 

select 
	distinct mcid 
from 
	test_1 
	
--Run specific Tests 
	
select 
	*
from 
	ufdm.sst 
where 
	mcid = '83879ac4-85b5-e711-8118-70106fa6f461'
and 
	snapshot_date = '2020-02-29'
	
select 
	*
from 
	ufdm.tat_upload_data tud 
where 
	mcid = '83879ac4-85b5-e711-8118-70106fa6f461'
and 
	snapshot_date = '2020-02-01'
	
select 
	*
from 
	sandbox.cohort2_tat_v1 
where 
	mcid = '83879ac4-85b5-e711-8118-70106fa6f461'
and 
	snapshot_date = '2020-02-01'
	

---
	
	