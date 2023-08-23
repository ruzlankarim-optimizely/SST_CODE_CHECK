--Code for Drag Ration 
DROP TABLE IF EXISTS sandbox.cohort2_drag_ration;
CREATE TABLE sandbox.cohort2_drag_ration AS (
with tat_info as 
(
select 
	distinct 
	utu."customer_name_d&b",
	utu.parent_customer,
	utu.parent_master_customer_id,	
	utu.customer_name,	
	utu.end_customer,	
	utu.mcid,	
	utu."Overage Y/N",	
	utu."NS ID",
	utu.subsidiary_name,	
--	utu.product_family,	
	utu.currency,	
	utu.snapshot_date,	
--	utu.arr,
	utu.fx_rate_ccfx,	
--	utu.arr_usd_ccfx,	
	utu.ccfx_date,	
	utu.mcid_old,	
	utu.is_deleted,
	utu.modified_comments
from 
	ufdm.tat_upload_data utu
where  
    utu.currency is not null 
    and 
    utu.mcid is not null 
    and 
    utu.is_deleted IS DISTINCT FROM 1
    and 
    utu."Overage Y/N" is distinct from 'Y'
    and not 
	(
	date_trunc('month', snapshot_date) = '2021-12-01'::DATE
	                  AND product_family ilike '%Campaign%'
	)
)

,   combined_table_1 as 
(
select 
	t1."customer_name_d&b",
	t1.parent_customer,
	t1.parent_master_customer_id,	
	t1.customer_name,	
	t1.end_customer,	
	t1.mcid,	
	t1."Overage Y/N",	
	t1."NS ID",
	t1.subsidiary_name,	
--	t1.product_family,	
	t1.currency,	
	t1.snapshot_date,	
--	t1.arr,
	t1.fx_rate_ccfx,	
--	t1.arr_usd_ccfx,	
	t1.ccfx_date,	
	t1.mcid_old,	
	t1.is_deleted,
	t1.modified_comments, 
    dr."End of Drag Ratio in TAT" as "MAX Snapshot Date of TAT", 
    dr."Product Family in UFDM ARR" as product_family_arr, 
    dr."Ratio of ARR for Each PF in UFDM ARR" as "Original Ratio",
    dr."Start of Drag Ratio in TAT" as "Date to Drag to Under Scenario 1",
    row_number() over(partition by t1.mcid, t1.snapshot_date, dr."Product Family in UFDM ARR" order by t1.mcid) as "Row Number of PF"
from 
    tat_info t1
inner join 
	sandbox.cohort2_drag_ratio  dr 
        on 
            t1.mcid = dr.mcid
)

	


--select 
--	*
--from 
--	combined_table_1 
--where 
--	mcid = '010e7f39-f251-c40b-5395-585a3a3f0723'


,   combined_table_2 as 
(
select 
	ct1."customer_name_d&b",
	ct1.parent_customer,
	ct1.parent_master_customer_id,	
	ct1.customer_name,	
	ct1.end_customer,	
	ct1.mcid,	
	ct1."Overage Y/N",	
	ct1."NS ID",
	ct1.subsidiary_name,	
--	ct1.product_family,	
	ct1.currency,	
	ct1.snapshot_date,	
--	ct1.arr,
	ct1.fx_rate_ccfx,	
--	ct1.arr_usd_ccfx,	
	ct1.ccfx_date,	
	ct1.mcid_old,	
	ct1.is_deleted,
	ct1.modified_comments, 
    ct1."MAX Snapshot Date of TAT", 
    ct1.product_family_arr,
    ct1."Original Ratio",
    ct1."Date to Drag to Under Scenario 1",
    max(ct1."Row Number of PF") over(partition by mcid, ct1.snapshot_date, product_family_arr) as "No of Rows Per PF"
from 
    combined_table_1 ct1 

)

--select 
--	*
--from 
--	combined_table_2
--where 
--	mcid = '5e479b1a-2251-e811-813c-70106fa51d21'


, comibined_table_2a as 
(
select 
	ct1."customer_name_d&b",
	ct1.parent_customer,
	ct1.parent_master_customer_id,	
	ct1.customer_name,	
	ct1.end_customer,	
	ct1.mcid,	
	ct1."Overage Y/N",	
	ct1."NS ID",
	ct1.subsidiary_name,	
--	ct1.product_family,	
	ct1.currency,	
	ct1.snapshot_date,	
--	ct1.arr,
	ct1.fx_rate_ccfx,	
--	ct1.arr_usd_ccfx,	
	ct1.ccfx_date,	
	ct1.mcid_old,	
	ct1.is_deleted,
	ct1.modified_comments, 
    ct1."MAX Snapshot Date of TAT", 
    ct1.product_family_arr,
    ct1."Original Ratio",
    ct1."Date to Drag to Under Scenario 1",
    ct1."No of Rows Per PF"
from 
	combined_table_2 ct1
--Get rid of Campaign in Dec 2021 if it's copied over from UFDM ARR. Campaign does not come from TAT in Dec 2021. Therefore ratios in Dec 2021 should be done without Campaign
where 
	not 
	(date_trunc('month', snapshot_date) = '2021-12-01'::DATE AND product_family_arr ilike 'Recurring: Cloud: Other Bookings: Campaign')
)


--select 
--	*
--from 
--	comibined_table_2a
--where 
--	mcid = '010e7f39-f251-c40b-5395-585a3a3f0723'

, combined_table_3 as 
(
select 
	ct2."customer_name_d&b",
	ct2.parent_customer,
	ct2.parent_master_customer_id,	
	ct2.customer_name,	
	ct2.end_customer,	
	ct2.mcid,	
	ct2."Overage Y/N",	
	ct2."NS ID",
	ct2.subsidiary_name,	
--	ct2.product_family,	
	ct2.currency,	
	ct2.snapshot_date,	
--	ct2.arr,
	ct2.fx_rate_ccfx,	
--	ct2.arr_usd_ccfx,	
	ct2.ccfx_date,	
	ct2.mcid_old,	
	ct2.is_deleted,
	ct2.modified_comments, 
    ct2."MAX Snapshot Date of TAT", 
    ct2.product_family_arr,
    ct2."Original Ratio",
    ct2."Date to Drag to Under Scenario 1",
    --Calculate the new ratios 
    --If it's not Dec 2021, then original ratio divided by number of rows 
    --If it's Dec 2021, since we got rid of campaign, get ratio of ratios 
    case
    	when date_trunc('MONTH', snapshot_date) != '2021-12-01' then ct2."Original Ratio"/ct2."No of Rows Per PF"
    	when date_trunc('MONTH', snapshot_date)  = '2021-12-01' then ct2."Original Ratio"/(sum(ct2."Original Ratio") over(partition by ct2.mcid, ct2.snapshot_date))
    end as "New Ratio Per Date for TAT" 
from 
    comibined_table_2a ct2
where 
    ct2.snapshot_date <= ct2."MAX Snapshot Date of TAT"::DATE
and 
    ct2.snapshot_date >= ct2."Date to Drag to Under Scenario 1"::DATE
)

--select 
--	*
--from 
--	combined_table_3 
--where 
--	mcid = '010e7f39-f251-c40b-5395-585a3a3f0723'

,   combined_table_4 as 
(
select 
	ct3."customer_name_d&b",
	ct3.parent_customer,
	ct3.parent_master_customer_id,	
	ct3.customer_name,	
	ct3.end_customer,	
	ct3.mcid,	
	ct3."Overage Y/N",	
	ct3."NS ID",
	ct3.subsidiary_name,	
--	ct3.product_family,	
	ct3.currency,	
	ct3.snapshot_date,	
--	ct3.arr,
	ct3.fx_rate_ccfx,	
--	ct3.arr_usd_ccfx,	
	ct3.ccfx_date,	
	ct3.mcid_old,	
	ct3.is_deleted,
	ct3.modified_comments, 
    ct3."MAX Snapshot Date of TAT", 
    ct3.product_family_arr,
    ct3."Original Ratio",
    ct3."Date to Drag to Under Scenario 1",
    ct3."New Ratio Per Date for TAT",
    sum(ct3."New Ratio Per Date for TAT") over(partition by ct3.mcid, ct3.snapshot_date) as "Sum of Ratios Per MCID and Snapshot Date"
from 
    combined_table_3 ct3
)

--Table for Drag Ration: Use this to create drag ration 
select 
    *
from 
    combined_table_4
);
    
--Test to see if Ratios Add up to More than 1
--where 
--	"Sum of Ratios Per MCID and Snapshot Date" > 1.01
	
--	

