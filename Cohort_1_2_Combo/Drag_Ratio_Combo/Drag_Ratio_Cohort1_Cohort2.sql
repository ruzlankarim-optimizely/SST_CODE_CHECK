--Code to create a combined drag ratio 
drop table if exists sandbox.combined_drag_ratio ;
create table sandbox.combined_drag_ratio as (
with combined_drag_ratio as 
(
SELECT 
      mcid_arr,
      "MAX Snapshot Date of TAT",
      product_family_arr,
      "Ratio of ARR Allocated to PF UFDM ARR" AS "Ratio of ARR",
      "Date to Drag to Under Scenario 1" AS "Date to Drag: Sol. 1"
FROM sandbox.cohort_1_drag_ratio AS a 
UNION ALL 
SELECT 
      mcid AS mcid_arr ,
      "End of Drag Ratio in TAT" AS "Max Snapshot Date in TAT",
      "Product Family in UFDM ARR" AS product_family_arr,
      "Ratio of ARR for Each PF in UFDM ARR" AS "Ratio of ARR",
      "Start of Drag Ratio in TAT" AS "Date to Drag: Sol. 1"
FROM sandbox.cohort_2_drag_ratio
WHERE mcid NOT IN(SELECT  DISTINCT mcid_arr FROM sandbox.cohort_1_drag_ratio)
)

select 
	*
from 
	combined_drag_ratio
); 
--Tests 

--select 
--	distinct mcid
--from 
--	sandbox.cohort2_drag_ratio
--where 
--	mcid in 
--		(
--		SELECT  
--			DISTINCT mcid_arr 
--		FROM 
--			sandbox.cohort1_drag_ratio
--		)
		
