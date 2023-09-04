CREATE OR REPLACE FUNCTION sandbox_pd.sp_populate_sst_sensitivity_analysis(run_cohort_1 integer, run_cohort_2 integer) RETURNS void LANGUAGE plpgsql AS $function$ BEGIN --#################################################
  --COHORT 1
  --#################################################
  if run_cohort_1 = 1 then --Use Mathias Original Formula: Churn at a snapshot date > Avg. ARR lst 12 Mths but also add a caveat: at that point in time what is the customers arr including that month and afterwards?
  --Is the Total Churn falling because the customer is totally churning out?
  --Introduce a new variable for total movements over 12 Mths
  --The highest number of movements is 11
  --Look at customers who have minimum 2 movements a year
  --For this version's Up and Down Movements
  --100% threshold
  -- 0k threshold
  -- Minimum 2 movements a year, with 1 +ve and 1 negative
  -- Add a column to look at sequence of movements +ve and -ve over the last 12 months
  -- Take customers who only have history for at least 12 months or more
  --Absolute Churn Values > 1.1*Average ARR of the Last 12 Months
  --Exclude GMBH overages
  --1) Count of distinct dates per mcid
  --2) Count of distinct products per date per mcid
  --3) Count of records per mcid per date
  DROP TABLE IF EXISTS sandbox_pd.SST_COHORT_1;
CREATE TABLE sandbox_pd.SST_COHORT_1 AS with initial_sst as (
  select *,
    max(
      isst."Ranking of PFs Per MCID & Date (Includ. Blank & Null)"
    ) over(partition by isst.snapshot_date, isst.mcid) as "No. of Dist. PFs Per MCID & Date (Includ. Blank & Null)",
    max(isst."Ranking of Distinct Dates Per MCID") over(partition by isst.mcid) as "No. of Distinct Dates Per MCID"
  from (
      select *,
        count(*) over(partition by snapshot_date, mcid) as "No of Records Per MCID & Date",
        dense_rank() over(
          partition by snapshot_date,
          mcid
          order by product_family
        ) as "Ranking of PFs Per MCID & Date (Includ. Blank & Null)",
        dense_rank() over(
          partition by mcid
          order by snapshot_date
        ) as "Ranking of Distinct Dates Per MCID"
      from sandbox_pd.sst
    ) as isst
  order by isst.snapshot_date,
    isst.mcid
),
sandbox_sst_1 as (
  select *
  from --watch which table
    initial_sst --put a filter for overages
  where overage_flag = 'N'
) --select
--	*
--from
--	sandbox_sst_1
--where
--	mcid in
--	('010239c8-c80f-c9d1-ba22-a48f9a5ed28b')
,
dist_mcid as (
  select distinct mcid
  from sandbox_sst_1
) --select
--	*
--from
--	dist_mcid
,
dist_date as (
  select distinct snapshot_date
  from sandbox_sst_1
) --select
--	*
--from
--	dist_date
--Cartesian Join
,
joined_cart_table as (
  select snapshot_date,
    mcid
  from dist_mcid,
    dist_date
  order by snapshot_date,
    mcid
) --select
--	*
--from
--	joined_cart_table
--where
--	mcid in
--	('010239c8-c80f-c9d1-ba22-a48f9a5ed28b')
--Sum up arr from sst by mcid and snapshot date
,
sst_sum_1 as (
  select distinct snapshot_date as snapshot_date_sst,
    mcid as mcid_sst,
    sum(arr) over(partition by snapshot_date, mcid) as sum_arr_sst
  from sandbox_sst_1
) --Left join the sum sst table to the cartesian table -- so that mcids which are missing values in certain dates will get null arr for those dates
--This is important to do before hand -- it's important to keep lagged sum null when mcid changes
,
joined_table_1 as (
  select jct.snapshot_date,
    jct.mcid,
    ss1.sum_arr_sst
  from joined_cart_table jct
    left join sst_sum_1 ss1 on jct.snapshot_date = ss1.snapshot_date_sst
    and jct.mcid = ss1.mcid_sst
) --select
--	*
--from
--	joined_table_1
--where
--	mcid in
--	('010239c8-c80f-c9d1-ba22-a48f9a5ed28b')
--Important: We treat null values as zero -- meaning if their is null value one month and value increases the next month -- it should be positive
,
joined_table_2 as (
  select snapshot_date,
    mcid,
    sum_arr_sst,
    coalesce(sum_arr_sst, 0) as "Formatted Sum by MCID and Date"
  from joined_table_1
  order by mcid
) --Get ARR of Previous Month for Each MCID
--Important: Introduce all the constants.
--In this case: 100% threshold, 3k as threshold value, and the time period is a year or 12 months
,
joined_table_3 as (
  select snapshot_date,
    mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    lag("Formatted Sum by MCID and Date") over(
      partition by mcid
      order by snapshot_date
    ) as "ARR of Previous Month",
    1 as threshold_pct,
    0 as threshold_ $,
    12 as time_period,
    2 as "Minimmum Number of Movmt. Per Year",
    (
      select min(snapshot_date) as start_date
      from joined_table_2
    ) as start_date
  from joined_table_2
) --select
--	*
--from
--	joined_table_3
--Step 1. Compare value to previous date. This is for each month
--Excel formula: IF(AND(new-old<0,ABS(new-old)>=new*threshold_pct),"-",IF(AND(new-old>0,ABS(new-old)>=new*threshold_pct),"+",""))
--The first day of the distinct snapshot date --- Jan 19 -- should always be null
,
joined_table_4 as (
  select snapshot_date,
    mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous Month",
    threshold_pct,
    threshold_ $,
    time_period,
    "Minimmum Number of Movmt. Per Year",
    start_date,
    sum("Formatted Sum by MCID and Date") over(
      partition by mcid
      order by snapshot_date rows between unbounded preceding and current row
    ) as "Total ARR for Cust. from start till now",
    --	 (start_date + CAST(time_period || ' MONTHS' AS interval)) as test,
    case
      when "Formatted Sum by MCID and Date" - "ARR of Previous Month" < 0
      and abs(
        "Formatted Sum by MCID and Date" - "ARR of Previous Month"
      ) >= "Formatted Sum by MCID and Date" *(threshold_pct) then '-'
      when "Formatted Sum by MCID and Date" - "ARR of Previous Month" > 0
      and abs(
        "Formatted Sum by MCID and Date" - "ARR of Previous Month"
      ) >= "Formatted Sum by MCID and Date" *(threshold_pct) then '+'
      else ' '
    end as Movement
  from joined_table_3
) --select
--	*
--from
--	joined_table_4
--Step 2: Count the +ves and -ves over the last 12 month period, including the current month and flag. This is for each month
--Excel formula: IF(IF(COUNTIFS(Last 12 months including itself,"+")>=1,"yes","")&IF(COUNTIFS(Last 12 months including itself,"-")>=1,"yes","")="yesyes","Review","")
--Also sum up the customers arr at any point in time for all dates preceeding
,
joined_table_5 as (
  select snapshot_date,
    mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous Month",
    threshold_pct,
    threshold_ $,
    time_period,
    "Minimmum Number of Movmt. Per Year",
    Movement,
    start_date,
    "Total ARR for Cust. from start till now",
    case
      when "Total ARR for Cust. from start till now" = 0 then 0
      else 1
    end as "Ranking for ARR from start till now",
    case
      when sum_arr_sst is null
      and snapshot_date = (
        select max(snapshot_date)
        from joined_table_4
      ) then 1
      else 0
    end as "No ARR in last snapshot date",
    case
      when Movement = '+' then 1
      else 0
    end as "Positive Movement",
    case
      when Movement = '-' then 1
      else 0
    end as "Negative Movement"
  from joined_table_4
),
joined_table_6 as (
  select snapshot_date,
    mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous Month",
    threshold_pct,
    threshold_ $,
    time_period,
    "Minimmum Number of Movmt. Per Year",
    Movement,
    start_date,
    "No ARR in last snapshot date",
    "Positive Movement",
    "Negative Movement",
    sum("Formatted Sum by MCID and Date") over(
      partition by mcid
      order by snapshot_date rows between current row
        and unbounded following
    ) as "Total ARR for Cust. from now till end",
    "Ranking for ARR from start till now",
    min(snapshot_date) filter (
      where "Ranking for ARR from start till now" = 1
    ) over(partition by mcid) as "Start Date of ARR",
    "Total ARR for Cust. from start till now",
    sum("Positive Movement") over(
      partition by mcid
      order by snapshot_date rows 12 -1 preceding
    ) as "Positive Movement Sum 12 Mths",
    sum("Negative Movement") over(
      partition by mcid
      order by snapshot_date rows 12 -1 preceding
    ) as "Negative Movement Sum 12 Mths",
    replace(
      string_agg(movement, '') over(
        partition by mcid
        order by snapshot_date rows 12 -1 preceding
      ),
      ' ',
      ''
    ) as "Sequence of Movement 12 Mths",
    sum("No ARR in last snapshot date") over(partition by mcid) as "Flag for Complete Churn"
  from joined_table_5
) --Step 3: Add up both +ve and -ve movements over the last 12 months and call it Total Number of Movements
,
joined_table_7 as (
  select snapshot_date,
    mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous Month",
    threshold_pct,
    threshold_ $,
    time_period,
    "Minimmum Number of Movmt. Per Year",
    Movement,
    start_date,
    "No ARR in last snapshot date",
    "Positive Movement",
    "Negative Movement",
    "Total ARR for Cust. from now till end",
    "Total ARR for Cust. from start till now",
    "Ranking for ARR from start till now",
    "Start Date of ARR",
    "Positive Movement Sum 12 Mths",
    "Negative Movement Sum 12 Mths",
    "Sequence of Movement 12 Mths",
    "Positive Movement Sum 12 Mths" + "Negative Movement Sum 12 Mths" as "All Movements Sum 12 Mths",
    "Flag for Complete Churn",
    case
      when "Positive Movement Sum 12 Mths" >= 1
      and "Negative Movement Sum 12 Mths" >= 1 --at least one positive and one negative movement
      and snapshot_date >= (
        start_date + CAST(time_period || ' MONTHS' AS interval)
      ) -- start from 12 months after the first snapshot date of the dataset
      and (
        (
          select max(snapshot_date)
          from joined_table_6
        ) -("Start Date of ARR")
      ) >= 365 --for customers who have at least 12 months of ARR history
      and (
        "Positive Movement Sum 12 Mths" + "Negative Movement Sum 12 Mths"
      ) >= "Minimmum Number of Movmt. Per Year" -- Number of Movements in the last 12 months must be greater than or equal to Minimum Number of Movements
      then 1
      else 0
    end as "Review (1 if Review)"
  from joined_table_6
) --Step 4: Count the number of flags over the entire period -- call this "Occurrences". This is over the entire time period and by customer.
,
joined_table_8 as (
  select snapshot_date,
    mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous Month",
    threshold_pct,
    threshold_ $,
    time_period,
    "Minimmum Number of Movmt. Per Year",
    Movement,
    start_date,
    "No ARR in last snapshot date",
    "Positive Movement",
    "Negative Movement",
    "Total ARR for Cust. from now till end",
    "Total ARR for Cust. from start till now",
    "Ranking for ARR from start till now",
    "Start Date of ARR",
    "Positive Movement Sum 12 Mths",
    "Negative Movement Sum 12 Mths",
    "Sequence of Movement 12 Mths",
    "All Movements Sum 12 Mths",
    "Flag for Complete Churn",
    "Review (1 if Review)",
    sum("Review (1 if Review)") over(partition by mcid) as Occurrences,
    case
      when "Review (1 if Review)" = 1
      and substring("Sequence of Movement 12 Mths", 1, 2) = '-+' then 'Dip'
      when "Review (1 if Review)" = 1
      and substring("Sequence of Movement 12 Mths", 1, 2) = '+-' then 'Spike'
    end as "Dip or Spike in last 12 Mths"
  from joined_table_7
) --Step 5: Measure relative & absolute month over month change for each month in ARR. This is for each month and each customer. For Sanity Check, make a column
--of churn values only
,
joined_table_9 as (
  select snapshot_date,
    date_trunc('YEAR', snapshot_date) as "Year",
    mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous Month",
    "Formatted Sum by MCID and Date" - "ARR of Previous Month" as "Change in ARR",
    abs(
      "Formatted Sum by MCID and Date" - "ARR of Previous Month"
    ) as "Absolute Change in ARR",
    case
      --change in ARR is less than 0 then fill the column with churn values
      when "Formatted Sum by MCID and Date" - "ARR of Previous Month" < 0 then (
        "Formatted Sum by MCID and Date" - "ARR of Previous Month"
      )
      else 0
    end as "Churn Values",
    threshold_pct,
    threshold_ $,
    time_period,
    "Minimmum Number of Movmt. Per Year",
    Movement,
    start_date,
    "No ARR in last snapshot date",
    "Positive Movement",
    "Negative Movement",
    "Total ARR for Cust. from now till end",
    "Total ARR for Cust. from start till now",
    "Ranking for ARR from start till now",
    "Start Date of ARR",
    "Positive Movement Sum 12 Mths",
    "Negative Movement Sum 12 Mths",
    "Sequence of Movement 12 Mths",
    "All Movements Sum 12 Mths",
    "Flag for Complete Churn",
    "Review (1 if Review)",
    Occurrences,
    "Dip or Spike in last 12 Mths",
    case
      when "Dip or Spike in last 12 Mths" = 'Dip' then -1
      when "Dip or Spike in last 12 Mths" = 'Spike' then + 1
      else 0
    end as "Dip/Spike Number in last 12 Mths"
  from joined_table_8
) --Step 6: Do an absolute sum of month over month change -- call this Total Abs. ARR Change Per Customer. This is over the entire time period and by customer.
--Step 7: Do a sum of month over month change -- call this Total ARR Change Per Customer. This is over the entire time period and by customer.
--Step 8: Add up churn values to find total churn for a customer in a year. Also find average ARR of customer per year/
,
joined_table_10 as (
  select snapshot_date,
    "Year",
    mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous Month",
    "Change in ARR",
    "Absolute Change in ARR",
    "Churn Values",
    threshold_pct,
    threshold_ $,
    time_period,
    "Minimmum Number of Movmt. Per Year",
    Movement,
    start_date,
    "No ARR in last snapshot date",
    "Positive Movement",
    "Negative Movement",
    "Total ARR for Cust. from now till end",
    "Total ARR for Cust. from start till now",
    "Ranking for ARR from start till now",
    "Start Date of ARR",
    "Positive Movement Sum 12 Mths",
    "Negative Movement Sum 12 Mths",
    "Sequence of Movement 12 Mths",
    "All Movements Sum 12 Mths",
    "Flag for Complete Churn",
    "Review (1 if Review)",
    Occurrences,
    "Dip or Spike in last 12 Mths",
    "Dip/Spike Number in last 12 Mths",
    min("Dip/Spike Number in last 12 Mths") over(
      partition by mcid
      order by snapshot_date rows between 1 following and 11 following
    ) as "Dips in next 11 Mths",
    max("Dip/Spike Number in last 12 Mths") over(
      partition by mcid
      order by snapshot_date rows between 1 following and 11 following
    ) as "Spikes in next 11 Mths",
    sum("Absolute Change in ARR") over(partition by mcid) as "Total Abs. ARR Change Per Customer",
    sum("Change in ARR") over(partition by mcid) as "Total ARR Change Per Customer",
    avg("Formatted Sum by MCID and Date") over(
      partition by mcid
      order by snapshot_date rows between 12 preceding and 1 preceding
    ) as "Avg. ARR of Cust. lst 12 Mths",
    avg("Formatted Sum by MCID and Date") filter (
      where "Ranking for ARR from start till now" = 1
    ) over(
      partition by mcid
      order by snapshot_date rows between 12 preceding and 1 preceding
    ) as "Avg. ARR of Cust. lst 12 Mths since start date"
  from joined_table_9
) --, test_table as
--(
--select
--	*
--from
--	joined_table_10
--where
--	mcid = '010239c8-c80f-c9d1-ba22-a48f9a5ed28b'
--order by
--	snapshot_date
--)
--
--select
--	*
--from
--	test_table
--select
--	distinct mcid
--from
--	joined_table_10
--where
--	"Dip or Spike in last 12 Mths" = 'Spike'
--limit
--	5
--Step 9: Flag Records to reviewed -- if "Occurrences" is greater than zero and "Total Abs. ARR Change Per Customer" > Threshold_$ then Cust. with Up & Down Mvmt.
--Step 10: Flag Churn Cust. with Up & Down Mvmt.s -- if "Occurrences" is greater than zero and "Total Abs. ARR Change Per Customer" > Threshold_$ and Total ARR Change Per Customer < 0 then Churn Cust. with Up & Down Mvmt.
--Step 11: Flag New Cust. with Up & Down Mvmt.s -- if "Occurrences" is greater than zero and "Total Abs. ARR Change Per Customer" > Threshold_$ and Total ARR Change Per Customer > 0 then New Cust. with Up & Down Mvmt.
--Step 12: Flag records where abs("Churn Values") > "Avg. ARR of Cust. lst 12 Mths" and Total ARR for Cust. from now till end > 0 -- this is therefore not
--happening due to churn
,
joined_table_11 as (
  select snapshot_date,
    "Year",
    mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous Month",
    "Change in ARR",
    "Churn Values",
    "Absolute Change in ARR",
    threshold_pct,
    threshold_ $,
    time_period,
    "Minimmum Number of Movmt. Per Year",
    Movement,
    start_date,
    "No ARR in last snapshot date",
    "Positive Movement",
    "Negative Movement",
    "Positive Movement Sum 12 Mths",
    "Negative Movement Sum 12 Mths",
    "Sequence of Movement 12 Mths",
    "All Movements Sum 12 Mths",
    "Flag for Complete Churn",
    "Review (1 if Review)",
    Occurrences,
    "Dip or Spike in last 12 Mths",
    "Dip/Spike Number in last 12 Mths",
    "Dips in next 11 Mths",
    "Spikes in next 11 Mths",
    "Total Abs. ARR Change Per Customer",
    "Total ARR Change Per Customer",
    "Avg. ARR of Cust. lst 12 Mths",
    "Avg. ARR of Cust. lst 12 Mths since start date",
    "Total ARR for Cust. from now till end",
    "Total ARR for Cust. from start till now",
    "Ranking for ARR from start till now",
    "Start Date of ARR",
    case
      when Occurrences > 0
      and "Total Abs. ARR Change Per Customer" > threshold_ $ then 1
      else 0
    end as "Cust. with Up & Down Mvmt.",
    case
      when Occurrences > 0
      and "Total Abs. ARR Change Per Customer" > threshold_ $
      and "Total ARR Change Per Customer" < 0 then 1
      else 0
    end as "Churn Cust. with Up & Down Mvmt.",
    case
      when Occurrences > 0
      and "Total Abs. ARR Change Per Customer" > threshold_ $
      and "Total ARR Change Per Customer" > 0 then 1
      else 0
    end as "New Cust. with Up & Down Mvmt.",
    case
      when abs("Churn Values") > (
        1.1 * "Avg. ARR of Cust. lst 12 Mths since start date"
      ) --use start date of customer
      and "Total ARR for Cust. from now till end" > 12
      and snapshot_date >= (
        start_date + CAST(time_period || ' MONTHS' AS interval)
      ) -- start from 12 months after the first snapshot date of the dataset
      and (
        (
          select max(snapshot_date)
          from joined_table_6
        ) -("Start Date of ARR")
      ) >= 365 --for customers who have at least 12 months of ARR history
      then 1
      else 0
    end as "Churn Values at Date > Avg. ARR lst 12 Mths"
  from joined_table_10
) --select
--	*
--from
--	joined_table_11
--Step 13: Sum up Occurrences where Total Churn/Year > Avg. ARR lst 12 Mths by customer over all time periods
,
joined_table_12 as (
  select snapshot_date,
    "Year",
    mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous Month",
    "Change in ARR",
    "Churn Values",
    "Absolute Change in ARR",
    threshold_pct,
    threshold_ $,
    time_period,
    "Minimmum Number of Movmt. Per Year",
    Movement,
    start_date,
    "No ARR in last snapshot date",
    "Positive Movement",
    "Negative Movement",
    "Positive Movement Sum 12 Mths",
    "Negative Movement Sum 12 Mths",
    "Sequence of Movement 12 Mths",
    "All Movements Sum 12 Mths",
    "Flag for Complete Churn",
    "Review (1 if Review)",
    Occurrences,
    "Dip or Spike in last 12 Mths",
    "Dip/Spike Number in last 12 Mths",
    "Dips in next 11 Mths",
    "Spikes in next 11 Mths",
    "Total Abs. ARR Change Per Customer",
    "Total ARR Change Per Customer",
    "Avg. ARR of Cust. lst 12 Mths",
    "Avg. ARR of Cust. lst 12 Mths since start date",
    "Total ARR for Cust. from now till end",
    "Total ARR for Cust. from start till now",
    "Ranking for ARR from start till now",
    "Start Date of ARR",
    "Cust. with Up & Down Mvmt.",
    "Churn Cust. with Up & Down Mvmt.",
    "New Cust. with Up & Down Mvmt.",
    "Churn Values at Date > Avg. ARR lst 12 Mths",
    sum("Churn Values at Date > Avg. ARR lst 12 Mths") over(partition by mcid) as "Occurence of Churn at Date > Avg. ARR lst 12 Mths"
  from joined_table_11
) --Step 14: Flag customers who at any point in time have of Occurence of Total Churn/Year > Avg. ARR lst 12 Mths. This is actually measured in months
,
joined_table_13 as (
  select snapshot_date,
    "Year",
    mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous Month",
    "Change in ARR",
    "Churn Values",
    "Absolute Change in ARR",
    threshold_pct,
    threshold_ $,
    time_period,
    "Minimmum Number of Movmt. Per Year",
    Movement,
    start_date,
    "No ARR in last snapshot date",
    "Positive Movement",
    "Negative Movement",
    "Positive Movement Sum 12 Mths",
    "Negative Movement Sum 12 Mths",
    "Sequence of Movement 12 Mths",
    "All Movements Sum 12 Mths",
    "Flag for Complete Churn",
    "Review (1 if Review)",
    Occurrences,
    "Dip or Spike in last 12 Mths",
    "Dip/Spike Number in last 12 Mths",
    "Dips in next 11 Mths",
    "Spikes in next 11 Mths",
    "Total Abs. ARR Change Per Customer",
    "Total ARR Change Per Customer",
    "Avg. ARR of Cust. lst 12 Mths",
    "Avg. ARR of Cust. lst 12 Mths since start date",
    "Total ARR for Cust. from now till end",
    "Total ARR for Cust. from start till now",
    "Ranking for ARR from start till now",
    "Start Date of ARR",
    "Cust. with Up & Down Mvmt.",
    "Churn Cust. with Up & Down Mvmt.",
    "New Cust. with Up & Down Mvmt.",
    "Churn Values at Date > Avg. ARR lst 12 Mths",
    max("Churn Values at Date > Avg. ARR lst 12 Mths") over(
      partition by mcid
      order by snapshot_date rows between 1 following and 11 following
    ) as "Churn Value > Avg. ARR Flag in next 11 months",
    "Occurence of Churn at Date > Avg. ARR lst 12 Mths",
    case
      when "Occurence of Churn at Date > Avg. ARR lst 12 Mths" > 0 then 1
      else 0
    end as "Cust. with Churn at Date > Avg. ARR lst 12 Mths"
  from joined_table_12
) --select
--	*
--from
--	joined_table_13
--where
--	mcid = '026d677d-34e4-e411-9afb-0050568d2da8'
--order by
--	snapshot_date
--Step 15: Count the number of distinct customers with Up and Down Movement -- this is the Total Cust. with U&D Mvmt.
--Step 16: Sum Total ARR Change Per Customer for Churn Cust. with Up & Down Mvmt.s -- this is Total Churn with U&D Mvmt.
--Step 17: Sum Total ARR Change Per Customer for New Cust. with Up & Down Mvmt.s -- this is Total New ARR with U&D Mvmt.
--Step 18: Sum Total ARR Change Per Customer for customers who have to reviewed -- this is Total ARR with U&D Mvmt.. It should equal = Total New ARR with U&D Mvmt.+Total Churn with U&D Mvmt.
,
joined_table_14 as (
  select snapshot_date,
    "Year",
    mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous Month",
    "Change in ARR",
    "Churn Values",
    "Absolute Change in ARR",
    threshold_pct,
    threshold_ $,
    time_period,
    "Minimmum Number of Movmt. Per Year",
    Movement,
    start_date,
    "No ARR in last snapshot date",
    "Positive Movement",
    "Negative Movement",
    "Positive Movement Sum 12 Mths",
    "Negative Movement Sum 12 Mths",
    "Sequence of Movement 12 Mths",
    "All Movements Sum 12 Mths",
    "Flag for Complete Churn",
    "Review (1 if Review)",
    Occurrences,
    "Dip or Spike in last 12 Mths",
    "Dip/Spike Number in last 12 Mths",
    "Dips in next 11 Mths",
    "Spikes in next 11 Mths",
    "Total Abs. ARR Change Per Customer",
    "Total ARR Change Per Customer",
    "Avg. ARR of Cust. lst 12 Mths",
    "Avg. ARR of Cust. lst 12 Mths since start date",
    "Total ARR for Cust. from now till end",
    "Total ARR for Cust. from start till now",
    "Ranking for ARR from start till now",
    "Start Date of ARR",
    "Cust. with Up & Down Mvmt.",
    "Churn Cust. with Up & Down Mvmt.",
    "New Cust. with Up & Down Mvmt.",
    "Churn Values at Date > Avg. ARR lst 12 Mths",
    "Churn Value > Avg. ARR Flag in next 11 months",
    case
      --Non U&D Customer
      when "Churn Value > Avg. ARR Flag in next 11 months" = 1
      and "Formatted Sum by MCID and Date" > (
        1.1 * "Avg. ARR of Cust. lst 12 Mths since start date"
      ) -- Look at ARR history for last 12 months only
      and "Cust. with Up & Down Mvmt." = 0 --No remedial actions after 2023 Jan
      and snapshot_date < '2023-01-01' then 'Overwrite with latest Non-Spike Value - Non U&D' --U&D Customers
      when "Dips in next 11 Mths" = -1
      and "Formatted Sum by MCID and Date" = 0
      and "Total ARR for Cust. from now till end" <> 0
      and "Cust. with Up & Down Mvmt." = 1 --No remedial actions after 2023 Jan
      and snapshot_date < '2023-01-01' then 'Overwrite with the latest non-zero value - U&D'
      when "Spikes in next 11 Mths" = 1
      and "Formatted Sum by MCID and Date" <> 0
      and "Total ARR for Cust. from now till end" <> 0
      and "Cust. with Up & Down Mvmt." = 1 --No remedial actions after 2023 Jan
      and snapshot_date < '2023-01-01' then 'Overwrite with latest non-Spike value - U&D'
    end as "Remedial Action",
    "Occurence of Churn at Date > Avg. ARR lst 12 Mths",
    "Cust. with Churn at Date > Avg. ARR lst 12 Mths",
    case
      when "Cust. with Churn at Date > Avg. ARR lst 12 Mths" > 0
      or "Cust. with Up & Down Mvmt." > 0 then 1
      else 0
    end as "Cust. either U&D or Churn > Avg. ARR",
    (
      select count (distinct mcid)
      from joined_table_13
      where "Cust. with Up & Down Mvmt." > 0
    ) as "Total Cust. with U&D Mvmt.",
    (
      select sum(cc."Total ARR Change Per Customer") as "Total Churn with U&D Mvmt."
      from (
          select distinct mcid,
            "Total ARR Change Per Customer",
            "Churn Cust. with Up & Down Mvmt."
          from joined_table_13
          where "Churn Cust. with Up & Down Mvmt." > 0
        ) as cc
      group by cc."Churn Cust. with Up & Down Mvmt."
    ) as "Total Churn with U&D Mvmt.",
    (
      select sum(cc."Total ARR Change Per Customer") as "Total New ARR with U&D Mvmt."
      from (
          select distinct mcid,
            "Total ARR Change Per Customer",
            "New Cust. with Up & Down Mvmt."
          from joined_table_13
          where "New Cust. with Up & Down Mvmt." > 0
        ) as cc
      group by cc."New Cust. with Up & Down Mvmt."
    ) as "Total New ARR with U&D Mvmt.",
    (
      select sum(cc."Total ARR Change Per Customer") as "Total ARR to be Reviewed with U&D Mvmt."
      from (
          select distinct mcid,
            "Total ARR Change Per Customer",
            "Cust. with Up & Down Mvmt."
          from joined_table_13
          where "Cust. with Up & Down Mvmt." > 0
        ) as cc
      group by cc."Cust. with Up & Down Mvmt."
    ) as "Total ARR to be Reviewed with U&D Mvmt.",
    (
      select count (distinct mcid)
      from joined_table_13
      where "Cust. with Churn at Date > Avg. ARR lst 12 Mths" > 0
    ) as "All Cust. Churn at Date> Avg. ARR lst 12 Mths",
    (
      select count (distinct mcid)
      from joined_table_13
      where "Cust. with Churn at Date > Avg. ARR lst 12 Mths" > 0
        and "Flag for Complete Churn" = 0
    ) as "All Exisiting Cust. Churn at Date> Avg. ARR lst 12 Mths",
    (
      select count (distinct mcid)
      from joined_table_13
      where "Cust. with Churn at Date > Avg. ARR lst 12 Mths" > 0
        or "Cust. with Up & Down Mvmt." > 0
    ) as "All Cust. either U&D or Churn > Avg. ARR",
    (
      select sum(cc."Total ARR Change Per Customer") as "Total ARR For Cust. w/t Churn > Avg. ARR lst 12 Mths"
      from (
          select distinct mcid,
            "Total ARR Change Per Customer",
            "Cust. with Churn at Date > Avg. ARR lst 12 Mths"
          from joined_table_13
          where "Cust. with Churn at Date > Avg. ARR lst 12 Mths" > 0
        ) as cc
      group by cc."Cust. with Churn at Date > Avg. ARR lst 12 Mths"
    ),
    (
      select sum(cc."Total ARR Change Per Customer") as "Total ARR Cust. either U&D or Churn > Avg. ARR"
      from (
          select distinct mcid,
            "Total ARR Change Per Customer",
            "Cust. with Churn at Date > Avg. ARR lst 12 Mths",
            "Cust. with Up & Down Mvmt.",
            1 as grouper
          from joined_table_13
          where "Cust. with Churn at Date > Avg. ARR lst 12 Mths" > 0
            or "Cust. with Up & Down Mvmt." > 0
        ) as cc
      group by cc.grouper
    )
  from joined_table_13
),
joined_table_15 as (
  select snapshot_date as "Snapshot Date (Month)",
    "Year",
    mcid as "End Customer MCID",
    start_date as "Start Date of the Dataset",
    "Formatted Sum by MCID and Date" as "ARR of Customer by Date",
    "ARR of Previous Month",
    "Change in ARR" as "Change in ARR Compared to Previous Month",
    "Absolute Change in ARR" as "Absolute Change in ARR Compared to Previous Month",
    threshold_pct * 100 as "Threshold (%) for Up and Down Movement",
    threshold_ $ as "Threshold ($) for Up and Down Movement",
    time_period as "Time Period (Months) for Up and Down Movement",
    "Minimmum Number of Movmt. Per Year" as "Minimum Number of Movements Per Year",
    Movement as "Positive or Negative Movement Compared to Previous Month",
    "No ARR in last snapshot date" as "No ARR in last snapshot date (1 if yes)",
    "Flag for Complete Churn" as "Flag for Complete Churn (1 if yes)",
    "Positive Movement" as "Positive Movement Compared to Previous Month (1 if yes)",
    "Negative Movement" as "Negative Movement Compared to Previous Month (1 if yes)",
    "Positive Movement Sum 12 Mths" as "Sum of Positive Movmt. Over Last 12 Months",
    "Negative Movement Sum 12 Mths" as "Sum of Negative Movmt. Over Last 12 Months",
    "Sequence of Movement 12 Mths",
    "All Movements Sum 12 Mths" as "Sum of All Movmt. Over Last 12 Months",
    "Review (1 if Review)" as "Both - and + ovr. lst. 12 Mths (1 if to be reviewed)",
    Occurrences as "Sum of Reviews by Customer (Occurrences)",
    "Dip or Spike in last 12 Mths",
    "Dip/Spike Number in last 12 Mths",
    "Dips in next 11 Mths",
    "Spikes in next 11 Mths",
    "Cust. with Up & Down Mvmt." as "Cust. with Up & Down Movmt. (1 if yes)",
    "Churn Cust. with Up & Down Mvmt." as "Churn Cust. with Up & Down Movmt. (1 if yes)",
    "New Cust. with Up & Down Mvmt." as "New Cust. with Up & Down Movmt. (1 if yes)",
    "Total Abs. ARR Change Per Customer" as "Total Absolute ARR Change Per Customer",
    "Total ARR Change Per Customer",
    "Churn Values" as "Churn Value of Customer at Date",
    "Avg. ARR of Cust. lst 12 Mths" as "Avg. ARR of Cust. last 12 Mths",
    "Avg. ARR of Cust. lst 12 Mths since start date",
    "Total ARR for Cust. from now till end",
    "Total ARR for Cust. from start till now",
    "Ranking for ARR from start till now",
    "Start Date of ARR",
    "Churn Values at Date > Avg. ARR lst 12 Mths" as "Churn Values at Date > Avg. ARR lst 12 Mths (1 if yes)",
    "Churn Value > Avg. ARR Flag in next 11 months",
    "Occurence of Churn at Date > Avg. ARR lst 12 Mths" as "Occurence of Churn at Date > Avg. ARR lst 12 Mths",
    "Cust. with Churn at Date > Avg. ARR lst 12 Mths" as "Cust. with Churn at Date > Avg. ARR lst 12 Mths (1 if yes)",
    "Cust. either U&D or Churn > Avg. ARR" as "Cust. either U&D or Churn > Avg. ARR (1 if yes)",
    "Total Cust. with U&D Mvmt.",
    "Total Churn with U&D Mvmt.",
    "Total New ARR with U&D Mvmt.",
    "Total ARR to be Reviewed with U&D Mvmt.",
    "All Cust. Churn at Date> Avg. ARR lst 12 Mths",
    "All Exisiting Cust. Churn at Date> Avg. ARR lst 12 Mths",
    "All Cust. either U&D or Churn > Avg. ARR",
    "Total ARR For Cust. w/t Churn > Avg. ARR lst 12 Mths",
    "Total ARR Cust. either U&D or Churn > Avg. ARR",
    "Remedial Action",
    case
      when "Remedial Action" is not null then (
        dense_rank() over(
          partition by mcid,
          (
            case
              when "Remedial Action" is not null then 1
              else 0
            end
          )
          order by "Remedial Action"
        )
      )
    end as rank_of_action,
    case
      --The first time a customer has ARR data. This could also be where ARR drops to zero and goes back up
      when "ARR of Previous Month" = 0
      and "Formatted Sum by MCID and Date" > 0 then 'New' --After the customer has data
      when (
        lag("Total ARR for Cust. from start till now") over(
          partition by mcid
          order by snapshot_date
        )
      ) > 0 --the customer had data the prior months
      and "Formatted Sum by MCID and Date" - "ARR of Previous Month" between -1 and 1 then 'Flat'
      when (
        lag("Total ARR for Cust. from start till now") over(
          partition by mcid
          order by snapshot_date
        )
      ) > 0 --the customer had data the prior months
      and "Formatted Sum by MCID and Date" - "ARR of Previous Month" > 1 then 'Upsell'
      when (
        lag("Total ARR for Cust. from start till now") over(
          partition by mcid
          order by snapshot_date
        )
      ) > 0 --the customer had data the prior months
      and "Formatted Sum by MCID and Date" - "ARR of Previous Month" < -1
      and "Formatted Sum by MCID and Date" > 0 --the customer has some ARR still
      and "ARR of Previous Month" > 0 then 'Partial Churn' --the customer had ARR in the previous month
      when (
        lag("Total ARR for Cust. from start till now") over(
          partition by mcid
          order by snapshot_date
        )
      ) > 0 --the customer had data the prior months
      and "Formatted Sum by MCID and Date" = 0 --totally churned out
      and "ARR of Previous Month" > 0 then 'Churn' --the customer had ARR in the previous month
      else 'N/A'
    end as "Churn",
    case
      when mcid in (
        '238e698a-be2d-d35f-d8da-b62391cd32ec',
        'ecb72ac5-0abd-7cb7-8383-bd7061bee3d7',
        '1edbf636-968f-97bc-b76b-6c385bddf203',
        '1edbf636-968f-97bc-b76b-6c385bddf203',
        'abb2cbc8-9bc6-3fba-9ea0-50d4d65c79d9'
      ) then 'Misassigned MCID'
      else ''
    end as "Misassigned MCIDs"
  from joined_table_14
),
joined_table_15a as (
  select "Snapshot Date (Month)",
    "Year",
    "End Customer MCID",
    "Start Date of the Dataset",
    "ARR of Customer by Date",
    "ARR of Previous Month",
    "Change in ARR Compared to Previous Month",
    "Absolute Change in ARR Compared to Previous Month",
    "Threshold (%) for Up and Down Movement",
    "Threshold ($) for Up and Down Movement",
    "Time Period (Months) for Up and Down Movement",
    "Minimum Number of Movements Per Year",
    "Positive or Negative Movement Compared to Previous Month",
    "No ARR in last snapshot date (1 if yes)",
    "Flag for Complete Churn (1 if yes)",
    "Positive Movement Compared to Previous Month (1 if yes)",
    "Negative Movement Compared to Previous Month (1 if yes)",
    "Sum of Positive Movmt. Over Last 12 Months",
    "Sum of Negative Movmt. Over Last 12 Months",
    "Sequence of Movement 12 Mths",
    "Sum of All Movmt. Over Last 12 Months",
    "Both - and + ovr. lst. 12 Mths (1 if to be reviewed)",
    "Sum of Reviews by Customer (Occurrences)",
    "Dip or Spike in last 12 Mths",
    "Dip/Spike Number in last 12 Mths",
    "Dips in next 11 Mths",
    "Spikes in next 11 Mths",
    "Cust. with Up & Down Movmt. (1 if yes)",
    "Churn Cust. with Up & Down Movmt. (1 if yes)",
    "New Cust. with Up & Down Movmt. (1 if yes)",
    "Total Absolute ARR Change Per Customer",
    "Total ARR Change Per Customer",
    "Churn Value of Customer at Date",
    "Avg. ARR of Cust. last 12 Mths",
    "Avg. ARR of Cust. lst 12 Mths since start date",
    "Total ARR for Cust. from now till end",
    "Total ARR for Cust. from start till now",
    "Ranking for ARR from start till now",
    "Start Date of ARR",
    "Churn Values at Date > Avg. ARR lst 12 Mths (1 if yes)",
    "Churn Value > Avg. ARR Flag in next 11 months",
    "Occurence of Churn at Date > Avg. ARR lst 12 Mths",
    "Cust. with Churn at Date > Avg. ARR lst 12 Mths (1 if yes)",
    "Cust. either U&D or Churn > Avg. ARR (1 if yes)",
    "Total Cust. with U&D Mvmt.",
    "Total Churn with U&D Mvmt.",
    "Total New ARR with U&D Mvmt.",
    "Total ARR to be Reviewed with U&D Mvmt.",
    "All Cust. Churn at Date> Avg. ARR lst 12 Mths",
    "All Exisiting Cust. Churn at Date> Avg. ARR lst 12 Mths",
    "All Cust. either U&D or Churn > Avg. ARR",
    "Total ARR For Cust. w/t Churn > Avg. ARR lst 12 Mths",
    "Total ARR Cust. either U&D or Churn > Avg. ARR",
    "Remedial Action",
    max(rank_of_action) over(partition by "End Customer MCID") as "Maximum Number of Remedial Actions",
    "Churn",
    "Misassigned MCIDs"
  from joined_table_15
) --select
--	*
--from
--	joined_table_15a
--where
--	"End Customer MCID" = '291f7878-2db2-9c73-36c7-f18e91aa01ee'
--Sensitivity Analysis
--select
--	distinct "End Customer MCID"
--from
--	joined_table_15
--where
--	"Remedial Action" is not null
--order by
--	"Snapshot Date (Month)"
--After Yannis Comments
--Put in Yanni's Comments
,
joined_table_15b as (
  select "Snapshot Date (Month)",
    "Year",
    "End Customer MCID",
    "Start Date of the Dataset",
    "ARR of Customer by Date",
    "ARR of Previous Month",
    "Change in ARR Compared to Previous Month",
    "Absolute Change in ARR Compared to Previous Month",
    "Threshold (%) for Up and Down Movement",
    "Threshold ($) for Up and Down Movement",
    "Time Period (Months) for Up and Down Movement",
    "Minimum Number of Movements Per Year",
    "Positive or Negative Movement Compared to Previous Month",
    "No ARR in last snapshot date (1 if yes)",
    "Flag for Complete Churn (1 if yes)",
    "Positive Movement Compared to Previous Month (1 if yes)",
    "Negative Movement Compared to Previous Month (1 if yes)",
    "Sum of Positive Movmt. Over Last 12 Months",
    "Sum of Negative Movmt. Over Last 12 Months",
    "Sequence of Movement 12 Mths",
    "Sum of All Movmt. Over Last 12 Months",
    "Both - and + ovr. lst. 12 Mths (1 if to be reviewed)",
    "Sum of Reviews by Customer (Occurrences)",
    "Dip or Spike in last 12 Mths",
    "Dip/Spike Number in last 12 Mths",
    "Dips in next 11 Mths",
    "Spikes in next 11 Mths",
    "Cust. with Up & Down Movmt. (1 if yes)",
    "Churn Cust. with Up & Down Movmt. (1 if yes)",
    "New Cust. with Up & Down Movmt. (1 if yes)",
    "Total Absolute ARR Change Per Customer",
    "Total ARR Change Per Customer",
    "Churn Value of Customer at Date",
    "Avg. ARR of Cust. last 12 Mths",
    "Avg. ARR of Cust. lst 12 Mths since start date",
    "Total ARR for Cust. from now till end",
    "Total ARR for Cust. from start till now",
    "Ranking for ARR from start till now",
    "Start Date of ARR",
    "Churn Values at Date > Avg. ARR lst 12 Mths (1 if yes)",
    "Churn Value > Avg. ARR Flag in next 11 months",
    "Occurence of Churn at Date > Avg. ARR lst 12 Mths",
    "Cust. with Churn at Date > Avg. ARR lst 12 Mths (1 if yes)",
    "Cust. either U&D or Churn > Avg. ARR (1 if yes)",
    "Total Cust. with U&D Mvmt.",
    "Total Churn with U&D Mvmt.",
    "Total New ARR with U&D Mvmt.",
    "Total ARR to be Reviewed with U&D Mvmt.",
    "All Cust. Churn at Date> Avg. ARR lst 12 Mths",
    "All Exisiting Cust. Churn at Date> Avg. ARR lst 12 Mths",
    "All Cust. either U&D or Churn > Avg. ARR",
    "Total ARR For Cust. w/t Churn > Avg. ARR lst 12 Mths",
    "Total ARR Cust. either U&D or Churn > Avg. ARR",
    "Remedial Action",
    "Maximum Number of Remedial Actions",
    "Churn",
    "Misassigned MCIDs",
    case
      when "End Customer MCID" = 'b6479b1a-2251-e811-813c-70106fa51d21' then 'Extend 85$ from 10/19 to 3/19'
      when "End Customer MCID" = '897145ea-48ca-89c6-9284-cb3e8bd3c17e' then 'Extend 61,800 from 3/22 to 3/21'
      when "End Customer MCID" = 'd2bef417-2502-cd69-5fcc-0fe86ca4fb20' then 'Extend 6174 to 9/30/2022'
      when "End Customer MCID" = 'ac4ab096-10ce-cf17-6ba7-94a36ab9333f' then 'Extend 9,756 from 2/20 to 7/19 & 7/21 to 4/21'
      when "End Customer MCID" = '4456543717' then 'Extend 18696 from 1/19 to 4/19'
      when "End Customer MCID" = 'c6c93d23-1651-e811-8143-70106fa67261' then 'Extend 900$ from 8/19 to 3/19'
      when "End Customer MCID" = '7650145182' then 'Extend 22,788 from 9/19-1/19'
      when "End Customer MCID" = 'e9654665-aa12-9468-c6ad-584064d9512b' then 'Extend 79800 from 6/22 - 12/21'
      when "End Customer MCID" = 'a574e1c4-34e4-e411-9afb-0050568d2da8' then 'Extend 5472 from 9/22 to 12/21'
      when "End Customer MCID" = '16c23d23-1651-e811-8143-70106fa67261' then 'Extend 6432 from 12/21 to 9/20'
      when "End Customer MCID" = '291f7878-2db2-9c73-36c7-f18e91aa01ee' then 'Extend 170K from 12/22 to 5/22'
      when "End Customer MCID" = '891c0d99-35e4-e411-9afb-0050568d2da8' then 'Extend 20854 from 11/22 to 1/22'
      when "End Customer MCID" = '3eea6afb-5a3d-a754-f496-43224112b3b7' then 'Extend 85549 from 3/22 to 3/21'
      when "End Customer MCID" = 'c4350de0-20b2-e911-a96d-000d3a441525' then 'Extend 2753 from 3/19 - 1/19 overwrite 6432'
      when "End Customer MCID" = 'cffbc28b-c2c7-46b0-c942-a0cd7082c73b' then 'Extend 1500 to 9/22'
      when "End Customer MCID" = '6cf747dc-ce3a-ed70-86c0-5ad6c076ec63' then 'Extend 2268 from 10/22 to 9/22'
      when "End Customer MCID" = '2fd73d23-1651-e811-8143-70106fa67261' then 'Extend 4569$ from 4/30/21 to 12/31/19'
      when "End Customer MCID" = 'bca7451d-1651-e811-8143-70106fa67261' then 'Extend 12,772 from 4/30/21 to 1/31/2021'
      when "End Customer MCID" = '54dd139f-c4b6-c22d-e4a8-04dfeb2341a3' then 'Extend 161,000 from 1/22 to 2/21' --		when "End Customer MCID"='da446f2e-fc5b-4f97-41e6-ff481d484452' then 'Fill in 31628 in January 2023? Is this in scope? This may fall under scenario b'
      when "End Customer MCID" = '8caf451d-1651-e811-8143-70106fa67261' then 'Fix 8/20 - 12/20, show 2316 in those months.  OK w/ 3/22 decision'
      when "End Customer MCID" = 'c76221f5-81e4-e411-9afb-0050568d2da8' then 'The positive value is not showing as positive churn in farahs example'
      when "End Customer MCID" = '672a4cfe-356f-6ffa-6e9c-20dd15bea4be' then 'The positive value is not showing as positive churn in farahs example'
      when "End Customer MCID" = 'd296da64-c1d6-b2ea-d5a6-c804605e360c' then 'The positive value is not showing as positive churn in farahs example'
      when "End Customer MCID" = '903c6b00-ece5-e411-9afb-0050568d2da8' then 'positive churn missing in table'
      when "End Customer MCID" = 'e4547a08-06e6-e411-9afb-0050568d2da8' then 'positive churn missing in table'
      else null
    end as "Yanni's May 19 Comments",
    case
      when "End Customer MCID" = 'b6479b1a-2251-e811-813c-70106fa51d21' then 'Extend 85$ from 10/19 to 3/19'
      when "End Customer MCID" = '897145ea-48ca-89c6-9284-cb3e8bd3c17e' then 'Extend 61,800 from 3/22 to 3/21'
      when "End Customer MCID" = 'd2bef417-2502-cd69-5fcc-0fe86ca4fb20' then 'Extend 6174 to 9/30/2022, and Ignore any Remedial Actions Beforehand'
      when "End Customer MCID" = 'ac4ab096-10ce-cf17-6ba7-94a36ab9333f' then 'Extend 9,756 from 2/20 to 7/19 & 7/21 to 4/21'
      when "End Customer MCID" = '4456543717' then 'Put everything to 0'
      when "End Customer MCID" = 'c6c93d23-1651-e811-8143-70106fa67261' then 'Extend 900$ from 8/19 to 3/19'
      when "End Customer MCID" = '7650145182' then 'Put everything to 0'
      when "End Customer MCID" = 'e9654665-aa12-9468-c6ad-584064d9512b' then 'Extend 79800 from 6/22 - 12/21, and Ignore any Remedial Actions Beforehand'
      when "End Customer MCID" = 'a574e1c4-34e4-e411-9afb-0050568d2da8' then 'Extend 5472 from 9/22 to 12/21'
      when "End Customer MCID" = '16c23d23-1651-e811-8143-70106fa67261' then 'Extend 6432 from 12/21 to 9/20'
      when "End Customer MCID" = '291f7878-2db2-9c73-36c7-f18e91aa01ee' then 'Extend 170K from 12/22 to 5/22'
      when "End Customer MCID" = '891c0d99-35e4-e411-9afb-0050568d2da8' then 'Extend 20854 from 11/22 to 1/22'
      when "End Customer MCID" = '3eea6afb-5a3d-a754-f496-43224112b3b7' then 'Extend 85549 from 3/22 to 3/21'
      when "End Customer MCID" = 'c4350de0-20b2-e911-a96d-000d3a441525' then 'Extend 2753 from 3/19 - 1/19 overwrite 6432'
      when "End Customer MCID" = 'cffbc28b-c2c7-46b0-c942-a0cd7082c73b' then 'Extend 1500 to 9/22, and Ignore any Remedial Actions Beforehand'
      when "End Customer MCID" = '6cf747dc-ce3a-ed70-86c0-5ad6c076ec63' then 'Extend 2268 from 10/22 to 9/22, and Ignore any Remedial Actions Beforehand'
      when "End Customer MCID" = '2fd73d23-1651-e811-8143-70106fa67261' then 'Extend 4569$ from 4/30/21 to 12/31/19'
      when "End Customer MCID" = 'bca7451d-1651-e811-8143-70106fa67261' then 'Extend 12,772 from 4/30/21 to 1/31/2021'
      when "End Customer MCID" = '54dd139f-c4b6-c22d-e4a8-04dfeb2341a3' then 'Extend 161,000 from 1/22 to 2/21'
      when "End Customer MCID" = '8caf451d-1651-e811-8143-70106fa67261' then 'Fix 8/20 - 12/20, show 2316 in those months.  OK w/ 3/22 decision'
      when "End Customer MCID" = '68c93d23-1651-e811-8143-70106fa67261' then 'Extend from 03/21 till 07/19'
      when "End Customer MCID" = '957fd526-b665-e811-812f-70106faab5f1' then 'Extend from 01/22 till 08/21'
      when "End Customer MCID" = 'd869d448-597a-e611-80e5-fc15b426ff90' then 'Extend from 12/20 till 11/20'
      when "End Customer MCID" = 'e1bf5345-9de0-e2d9-5bd7-083c58d9971d' then 'Extend from 02/22 till 05/21'
      else null
    end as "DE Instructions for Option C"
  from joined_table_15a
)
select *
from joined_table_15b;
end if;
--#################################################
--COHORT 2
--#################################################
if run_cohort_2 = 1 then --Cohort 2: Look at Customers who have Alternate churns within a certain period -- e.g., 1 month, 2 month and 6 month
--Step 1: Prep the Number of Distinct Product Families and Dates Per MCID
drop table if exists sandbox_pd.sst_cohort_2;
create table sandbox_pd.sst_cohort_2 as --Cohort 2: Look at Customers who have Alternate churns within a certain period -- e.g., 1 month, 2 month and 6 month
--Step 1: Prep the Number of Distinct Product Families and Dates Per MCID
with initial_sst as (
  select *,
    max(
      isst."Ranking of PFs Per MCID & Date (Includ. Blank & Null)"
    ) over(partition by isst.snapshot_date, isst.mcid) as "No. of Dist. PFs Per MCID & Date (Includ. Blank & Null)",
    max(isst."Ranking of Distinct Dates Per MCID") over(partition by isst.mcid) as "No. of Distinct Dates Per MCID"
  from (
      select *,
        count(*) over(partition by snapshot_date, mcid) as "No of Records Per MCID & Date",
        dense_rank() over(
          partition by snapshot_date,
          mcid
          order by product_family
        ) as "Ranking of PFs Per MCID & Date (Includ. Blank & Null)",
        dense_rank() over(
          partition by mcid
          order by snapshot_date
        ) as "Ranking of Distinct Dates Per MCID"
      from sandbox_pd.sst
    ) as isst
  order by isst.snapshot_date,
    isst.mcid
) --Step 2: Exclude GmBH
,
sandbox_sst_1 as (
  select *
  from --watch which table
    initial_sst --put a filter for overages
  where overage_flag = 'N'
) --Step 3: Make a cartesian table
,
dist_mcid as (
  select distinct mcid
  from sandbox_sst_1
) --select
--	*
--from
--	dist_mcid
,
dist_date as (
  select distinct snapshot_date
  from sandbox_sst_1
) --select
--	*
--from
--	dist_date
--Cartesian Join
,
joined_cart_table as (
  select snapshot_date,
    mcid
  from dist_mcid,
    dist_date
  order by snapshot_date,
    mcid
) --select
--	*
--from
--	joined_cart_table
--where
--	mcid in
--	('010239c8-c80f-c9d1-ba22-a48f9a5ed28b')
--Step 5: Sum up arr from sst by mcid and snapshot date
,
sst_sum_1 as (
  select distinct snapshot_date as snapshot_date_sst,
    mcid as mcid_sst,
    sum(arr) over(partition by snapshot_date, mcid) as sum_arr_sst
  from sandbox_sst_1
) --Left join the sum sst table to the cartesian table -- so that mcids which are missing values in certain dates will get null arr for those dates
--This is important to do before hand -- it's important to keep lagged sum null when mcid changes
,
joined_table_1 as (
  select jct.snapshot_date,
    jct.mcid,
    ss1.sum_arr_sst
  from joined_cart_table jct
    left join sst_sum_1 ss1 on jct.snapshot_date = ss1.snapshot_date_sst
    and jct.mcid = ss1.mcid_sst
) --select
--	*
--from
--	joined_table_1
--where
--	mcid in
--	('010239c8-c80f-c9d1-ba22-a48f9a5ed28b')
--Important: We treat null values as zero -- meaning if their is null value one month and value increases the next month -- it should be positive
,
joined_table_2 as (
  select snapshot_date,
    mcid,
    sum_arr_sst,
    coalesce(sum_arr_sst, 0) as "Formatted Sum by MCID and Date"
  from joined_table_1
  order by mcid
) --Step 6: Get ARR of Previous month for Each MCID
--Important: Introduce all the constants.
,
joined_table_3 as (
  select snapshot_date,
    mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    lag("Formatted Sum by MCID and Date") over(
      partition by mcid
      order by snapshot_date
    ) as "ARR of Previous month",
    1.2 as upper_limit,
    0.8 as lower_limit,
    12 as time_period,
    2 as "Minimmum Number of Movmt. Per Year",
    (
      select min(snapshot_date) as start_date
      from joined_table_2
    ) as start_date
  from joined_table_2
) --Step 7: Get ARR for each customer from start till now and now till end
,
joined_table_4 as (
  select snapshot_date,
    mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous month",
    "Formatted Sum by MCID and Date" - "ARR of Previous month" as "Change in ARR",
    upper_limit,
    lower_limit,
    time_period,
    "Minimmum Number of Movmt. Per Year",
    start_date,
    sum("Formatted Sum by MCID and Date") over(
      partition by mcid
      order by snapshot_date rows between unbounded preceding and current row
    ) as "Total ARR for Cust. from start till now",
    sum("Formatted Sum by MCID and Date") over(
      partition by mcid
      order by snapshot_date rows between current row
        and unbounded following
    ) as "Total ARR for Cust. from now till end"
  from joined_table_3
) --select
--	*
--from
--	joined_table_4
--where
--	mcid = '017baac6-2460-e8d1-11e5-3670734b5e82'
--Step 8: Calculate the churn values excluding start and end
,
joined_table_5 as (
  select date_trunc('month', snapshot_date) as snapshot_date,
    mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous month",
    "Change in ARR",
    upper_limit,
    lower_limit,
    time_period,
    "Minimmum Number of Movmt. Per Year",
    start_date,
    "Total ARR for Cust. from start till now",
    "Total ARR for Cust. from now till end",
    case
      --exclude churn when the customer just starts ARR movement
      when lag("Total ARR for Cust. from start till now") over(
        partition by mcid
        order by snapshot_date
      ) = 0 then null --exclude churn when customer totally churns out
      when "Total ARR for Cust. from now till end" = 0 then null --exclude churn when no churn values
      when "Change in ARR" = 0 then null --exclude 0.5 and -0.5 values
      when ABS("Change in ARR") < 1 then null
      else "Change in ARR"
    end as "Churn (Exluding Start and End)",
    case
      --exclude churn when the customer just starts ARR movement
      when lag("Total ARR for Cust. from start till now") over(
        partition by mcid
        order by snapshot_date
      ) = 0 then null --exclude churn when customer totally churns out
      when "Total ARR for Cust. from now till end" = 0 then null --exclude churn when no churn values
      when "Change in ARR" = 0 then null --exclude 0.5 and -0.5 values
      when ABS("Change in ARR") < 1 then null
      else round(abs("Change in ARR")::numeric, 2)
    end as "Absolute Churn (Exluding Start and End)",
    case
      when "Total ARR for Cust. from start till now" = 0 then 0
      else 1
    end as "Ranking for ARR from start till now",
    case
      when sum_arr_sst is null
      and snapshot_date = (
        select max(snapshot_date)
        from joined_table_4
      ) then 1
      else 0
    end as "No ARR in last snapshot date"
  from joined_table_4
) --select
--	*
--from
--	joined_table_5
--where
--	mcid = '001ea07d-2184-df11-8804-0018717a8c82'
--Step 9: Do a self join to find out churn values of the previous and next month (or any other time period)
,
joined_table_6 as (
  select t2.snapshot_date as initial_snapshot_date,
    t1.snapshot_date as snapshot_date,
    t2.mcid as initial_mcid,
    t1.mcid,
    t2.sum_arr_sst,
    t2."Formatted Sum by MCID and Date",
    t2."ARR of Previous month",
    t2."Change in ARR",
    t2."Churn (Exluding Start and End)",
    t2."Absolute Churn (Exluding Start and End)",
    case
      when (t1."Churn (Exluding Start and End)") *(t2."Churn (Exluding Start and End)") < 0 then t1."Churn (Exluding Start and End)"
      else null
    end as "Churn Values of 2 month period",
    case
      when (t1."Churn (Exluding Start and End)") *(t2."Churn (Exluding Start and End)") < 0 then t1.snapshot_date
      else null
    end as "Date of Churn Values of 2 month period",
    t2.upper_limit,
    t2.lower_limit,
    t2.time_period,
    t2."Minimmum Number of Movmt. Per Year",
    t2.start_date,
    t2."Total ARR for Cust. from start till now",
    t2."Total ARR for Cust. from now till end",
    t2."Ranking for ARR from start till now",
    t2."No ARR in last snapshot date"
  from joined_table_5 t2
    left join joined_table_5 t1 on trim(t2.mcid) = trim(t1.mcid)
    and (
      (
        t2.snapshot_date >= t1.snapshot_date - interval '1 month'
      )
      and (
        t2.snapshot_date <= t1.snapshot_date - interval '1 month'
      )
      or (
        t2.snapshot_date <= t1.snapshot_date + interval '1 month'
      )
      and (
        t2.snapshot_date >= t1.snapshot_date + interval '1 month'
      )
    )
) --Step 10: Since we have duplicates rank the churn values of the previous and next period to only get the closest churn values to the current period
,
joined_table_7 as (
  select initial_snapshot_date,
    snapshot_date,
    initial_mcid,
    mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous month",
    "Change in ARR",
    "Churn (Exluding Start and End)",
    "Absolute Churn (Exluding Start and End)",
    "Churn Values of 2 month period",
    "Date of Churn Values of 2 month period",
    upper_limit,
    lower_limit,
    time_period,
    "Minimmum Number of Movmt. Per Year",
    start_date,
    "Total ARR for Cust. from start till now",
    "Total ARR for Cust. from now till end",
    "Ranking for ARR from start till now",
    "No ARR in last snapshot date",
    --	case
    --		when "Churn Values of 2 month period" is null then 0
    row_number() over(
      partition by mcid,
      initial_snapshot_date
      order by ABS(
          ABS("Churn Values of 2 month period") - ABS("Churn (Exluding Start and End)")
        ) ASC
    ) as "Ranking of ARR 2 month period"
  from joined_table_6
  order by initial_snapshot_date
) --Step 11: Remove the duplicates, keep only the values with the closest churn
,
joined_table_8 as (
  select initial_snapshot_date,
    initial_mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous month",
    "Change in ARR",
    "Churn (Exluding Start and End)",
    "Absolute Churn (Exluding Start and End)",
    "Churn Values of 2 month period",
    "Date of Churn Values of 2 month period",
    upper_limit,
    lower_limit,
    start_date,
    "Total ARR for Cust. from start till now",
    "Total ARR for Cust. from now till end",
    "No ARR in last snapshot date"
  from joined_table_7
  where "Ranking of ARR 2 month period" = 1
) --select
--	*
--from
--	joined_table_8
--where
--	initial_mcid = '017baac6-2460-e8d1-11e5-3670734b5e82'
--order by
--	initial_snapshot_date
--Step 12: Do the same thing but for a 6 month window -- before and after 5 months
,
joined_table_9 as (
  select t2.initial_snapshot_date,
    t1.initial_snapshot_date as snapshot_date,
    t2.initial_mcid,
    t1.initial_mcid as mcid,
    t2.sum_arr_sst,
    t2."Formatted Sum by MCID and Date",
    t2."ARR of Previous month",
    t2."Change in ARR",
    t2."Churn (Exluding Start and End)",
    t2."Absolute Churn (Exluding Start and End)",
    t2."Churn Values of 2 month period",
    t2."Date of Churn Values of 2 month period",
    case
      when (t1."Churn (Exluding Start and End)") *(t2."Churn (Exluding Start and End)") < 0 then t1."Churn (Exluding Start and End)"
      else null
    end as "Churn Values of 6 month period",
    case
      when (t1."Churn (Exluding Start and End)") *(t2."Churn (Exluding Start and End)") < 0 then t1."initial_snapshot_date"
      else null
    end as "Date of Churn Values of 6 month period",
    t2.upper_limit,
    t2.lower_limit,
    t2.start_date,
    t2."Total ARR for Cust. from start till now",
    t2."Total ARR for Cust. from now till end",
    t2."No ARR in last snapshot date"
  from joined_table_8 t2
    left join joined_table_8 t1 on trim(t1.initial_mcid) = trim(t2.initial_mcid)
    and (
      (
        t1.initial_snapshot_date >= t2.initial_snapshot_date - interval '5 month'
      )
      and (
        t1.initial_snapshot_date <= t2.initial_snapshot_date - interval '1 month'
      )
      or (
        t1.initial_snapshot_date <= t2.initial_snapshot_date + interval '5 month'
      )
      and (
        t1.initial_snapshot_date >= t2.initial_snapshot_date + interval '1 month'
      )
    )
) --Step 13: Rank the Churn Values of 6 month period
,
joined_table_10 as (
  select initial_snapshot_date,
    initial_mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous month",
    "Change in ARR",
    "Churn (Exluding Start and End)",
    "Absolute Churn (Exluding Start and End)",
    "Churn Values of 2 month period",
    "Date of Churn Values of 2 month period",
    "Churn Values of 6 month period",
    "Date of Churn Values of 6 month period",
    upper_limit,
    lower_limit,
    start_date,
    "Total ARR for Cust. from start till now",
    "Total ARR for Cust. from now till end",
    "No ARR in last snapshot date",
    row_number() over(
      partition by mcid,
      initial_snapshot_date
      order by ABS(
          ABS("Churn Values of 6 month period") - ABS("Churn (Exluding Start and End)")
        ) ASC
    ) as "Ranking of ARR 6 month period"
  from joined_table_9
) --Step 14: Drop all the duplicates
,
joined_table_11 as (
  select initial_snapshot_date,
    initial_mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous month",
    "Change in ARR",
    "Churn (Exluding Start and End)",
    "Absolute Churn (Exluding Start and End)",
    "Churn Values of 2 month period",
    "Date of Churn Values of 2 month period",
    "Churn Values of 6 month period",
    "Date of Churn Values of 6 month period",
    upper_limit,
    lower_limit,
    start_date,
    "Total ARR for Cust. from start till now",
    "Total ARR for Cust. from now till end",
    "No ARR in last snapshot date"
  from joined_table_10
  where "Ranking of ARR 6 month period" = 1
) --Step 15: Do it for 12 month period
,
joined_table_12 as (
  select t2.initial_snapshot_date,
    t1.initial_snapshot_date as snapshot_date,
    t2.initial_mcid,
    t1.initial_mcid as mcid,
    t2.sum_arr_sst,
    t2."Formatted Sum by MCID and Date",
    t2."ARR of Previous month",
    t2."Change in ARR",
    t2."Churn (Exluding Start and End)",
    t2."Absolute Churn (Exluding Start and End)",
    t2."Churn Values of 2 month period",
    t2."Date of Churn Values of 2 month period",
    t2."Churn Values of 6 month period",
    t2."Date of Churn Values of 6 month period",
    case
      when (t1."Churn (Exluding Start and End)") *(t2."Churn (Exluding Start and End)") < 0 then t1."Churn (Exluding Start and End)"
      else null
    end as "Churn Values of 12 month period",
    case
      when (t1."Churn (Exluding Start and End)") *(t2."Churn (Exluding Start and End)") < 0 then t1."initial_snapshot_date"
      else null
    end as "Date of Churn Values of 12 month period",
    t2.upper_limit,
    t2.lower_limit,
    t2.start_date,
    t2."Total ARR for Cust. from start till now",
    t2."Total ARR for Cust. from now till end",
    t2."No ARR in last snapshot date"
  from joined_table_11 t2
    left join joined_table_11 t1 on trim(t1.initial_mcid) = trim(t2.initial_mcid)
    and (
      (
        t1.initial_snapshot_date >= t2.initial_snapshot_date - interval '11 month'
      )
      and (
        t1.initial_snapshot_date <= t2.initial_snapshot_date - interval '1 month'
      )
      or (
        t1.initial_snapshot_date <= t2.initial_snapshot_date + interval '11 month'
      )
      and (
        t1.initial_snapshot_date >= t2.initial_snapshot_date + interval '1 month'
      )
    )
) --select
--	*
--from
--	joined_table_12
--where
--	initial_mcid = '017baac6-2460-e8d1-11e5-3670734b5e82'
--and
--	initial_snapshot_date = '2022-03-01'
--order by
--	initial_snapshot_date
--Step 16: Rank the churn values of each month
,
joined_table_13 as (
  select initial_snapshot_date,
    initial_mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous month",
    "Change in ARR",
    "Churn (Exluding Start and End)",
    "Absolute Churn (Exluding Start and End)",
    "Churn Values of 2 month period",
    "Date of Churn Values of 2 month period",
    "Churn Values of 6 month period",
    "Date of Churn Values of 6 month period",
    "Churn Values of 12 month period",
    "Date of Churn Values of 12 month period",
    upper_limit,
    lower_limit,
    start_date,
    "Total ARR for Cust. from start till now",
    "Total ARR for Cust. from now till end",
    "No ARR in last snapshot date",
    row_number() over(
      partition by mcid,
      initial_snapshot_date
      order by ABS(
          ABS("Churn Values of 12 month period") - ABS("Churn (Exluding Start and End)")
        ) ASC
    ) as "Ranking of ARR 12 month period"
  from joined_table_12
) --Step 15: Drop all duplicates
,
joined_table_14 as (
  select initial_snapshot_date,
    initial_mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous month",
    "Change in ARR",
    "Churn (Exluding Start and End)",
    "Absolute Churn (Exluding Start and End)",
    "Churn Values of 2 month period",
    "Date of Churn Values of 2 month period",
    "Churn Values of 6 month period",
    "Date of Churn Values of 6 month period",
    "Churn Values of 12 month period",
    "Date of Churn Values of 12 month period",
    upper_limit,
    lower_limit,
    start_date,
    "Total ARR for Cust. from start till now",
    "Total ARR for Cust. from now till end",
    "No ARR in last snapshot date"
  from joined_table_13
  where "Ranking of ARR 12 month period" = 1
) --select
--	*
--from
--	joined_table_14
--where
--	initial_mcid = '017baac6-2460-e8d1-11e5-3670734b5e82'
--and
--	initial_snapshot_date = '2022-03-01'
--order by
--	initial_snapshot_date
--Step 16: This is the table with Churn Values of 2 month, 6 month & 12 month period. Add Flags for 2, 6 and 12 month with threshold and without threshold.
,
joined_table_15 as (
  select initial_snapshot_date,
    initial_mcid,
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous month",
    "Change in ARR",
    "Churn (Exluding Start and End)",
    "Absolute Churn (Exluding Start and End)",
    "Churn Values of 2 month period",
    "Date of Churn Values of 2 month period",
    case
      when "Churn (Exluding Start and End)" is not null
      and "Churn Values of 2 month period" is not null then 1
      else 0
    end as "Alt. Flag for 2 month Churn (1 if yes)",
    case
      when abs("Churn Values of 2 month period") / nullif("Absolute Churn (Exluding Start and End)", 0) between lower_limit and upper_limit then 1
      else 0
    end as "Flag for 2 month Churn (1 if yes)",
    "Churn Values of 6 month period",
    "Date of Churn Values of 6 month period",
    case
      when abs("Churn Values of 6 month period") / nullif("Absolute Churn (Exluding Start and End)", 0) between lower_limit and upper_limit then 1
      else 0
    end as "Flag for 6 month Churn (1 if yes)",
    case
      when "Churn (Exluding Start and End)" is not null
      and "Churn Values of 6 month period" is not null then 1
      else 0
    end as "Alt. Flag for 6 month Churn (1 if yes)",
    "Churn Values of 12 month period",
    "Date of Churn Values of 12 month period",
    case
      when abs("Churn Values of 12 month period") / nullif("Absolute Churn (Exluding Start and End)", 0) between lower_limit and upper_limit then 1
      else 0
    end as "Flag for 12 month Churn (1 if yes)",
    case
      when "Churn (Exluding Start and End)" is not null
      and "Churn Values of 12 month period" is not null then 1
      else 0
    end as "Alt. Flag for 12 month Churn (1 if yes)",
    upper_limit,
    lower_limit,
    start_date,
    "Total ARR for Cust. from start till now",
    "Total ARR for Cust. from now till end",
    "No ARR in last snapshot date"
  from joined_table_14
) --select
--	*
--from
--	joined_table_15
--where
--	initial_mcid = '001ea07d-2184-df11-8804-0018717a8c82'
--where
--	"Alt. Flag for 2 month Churn (1 if yes)" = 1
--limit
--	1
--Step 17: Coalesce the flags over all dates for customers.
,
joined_table_16 as (
  select initial_snapshot_date as "Formatted Snapshot_Date",
    initial_mcid as "Formatted MCID",
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous month",
    "Change in ARR",
    "Churn (Exluding Start and End)",
    "Absolute Churn (Exluding Start and End)",
    --2 month
    "Churn Values of 2 month period",
    "Date of Churn Values of 2 month period",
    "Flag for 2 month Churn (1 if yes)",
    case
      when "Flag for 2 month Churn (1 if yes)" = 1
      and "Date of Churn Values of 2 month period" > initial_snapshot_date then "Date of Churn Values of 2 month period"
      when "Flag for 2 month Churn (1 if yes)" = 1
      and "Date of Churn Values of 2 month period" < initial_snapshot_date then initial_snapshot_date
      else null
    end as "Date of Churn Values of 2 month period - Formatted",
    case
      when sum("Flag for 2 month Churn (1 if yes)") over(partition by initial_mcid) > 0 then 1
      else 0
    end as "Review for 2 month Churn (1 if yes)",
    "Alt. Flag for 2 month Churn (1 if yes)",
    case
      when "Alt. Flag for 2 month Churn (1 if yes)" = 1
      and "Date of Churn Values of 2 month period" > initial_snapshot_date then "Date of Churn Values of 2 month period"
      when "Alt. Flag for 2 month Churn (1 if yes)" = 1
      and "Date of Churn Values of 2 month period" < initial_snapshot_date then initial_snapshot_date
      else null
    end as "Date of Alt. Churn Values of 2 month period - Formatted",
    case
      when sum("Alt. Flag for 2 month Churn (1 if yes)") over(partition by initial_mcid) > 0 then 1
      else 0
    end as "Alt. Review for 2 month Churn (1 if yes)",
    --6 month
    "Churn Values of 6 month period",
    "Date of Churn Values of 6 month period",
    "Flag for 6 month Churn (1 if yes)",
    case
      when "Flag for 6 month Churn (1 if yes)" = 1
      and "Date of Churn Values of 6 month period" > initial_snapshot_date then "Date of Churn Values of 6 month period"
      when "Flag for 6 month Churn (1 if yes)" = 1
      and "Date of Churn Values of 6 month period" < initial_snapshot_date then initial_snapshot_date
      else null
    end as "Date of Churn Values of 6 month period - Formatted",
    case
      when sum("Flag for 6 month Churn (1 if yes)") over(partition by initial_mcid) > 0 then 1
      else 0
    end as "Review for 6 month Churn (1 if yes)",
    "Alt. Flag for 6 month Churn (1 if yes)",
    case
      when "Alt. Flag for 6 month Churn (1 if yes)" = 1
      and "Date of Churn Values of 6 month period" > initial_snapshot_date then "Date of Churn Values of 6 month period"
      when "Alt. Flag for 6 month Churn (1 if yes)" = 1
      and "Date of Churn Values of 6 month period" < initial_snapshot_date then initial_snapshot_date
      else null
    end as "Date of Alt. Churn Values of 6 month period - Formatted",
    case
      when sum("Alt. Flag for 6 month Churn (1 if yes)") over(partition by initial_mcid) > 0 then 1
      else 0
    end as "Alt. Review for 6 month Churn (1 if yes)",
    --12 month
    "Churn Values of 12 month period",
    "Date of Churn Values of 12 month period",
    "Flag for 12 month Churn (1 if yes)",
    case
      when "Flag for 12 month Churn (1 if yes)" = 1
      and "Date of Churn Values of 12 month period" > initial_snapshot_date then "Date of Churn Values of 12 month period"
      when "Flag for 12 month Churn (1 if yes)" = 1
      and "Date of Churn Values of 12 month period" < initial_snapshot_date then initial_snapshot_date
      else null
    end as "Date of Churn Values of 12 month period - Formatted",
    case
      when sum("Flag for 12 month Churn (1 if yes)") over(partition by initial_mcid) > 0 then 1
      else 0
    end as "Review for 12 month Churn (1 if yes)",
    "Alt. Flag for 12 month Churn (1 if yes)",
    case
      when "Alt. Flag for 12 month Churn (1 if yes)" = 1
      and "Date of Churn Values of 12 month period" > initial_snapshot_date then "Date of Churn Values of 12 month period"
      when "Alt. Flag for 12 month Churn (1 if yes)" = 1
      and "Date of Churn Values of 12 month period" < initial_snapshot_date then initial_snapshot_date
      else null
    end as "Date of Alt. Churn Values of 12 month period - Formatted",
    case
      when sum("Alt. Flag for 12 month Churn (1 if yes)") over(partition by initial_mcid) > 0 then 1
      else 0
    end as "Alt. Review for 12 month Churn (1 if yes)",
    upper_limit,
    lower_limit,
    start_date,
    "Total ARR for Cust. from start till now",
    "Total ARR for Cust. from now till end",
    "No ARR in last snapshot date"
  from joined_table_15
),
joined_table_17 as (
  select "Formatted Snapshot_Date",
    "Formatted MCID",
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous month",
    "Change in ARR",
    "Churn (Exluding Start and End)",
    "Absolute Churn (Exluding Start and End)",
    --2 month
    "Churn Values of 2 month period",
    "Date of Churn Values of 2 month period",
    "Flag for 2 month Churn (1 if yes)",
    "Review for 2 month Churn (1 if yes)",
    "Date of Churn Values of 2 month period - Formatted",
    case
      when max(
        "Date of Churn Values of 2 month period - Formatted"
      ) over(
        partition by "Formatted MCID"
        order by "Formatted Snapshot_Date" rows between unbounded preceding and current row
      ) >= "Formatted Snapshot_Date" then max(
        "Date of Churn Values of 2 month period - Formatted"
      ) over(
        partition by "Formatted MCID"
        order by "Formatted Snapshot_Date" rows between unbounded preceding and current row
      )
      else null
    end as "Date of Churn Values of 2 month period - Group",
    "Alt. Flag for 2 month Churn (1 if yes)",
    "Date of Alt. Churn Values of 2 month period - Formatted",
    case
      when max(
        "Date of Alt. Churn Values of 2 month period - Formatted"
      ) over(
        partition by "Formatted MCID"
        order by "Formatted Snapshot_Date" rows between unbounded preceding and current row
      ) >= "Formatted Snapshot_Date" then max(
        "Date of Alt. Churn Values of 2 month period - Formatted"
      ) over(
        partition by "Formatted MCID"
        order by "Formatted Snapshot_Date" rows between unbounded preceding and current row
      )
      else null
    end as "Date of Alt. Churn Values of 2 month period - Group",
    "Alt. Review for 2 month Churn (1 if yes)",
    --6 month
    "Churn Values of 6 month period",
    "Date of Churn Values of 6 month period",
    "Flag for 6 month Churn (1 if yes)",
    "Review for 6 month Churn (1 if yes)",
    "Date of Churn Values of 6 month period - Formatted",
    case
      when max(
        "Date of Churn Values of 6 month period - Formatted"
      ) over(
        partition by "Formatted MCID"
        order by "Formatted Snapshot_Date" rows between unbounded preceding and current row
      ) >= "Formatted Snapshot_Date" then max(
        "Date of Churn Values of 6 month period - Formatted"
      ) over(
        partition by "Formatted MCID"
        order by "Formatted Snapshot_Date" rows between unbounded preceding and current row
      )
      else null
    end as "Date of Churn Values of 6 month period - Group",
    "Alt. Flag for 6 month Churn (1 if yes)",
    "Date of Alt. Churn Values of 6 month period - Formatted",
    case
      when max(
        "Date of Alt. Churn Values of 6 month period - Formatted"
      ) over(
        partition by "Formatted MCID"
        order by "Formatted Snapshot_Date" rows between unbounded preceding and current row
      ) >= "Formatted Snapshot_Date" then max(
        "Date of Alt. Churn Values of 6 month period - Formatted"
      ) over(
        partition by "Formatted MCID"
        order by "Formatted Snapshot_Date" rows between unbounded preceding and current row
      )
      else null
    end as "Date of Alt. Churn Values of 6 month period - Group",
    "Alt. Review for 6 month Churn (1 if yes)",
    --12 month
    "Churn Values of 12 month period",
    "Date of Churn Values of 12 month period",
    "Flag for 12 month Churn (1 if yes)",
    "Review for 12 month Churn (1 if yes)",
    "Date of Churn Values of 12 month period - Formatted",
    case
      when max(
        "Date of Churn Values of 12 month period - Formatted"
      ) over(
        partition by "Formatted MCID"
        order by "Formatted Snapshot_Date" rows between unbounded preceding and current row
      ) >= "Formatted Snapshot_Date" then max(
        "Date of Churn Values of 12 month period - Formatted"
      ) over(
        partition by "Formatted MCID"
        order by "Formatted Snapshot_Date" rows between unbounded preceding and current row
      )
      else null
    end as "Date of Churn Values of 12 month period - Group",
    "Alt. Flag for 12 month Churn (1 if yes)",
    "Date of Alt. Churn Values of 12 month period - Formatted",
    case
      when max(
        "Date of Alt. Churn Values of 12 month period - Formatted"
      ) over(
        partition by "Formatted MCID"
        order by "Formatted Snapshot_Date" rows between unbounded preceding and current row
      ) >= "Formatted Snapshot_Date" then max(
        "Date of Alt. Churn Values of 12 month period - Formatted"
      ) over(
        partition by "Formatted MCID"
        order by "Formatted Snapshot_Date" rows between unbounded preceding and current row
      )
      else null
    end as "Date of Alt. Churn Values of 12 month period - Group",
    "Alt. Review for 12 month Churn (1 if yes)",
    upper_limit,
    lower_limit,
    start_date,
    "Total ARR for Cust. from start till now",
    "Total ARR for Cust. from now till end",
    "No ARR in last snapshot date"
  from joined_table_16
) --select
--	*
--from
--	joined_table_17
--where
--	"Formatted MCID" = '0011a891-432b-b1b5-187a-08a3d0b1f81b'
,
joined_table_18 as (
  select "Formatted Snapshot_Date",
    "Formatted MCID",
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous month",
    "Change in ARR",
    "Churn (Exluding Start and End)",
    "Absolute Churn (Exluding Start and End)",
    --2 month
    "Churn Values of 2 month period",
    "Date of Churn Values of 2 month period",
    "Flag for 2 month Churn (1 if yes)",
    "Date of Churn Values of 2 month period - Formatted",
    "Date of Churn Values of 2 month period - Group",
    case
      when "Date of Churn Values of 2 month period - Group" is null then "Formatted Sum by MCID and Date"
      else first_value("Formatted Sum by MCID and Date") over(
        partition by "Formatted MCID",
        "Date of Churn Values of 2 month period - Group"
        order by "Formatted Snapshot_Date" desc
      )
    end as "Formatted Sum by MCID and Date - 2 month",
    "Review for 2 month Churn (1 if yes)",
    "Alt. Flag for 2 month Churn (1 if yes)",
    "Date of Alt. Churn Values of 2 month period - Formatted",
    "Date of Alt. Churn Values of 2 month period - Group",
    case
      when "Date of Alt. Churn Values of 2 month period - Group" is null then "Formatted Sum by MCID and Date"
      else first_value("Formatted Sum by MCID and Date") over(
        partition by "Formatted MCID",
        "Date of Alt. Churn Values of 2 month period - Group"
        order by "Formatted Snapshot_Date" desc
      )
    end as "Formatted Sum by MCID and Date - Alt. 2 month",
    "Alt. Review for 2 month Churn (1 if yes)",
    --6 month
    "Churn Values of 6 month period",
    "Date of Churn Values of 6 month period",
    "Flag for 6 month Churn (1 if yes)",
    "Date of Churn Values of 6 month period - Formatted",
    "Date of Churn Values of 6 month period - Group",
    case
      when "Date of Churn Values of 6 month period - Group" is null then "Formatted Sum by MCID and Date"
      else first_value("Formatted Sum by MCID and Date") over(
        partition by "Formatted MCID",
        "Date of Churn Values of 6 month period - Group"
        order by "Formatted Snapshot_Date" desc
      )
    end as "Formatted Sum by MCID and Date - 6 month",
    "Review for 6 month Churn (1 if yes)",
    "Alt. Flag for 6 month Churn (1 if yes)",
    "Date of Alt. Churn Values of 6 month period - Formatted",
    "Date of Alt. Churn Values of 6 month period - Group",
    case
      when "Date of Alt. Churn Values of 6 month period - Group" is null then "Formatted Sum by MCID and Date"
      else first_value("Formatted Sum by MCID and Date") over(
        partition by "Formatted MCID",
        "Date of Alt. Churn Values of 6 month period - Group"
        order by "Formatted Snapshot_Date" desc
      )
    end as "Formatted Sum by MCID and Date - Alt. 6 month",
    "Alt. Review for 6 month Churn (1 if yes)",
    --12 month
    "Churn Values of 12 month period",
    "Date of Churn Values of 12 month period",
    "Flag for 12 month Churn (1 if yes)",
    "Date of Churn Values of 12 month period - Formatted",
    "Date of Churn Values of 12 month period - Group",
    case
      when "Date of Churn Values of 12 month period - Group" is null then "Formatted Sum by MCID and Date"
      else first_value("Formatted Sum by MCID and Date") over(
        partition by "Formatted MCID",
        "Date of Churn Values of 12 month period - Group"
        order by "Formatted Snapshot_Date" desc
      )
    end as "Formatted Sum by MCID and Date - 12 month",
    "Review for 12 month Churn (1 if yes)",
    "Alt. Flag for 12 month Churn (1 if yes)",
    "Date of Alt. Churn Values of 12 month period - Formatted",
    "Date of Alt. Churn Values of 12 month period - Group",
    case
      when "Date of Alt. Churn Values of 12 month period - Group" is null then "Formatted Sum by MCID and Date"
      else first_value("Formatted Sum by MCID and Date") over(
        partition by "Formatted MCID",
        "Date of Alt. Churn Values of 12 month period - Group"
        order by "Formatted Snapshot_Date" desc
      )
    end as "Formatted Sum by MCID and Date - Alt. 12 month",
    "Alt. Review for 12 month Churn (1 if yes)",
    upper_limit,
    lower_limit,
    start_date,
    "Total ARR for Cust. from start till now",
    "Total ARR for Cust. from now till end",
    "No ARR in last snapshot date"
  from joined_table_17
),
joined_table_19 as (
  select "Formatted Snapshot_Date",
    "Formatted MCID",
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous month",
    "Change in ARR",
    "Churn (Exluding Start and End)",
    "Absolute Churn (Exluding Start and End)",
    --2 month
    "Churn Values of 2 month period",
    "Date of Churn Values of 2 month period",
    "Flag for 2 month Churn (1 if yes)",
    "Date of Churn Values of 2 month period - Formatted",
    "Date of Churn Values of 2 month period - Group",
    "Formatted Sum by MCID and Date - 2 month",
    case
      --exclude churn when the customer just starts ARR movement
      when lag("Total ARR for Cust. from start till now") over(
        partition by "Formatted MCID"
        order by "Formatted Snapshot_Date"
      ) = 0 then null --exclude churn when customer totally churns out
      when "Total ARR for Cust. from now till end" = 0 then null
      else "Formatted Sum by MCID and Date - 2 month" -(
        lag("Formatted Sum by MCID and Date - 2 month") over(
          partition by "Formatted MCID"
          order by "Formatted Snapshot_Date"
        )
      )
    end as "Churn: Formatted Sum by MCID and Date - 2 month",
    "Review for 2 month Churn (1 if yes)",
    "Alt. Flag for 2 month Churn (1 if yes)",
    "Date of Alt. Churn Values of 2 month period - Formatted",
    "Date of Alt. Churn Values of 2 month period - Group",
    "Formatted Sum by MCID and Date - Alt. 2 month",
    case
      --exclude churn when the customer just starts ARR movement
      when lag("Total ARR for Cust. from start till now") over(
        partition by "Formatted MCID"
        order by "Formatted Snapshot_Date"
      ) = 0 then null --exclude churn when customer totally churns out
      when "Total ARR for Cust. from now till end" = 0 then null
      else "Formatted Sum by MCID and Date - Alt. 2 month" -(
        lag("Formatted Sum by MCID and Date - Alt. 2 month") over(
          partition by "Formatted MCID"
          order by "Formatted Snapshot_Date"
        )
      )
    end as "Churn: Formatted Sum by MCID and Date - Alt. 2 month",
    "Alt. Review for 2 month Churn (1 if yes)",
    --6 month
    "Churn Values of 6 month period",
    "Date of Churn Values of 6 month period",
    "Flag for 6 month Churn (1 if yes)",
    "Date of Churn Values of 6 month period - Formatted",
    "Date of Churn Values of 6 month period - Group",
    "Formatted Sum by MCID and Date - 6 month",
    case
      --exclude churn when the customer just starts ARR movement
      when lag("Total ARR for Cust. from start till now") over(
        partition by "Formatted MCID"
        order by "Formatted Snapshot_Date"
      ) = 0 then null --exclude churn when customer totally churns out
      when "Total ARR for Cust. from now till end" = 0 then null
      else "Formatted Sum by MCID and Date - 6 month" -(
        lag("Formatted Sum by MCID and Date - 6 month") over(
          partition by "Formatted MCID"
          order by "Formatted Snapshot_Date"
        )
      )
    end as "Churn: Formatted Sum by MCID and Date - 6 month",
    "Review for 6 month Churn (1 if yes)",
    "Alt. Flag for 6 month Churn (1 if yes)",
    "Date of Alt. Churn Values of 6 month period - Formatted",
    "Date of Alt. Churn Values of 6 month period - Group",
    "Formatted Sum by MCID and Date - Alt. 6 month",
    case
      --exclude churn when the customer just starts ARR movement
      when lag("Total ARR for Cust. from start till now") over(
        partition by "Formatted MCID"
        order by "Formatted Snapshot_Date"
      ) = 0 then null --exclude churn when customer totally churns out
      when "Total ARR for Cust. from now till end" = 0 then null
      else "Formatted Sum by MCID and Date - Alt. 6 month" -(
        lag("Formatted Sum by MCID and Date - Alt. 6 month") over(
          partition by "Formatted MCID"
          order by "Formatted Snapshot_Date"
        )
      )
    end as "Churn: Formatted Sum by MCID and Date - Alt. 6 month",
    "Alt. Review for 6 month Churn (1 if yes)",
    --12 month
    "Churn Values of 12 month period",
    "Date of Churn Values of 12 month period",
    "Flag for 12 month Churn (1 if yes)",
    "Date of Churn Values of 12 month period - Formatted",
    "Date of Churn Values of 12 month period - Group",
    "Formatted Sum by MCID and Date - 12 month",
    case
      --exclude churn when the customer just starts ARR movement
      when lag("Total ARR for Cust. from start till now") over(
        partition by "Formatted MCID"
        order by "Formatted Snapshot_Date"
      ) = 0 then null --exclude churn when customer totally churns out
      when "Total ARR for Cust. from now till end" = 0 then null
      else "Formatted Sum by MCID and Date - 12 month" -(
        lag("Formatted Sum by MCID and Date - 12 month") over(
          partition by "Formatted MCID"
          order by "Formatted Snapshot_Date"
        )
      )
    end as "Churn: Formatted Sum by MCID and Date - 12 month",
    "Review for 12 month Churn (1 if yes)",
    "Alt. Flag for 12 month Churn (1 if yes)",
    "Date of Alt. Churn Values of 12 month period - Formatted",
    "Date of Alt. Churn Values of 12 month period - Group",
    "Formatted Sum by MCID and Date - Alt. 12 month",
    case
      --exclude churn when the customer just starts ARR movement
      when lag("Total ARR for Cust. from start till now") over(
        partition by "Formatted MCID"
        order by "Formatted Snapshot_Date"
      ) = 0 then null --exclude churn when customer totally churns out
      when "Total ARR for Cust. from now till end" = 0 then null
      else "Formatted Sum by MCID and Date - Alt. 12 month" -(
        lag("Formatted Sum by MCID and Date - Alt. 12 month") over(
          partition by "Formatted MCID"
          order by "Formatted Snapshot_Date"
        )
      )
    end as "Churn: Formatted Sum by MCID and Date - Alt. 12 month",
    "Alt. Review for 12 month Churn (1 if yes)",
    upper_limit,
    lower_limit,
    start_date,
    "Total ARR for Cust. from start till now",
    "Total ARR for Cust. from now till end",
    "No ARR in last snapshot date"
  from joined_table_18 --where
    --	"Formatted MCID" = '017baac6-2460-e8d1-11e5-3670734b5e82'
  order by "Formatted Snapshot_Date"
),
joined_table_20 as (
  select "Formatted Snapshot_Date",
    "Formatted MCID",
    sum_arr_sst,
    "Formatted Sum by MCID and Date",
    "ARR of Previous month",
    "Change in ARR",
    "Churn (Exluding Start and End)",
    case
      when "Churn (Exluding Start and End)" > 0 then 'Upsell'
      when "Churn (Exluding Start and End)" < 0 then 'Churn'
      else null
    end as "Original Churn",
    "Absolute Churn (Exluding Start and End)",
    --2 month
    "Churn Values of 2 month period",
    "Date of Churn Values of 2 month period",
    "Flag for 2 month Churn (1 if yes)",
    "Date of Churn Values of 2 month period - Formatted",
    "Date of Churn Values of 2 month period - Group",
    "Formatted Sum by MCID and Date - 2 month",
    "Churn: Formatted Sum by MCID and Date - 2 month",
    case
      when "Churn: Formatted Sum by MCID and Date - 2 month" > 0 then 'Upsell'
      when "Churn: Formatted Sum by MCID and Date - 2 month" < 0 then 'Churn'
    end as "Churn Class: Formatted Sum by MCID and Date - 2 month",
    "Review for 2 month Churn (1 if yes)",
    "Alt. Flag for 2 month Churn (1 if yes)",
    "Date of Alt. Churn Values of 2 month period - Formatted",
    "Date of Alt. Churn Values of 2 month period - Group",
    "Formatted Sum by MCID and Date - Alt. 2 month",
    "Churn: Formatted Sum by MCID and Date - Alt. 2 month",
    case
      when "Churn: Formatted Sum by MCID and Date - Alt. 2 month" > 0 then 'Upsell'
      when "Churn: Formatted Sum by MCID and Date - Alt. 2 month" < 0 then 'Churn'
    end as "Churn Class: Formatted Sum by MCID and Date - Alt. 2 month",
    "Alt. Review for 2 month Churn (1 if yes)",
    --6 month
    "Churn Values of 6 month period",
    "Date of Churn Values of 6 month period",
    "Flag for 6 month Churn (1 if yes)",
    "Date of Churn Values of 6 month period - Formatted",
    "Date of Churn Values of 6 month period - Group",
    "Formatted Sum by MCID and Date - 6 month",
    "Churn: Formatted Sum by MCID and Date - 6 month",
    case
      when "Churn: Formatted Sum by MCID and Date - 6 month" > 0 then 'Upsell'
      when "Churn: Formatted Sum by MCID and Date - 6 month" < 0 then 'Churn'
    end as "Churn Class: Formatted Sum by MCID and Date - 6 month",
    "Review for 6 month Churn (1 if yes)",
    "Alt. Flag for 6 month Churn (1 if yes)",
    "Date of Alt. Churn Values of 6 month period - Formatted",
    "Date of Alt. Churn Values of 6 month period - Group",
    "Formatted Sum by MCID and Date - Alt. 6 month",
    "Churn: Formatted Sum by MCID and Date - Alt. 6 month",
    case
      when "Churn: Formatted Sum by MCID and Date - Alt. 6 month" > 0 then 'Upsell'
      when "Churn: Formatted Sum by MCID and Date - Alt. 6 month" < 0 then 'Churn'
    end as "Churn Class: Formatted Sum by MCID and Date - Alt. 6 month",
    "Alt. Review for 6 month Churn (1 if yes)",
    --12 month
    "Churn Values of 12 month period",
    "Date of Churn Values of 12 month period",
    "Flag for 12 month Churn (1 if yes)",
    "Date of Churn Values of 12 month period - Formatted",
    "Date of Churn Values of 12 month period - Group",
    "Formatted Sum by MCID and Date - 12 month",
    "Churn: Formatted Sum by MCID and Date - 12 month",
    case
      when "Churn: Formatted Sum by MCID and Date - 12 month" > 0 then 'Upsell'
      when "Churn: Formatted Sum by MCID and Date - 12 month" < 0 then 'Churn'
    end as "Churn Class: Formatted Sum by MCID and Date - 12 month",
    "Review for 12 month Churn (1 if yes)",
    "Alt. Flag for 12 month Churn (1 if yes)",
    "Date of Alt. Churn Values of 12 month period - Formatted",
    "Date of Alt. Churn Values of 12 month period - Group",
    "Formatted Sum by MCID and Date - Alt. 12 month",
    "Churn: Formatted Sum by MCID and Date - Alt. 12 month",
    case
      when "Churn: Formatted Sum by MCID and Date - Alt. 12 month" > 0 then 'Upsell'
      when "Churn: Formatted Sum by MCID and Date - Alt. 12 month" < 0 then 'Churn'
    end as "Churn Class: Formatted Sum by MCID and Date - Alt. 12 month",
    "Alt. Review for 12 month Churn (1 if yes)",
    upper_limit,
    lower_limit,
    start_date,
    "Total ARR for Cust. from start till now",
    "Total ARR for Cust. from now till end",
    "No ARR in last snapshot date" --	DENSE_RANK() over(partition by "Formatted MCID" order by "Formatted Snapshot_Date") as test
  from joined_table_19
) --Pavan please export
select *
from joined_table_20 --where
  --	"Formatted MCID" = '001ea07d-2184-df11-8804-0018717a8c82'
where "Formatted MCID" not in (
    'eae4654c-1bb7-e411-9afb-0050568d2da8',
    'a78cd87f-5f5a-df11-a462-0018717a8c82'
  )
order by "Formatted MCID",
  "Formatted Snapshot_Date";
end if;
END;
$function$;