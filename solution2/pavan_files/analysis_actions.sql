CREATE OR REPLACE FUNCTION sandbox_pd.sp_populate_run_sst_sensitivity_analysis_actions(run_cohort_1 integer, run_cohort_2 integer) RETURNS void LANGUAGE plpgsql AS $function$ BEGIN --#################################################
  --COHORT 1 ACTIONS
  --#################################################
  if run_cohort_1 = 1 then drop table if exists sensitivity_analysis_actions_temp;
create table sensitivity_analysis_actions_temp as
select distinct "End Customer MCID",
  "Snapshot Date (Month)",
  "Remedial Action",
  null as arr_old,
  null::numeric as arr_previous_month,
  null as arr_new,
  null::Text as de_comments,
  null::Text as da_comments,
  null::Text as no_of_records_in_sst_postive_arr,
  null::Date as snapshot_date_copied
from sandbox_pd.SST_COHORT_1
where 1 = 1
  and "Cust. either U&D or Churn > Avg. ARR (1 if yes)" = 1
  and "Remedial Action" is not null
  and "Misassigned MCIDs" = ''
  and "Remedial Action" in (
    'Overwrite with latest Non-Spike Value - Non U&D',
    'Overwrite with latest non-Spike value - U&D',
    'Overwrite with the latest non-zero value - U&D'
  )
  and "Snapshot Date (Month)" < '2023-01-01'
  and (
    "DE Instructions for Option C" is null
    and "Yanni's May 19 Comments" is null
  )
union
select distinct "End Customer MCID",
  "Snapshot Date (Month)",
  "Remedial Action",
  null as arr_old,
  "ARR of Previous Month"::numeric as arr_previous_month,
  null as arr_new,
  null::Text as de_comments,
  null::Text as da_comments,
  null::Text as no_of_records_in_sst_postive_arr,
  null::Date as snapshot_date_copied --select *
from sandbox_pd.SST_COHORT_1
where 1 = 1
  and (
    "Remedial Action" in ('Overwrite with the latest non-zero value - U&D')
    and "DE Instructions for Option C" is null
  )
  and "Misassigned MCIDs" = ''
  and "Snapshot Date (Month)" < '2023-01-01'
  and "Churn" != 'Flat'
  and 1 = 2;
--#################################################################
--case 1: update arr to 0
--#################################################################
update sandbox_pd.sst b
set arr = 0,
  baseline_arr_local_currency = 0,
  cohort_actions = concat('Cohort 1: Updated arr to 0 from ', arr::text)
from sensitivity_analysis_actions_temp a
where 1 = 1
  and a."End Customer MCID" = b.mcid
  and a."Snapshot Date (Month)" = b.snapshot_date
  and "Remedial Action" = 'Overwrite with zero value - U&D'
  and b.overage_flag ilike '%N%';
--#################################################################
--case 2: get latest arr and delete + insert
--#################################################################
drop table if exists temp_to_delete_records;
create temporary table temp_to_delete_records as
select distinct a."End Customer MCID",
  a."Snapshot Date (Month)",
  b.overage_flag
from sensitivity_analysis_actions_temp a
  left join sandbox_pd.sst b on a."End Customer MCID" = b.mcid
  and a."Snapshot Date (Month)" = b.snapshot_date
where 1 = 1
  and "Remedial Action" in (
    'Overwrite with latest Non-Spike Value - Non U&D',
    'Overwrite with latest non-Spike value - U&D',
    'Overwrite with the latest non-zero value - U&D'
  )
order by 1,
  2;
--insert records from sst based on latest snapshot after sens analysis snapshots
drop table if exists temp_to_insert_records_max_snapshot;
create temporary table temp_to_insert_records_max_snapshot as with temp as (
  select "End Customer MCID",
    "Snapshot Date (Month)" as snapshot_date,
    lead("Snapshot Date (Month)"::date) over (
      partition by "End Customer MCID"
      order by "Snapshot Date (Month)"::date
    ) as snapshot_date_lead
  from sensitivity_analysis_actions_temp a
  where 1 = 1
    and "Remedial Action" in (
      'Overwrite with latest Non-Spike Value - Non U&D',
      'Overwrite with latest non-Spike value - U&D',
      'Overwrite with the latest non-zero value - U&D'
    )
),
temp1 as (
  select *,
    (
      DATE_PART(
        'year',
        coalesce(snapshot_date_lead, snapshot_date)::date
      ) - DATE_PART('year', snapshot_date::date)
    ) * 12 + (
      DATE_PART(
        'month',
        coalesce(snapshot_date_lead, snapshot_date)::date
      ) - DATE_PART('month', snapshot_date::date)
    ) as month_diff,
    (
      (
        date_trunc('month', snapshot_date) + interval '2 month'
      ) - interval '1 day'
    )::Date as snapshot_date_to_copy
  from temp a
),
temp2 as (
  select "End Customer MCID",
    snapshot_date,
    snapshot_date_lead,
    snapshot_date_to_copy
  from temp1
  where month_diff <> 1
)
select b."End Customer MCID",
  b."Snapshot Date (Month)",
  min(a.snapshot_date_to_copy) as snapshot_date_to_copy
from temp2 a
  join temp_to_delete_records b on a."End Customer MCID" = b."End Customer MCID"
  and b."Snapshot Date (Month)" <= a.snapshot_date
group by 1,
  2;
--delete records from sst table
delete from sandbox_pd.sst a using temp_to_insert_records_max_snapshot b
where a.mcid = b."End Customer MCID"
  and b."Snapshot Date (Month)" = a.snapshot_date
  and a.overage_flag ilike '%N%'
  and exists (
    select 1
    from sandbox_pd.sst c
    where a.mcid = c.mcid
      and c.snapshot_date = b.snapshot_date_to_copy
  );
with temp as (
  select distinct b."End Customer MCID",
    b."Snapshot Date (Month)"
  from sandbox_pd.sst a
    join temp_to_insert_records_max_snapshot b on a.mcid = b."End Customer MCID"
    and b."Snapshot Date (Month)" = a.snapshot_date
    and a.overage_flag ilike '%N%'
  where not exists (
      select 1
      from sandbox_pd.sst c
      where a.mcid = c.mcid
        and c.snapshot_date = b.snapshot_date_to_copy
    )
)
update sandbox_pd.sst a
set arr = 0::numeric,
  baseline_arr_local_currency = 0,
  cohort_actions = 'updated arr to 0'
from temp b
where a.mcid = b."End Customer MCID"
  and b."Snapshot Date (Month)" = a.snapshot_date
  and a.overage_flag ilike '%N%';
--copy records from sst
drop table if exists temp_to_insert_records;
create temporary table temp_to_insert_records as
select b."Snapshot Date (Month)" AS snapshot_date,
  a.ultimate_parent_id,
  a.ultimate_parent_name,
  a.duns_name,
  a.duns_number,
  a.parent_duns_name,
  a.parent_duns_number,
  a.domesticultimatedunsnumber,
  a.globalultimatedunsnumber,
  a.new_product_solution,
  a.new_product_line,
  a.updated_product_group,
  a.new_product,
  a.new_line_of_business,
  a.new_line_of_business_sub_category,
  a.c_name,
  a.parent_ns_id,
  a.end_ns_id,
  a.name,
  a.parent_name,
  a.end_name,
  a.mcid,
  a.parent_mcid,
  a.end_mcid,
  a.subsidiary_entity_name,
  a.overage_flag,
  a.segment,
  a.region,
  a.product_family,
  a.base_currency,
  a.cc_fx_rate::double PRECISION AS cc_fx_rate,
  a.fx_date,
  a.arr,
  a.baseline_arr_local_currency,
  a.dw_modified_date,
  a.dw_created_date,
  a.parent_sf_id,
  a.parent_sf_name,
  a.sku
from sandbox_pd.sst a
  join temp_to_insert_records_max_snapshot b on a.mcid = b."End Customer MCID"
  and a.snapshot_date = b.snapshot_date_to_copy
  and a.overage_flag ilike '%N%';
--update snapshot_date_copied and new arr value in sensitivity_analysis_actions
with temp as (
  select a."End Customer MCID",
    a."Snapshot Date (Month)",
    a.snapshot_date_to_copy,
    sum(b.arr) as arr_new,
    sum(
      case
        when b.mcid is null then 1
        else 0
      end
    ) as new_snapshot_not_exists
  from temp_to_insert_records_max_snapshot a
    left join temp_to_insert_records b on a."End Customer MCID" = b.mcid
    and a."Snapshot Date (Month)" = b.snapshot_date
  group by 1,
    2,
    3
)
update sensitivity_analysis_actions_temp a
set snapshot_date_copied = b.snapshot_date_to_copy,
  arr_new = b.arr_new,
  da_comments = case
    when new_snapshot_not_exists > 0 then 'New snapshot_date not exists'
    else null
  end
from temp b
where a."End Customer MCID" = b."End Customer MCID"
  and a."Snapshot Date (Month)" = b."Snapshot Date (Month)"
  and a."Remedial Action" in (
    'Overwrite with latest Non-Spike Value - Non U&D',
    'Overwrite with latest non-Spike value - U&D',
    'Overwrite with the latest non-zero value - U&D'
  );
insert into sandbox_pd.sst (
    snapshot_date,
    ultimate_parent_id,
    ultimate_parent_name,
    duns_name,
    duns_number,
    parent_duns_name,
    parent_duns_number,
    domesticultimatedunsnumber,
    globalultimatedunsnumber,
    new_product_solution,
    new_product_line,
    updated_product_group,
    new_product,
    new_line_of_business,
    new_line_of_business_sub_category,
    c_name,
    parent_ns_id,
    end_ns_id,
    name,
    parent_name,
    end_name,
    mcid,
    parent_mcid,
    end_mcid,
    subsidiary_entity_name,
    overage_flag,
    segment,
    region,
    product_family,
    base_currency,
    cc_fx_rate,
    fx_date,
    arr,
    baseline_arr_local_currency,
    dw_modified_date,
    dw_created_date,
    parent_sf_id,
    parent_sf_name,
    cohort_actions,
    sku
  )
SELECT snapshot_date,
  ultimate_parent_id,
  ultimate_parent_name,
  duns_name,
  duns_number,
  parent_duns_name,
  parent_duns_number,
  domesticultimatedunsnumber,
  globalultimatedunsnumber,
  new_product_solution,
  new_product_line,
  updated_product_group,
  new_product,
  new_line_of_business,
  new_line_of_business_sub_category,
  c_name,
  parent_ns_id,
  end_ns_id,
  name,
  parent_name,
  end_name,
  mcid,
  parent_mcid,
  end_mcid,
  subsidiary_entity_name,
  overage_flag,
  segment,
  region,
  product_family,
  base_currency,
  cc_fx_rate::double PRECISION,
  fx_date,
  arr,
  baseline_arr_local_currency,
  dw_modified_date,
  dw_created_date,
  parent_sf_id,
  parent_sf_name,
  concat(
    'Deleted data and inserted data snapshot from latest snapshot',
    ''::TEXT
  ) AS cohort_actions,
  sku
from temp_to_insert_records;
update sandbox_pd.sst b
set arr = 0,
  cohort_actions = 'updated arr to 0'
from sensitivity_analysis_actions_temp a
where a."End Customer MCID" = b.mcid
  and a."Snapshot Date (Month)" = b.snapshot_date
  and da_comments = 'New snapshot_date not exists'
  and b.overage_flag ilike '%N%';
--#################################################################
--case 4: manual overrides
--#################################################################
drop table if exists temp_option_c_customers;
create temporary table temp_option_c_customers as
select distinct "End Customer MCID",
  "Remedial Action",
  "DE Instructions for Option C",
  "Yanni's May 19 Comments"
from sandbox_pd.SST_COHORT_1
where 1 = 1
  and "Cust. either U&D or Churn > Avg. ARR (1 if yes)" = 1
  and "Remedial Action" is not null
  and "Misassigned MCIDs" = '' --and "Sum of Reviews by Customer (Occurrences)" in (0,1)
  --and "Occurence of Churn at Date > Avg. ARR lst 12 Mths" in (0,1)
  and "Remedial Action" in (
    'Overwrite with latest Non-Spike Value - Non U&D',
    'Overwrite with latest non-Spike value - U&D'
  ) --,'Overwrite with the latest non-zero value - U&D')
  and "Snapshot Date (Month)" < '2023-01-01'
  and (
    "DE Instructions for Option C" is not null
    and "Yanni's May 19 Comments" is not null
  );
drop table if exists temp_option_c;
create temporary table temp_option_c as
select '16c23d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-09-30'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '16c23d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-10-31'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied --need to check with farah as data changed
union all
select '16c23d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-11-30'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '16c23d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-12-31'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '16c23d23-1651-e811-8143-70106fa67261' as mcid,
  '2021-01-31'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '16c23d23-1651-e811-8143-70106fa67261' as mcid,
  '2021-02-28'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '16c23d23-1651-e811-8143-70106fa67261' as mcid,
  '2021-03-31'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '16c23d23-1651-e811-8143-70106fa67261' as mcid,
  '2021-04-30'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '16c23d23-1651-e811-8143-70106fa67261' as mcid,
  '2021-05-31'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '16c23d23-1651-e811-8143-70106fa67261' as mcid,
  '2021-06-30'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '16c23d23-1651-e811-8143-70106fa67261' as mcid,
  '2021-07-31'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '16c23d23-1651-e811-8143-70106fa67261' as mcid,
  '2021-08-31'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '16c23d23-1651-e811-8143-70106fa67261' as mcid,
  '2021-09-30'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '16c23d23-1651-e811-8143-70106fa67261' as mcid,
  '2021-10-31'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '16c23d23-1651-e811-8143-70106fa67261' as mcid,
  '2021-11-30'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '291f7878-2db2-9c73-36c7-f18e91aa01ee' as mcid,
  '2022-05-31'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '291f7878-2db2-9c73-36c7-f18e91aa01ee' as mcid,
  '2022-06-30'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '291f7878-2db2-9c73-36c7-f18e91aa01ee' as mcid,
  '2022-07-31'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '291f7878-2db2-9c73-36c7-f18e91aa01ee' as mcid,
  '2022-08-31'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '291f7878-2db2-9c73-36c7-f18e91aa01ee' as mcid,
  '2022-09-30'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '291f7878-2db2-9c73-36c7-f18e91aa01ee' as mcid,
  '2022-10-31'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '291f7878-2db2-9c73-36c7-f18e91aa01ee' as mcid,
  '2022-11-30'::date as snapshot_date,
  '2022-12-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-01-31'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-02-28'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-03-31'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-04-30'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-05-31'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-06-30'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-07-31'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-08-31'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-09-30'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-10-31'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-11-30'::date as snapshot_date,
  '2021-05-30'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-12-31'::date as snapshot_date,
  '2021-05-30'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-01-31'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-02-29'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-03-31'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-04-30'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-05-31'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-06-30'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-07-31'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-08-31'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-09-30'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-10-31'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-11-30'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-12-31'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2021-01-31'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2021-02-28'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2021-03-31'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied
union all
select '2fd73d23-1651-e811-8143-70106fa67261' as mcid,
  '2021-04-30'::date as snapshot_date,
  '2021-05-31'::date as snapshot_date_copied --need to check with farah as date changed
  --union all select '3eea6afb-5a3d-a754-f496-43224112b3b7' as mcid,'2021-03-31'::date as snapshot_date, '2022-03-31'::date as snapshot_date_copied  --need to check with farah as data looks good now no action required
union all
select '54dd139f-c4b6-c22d-e4a8-04dfeb2341a3' as mcid,
  '2021-02-28'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select '54dd139f-c4b6-c22d-e4a8-04dfeb2341a3' as mcid,
  '2021-03-31'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select '54dd139f-c4b6-c22d-e4a8-04dfeb2341a3' as mcid,
  '2021-04-30'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select '54dd139f-c4b6-c22d-e4a8-04dfeb2341a3' as mcid,
  '2021-05-31'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select '54dd139f-c4b6-c22d-e4a8-04dfeb2341a3' as mcid,
  '2021-06-30'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select '54dd139f-c4b6-c22d-e4a8-04dfeb2341a3' as mcid,
  '2021-07-31'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select '54dd139f-c4b6-c22d-e4a8-04dfeb2341a3' as mcid,
  '2021-08-31'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select '54dd139f-c4b6-c22d-e4a8-04dfeb2341a3' as mcid,
  '2021-09-30'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select '54dd139f-c4b6-c22d-e4a8-04dfeb2341a3' as mcid,
  '2021-10-31'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select '54dd139f-c4b6-c22d-e4a8-04dfeb2341a3' as mcid,
  '2021-11-30'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select '54dd139f-c4b6-c22d-e4a8-04dfeb2341a3' as mcid,
  '2021-12-31'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select '54dd139f-c4b6-c22d-e4a8-04dfeb2341a3' as mcid,
  '2022-01-31'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select '6cf747dc-ce3a-ed70-86c0-5ad6c076ec63' as mcid,
  '2022-09-30'::date as snapshot_date,
  '2022-10-31'::date as snapshot_date_copied
union all
select '891c0d99-35e4-e411-9afb-0050568d2da8' as mcid,
  '2021-11-30'::date as snapshot_date,
  '2022-11-30'::date as snapshot_date_copied
union all
select '891c0d99-35e4-e411-9afb-0050568d2da8' as mcid,
  '2021-12-31'::date as snapshot_date,
  '2022-11-30'::date as snapshot_date_copied
union all
select '891c0d99-35e4-e411-9afb-0050568d2da8' as mcid,
  '2022-01-31'::date as snapshot_date,
  '2022-11-30'::date as snapshot_date_copied
union all
select '891c0d99-35e4-e411-9afb-0050568d2da8' as mcid,
  '2022-02-28'::date as snapshot_date,
  '2022-11-30'::date as snapshot_date_copied
union all
select '891c0d99-35e4-e411-9afb-0050568d2da8' as mcid,
  '2022-03-31'::date as snapshot_date,
  '2022-11-30'::date as snapshot_date_copied
union all
select '891c0d99-35e4-e411-9afb-0050568d2da8' as mcid,
  '2022-04-30'::date as snapshot_date,
  '2022-11-30'::date as snapshot_date_copied
union all
select '891c0d99-35e4-e411-9afb-0050568d2da8' as mcid,
  '2022-05-31'::date as snapshot_date,
  '2022-11-30'::date as snapshot_date_copied
union all
select '891c0d99-35e4-e411-9afb-0050568d2da8' as mcid,
  '2022-06-30'::date as snapshot_date,
  '2022-11-30'::date as snapshot_date_copied
union all
select '891c0d99-35e4-e411-9afb-0050568d2da8' as mcid,
  '2022-07-31'::date as snapshot_date,
  '2022-11-30'::date as snapshot_date_copied
union all
select '891c0d99-35e4-e411-9afb-0050568d2da8' as mcid,
  '2022-08-31'::date as snapshot_date,
  '2022-11-30'::date as snapshot_date_copied
union all
select '891c0d99-35e4-e411-9afb-0050568d2da8' as mcid,
  '2022-09-30'::date as snapshot_date,
  '2022-11-30'::date as snapshot_date_copied
union all
select '891c0d99-35e4-e411-9afb-0050568d2da8' as mcid,
  '2022-10-31'::date as snapshot_date,
  '2022-11-30'::date as snapshot_date_copied
union all
select '897145ea-48ca-89c6-9284-cb3e8bd3c17e' as mcid,
  '2022-01-31'::date as snapshot_date,
  '2022-04-30'::date as snapshot_date_copied
union all
select '897145ea-48ca-89c6-9284-cb3e8bd3c17e' as mcid,
  '2022-02-28'::date as snapshot_date,
  '2022-04-30'::date as snapshot_date_copied
union all
select '897145ea-48ca-89c6-9284-cb3e8bd3c17e' as mcid,
  '2022-03-31'::date as snapshot_date,
  '2022-04-30'::date as snapshot_date_copied
union all
select '8caf451d-1651-e811-8143-70106fa67261' as mcid,
  '2022-08-31'::date as snapshot_date,
  '2021-01-31'::date as snapshot_date_copied
union all
select '8caf451d-1651-e811-8143-70106fa67261' as mcid,
  '2022-09-30'::date as snapshot_date,
  '2021-01-31'::date as snapshot_date_copied
union all
select '8caf451d-1651-e811-8143-70106fa67261' as mcid,
  '2022-10-31'::date as snapshot_date,
  '2021-01-31'::date as snapshot_date_copied
union all
select '8caf451d-1651-e811-8143-70106fa67261' as mcid,
  '2022-11-30'::date as snapshot_date,
  '2021-01-31'::date as snapshot_date_copied
union all
select '8caf451d-1651-e811-8143-70106fa67261' as mcid,
  '2022-12-31'::date as snapshot_date,
  '2021-01-31'::date as snapshot_date_copied
union all
select 'a574e1c4-34e4-e411-9afb-0050568d2da8' as mcid,
  '2022-01-31'::date as snapshot_date,
  '2022-09-30'::date as snapshot_date_copied
union all
select 'a574e1c4-34e4-e411-9afb-0050568d2da8' as mcid,
  '2022-02-28'::date as snapshot_date,
  '2022-09-30'::date as snapshot_date_copied
union all
select 'a574e1c4-34e4-e411-9afb-0050568d2da8' as mcid,
  '2022-03-31'::date as snapshot_date,
  '2022-09-30'::date as snapshot_date_copied
union all
select 'a574e1c4-34e4-e411-9afb-0050568d2da8' as mcid,
  '2022-04-30'::date as snapshot_date,
  '2022-09-30'::date as snapshot_date_copied
union all
select 'a574e1c4-34e4-e411-9afb-0050568d2da8' as mcid,
  '2022-05-31'::date as snapshot_date,
  '2022-09-30'::date as snapshot_date_copied
union all
select 'a574e1c4-34e4-e411-9afb-0050568d2da8' as mcid,
  '2022-06-30'::date as snapshot_date,
  '2022-09-30'::date as snapshot_date_copied
union all
select 'a574e1c4-34e4-e411-9afb-0050568d2da8' as mcid,
  '2022-07-31'::date as snapshot_date,
  '2022-09-30'::date as snapshot_date_copied
union all
select 'a574e1c4-34e4-e411-9afb-0050568d2da8' as mcid,
  '2022-08-31'::date as snapshot_date,
  '2022-09-30'::date as snapshot_date_copied
union all
select 'ac4ab096-10ce-cf17-6ba7-94a36ab9333f' as mcid,
  '2020-01-31'::date as snapshot_date,
  '2020-02-29'::date as snapshot_date_copied
union all
select 'ac4ab096-10ce-cf17-6ba7-94a36ab9333f' as mcid,
  '2021-05-31'::date as snapshot_date,
  '2021-07-31'::date as snapshot_date_copied
union all
select 'ac4ab096-10ce-cf17-6ba7-94a36ab9333f' as mcid,
  '2021-06-30'::date as snapshot_date,
  '2021-07-31'::date as snapshot_date_copied
union all
select 'b6479b1a-2251-e811-813c-70106fa51d21' as mcid,
  '2019-03-31'::date as snapshot_date,
  '2019-11-30'::date as snapshot_date_copied
union all
select 'b6479b1a-2251-e811-813c-70106fa51d21' as mcid,
  '2019-04-30'::date as snapshot_date,
  '2019-11-30'::date as snapshot_date_copied
union all
select 'b6479b1a-2251-e811-813c-70106fa51d21' as mcid,
  '2019-05-31'::date as snapshot_date,
  '2019-11-30'::date as snapshot_date_copied
union all
select 'b6479b1a-2251-e811-813c-70106fa51d21' as mcid,
  '2019-06-30'::date as snapshot_date,
  '2019-11-30'::date as snapshot_date_copied
union all
select 'b6479b1a-2251-e811-813c-70106fa51d21' as mcid,
  '2019-07-31'::date as snapshot_date,
  '2019-11-30'::date as snapshot_date_copied
union all
select 'b6479b1a-2251-e811-813c-70106fa51d21' as mcid,
  '2019-08-31'::date as snapshot_date,
  '2019-11-30'::date as snapshot_date_copied
union all
select 'b6479b1a-2251-e811-813c-70106fa51d21' as mcid,
  '2019-09-30'::date as snapshot_date,
  '2019-11-30'::date as snapshot_date_copied
union all
select 'b6479b1a-2251-e811-813c-70106fa51d21' as mcid,
  '2019-10-31'::date as snapshot_date,
  '2019-11-30'::date as snapshot_date_copied
union all
select 'bca7451d-1651-e811-8143-70106fa67261' as mcid,
  '2021-01-31'::date as snapshot_date,
  '2021-04-30'::date as snapshot_date_copied
union all
select 'bca7451d-1651-e811-8143-70106fa67261' as mcid,
  '2021-02-28'::date as snapshot_date,
  '2021-04-30'::date as snapshot_date_copied
union all
select 'bca7451d-1651-e811-8143-70106fa67261' as mcid,
  '2021-03-31'::date as snapshot_date,
  '2021-04-30'::date as snapshot_date_copied
union all
select 'c4350de0-20b2-e911-a96d-000d3a441525' as mcid,
  '2019-01-31'::date as snapshot_date,
  '2019-03-31'::date as snapshot_date_copied
union all
select 'c4350de0-20b2-e911-a96d-000d3a441525' as mcid,
  '2019-02-28'::date as snapshot_date,
  '2019-03-31'::date as snapshot_date_copied
union all
select 'c6c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-03-31'::date as snapshot_date,
  '2019-11-30'::date as snapshot_date_copied
union all
select 'c6c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-04-30'::date as snapshot_date,
  '2019-11-30'::date as snapshot_date_copied
union all
select 'c6c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-05-31'::date as snapshot_date,
  '2019-11-30'::date as snapshot_date_copied
union all
select 'c6c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-06-30'::date as snapshot_date,
  '2019-11-30'::date as snapshot_date_copied
union all
select 'c6c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-07-31'::date as snapshot_date,
  '2019-11-30'::date as snapshot_date_copied
union all
select 'c6c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-08-31'::date as snapshot_date,
  '2019-11-30'::date as snapshot_date_copied --need to check with farah
union all
select 'c6c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-09-30'::date as snapshot_date,
  '2019-11-30'::date as snapshot_date_copied --need to check with farah
union all
select 'c6c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-10-31'::date as snapshot_date,
  '2019-11-30'::date as snapshot_date_copied --need to check with farah
union all
select 'cffbc28b-c2c7-46b0-c942-a0cd7082c73b' as mcid,
  '2022-09-30'::date as snapshot_date,
  '2022-10-31'::date as snapshot_date_copied
union all
select 'd2bef417-2502-cd69-5fcc-0fe86ca4fb20' as mcid,
  '2022-09-30'::date as snapshot_date,
  '2022-10-31'::date as snapshot_date_copied
union all
select 'e9654665-aa12-9468-c6ad-584064d9512b' as mcid,
  '2022-01-31'::date as snapshot_date,
  '2022-06-30'::date as snapshot_date_copied
union all
select 'e9654665-aa12-9468-c6ad-584064d9512b' as mcid,
  '2022-02-28'::date as snapshot_date,
  '2022-06-30'::date as snapshot_date_copied
union all
select 'e9654665-aa12-9468-c6ad-584064d9512b' as mcid,
  '2022-03-31'::date as snapshot_date,
  '2022-06-30'::date as snapshot_date_copied
union all
select 'e9654665-aa12-9468-c6ad-584064d9512b' as mcid,
  '2022-04-30'::date as snapshot_date,
  '2022-06-30'::date as snapshot_date_copied
union all
select 'e9654665-aa12-9468-c6ad-584064d9512b' as mcid,
  '2022-05-31'::date as snapshot_date,
  '2022-06-30'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-07-31'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-08-31'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-09-30'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-10-31'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-11-30'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2019-12-31'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-01-31'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-02-29'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-03-31'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-04-30'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-05-31'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-06-30'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-07-31'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-08-31'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-09-30'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-10-31'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-11-30'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2020-12-31'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2021-01-31'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '68c93d23-1651-e811-8143-70106fa67261' as mcid,
  '2021-02-28'::date as snapshot_date,
  '2021-03-31'::date as snapshot_date_copied
union all
select '957fd526-b665-e811-812f-70106faab5f1' as mcid,
  '2021-08-31'::date as snapshot_date,
  '2022-01-31'::date as snapshot_date_copied
union all
select '957fd526-b665-e811-812f-70106faab5f1' as mcid,
  '2021-09-30'::date as snapshot_date,
  '2022-01-31'::date as snapshot_date_copied
union all
select '957fd526-b665-e811-812f-70106faab5f1' as mcid,
  '2021-10-31'::date as snapshot_date,
  '2022-01-31'::date as snapshot_date_copied
union all
select '957fd526-b665-e811-812f-70106faab5f1' as mcid,
  '2021-11-30'::date as snapshot_date,
  '2022-01-31'::date as snapshot_date_copied
union all
select '957fd526-b665-e811-812f-70106faab5f1' as mcid,
  '2021-12-31'::date as snapshot_date,
  '2022-01-31'::date as snapshot_date_copied
union all
select 'd869d448-597a-e611-80e5-fc15b426ff90' as mcid,
  '2020-11-30'::date as snapshot_date,
  '2020-12-31'::date as snapshot_date_copied
union all
select 'e1bf5345-9de0-e2d9-5bd7-083c58d9971d' as mcid,
  '2021-05-31'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select 'e1bf5345-9de0-e2d9-5bd7-083c58d9971d' as mcid,
  '2021-06-30'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select 'e1bf5345-9de0-e2d9-5bd7-083c58d9971d' as mcid,
  '2021-07-31'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select 'e1bf5345-9de0-e2d9-5bd7-083c58d9971d' as mcid,
  '2021-08-31'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select 'e1bf5345-9de0-e2d9-5bd7-083c58d9971d' as mcid,
  '2021-09-30'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select 'e1bf5345-9de0-e2d9-5bd7-083c58d9971d' as mcid,
  '2021-10-31'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select 'e1bf5345-9de0-e2d9-5bd7-083c58d9971d' as mcid,
  '2021-11-30'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select 'e1bf5345-9de0-e2d9-5bd7-083c58d9971d' as mcid,
  '2021-12-31'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied
union all
select 'e1bf5345-9de0-e2d9-5bd7-083c58d9971d' as mcid,
  '2022-01-31'::date as snapshot_date,
  '2022-02-28'::date as snapshot_date_copied;
alter table temp_option_c
add column arr_old numeric;
alter table temp_option_c
add column arr_new numeric;
--select * from temp_option_c
--update old arr
with temp as (
  select b.mcid,
    b.snapshot_date,
    sum(a.arr) as arr_old
  from sandbox_pd.sst a
    join temp_option_c b on a.mcid = b.mcid
    and b.snapshot_date = a.snapshot_date
    and a.overage_flag ilike '%N%'
  group by b.mcid,
    b.snapshot_date
)
update temp_option_c a
set arr_old = b.arr_old
from temp b
where a.mcid = b.mcid
  and b.snapshot_date = a.snapshot_date;
--update new arr
with temp as (
  select b.mcid,
    b.snapshot_date_copied,
    sum(a.arr) as arr_new
  from sandbox_pd.sst a
    join (
      select distinct mcid,
        snapshot_date_copied
      from temp_option_c
    ) b on a.mcid = b.mcid
    and b.snapshot_date_copied = a.snapshot_date
    and a.overage_flag ilike '%N%'
  group by b.mcid,
    b.snapshot_date_copied
)
update temp_option_c a
set arr_new = b.arr_new
from temp b
where a.mcid = b.mcid
  and b.snapshot_date_copied = a.snapshot_date_copied;
--delete records from sst table
delete from sandbox_pd.sst a using temp_option_c b
where a.mcid = b.mcid
  and b.snapshot_date = a.snapshot_date
  and a.overage_flag ilike '%N%';
update sandbox_pd.sst a
set arr = 0,
  baseline_arr_local_currency = 0
where a.mcid in ('4456543717', '7650145182')
  and a.overage_flag ilike '%N%';
--copy records from sst
drop table if exists temp_to_insert_records_option_c;
create temporary table temp_to_insert_records_option_c as
select b.snapshot_date as snapshot_date,
  a.ultimate_parent_id,
  a.ultimate_parent_name,
  a.duns_name,
  a.duns_number,
  a.parent_duns_name,
  a.parent_duns_number,
  a.domesticultimatedunsnumber,
  a.globalultimatedunsnumber,
  a.new_product_solution,
  a.new_product_line,
  a.updated_product_group,
  a.new_product,
  a.new_line_of_business,
  a.new_line_of_business_sub_category,
  a.c_name,
  a.parent_ns_id,
  a.end_ns_id,
  a.name,
  a.parent_name,
  a.end_name,
  a.mcid,
  a.parent_mcid,
  a.end_mcid,
  a.subsidiary_entity_name,
  a.overage_flag,
  a.segment,
  a.region,
  a.product_family,
  a.base_currency,
  a.cc_fx_rate,
  a.fx_date,
  a.arr,
  a.baseline_arr_local_currency,
  a.dw_modified_date,
  a.dw_created_date,
  a.parent_sf_id,
  a.parent_sf_name,
  a.sku
from sandbox_pd.sst a
  join temp_option_c b on a.mcid = b.mcid
  and a.snapshot_date = b.snapshot_date_copied
  and a.overage_flag ilike '%N%';
insert into sandbox_pd.sst (
    snapshot_date,
    ultimate_parent_id,
    ultimate_parent_name,
    duns_name,
    duns_number,
    parent_duns_name,
    parent_duns_number,
    domesticultimatedunsnumber,
    globalultimatedunsnumber,
    new_product_solution,
    new_product_line,
    updated_product_group,
    new_product,
    new_line_of_business,
    new_line_of_business_sub_category,
    c_name,
    parent_ns_id,
    end_ns_id,
    name,
    parent_name,
    end_name,
    mcid,
    parent_mcid,
    end_mcid,
    subsidiary_entity_name,
    overage_flag,
    segment,
    region,
    product_family,
    base_currency,
    cc_fx_rate,
    fx_date,
    arr,
    baseline_arr_local_currency,
    dw_modified_date,
    dw_created_date,
    parent_sf_id,
    parent_sf_name,
    cohort_actions,
    sku
  )
select snapshot_date,
  ultimate_parent_id,
  ultimate_parent_name,
  duns_name,
  duns_number,
  parent_duns_name,
  parent_duns_number,
  domesticultimatedunsnumber,
  globalultimatedunsnumber,
  new_product_solution,
  new_product_line,
  updated_product_group,
  new_product,
  new_line_of_business,
  new_line_of_business_sub_category,
  c_name,
  parent_ns_id,
  end_ns_id,
  name,
  parent_name,
  end_name,
  mcid,
  parent_mcid,
  end_mcid,
  subsidiary_entity_name,
  overage_flag,
  segment,
  region,
  product_family,
  base_currency,
  cc_fx_rate,
  fx_date,
  arr,
  baseline_arr_local_currency,
  dw_modified_date,
  dw_created_date,
  parent_sf_id,
  parent_sf_name,
  'New record inserted for Option C',
  sku
from temp_to_insert_records_option_c;
insert into sensitivity_analysis_actions_temp
select mcid,
  snapshot_date,
  'Manual',
  arr_old,
  null,
  arr_new,
  null,
  'Option C',
  null,
  snapshot_date_copied
from temp_option_c a;
drop table if exists sandbox_pd.sensitivity_analysis_actions_cohort_1;
create table sandbox_pd.sensitivity_analysis_actions_cohort_1 as
select *
from sensitivity_analysis_actions_temp;
end if;
--#################################################
--COHORT 2 ACTIONS
--#################################################
if run_cohort_2 = 1 then drop table if exists sensitivity_analysis_actions_cohort2_temp;
create table sensitivity_analysis_actions_cohort2_temp as
select distinct "Formatted MCID" as "End Customer MCID" --, "Formatted Snapshot_Date"::date as "Snapshot Date (Month)"
,
  (
    date_trunc('month', "Formatted Snapshot_Date"::date) + interval '1 month' - interval '1 day'
  )::date as "Snapshot Date (Month)",
  'Overwrite with latest Non-Spike Value - Non U&D' as "Remedial Action",
  null as arr_old,
  null::numeric as arr_previous_month,
  null as arr_new,
  null::Text as de_comments,
  null::Text as da_comments,
  null::Text as no_of_records_in_sst_postive_arr,
  (
    date_trunc(
      'month',
      "Date of Alt. Churn Values of 6 month period - Group"::Date
    ) + interval '1 month' - interval '1 day'
  )::date as snapshot_date_copied_farah,
  null::date as snapshot_date_copied
from sandbox_pd.SST_COHORT_2
where 1 = 1
  and "Date of Alt. Churn Values of 6 month period - Group" is not null;
--delete data that has got only one in group
delete from sensitivity_analysis_actions_cohort2_temp a using (
    select "End Customer MCID",
      max(snapshot_date_copied_farah) as snapshot_date_copied_farah
    from sensitivity_analysis_actions_cohort2_temp
    group by "End Customer MCID",
      snapshot_date_copied_farah
    having count("Snapshot Date (Month)") = 1
  ) b
where a."End Customer MCID" = b."End Customer MCID"
  and a.snapshot_date_copied_farah = b.snapshot_date_copied_farah;
update sensitivity_analysis_actions_cohort2_temp a
set snapshot_date_copied = b.snapshot_date_copied
from (
    select "End Customer MCID",
      max("Snapshot Date (Month)") as snapshot_date_copied,
      snapshot_date_copied_farah
    from sensitivity_analysis_actions_cohort2_temp
    group by "End Customer MCID",
      snapshot_date_copied_farah
  ) b
where a."End Customer MCID" = b."End Customer MCID"
  and a.snapshot_date_copied_farah = b.snapshot_date_copied_farah;
delete --select *
from sensitivity_analysis_actions_cohort2_temp a
where snapshot_date_copied = "Snapshot Date (Month)";
delete from sensitivity_analysis_actions_cohort2_temp a
where "Snapshot Date (Month)" >= '2023-01-01';
--#################################################################
--case 2: get latest arr and delete + insert
--#################################################################
drop table if exists temp_to_delete_records;
create temporary table temp_to_delete_records as
select distinct "End Customer MCID",
  "Snapshot Date (Month)",
  b.overage_flag,
  a.snapshot_date_copied
from sensitivity_analysis_actions_cohort2_temp a
  left join sandbox_pd.sst b on a."End Customer MCID" = b.mcid
  and a."Snapshot Date (Month)" = b.snapshot_date
where 1 = 1
order by 1,
  2;
--delete records from sst table
delete from sandbox_pd.sst a using temp_to_delete_records b
where a.mcid = b."End Customer MCID"
  and b."Snapshot Date (Month)" = a.snapshot_date
  and a.overage_flag ilike '%N%';
--copy records from sst
drop table if exists temp_to_insert_records;
create temporary table temp_to_insert_records as with temp as (
  select distinct b.snapshot_date_copied as snapshot_date_copied,
    a.ultimate_parent_id,
    a.ultimate_parent_name,
    a.duns_name,
    a.duns_number,
    a.parent_duns_name,
    a.parent_duns_number,
    a.domesticultimatedunsnumber,
    a.globalultimatedunsnumber,
    a.new_product_solution,
    a.new_product_line,
    a.updated_product_group,
    a.new_product,
    a.new_line_of_business,
    a.new_line_of_business_sub_category,
    a.c_name,
    a.parent_ns_id,
    a.end_ns_id,
    a.name,
    a.parent_name,
    a.end_name,
    a.mcid,
    a.parent_mcid,
    a.end_mcid,
    a.subsidiary_entity_name,
    a.overage_flag,
    a.segment,
    a.region,
    a.product_family,
    a.base_currency,
    a.cc_fx_rate,
    a.fx_date,
    a.arr,
    a.baseline_arr_local_currency,
    a.dw_modified_date,
    a.dw_created_date,
    a.parent_sf_id,
    a.parent_sf_name,
    a.sku
  from sandbox_pd.sst a
    join (
      select distinct "End Customer MCID",
        snapshot_date_copied
      from temp_to_delete_records
    ) b on a.mcid = b."End Customer MCID"
    and a.snapshot_date = b.snapshot_date_copied
    and a.overage_flag ilike '%N%'
),
temp1 as (
  select distinct "End Customer MCID",
    "Snapshot Date (Month)",
    snapshot_date_copied
  from sensitivity_analysis_actions_cohort2_temp
)
select distinct b."Snapshot Date (Month)" as snapshot_date,
  a.ultimate_parent_id,
  a.ultimate_parent_name,
  a.duns_name,
  a.duns_number,
  a.parent_duns_name,
  a.parent_duns_number,
  a.domesticultimatedunsnumber,
  a.globalultimatedunsnumber,
  a.new_product_solution,
  a.new_product_line,
  a.updated_product_group,
  a.new_product,
  a.new_line_of_business,
  a.new_line_of_business_sub_category,
  a.c_name,
  a.parent_ns_id,
  a.end_ns_id,
  a.name,
  a.parent_name,
  a.end_name,
  a.mcid,
  a.parent_mcid,
  a.end_mcid,
  a.subsidiary_entity_name,
  a.overage_flag,
  a.segment,
  a.region,
  a.product_family,
  a.base_currency,
  a.cc_fx_rate,
  a.fx_date,
  a.arr,
  a.baseline_arr_local_currency,
  a.dw_modified_date,
  a.dw_created_date,
  a.parent_sf_id,
  a.parent_sf_name,
  a.sku
from temp a
  join temp1 b on a.mcid = b."End Customer MCID"
  and a.snapshot_date_copied = b.snapshot_date_copied;
--update snapshot_date_copied and new arr value in sensitivity_analysis_actions_cohort2
with temp as (
  select a.mcid,
    a.snapshot_date,
    sum(a.arr) as arr_new
  from temp_to_insert_records a
  group by 1,
    2
)
update sensitivity_analysis_actions_cohort2_temp a
set arr_new = b.arr_new
from temp b
where a."End Customer MCID" = b.mcid
  and a."Snapshot Date (Month)" = b.snapshot_date;
insert into sandbox_pd.sst (
    snapshot_date,
    ultimate_parent_id,
    ultimate_parent_name,
    duns_name,
    duns_number,
    parent_duns_name,
    parent_duns_number,
    domesticultimatedunsnumber,
    globalultimatedunsnumber,
    new_product_solution,
    new_product_line,
    updated_product_group,
    new_product,
    new_line_of_business,
    new_line_of_business_sub_category,
    c_name,
    parent_ns_id,
    end_ns_id,
    name,
    parent_name,
    end_name,
    mcid,
    parent_mcid,
    end_mcid,
    subsidiary_entity_name,
    overage_flag,
    segment,
    region,
    product_family,
    base_currency,
    cc_fx_rate,
    fx_date,
    arr,
    baseline_arr_local_currency,
    dw_modified_date,
    dw_created_date,
    parent_sf_id,
    parent_sf_name,
    cohort_actions,
    sku
  )
select snapshot_date,
  ultimate_parent_id,
  ultimate_parent_name,
  duns_name,
  duns_number,
  parent_duns_name,
  parent_duns_number,
  domesticultimatedunsnumber,
  globalultimatedunsnumber,
  new_product_solution,
  new_product_line,
  updated_product_group,
  new_product,
  new_line_of_business,
  new_line_of_business_sub_category,
  c_name,
  parent_ns_id,
  end_ns_id,
  name,
  parent_name,
  end_name,
  mcid,
  parent_mcid,
  end_mcid,
  subsidiary_entity_name,
  overage_flag,
  segment,
  region,
  product_family,
  base_currency,
  cc_fx_rate,
  fx_date,
  arr,
  baseline_arr_local_currency,
  dw_modified_date,
  dw_created_date,
  parent_sf_id,
  parent_sf_name,
  concat(
    'Deleted data and inserted data snapshot from latest snapshot',
    ''::Text
  ) as cohort_actions,
  sku
from temp_to_insert_records;
drop table if exists sandbox_pd.sensitivity_analysis_actions_cohort_2;
create table sandbox_pd.sensitivity_analysis_actions_cohort_2 as
select *
from sensitivity_analysis_actions_cohort2_temp;
end if;
END;
$function$;