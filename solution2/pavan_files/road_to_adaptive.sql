--###################################
--1. 50 50 data export with product tat
--###################################
select *
from sandbox_pd.sst;
--restore sandbox_pd sst table from sst 50 50
drop table if exists sandbox_pd.sst;
--create table sandbox_pd.sst as select * from sandbox.sst_60_40;
create table sandbox_pd.sst as
select *
from sandbox.sst_with_sku_before_manual_changes_with_proposal;
select sandbox_pd.sp_ufdm_sst_updates_manual();
--cohort 1
select sandbox_pd.sp_populate_sst_sensitivity_analysis(1, 0);
--parameters run_cohort_1 int,run_cohort_2 int
select sandbox_pd.sp_populate_run_sst_sensitivity_analysis_actions(1, 0);
--parameters run_cohort_1 int,run_cohort_2 int
--cohort 2
select sandbox_pd.sp_populate_sst_sensitivity_analysis(0, 1);
--parameters run_cohort_1 int,run_cohort_2 int
select sandbox_pd.sp_populate_run_sst_sensitivity_analysis_actions(0, 1);
--parameters run_cohort_1 int,run_cohort_2 int
select sandbox_pd.sp_populate_sst_updates_manual_after_sensitivity_analysis();
---##################################################################################
drop table if exists sandbox_pd.sst_adhoc;
create table sandbox_pd.sst_adhoc as
select *
from sandbox_pd.sst;
--Running customer/product bridges
delete from sandbox_pd.sst_customer_bridge
where 1 = 1;
delete from sandbox_pd.sst_product_bridge
where 1 = 1;
delete from sandbox_pd.sst_product_bridge_product_solution
where 1 = 1;
delete from sandbox_pd.sst_product_bridge_product_group
where 1 = 1;
delete from sandbox_pd.sst_product_bridge_product_family_solution
where 1 = 1;
reindex table sandbox_pd.sst_adhoc;
select sandbox_pd.sp_populate_sst_customer_product_bridge_refresh_snapshots (
    refresh_all_snapshots := 1,
    snapshot_date_from := null,
    snapshot_date_to := null,
    run_customer_bridge := 1,
    run_product_bridge := null,
    run_overages := null,
    run_pf_ps := null,
    run_pg := null,
    run_ps := null
  );
reindex table sandbox_pd.sst_customer_bridge;
select sandbox_pd.sp_populate_sst_customer_product_bridge_refresh_snapshots (
    refresh_all_snapshots := 1,
    snapshot_date_from := null,
    snapshot_date_to := null,
    run_customer_bridge := null,
    run_product_bridge := 1,
    run_overages := null,
    run_pf_ps := null,
    run_pg := null,
    run_ps := null
  );
select sandbox_pd.sp_populate_sst_customer_product_bridge_refresh_snapshots (
    refresh_all_snapshots := 1,
    snapshot_date_from := null,
    snapshot_date_to := null,
    run_customer_bridge := null,
    run_product_bridge := null,
    run_overages := null,
    run_pf_ps := 1,
    run_pg := null,
    run_ps := null
  );
select sandbox_pd.sp_populate_sst_customer_product_bridge_refresh_snapshots (
    refresh_all_snapshots := 1,
    snapshot_date_from := null,
    snapshot_date_to := null,
    run_customer_bridge := null,
    run_product_bridge := null,
    run_overages := null,
    run_pf_ps := null,
    run_pg := 1,
    run_ps := null
  );
select sandbox_pd.sp_populate_sst_customer_product_bridge_refresh_snapshots (
    refresh_all_snapshots := 1,
    snapshot_date_from := null,
    snapshot_date_to := null,
    run_customer_bridge := null,
    run_product_bridge := null,
    run_overages := null,
    run_pf_ps := null,
    run_pg := null,
    run_ps := 1
  );
-- #########################################################
--###############################################
--PRODUCTION refreshes and Adaptive exports
--##############################################
select public.sp_populate_adaptive_exports('2019-01-01', '2023-07-31');
select snapshot_date,
  c_full_name,
  coalesce(end_customer, 'a'),
  master_customer_id,
  baseline_currency,
  baseline_arr_local_currency,
  arr_usd_ccfx,
  product_family,
  subsidiary_entity_name,
  overage_flag --select *
from ufdm.SST_adaptive_ending_ARR;
select *
from ufdm.SST_adaptive_customer_metadata;
select *
from ufdm.SST_adaptive_customer_bridge_movements
union all
select *
from ufdm.SST_adaptive_product_bridge_movements
union all
select *
from ufdm.SST_adaptive_product_bridge_pf_ps_movements
union all
select *
from ufdm.SST_adaptive_product_bridge_pg_movements
union all
select *
from ufdm.SST_adaptive_product_bridge_ps_movements;