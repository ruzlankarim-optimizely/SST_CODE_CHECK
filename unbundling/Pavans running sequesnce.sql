drop table sandbox_pd.monthly_metrics;
drop table sandbox_pd.product_allocated;
drop table sandbox_pd.arr;
create table sandbox_pd.product_allocated (
    id serial primary key,
    snapshot_date date not null,
    c_name text,
    parent_customer text,
    end_customer text,
    parent_master_customer_id text,
    end_customer_master_customer_id text,
    parent_salesforce_id text,
    end_customer_salesforce_id text,
    line_type text,
    baseline_currency text,
    subsidiary_base_currency text,
    recurring_amount double precision,
    baseline_mrr_local_currency double precision,
    baseline_arr_local_currency double precision,
    ccfx_date date,
    mefx_date date,
    fx_rate_ccfx double precision,
    mrr_usd_ccfx double precision,
    arr_usd_ccfx double precision,
    fx_rate_mefx double precision,
    mrr_usd_mefx double precision,
    arr_usd_mefx double precision,
    fx_rate_actualfx double precision,
    mrr_usd_actualfx double precision,
    arr_usd_actualfx double precision,
    bill_freq text,
    term_months double precision,
    date_start date,
    date_end date,
    date_termination date,
    subline_id double precision,
    reference_number text,
    line_number text,
    revision_number double precision,
    change_order text,
    status text,
    catalog_type text,
    sku text,
    sku_name text,
    product_name text,
    product_group text,
    product_family text,
    arr_source text,
    sco_action_id text,
    sco_memo text,
    sco_modification_type text,
    subsidiary_entity_name text,
    legacy_org text not null,
    parent_customer_ns_id text,
    end_customer_ns_id text,
    mcid text,
    created_date timestamp default CURRENT_TIMESTAMP,
    modified_date timestamp,
    new_product_solution text,
    new_product_line text,
    updated_product_group text,
    new_product text,
    new_line_of_business text,
    new_line_of_business_sub_category text
);
create index sandbox_pdix_pa_mc_id on sandbox_pd.product_allocated (parent_master_customer_id);
create index sandbox_pdix_pa_cname on sandbox_pd.product_allocated (c_name);
create index sandbox_pdix_pa_line_number on sandbox_pd.product_allocated (line_number);
create index sandbox_pdix_pa_ssdate on sandbox_pd.product_allocated (snapshot_date);
create index sandbox_pdix_pa_ssdate_line on sandbox_pd.product_allocated (snapshot_date, line_number);
create table sandbox_pd.monthly_metrics (
    id serial primary key,
    snapshot_date date not null,
    c_name text,
    parent_customer text,
    end_customer text,
    parent_master_customer_id text,
    end_customer_master_customer_id text,
    parent_salesforce_id text,
    end_customer_salesforce_id text,
    line_type text,
    baseline_currency text,
    subsidiary_base_currency text,
    recurring_amount double precision,
    baseline_mrr_local_currency double precision,
    baseline_arr_local_currency double precision,
    ccfx_date date,
    mefx_date date,
    fx_rate_ccfx double precision,
    mrr_usd_ccfx double precision,
    arr_usd_ccfx double precision,
    fx_rate_mefx double precision,
    mrr_usd_mefx double precision,
    arr_usd_mefx double precision,
    fx_rate_actualfx double precision,
    mrr_usd_actualfx double precision,
    arr_usd_actualfx double precision,
    bill_freq text,
    term_months double precision,
    date_start date,
    date_end date,
    date_termination date,
    subline_id double precision,
    reference_number text,
    line_number text,
    revision_number double precision,
    change_order text,
    status text,
    catalog_type text,
    sku text,
    sku_name text,
    product_name text,
    product_group text,
    product_family text,
    arr_source text,
    sco_action_id text,
    sco_memo text,
    sco_modification_type text,
    subsidiary_entity_name text,
    legacy_org text not null,
    parent_customer_ns_id text,
    end_customer_ns_id text,
    mcid text,
    created_date timestamp default CURRENT_TIMESTAMP,
    modified_date timestamp,
    modified_comments text,
    new_product_solution text,
    new_product_line text,
    updated_product_group text,
    new_product text,
    new_line_of_business text,
    new_line_of_business_sub_category text
);
create index sandbox_pdix_mm_accound_id on sandbox_pd.monthly_metrics (parent_salesforce_id);
create index sandbox_pdix_mm_arrsource on sandbox_pd.monthly_metrics (arr_source);
create index sandbox_pdix_mm_cfullname on sandbox_pd.monthly_metrics (parent_customer);
create index sandbox_pdix_mm_cname on sandbox_pd.monthly_metrics (c_name);
create index sandbox_pdix_mm_currency on sandbox_pd.monthly_metrics (baseline_currency);
create index sandbox_pdix_mm_date_comp_sourcek on sandbox_pd.monthly_metrics (snapshot_date) include (legacy_org, arr_source);
create index sandbox_pdix_mm_sku on sandbox_pd.monthly_metrics (sku);
create index sandbox_pdix_mm_ssdate on sandbox_pd.monthly_metrics (snapshot_date);
create index sandbox_pdix_mm_line_number on sandbox_pd.monthly_metrics (line_number);
create index sandbox_pdix_mm_ssdate_line on sandbox_pd.monthly_metrics (snapshot_date, line_number);
create table sandbox_pd.arr (
    id serial constraint arr_metrics_pkey primary key,
    snapshot_date date not null,
    c_name text,
    parent_customer_ns_id text,
    end_customer_ns_id text,
    parent_customer text,
    end_customer text,
    parent_master_customer_id text,
    end_customer_master_customer_id text,
    parent_salesforce_id text,
    end_customer_salesforce_id text,
    line_type text,
    baseline_currency text,
    subsidiary_base_currency text,
    recurring_amount double precision,
    baseline_mrr_local_currency double precision,
    baseline_arr_local_currency double precision,
    ccfx_date date,
    mefx_date date,
    fx_rate_ccfx double precision,
    mrr_usd_ccfx double precision,
    arr_usd_ccfx double precision,
    fx_rate_mefx double precision,
    mrr_usd_mefx double precision,
    arr_usd_mefx double precision,
    fx_rate_actualfx double precision,
    mrr_usd_actualfx double precision,
    arr_usd_actualfx double precision,
    bill_freq text,
    term_months double precision,
    date_start date,
    date_end date,
    date_termination date,
    subline_id double precision,
    reference_number text,
    line_number text,
    revision_number double precision,
    change_order text,
    status text,
    catalog_type text,
    sku text,
    sku_name text,
    product_name text,
    product_group text,
    product_family text,
    arr_source text,
    sco_action_id text,
    sco_memo text,
    sco_modification_type text,
    subsidiary_entity_name text,
    legacy_org text not null,
    mcid text,
    created_date timestamp default CURRENT_TIMESTAMP,
    modified_date timestamp,
    snapshot_date_revised date,
    new_product_solution text,
    new_product_line text,
    updated_product_group text,
    new_product text,
    new_line_of_business text,
    new_line_of_business_sub_category text,
    modified_comments text
);
create index sandbox_pd_ix_mm_accound_id on sandbox_pd.arr (parent_salesforce_id);
create index sandbox_pd_ix_mm_arrsource on sandbox_pd.arr (arr_source);
create index sandbox_pd_ix_mm_cfullname on sandbox_pd.arr (parent_customer);
create index sandbox_pd_ix_mm_cname on sandbox_pd.arr (c_name);
create index sandbox_pd_ix_mm_currency on sandbox_pd.arr (baseline_currency);
create index sandbox_pd_ix_mm_date_comp_sourcek on sandbox_pd.arr (snapshot_date) include (legacy_org, arr_source);
create index sandbox_pd_ix_mm_line_number on sandbox_pd.arr (line_number);
create index sandbox_pd_ix_mm_sku on sandbox_pd.arr (sku);
create index sandbox_pd_ix_mm_ssdate on sandbox_pd.arr (snapshot_date);
create index sandbox_pd_ix_mm_ssdate_line on sandbox_pd.arr (snapshot_date, line_number);
create index sandbox_pd_nci_arr_snapshot_date on sandbox_pd.arr (snapshot_date);
insert into sandbox_pd.monthly_metrics (
        snapshot_date,
        c_name,
        parent_customer,
        end_customer,
        parent_master_customer_id,
        end_customer_master_customer_id,
        parent_salesforce_id,
        end_customer_salesforce_id,
        line_type,
        baseline_currency,
        subsidiary_base_currency,
        recurring_amount,
        baseline_mrr_local_currency,
        baseline_arr_local_currency,
        ccfx_date,
        mefx_date,
        fx_rate_ccfx,
        mrr_usd_ccfx,
        arr_usd_ccfx,
        fx_rate_mefx,
        mrr_usd_mefx,
        arr_usd_mefx,
        fx_rate_actualfx,
        mrr_usd_actualfx,
        arr_usd_actualfx,
        bill_freq,
        term_months,
        date_start,
        date_end,
        date_termination,
        subline_id,
        reference_number,
        line_number,
        revision_number,
        change_order,
        status,
        catalog_type,
        sku,
        sku_name,
        product_name,
        product_group,
        product_family,
        arr_source,
        sco_action_id,
        sco_memo,
        sco_modification_type,
        subsidiary_entity_name,
        legacy_org,
        parent_customer_ns_id,
        end_customer_ns_id,
        mcid,
        created_date,
        modified_date,
        modified_comments,
        new_product_solution,
        new_product_line,
        updated_product_group,
        new_product,
        new_line_of_business,
        new_line_of_business_sub_category
    )
select snapshot_date,
    c_name,
    parent_customer,
    end_customer,
    parent_master_customer_id,
    end_customer_master_customer_id,
    parent_salesforce_id,
    end_customer_salesforce_id,
    line_type,
    baseline_currency,
    subsidiary_base_currency,
    recurring_amount,
    baseline_mrr_local_currency,
    baseline_arr_local_currency,
    ccfx_date,
    mefx_date,
    fx_rate_ccfx,
    mrr_usd_ccfx,
    arr_usd_ccfx,
    fx_rate_mefx,
    mrr_usd_mefx,
    arr_usd_mefx,
    fx_rate_actualfx,
    mrr_usd_actualfx,
    arr_usd_actualfx,
    bill_freq,
    term_months,
    date_start,
    date_end,
    date_termination,
    subline_id,
    reference_number,
    line_number,
    revision_number,
    change_order,
    status,
    catalog_type,
    sku,
    sku_name,
    product_name,
    product_group,
    product_family,
    arr_source,
    sco_action_id,
    sco_memo,
    sco_modification_type,
    subsidiary_entity_name,
    legacy_org,
    parent_customer_ns_id,
    end_customer_ns_id,
    mcid,
    created_date,
    modified_date,
    modified_comments,
    new_product_solution,
    new_product_line,
    updated_product_group,
    new_product,
    new_line_of_business,
    new_line_of_business_sub_category
from ufdm_blue.monthly_metrics;
insert into sandbox_pd.product_allocated (
        snapshot_date,
        c_name,
        parent_customer,
        end_customer,
        parent_master_customer_id,
        end_customer_master_customer_id,
        parent_salesforce_id,
        end_customer_salesforce_id,
        line_type,
        baseline_currency,
        subsidiary_base_currency,
        recurring_amount,
        baseline_mrr_local_currency,
        baseline_arr_local_currency,
        ccfx_date,
        mefx_date,
        fx_rate_ccfx,
        mrr_usd_ccfx,
        arr_usd_ccfx,
        fx_rate_mefx,
        mrr_usd_mefx,
        arr_usd_mefx,
        fx_rate_actualfx,
        mrr_usd_actualfx,
        arr_usd_actualfx,
        bill_freq,
        term_months,
        date_start,
        date_end,
        date_termination,
        subline_id,
        reference_number,
        line_number,
        revision_number,
        change_order,
        status,
        catalog_type,
        sku,
        sku_name,
        product_name,
        product_group,
        product_family,
        arr_source,
        sco_action_id,
        sco_memo,
        sco_modification_type,
        subsidiary_entity_name,
        legacy_org,
        parent_customer_ns_id,
        end_customer_ns_id,
        mcid,
        created_date,
        modified_date,
        new_product_solution,
        new_product_line,
        updated_product_group,
        new_product,
        new_line_of_business,
        new_line_of_business_sub_category
    )
select snapshot_date,
    c_name,
    parent_customer,
    end_customer,
    parent_master_customer_id,
    end_customer_master_customer_id,
    parent_salesforce_id,
    end_customer_salesforce_id,
    line_type,
    baseline_currency,
    subsidiary_base_currency,
    recurring_amount,
    baseline_mrr_local_currency,
    baseline_arr_local_currency,
    ccfx_date,
    mefx_date,
    fx_rate_ccfx,
    mrr_usd_ccfx,
    arr_usd_ccfx,
    fx_rate_mefx,
    mrr_usd_mefx,
    arr_usd_mefx,
    fx_rate_actualfx,
    mrr_usd_actualfx,
    arr_usd_actualfx,
    bill_freq,
    term_months,
    date_start,
    date_end,
    date_termination,
    subline_id,
    reference_number,
    line_number,
    revision_number,
    change_order,
    status,
    catalog_type,
    sku,
    sku_name,
    product_name,
    product_group,
    product_family,
    arr_source,
    sco_action_id,
    sco_memo,
    sco_modification_type,
    subsidiary_entity_name,
    legacy_org,
    parent_customer_ns_id,
    end_customer_ns_id,
    mcid,
    created_date,
    modified_date,
    new_product_solution,
    new_product_line,
    updated_product_group,
    new_product,
    new_line_of_business,
    new_line_of_business_sub_category
from ufdm_blue.product_allocated;
insert into sandbox_pd.arr (
        snapshot_date,
        c_name,
        parent_customer_ns_id,
        end_customer_ns_id,
        parent_customer,
        end_customer,
        parent_master_customer_id,
        end_customer_master_customer_id,
        parent_salesforce_id,
        end_customer_salesforce_id,
        line_type,
        baseline_currency,
        subsidiary_base_currency,
        recurring_amount,
        baseline_mrr_local_currency,
        baseline_arr_local_currency,
        ccfx_date,
        mefx_date,
        fx_rate_ccfx,
        mrr_usd_ccfx,
        arr_usd_ccfx,
        fx_rate_mefx,
        mrr_usd_mefx,
        arr_usd_mefx,
        fx_rate_actualfx,
        mrr_usd_actualfx,
        arr_usd_actualfx,
        bill_freq,
        term_months,
        date_start,
        date_end,
        date_termination,
        subline_id,
        reference_number,
        line_number,
        revision_number,
        change_order,
        status,
        catalog_type,
        sku,
        sku_name,
        product_name,
        product_group,
        product_family,
        arr_source,
        sco_action_id,
        sco_memo,
        sco_modification_type,
        subsidiary_entity_name,
        legacy_org,
        mcid,
        created_date,
        modified_date,
        snapshot_date_revised,
        new_product_solution,
        new_product_line,
        updated_product_group,
        new_product,
        new_line_of_business,
        new_line_of_business_sub_category,
        modified_comments
    )
select snapshot_date,
    c_name,
    parent_customer_ns_id,
    end_customer_ns_id,
    parent_customer,
    end_customer,
    parent_master_customer_id,
    end_customer_master_customer_id,
    parent_salesforce_id,
    end_customer_salesforce_id,
    line_type,
    baseline_currency,
    subsidiary_base_currency,
    recurring_amount,
    baseline_mrr_local_currency,
    baseline_arr_local_currency,
    ccfx_date,
    mefx_date,
    fx_rate_ccfx,
    mrr_usd_ccfx,
    arr_usd_ccfx,
    fx_rate_mefx,
    mrr_usd_mefx,
    arr_usd_mefx,
    fx_rate_actualfx,
    mrr_usd_actualfx,
    arr_usd_actualfx,
    bill_freq,
    term_months,
    date_start,
    date_end,
    date_termination,
    subline_id,
    reference_number,
    line_number,
    revision_number,
    change_order,
    status,
    catalog_type,
    sku,
    sku_name,
    product_name,
    product_group,
    product_family,
    arr_source,
    sco_action_id,
    sco_memo,
    sco_modification_type,
    subsidiary_entity_name,
    legacy_org,
    mcid,
    created_date,
    modified_date,
    snapshot_date_revised,
    new_product_solution,
    new_product_line,
    updated_product_group,
    new_product,
    new_line_of_business,
    new_line_of_business_sub_category,
    modified_comments
from ufdm.arr;
reindex table sandbox_pd.monthly_metrics;
reindex table sandbox_pd.product_allocated;
reindex table sandbox_pd.arr;
select sandbox_pd.sp_ufdm_monthly_metrics_updates_manual();
---#####################################################
--validate MM numbers before and after
---#####################################################
with temp_new as (
    select 'new' as source,
        left(snapshot_date::text, 12) as period,
        sum(arr_usd_ccfx) as arr_new
    from sandbox_pd.monthly_metrics a
    group by 1,
        2
),
temp_old as (
    select 'old' as source,
        left (snapshot_date::text, 12) as period,
        sum(arr_usd_ccfx) as arr_old
    from ufdm_blue.monthly_metrics a
    group by 1,
        2
)
select a.*,
    b.arr_old,
    a.arr_new - b.arr_old as diff
from temp_new a
    join temp_old b on a.period = b.period
where a.arr_new - b.arr_old not between -1 and 1
order by diff desc;
with temp_new as (
    select 'new' as source,
        left(snapshot_date::text, 12) as period,
        mcid,
        sum(arr_usd_ccfx) as arr_new
    from sandbox_pd.monthly_metrics
    where 1 = 1
        and coalesce(mcid, '') <> ''
    group by 1,
        2,
        3
),
temp_old as (
    select 'old' as source,
        left (snapshot_date::text, 12) as period,
        mcid,
        sum(arr_usd_ccfx) as arr_old
    from ufdm_blue.monthly_metrics
    where 1 = 1
        and coalesce(mcid, '') <> ''
    group by 1,
        2,
        3
)
select coalesce(a.period, b.period) as snapshot_Date,
    *,
    coalesce(a.arr_new, 0) - coalesce(b.arr_old, 0) as diff
from temp_new a
    full join temp_old b on a.period = b.period
    and coalesce(a.mcid, '') = coalesce(b.mcid, '')
where 1 = 1
    and (
        a.arr_new - b.arr_old not between -1 and 1
        or (
            a.mcid is null
            or b.mcid is null
        )
    )
order by diff desc;
select sandbox_pd.sp_populate_snapshot_arr('2019-01-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2019-02-28', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2019-03-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2019-04-30', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2019-05-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2019-06-30', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2019-07-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2019-08-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2019-09-30', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2019-10-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2019-11-30', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2019-12-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2020-01-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2020-02-29', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2020-03-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2020-04-30', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2020-05-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2020-06-30', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2020-07-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2020-08-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2020-09-30', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2020-10-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2020-11-30', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2020-12-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2021-01-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2021-02-28', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2021-03-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2021-04-30', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2021-05-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2021-06-30', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2021-07-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2021-08-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2021-09-30', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2021-10-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2021-11-30', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2021-12-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2022-01-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2022-02-28', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2022-03-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2022-04-30', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2022-05-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2022-06-30', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2022-07-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2022-08-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2022-09-30', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2022-10-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2022-11-30', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2022-12-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2023-01-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2023-02-28', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2023-03-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2023-04-30', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2023-05-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2023-06-30', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2023-07-31', 0, 1, 1, 1);
select sandbox_pd.sp_populate_snapshot_arr('2023-08-31', 0, 1, 1, 1);
--#################################################
-- removing fopti duplicates
--#################################################
drop table if exists temp_dups;
create table temp_dups as WITH fopti AS (
    SELECT a.snapshot_date,
        a.c_name,
        a.mcid,
        a.line_type,
        a.product_family,
        a.subsidiary_entity_name,
        a.arr_usd_ccfx
    FROM sandbox_pd.arr a
    WHERE a.subsidiary_entity_name ilike '%FOpti%' --and (DATE_TRUNC('month' , a.snapshot_date) + interval '1 month' - interval '1 day')::DATE = var_date
),
non_fopti AS (
    SELECT a.snapshot_date,
        a.c_name,
        a.mcid,
        a.line_type,
        a.product_family,
        a.subsidiary_entity_name
    FROM sandbox_pd.arr a
    WHERE a.subsidiary_entity_name not ilike '%FOpti%' --and (DATE_TRUNC('month' , a.snapshot_date) + interval '1 month' - interval '1 day')::DATE = var_date
),
fopti_removed as (
    SELECT f.*
    FROM fopti f
        INNER JOIN non_fopti nf ON f.c_name = nf.c_name
        AND (
            DATE_TRUNC('month', f.snapshot_date) + interval '1 month' - interval '1 day'
        )::DATE = nf.snapshot_date::date
        AND f.mcid = nf.mcid
        AND f.line_type = nf.line_type
        AND f.product_family = nf.product_family
)
select *
from fopti_removed;
DELETE FROM sandbox_pd.arr a USING temp_dups f --select * from   sandbox_pd.arr a, temp_dups f
WHERE a.c_name = f.c_name
    AND a.snapshot_date = f.snapshot_date
    AND a.mcid = f.mcid
    AND a.line_type = f.line_type
    AND a.product_family = f.product_family;
select sandbox_pd.populate_arr_integration_period();
update sandbox_pd.arr uaa
set arr_usd_ccfx = fi.final_value,
    baseline_currency = fi.baseline_currency,
    subsidiary_base_currency = fi.subsidiary_base_currency,
    recurring_amount = fi.end_recurring_amount,
    baseline_mrr_local_currency = fi.end_baseline_mrr_local_currency,
    baseline_arr_local_currency = fi.end_baseline_arr_local_currency,
    ccfx_date = fi.ccfx_date,
    mefx_date = fi.mefx_date,
    fx_rate_ccfx = fi.fx_rate_ccfx,
    mrr_usd_ccfx = fi.end_mrr_usd_ccfx,
    fx_rate_mefx = fi.fx_rate_mefx,
    mrr_usd_mefx = fi.end_mrr_usd_mefx,
    arr_usd_mefx = fi.end_arr_usd_mefx,
    fx_rate_actualfx = fi.fx_rate_actualfx,
    mrr_usd_actualfx = fi.end_mrr_usd_actualfx,
    arr_usd_actualfx = fi.end_arr_usd_actualfx
from --select uaa.snapshot_date,arr_usd_ccfx,final_value,arr_usd_ccfx - final_value from sandbox_pd.arr uaa,
    (
        select final_value,
            mcid,
            snapshot_date,
            baseline_currency,
            subsidiary_base_currency,
            end_recurring_amount,
            end_baseline_mrr_local_currency,
            end_baseline_arr_local_currency,
            ccfx_date,
            mefx_date,
            fx_rate_ccfx,
            end_mrr_usd_ccfx,
            fx_rate_mefx,
            end_mrr_usd_mefx,
            end_arr_usd_mefx,
            fx_rate_actualfx,
            end_mrr_usd_actualfx,
            end_arr_usd_actualfx
        from sandbox_pd.arr_inte_corrections fi
    ) as fi
where uaa.snapshot_date = fi.snapshot_date
    and uaa.mcid = fi.mcid
    and uaa.line_type ilike '%inflight%'
    and (
        uaa.subsidiary_entity_name ilike '%welcome%'
        or uaa.subsidiary_entity_name ilike '%Optimizely North America Inc (6)%'
    )
    and uaa.product_name <> 'Expert Services';
---#####################################################
--validate PA numbers
---#####################################################
with temp_new as (
    select 'new' as source,
        left(snapshot_date::text, 12) as period,
        sum(arr_usd_ccfx) as arr_new
    from sandbox_pd.product_allocated a
    group by 1,
        2
),
temp_old as (
    select 'old' as source,
        left (snapshot_date::text, 12) as period,
        sum(arr_usd_ccfx) as arr_old
    from ufdm_blue.product_allocated a
    group by 1,
        2
)
select a.*,
    b.arr_old,
    a.arr_new - b.arr_old as diff
from temp_new a
    join temp_old b on a.period = b.period
where a.arr_new - b.arr_old not between -1 and 1
order by diff desc;
with temp_new as (
    select 'new' as source,
        left(snapshot_date::text, 12) as period,
        mcid,
        sum(arr_usd_ccfx) as arr_new
    from sandbox_pd.product_allocated
    where 1 = 1
        and coalesce(mcid, '') <> ''
    group by 1,
        2,
        3
),
temp_old as (
    select 'old' as source,
        left (snapshot_date::text, 12) as period,
        mcid,
        sum(arr_usd_ccfx) as arr_old
    from ufdm_blue.product_allocated
    where 1 = 1
        and coalesce(mcid, '') <> ''
    group by 1,
        2,
        3
)
select coalesce(a.period, b.period) as snapshot_Date,
    *,
    coalesce(a.arr_new, 0) - coalesce(b.arr_old, 0) as diff
from temp_new a
    full join temp_old b on a.period = b.period
    and coalesce(a.mcid, '') = coalesce(b.mcid, '')
where 1 = 1
    and (
        a.arr_new - b.arr_old not between -1 and 1
        or (
            a.mcid is null
            or b.mcid is null
        )
    )
order by diff desc;
--run arr updates manual
select sandbox_pd.sp_ufdm_arr_updates_manual();
---#####################################################
--validate ARR numbers
---#####################################################
with temp_new as (
    select 'new' as source,
        left(snapshot_date::text, 12) as period,
        sum(arr_usd_ccfx) as arr_new
    from sandbox_pd.arr a
    group by 1,
        2
),
temp_old as (
    select 'old' as source,
        left (snapshot_date::text, 12) as period,
        sum(arr_usd_ccfx) as arr_old
    from ufdm_archive.arr_lcoked_13092023_1837 a
    group by 1,
        2
)
select a.*,
    b.arr_old,
    a.arr_new - b.arr_old as diff
from temp_new a
    join temp_old b on a.period = b.period
where a.arr_new - b.arr_old not between -1 and 1
order by diff desc;
with temp_new as (
    select 'new' as source,
        left(snapshot_date::text, 12) as period,
        mcid,
        sum(arr_usd_ccfx) as arr_new
    from sandbox_pd.arr
    where 1 = 1
        and coalesce(mcid, '') <> ''
    group by 1,
        2,
        3
),
temp_old as (
    select 'old' as source,
        left (snapshot_date::text, 12) as period,
        mcid,
        sum(arr_usd_ccfx) as arr_old
    from ufdm_archive.arr_lcoked_13092023_1837
    where 1 = 1
        and coalesce(mcid, '') <> ''
    group by 1,
        2,
        3
)
select coalesce(a.period, b.period) as snapshot_Date,
    *,
    coalesce(a.arr_new, 0) - coalesce(b.arr_old, 0) as diff
from temp_new a
    full join temp_old b on a.period = b.period
    and coalesce(a.mcid, '') = coalesce(b.mcid, '')
where 1 = 1
    and (
        a.arr_new - b.arr_old not between -1 and 1
        or (
            a.mcid is null
            or b.mcid is null
        )
    )
order by diff desc;
--###################################
--1. 70 30 data export
--###################################
--restore arr with 70_30 to refresh sst table
create table sandbox_pd.arr_70_30_bkup_13092023_0302 as
select *
from sandbox_pd.arr_70_30;
create table sandbox_pd.arr_70_30_bkup_13092023_173702 as
select *
from sandbox_pd.arr_70_30;
drop table if exists sandbox_pd.arr_70_30;
create table sandbox_pd.arr_70_30 as
select *
from sandbox_pd.arr;
select sandbox_pd.sp_populate_snapshot_sst_70_30_ryzlan(null);
drop table if exists sandbox_pd.sst;
create table sandbox_pd.sst as
select *,
    null::text as Updated_Product_Group_manual,
    null::text as updated_product_solution_manual
from sandbox_pd.sst_70_30_sku;
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
---#####################################################
--validate SST numbers
---#####################################################
with temp_new as (
    select 'new' as source,
        left(snapshot_date::text, 12) as period,
        sum(arr) as arr_new
    from sandbox_pd.sst a
    group by 1,
        2
),
temp_old as (
    select 'old' as source,
        left (snapshot_date::text, 12) as period,
        sum(arr) as arr_old
    from ufdm_archive.sst_lcoked_13092023_1837 a
    group by 1,
        2
)
select a.*,
    b.arr_old,
    a.arr_new - b.arr_old as diff
from temp_new a
    join temp_old b on a.period = b.period
where a.arr_new - b.arr_old not between -1 and 1
order by diff desc;
with temp_new as (
    select 'new' as source,
        left(snapshot_date::text, 12) as period,
        mcid,
        sum(arr) as arr_new
    from sandbox_pd.sst
    where 1 = 1
        and coalesce(mcid, '') <> ''
    group by 1,
        2,
        3
),
temp_old as (
    select 'old' as source,
        left (snapshot_date::text, 12) as period,
        mcid,
        sum(arr) as arr_old
    from ufdm_archive.sst_lcoked_13092023_1837
    where 1 = 1
        and coalesce(mcid, '') <> ''
    group by 1,
        2,
        3
)
select coalesce(a.period, b.period) as snapshot_Date,
    *,
    coalesce(a.arr_new, 0) - coalesce(b.arr_old, 0) as diff
from temp_new a
    full join temp_old b on a.period = b.period
    and coalesce(a.mcid, '') = coalesce(b.mcid, '')
where 1 = 1
    and (
        a.arr_new - b.arr_old not between -1 and 1
        or (
            a.mcid is null
            or b.mcid is null
        )
    )
order by diff desc;
--#####################################
--refersh sst bridges
--#####################################
grant select on all tables in schema sandbox_pd to vminnekanti;
grant select on all tables in schema sandbox_pd to mrhaman;
grant select on all tables in schema sandbox_pd to fkhan;
grant select on all tables in schema sandbox_pd to rkarim;
drop table if exists sandbox_pd.sst_adhoc;
create table sandbox_pd.sst_adhoc as
select *
from sandbox_pd.sst;
--CREATE TABLE sandbox_pd.sst_customer_bridge_bkup_130923_1309 as select *  from sandbox_pd.sst_customer_bridge where 1=1;
--Running customer/product bridges
delete from sandbox_pd.sst_customer_bridge
where 1 = 1;
delete from sandbox_pd.sst_product_bridge_product_solution
where 1 = 1;
delete from sandbox_pd.sst_product_bridge_product_group
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
select distinct evaluation_period
from sandbox_pd.sst_customer_bridge
where 1 = 1;
select distinct evaluation_period
from sandbox_pd.sst_product_bridge_product_solution
where 1 = 1;
select distinct evaluation_period
from sandbox_pd.sst_product_bridge_product_group
where 1 = 1;
--###############################################
--bridges validation
--##############################################
drop table if exists sandbox_pd.temp_bride_numbers;
create table sandbox_pd.temp_bride_numbers as
SELECT period,
    "Churn"::numeric,
    "New"::numeric,
    "Up Sell"::numeric,
    "Flat"::numeric
FROM crosstab(
        'select left(evaluation_period::text,4) as period
                     ,case when customer_bridge ilike ''%churn%'' or customer_bridge ilike ''%wi%'' then ''Churn''
                          when customer_bridge ilike ''%cpi%'' then ''Up Sell''
                     else customer_bridge end
                      ,sum(customer_arr_change_ccfx) as arr
             from sandbox_pd.sst_customer_bridge
             where 1=1
             and evaluation_period not in (''2023M07'',''2023M08'')
             group by 1,2;
             ',
        'SELECT DISTINCT case when customer_bridge ilike ''%churn%'' or customer_bridge ilike ''%wi%'' then ''Churn''
                    when customer_bridge ilike ''%cpi%'' then ''Up Sell''
                    else customer_bridge end FROM sandbox_pd.sst_customer_bridge
              where 1=1
              ORDER BY 1
            '
    ) AS ct (
        period text,
        "Churn" text,
        "Flat" text,
        "New" text,
        "Up Sell" text
    );
--only 2023 churn numbers
drop table if exists sandbox_pd.temp_bride_numbers_2023;
create table sandbox_pd.temp_bride_numbers_2023 as
SELECT period,
    "Churn"::numeric,
    "New"::numeric,
    "Up Sell"::numeric,
    "Flat"::numeric
FROM crosstab(
        'select left(evaluation_period::text,8) as period
                     ,case when customer_bridge ilike ''%churn%'' or customer_bridge ilike ''%wi%'' then ''Churn''
                          when customer_bridge ilike ''%cpi%'' then ''Up Sell''
                     else customer_bridge end
                      ,sum(customer_arr_change_ccfx) as arr
             from sandbox_pd.sst_customer_bridge
             where 1=1
             and evaluation_period not in (''2023M07'',''2023M08'')
             and left(evaluation_period::text,4)= ''2023''
             group by 1,2;
             ',
        'SELECT DISTINCT case when customer_bridge ilike ''%churn%'' or customer_bridge ilike ''%wi%'' then ''Churn''
                    when customer_bridge ilike ''%cpi%'' then ''Up Sell''
                    else customer_bridge end FROM sandbox_pd.sst_customer_bridge
              where 1=1
              ORDER BY 1
            '
    ) AS ct (
        period text,
        "Churn" text,
        "Flat" text,
        "New" text,
        "Up Sell" text
    );
select *
from sandbox_pd.temp_bride_numbers
union all
select *
from sandbox_pd.temp_bride_numbers_2023;
--###############################################
--check customer bridge vs product bridge numbers should add up to 0
--##############################################
with t1 as (
    select evaluation_period,
        sum(customer_arr_change_ccfx) as arr
    from sandbox_pd.sst_customer_bridge
    group by 1
),
t2 as (
    select evaluation_period,
        sum(product_arr_change_ccfx) as arr
    from sandbox_pd.sst_product_bridge_product_group
    group by 1
)
select *,
    coalesce(t1.arr, 0) - coalesce(t2.arr, 0)
from t1
    full join t2 on t1.evaluation_period = t2.evaluation_period
where 1 = 1
    and coalesce(t1.arr, 0) - coalesce(t2.arr, 0) not between -1 and 1;
with t1 as (
    select evaluation_period,
        sum(customer_arr_change_ccfx) as arr
    from sandbox_pd.sst_customer_bridge
    group by 1
),
t2 as (
    select evaluation_period,
        sum(product_arr_change_ccfx) as arr
    from sandbox_pd.sst_product_bridge_product_solution
    group by 1
)
select *,
    coalesce(t1.arr, 0) - coalesce(t2.arr, 0)
from t1
    full join t2 on t1.evaluation_period = t2.evaluation_period
where 1 = 1
    and coalesce(t1.arr, 0) - coalesce(t2.arr, 0) not between -1 and 1;
--###############################################
--Adaptive exports
--##############################################
delete from sandbox_pd.SST_adaptive_ending_ARR;
delete from sandbox_pd.SST_adaptive_customer_metadata;
delete from sandbox_pd.SST_adaptive_customer_bridge_movements;
delete from sandbox_pd.SST_adaptive_product_bridge_pg_movements;
delete from sandbox_pd.SST_adaptive_product_bridge_ps_movements;
select sandbox_pd.sp_populate_adaptive_exports('2019-01-01', '2023-08-31');
select snapshot_date,
    c_full_name,
    coalesce(end_customer, 'a'),
    master_customer_id,
    baseline_currency,
    baseline_arr_local_currency,
    arr_usd_ccfx,
    product_family,
    subsidiary_entity_name,
    overage_flag,
    arr_usd_ccfx_sst --select *
from sandbox_pd.SST_adaptive_ending_ARR;
select *
from sandbox_pd.SST_adaptive_customer_metadata;
with temp as (
    select *
    from sandbox_pd.SST_adaptive_customer_bridge_movements
    union all
    select *
    from sandbox_pd.SST_adaptive_product_bridge_pg_movements
    union all
    select *
    from sandbox_pd.SST_adaptive_product_bridge_ps_movements
)
select snapshot_date,
    c_full_name,
    end_customer,
    master_customer_id,
    baseline_currency,
    baseline_arr_local_currency,
    arr_usd_ccfx,
    sku,
    subsidiary_entity_name,
    "Bridge_Account",
    "Type"
from temp;
--###############################################
--bkup tables
--##############################################
create table ufdm_archive.monthly_metrics_lcoked_13092023_1837 as
select *
from sandbox_pd.monthly_metrics;
create table ufdm_archive.arr_lcoked_13092023_1837 as
select *
from sandbox_pd.arr;
create table ufdm_archive.product_allocated_lcoked_13092023_1837 as
select *
from sandbox_pd.product_allocated;
create table ufdm_archive.sst_lcoked_13092023_1837 as
select *
from sandbox_pd.sst;
create table ufdm_archive.sst_customer_bridge_lcoked_13092023_1837 as
select *
from sandbox_pd.sst_customer_bridge;
create table ufdm_archive.sst_product_bridge_product_group_lcoked_13092023_1837 as
select *
from sandbox_pd.sst_product_bridge_product_group;
create table ufdm_archive.sst_product_bridge_product_solution_lcoked_13092023_1837 as
select *
from sandbox_pd.sst_product_bridge_product_solution;
create table ufdm_archive.monthly_metrics_lcoked_14092023_1437 as
select *
from sandbox_pd.monthly_metrics;
create table ufdm_archive.arr_lcoked_14092023_1437 as
select *
from sandbox_pd.arr;
create table ufdm_archive.product_allocated_lcoked_14092023_1437 as
select *
from sandbox_pd.product_allocated;
create table ufdm_archive.sst_lcoked_14092023_1437 as
select *
from sandbox_pd.sst;
create table ufdm_archive.sst_customer_bridge_lcoked_14092023_1437 as
select *
from sandbox_pd.sst_customer_bridge;
create table ufdm_archive.sst_product_bridge_product_group_lcoked_14092023_1437 as
select *
from sandbox_pd.sst_product_bridge_product_group;
create table ufdm_archive.sst_product_bridge_product_solution_lcoked_14092023_1437 as
select *
from sandbox_pd.sst_product_bridge_product_solution;
create table ufdm_archive.SST_adaptive_ending_ARR_lcoked_13092023_1837 as
select *
from sandbox_pd.SST_adaptive_ending_ARR;
create table ufdm_archive.SST_adaptive_customer_metadata_lcoked_13092023_1837 as
select *
from sandbox_pd.SST_adaptive_customer_metadata;
create table ufdm_archive.SST_adaptive_customer_bridge_movements_lcoked_13092023_1837 as
select *
from sandbox_pd.SST_adaptive_customer_bridge_movements;
create table ufdm_archive.SST_adaptive_product_bridge_pg_movements_lcoked_13092023_1837 as
select *
from sandbox_pd.SST_adaptive_product_bridge_pg_movements;
create table ufdm_archive.SST_adaptive_product_bridge_ps_movements_lcoked_13092023_1837 as
select *
from sandbox_pd.SST_adaptive_product_bridge_ps_movements;