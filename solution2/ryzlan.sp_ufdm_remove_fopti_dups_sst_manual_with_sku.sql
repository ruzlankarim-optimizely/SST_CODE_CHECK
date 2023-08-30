CREATE OR REPLACE FUNCTION ryzlan.sp_ufdm_remove_fopti_dups_sst_manual_with_sku()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

BEGIN

    drop table if exists fopti_dups_temp ;

    create temp table fopti_dups_temp as
    with fopti_ufdm_arr as
    (
        select * from ufdm.arr where product_family in ('Web', 'Full Stack')
    )

    ,	fopti_ufdm_arr_2 as
    (
        select
            distinct mcid as mcid_ufdm,
                        snapshot_date as snapshot_date_ufdm,
                        sum(arr_usd_ccfx) over(partition by mcid, snapshot_date) as "Sum Fopti UFDM"
        from
            fopti_ufdm_arr
    )
    ,fopti_sst as
    (
        select
            *
        from
            ryzlan.sku_sst
        where
            product_family in ('Web', 'Full Stack')
    )
    ,fopti_sst_2 as
    (
        select
            distinct mcid as mcid_sst,
                        snapshot_date as snapshot_date_sst,
                        sum(arr) over(partition by mcid, snapshot_date) as "Sum Fopti SST"
        from
            fopti_sst
    )
    ,combined_table_1 as
    (
        select
            fa.mcid_ufdm,
            fa.snapshot_date_ufdm,
            fa."Sum Fopti UFDM",
            fsa."Sum Fopti SST",
            fsa."Sum Fopti SST"-fa."Sum Fopti UFDM" as "Difference in ARR"
        from
            fopti_ufdm_arr_2 fa
                inner join
            fopti_sst_2 fsa
            on
                        fa.mcid_ufdm = fsa.mcid_sst
                    and
                        fa.snapshot_date_ufdm = fsa.snapshot_date_sst
    )
    --Run this code to find out the difference in values between UFDM ARR and SST
    select
        *
    from combined_table_1
    where
        "Difference in ARR" > 1
    ;

    with temp as (
        select
              a.ctid
             ,row_number() over (partition by a.mcid, a.snapshot_date,a.product_family,a.arr) as rnk
        from ryzlan.sku_sst a
        join (select distinct mcid_ufdm,snapshot_date_ufdm,"Sum Fopti SST"
              from fopti_dups_temp
             ) b on a.mcid = b.mcid_ufdm and a.snapshot_date = b.snapshot_date_ufdm
        where a.arr > 0
          and a.product_family in ('Web','Full Stack')
    )
    delete from ryzlan.sku_sst a
    using temp b where a.ctid = b.ctid
    and b.rnk >= 2
    ;

END;
$function$
;
