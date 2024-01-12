-- New script in dw-prod-rds-master.cr9dekxonyuj.us-east-1.rds.amaz.
-- Date: Sep 19, 2023
-- Time: 4:10:26 PM
SELECT *
FROM sandbox_pd.arr 
drop table if exists sandbox.drag_ratio_with_sku_c1_with_flat;
CREATE TABLE sandbox.drag_ratio_with_sku_c1_with_flat AS (
  with tat_dates as (
    select distinct mcid,
      max(date_trunc('MONTH', snapshot_date)) over(partition by mcid, record_source) as "MAX Snapshot Date of TAT"
    from --    sandbox.sst_temp
      --I use sst backup which has the record sources without sensitivity analysis or manual changes
      sandbox.sst_temp
    where record_source ilike '%sst_tat%'
  ) --select
  --    distinct mcid,
  --    snapshot_date,
  --    record_source
  --from
  --    sandbox.sst_recreate_backup
  --where
  --    mcid = '10d0858f-9f42-dd11-93be-0018717a8c82'
  --Start prepping UFDM for Join
,
  ufdm_arr_1a as (
    select *,
      case
        when strpos(reverse(product_family), ':') = 0 then length(product_family)
        else strpos(reverse(product_family), ':') -1
      end as num_charac
    from sandbox_pd.arr
  ),
  ufdm_arr_1b as (
    select mcid,
      snapshot_date,
      arr_usd_ccfx,
      sku,
      product_family,
      trim(right(product_family, num_charac)) as product_family_ufdm
    from ufdm_arr_1a --Take only Non-Fopti Data from UFDM ARR
    where --not
      --(
      --date_trunc('month', snapshot_date) = '2022-01-01'::DATE
      --                  AND product_family = 'Recurring: Cloud: Other Bookings: Campaign'
      --)
      line_type not ILIKE '%Fopti%'
      and arr_source not ilike '%GMBH overages%'
  ) --select
  --    distinct arr_source
  --from
  --    sandbox_pd.arr
  --where
  --    mcid = '5e479b1a-2251-e811-813c-70106fa51d21'
  --and
  --    snapshot_date in ('2022-01-31')
,
  ufdm_arr_1c as (
    select mcid,
      snapshot_date,
      arr_usd_ccfx,
      case
        when product_family = 'Recurring: Cloud: Content Cloud: Content PaaS' then 'Recurring: Cloud: Content Cloud: Content SaaS'
        else product_family
      end as product_family,
      sku
    from ufdm_arr_1b
  ) --select
  --    *
  --from
  --    ufdm_arr_1c
  --where
  --    mcid =      '50661331-d24e-e811-813c-70106fa6f451'
  --This is the ufdm file with product family and arr data
,
  ufdm_arr_1 as (
    select distinct mcid as mcid_arr,
      product_family as product_family_arr,
      sku,
      snapshot_date,
      date_trunc('MONTH', snapshot_date) as snapshot_date_arr,
      sum(arr_usd_ccfx) over(
        partition by mcid,
        product_family,
        sku,
        snapshot_date
      ) as "Sum by Product Family,SKU and Date - UFDM ARR",
      sum(arr_usd_ccfx) over(partition by mcid, snapshot_date) as "Sum by MCID & Date - UFDM ARR"
    from ufdm_arr_1c
  ) --We need min. date when the MCID starts
,
  ufdm_arr_11 as (
    select mcid_arr,
      product_family_arr,
      sku,
      snapshot_date_arr,
      MIN(snapshot_date) filter(
        where "Sum by MCID & Date - UFDM ARR" > 0
      ) over(partition by mcid_arr) as "Start Date in UFDM ARR",
      "Sum by Product Family,SKU and Date - UFDM ARR",
      "Sum by MCID & Date - UFDM ARR"
    from ufdm_arr_1
  ) --select
  --    distinct product_family_arr
  --from
  --    ufdm_arr_11
  --where
  --    mcid_arr = '003463de-d300-df11-b498-0018717a8c82'
  --Prepare the TAT data
  --Change Content PaaS to Content SaaS
,
  tat_0 as (
    select mcid,
      date_trunc('MONTH', snapshot_date) as snapshot_date,
      arr,
      case
        when product_family = 'Recurring: Cloud: Content Cloud: Content PaaS' then 'Recurring: Cloud: Content Cloud: Content SaaS'
        else product_family
      end as product_family_tat
    from --I use sst backup which has the record sources without sensitivity analysis or manual changes
      sandbox.sst_temp
    where record_source ilike '%sst_tat%'
      and not (
        date_trunc('month', snapshot_date) = '2021-12-01'::DATE
        AND product_family = 'Recurring: Cloud: Other Bookings: Campaign'
      )
      and overage_flag is distinct
    from 'Y'
  ) --select
  --    *
  --from
  --    sandbox.sst_temp
  --where
  --    mcid = '5e479b1a-2251-e811-813c-70106fa51d21'
  --and
  --    date_trunc('MONTH',snapshot_date) = '2021-12-01'
  --select
  --    *
  --from
  --    sandbox.control_sst_before_manual_changes
  --where
  --    record_source = 'sst_tat'
  --and
  --    mcid = '5e479b1a-2251-e811-813c-70106fa51d21'
,
  tat_1 as (
    select distinct mcid as mcid_tat,
      product_family_tat as product_family_tat,
      snapshot_date as snapshot_date_tat,
      sum(arr) over(
        partition by mcid,
        product_family_tat,
        snapshot_date
      ) as "Sum by Product Family and Date - TAT",
      sum(arr) over(partition by mcid, snapshot_date) as "Sum by MICD & Date - TAT"
    from tat_0
  ) --select
  --    *
  --from
  --    tat_1
  --where
  --    mcid_tat = '95a4a5fa-47f8-8e25-9b73-57e19bd1e791'
  --Filter the TAT data only to have snapshot dates which are the last snapshot dates for TAT in SST
,
  tat_2 as (
    select t1.mcid_tat,
      t1.product_family_tat,
      t1.snapshot_date_tat,
      td."MAX Snapshot Date of TAT",
      t1."Sum by Product Family and Date - TAT"
    from tat_1 t1
      inner join tat_dates td on t1.mcid_tat = td.mcid
      and date_trunc('MONTH', t1.snapshot_date_tat) = date_trunc('MONTH', td."MAX Snapshot Date of TAT")
    where t1."Sum by Product Family and Date - TAT" > 0
      or t1.product_family_tat is not null
  ) --select
  --    distinct mcid
  --from
  --    tat_dates
  --select
  --    *
  --from
  --    tat_2
  --where
  --    mcid_tat = '00ec9665-e386-18a4-2d14-40b5803bac2c'
  --take the UFDM ARR table and only take the MCIDs that are present in TAT at transition date and take only the snapshot dates that are Transition Date + 1 month
  --This is the ufdm_arr_2 -- that needs to be joined to
  --We need to do an inner join on mcid and snapshot date.
,
  ufdm_arr_2 as (
    select ua1.mcid_arr,
      ua1.product_family_arr,
      ua1.sku,
      ua1.snapshot_date_arr,
      ua1."Sum by Product Family,SKU and Date - UFDM ARR",
      ua1."Start Date in UFDM ARR"
    from ufdm_arr_11 ua1
      inner join tat_dates td on ua1.mcid_arr = td.mcid
      and (ua1.snapshot_date_arr) = td."MAX Snapshot Date of TAT" + interval '1 Month'
  ) --select
  --    *
  ----  "MAX Snapshot Date of TAT"+ interval '1 Month'
  --from
  --    ufdm_arr_2
  --where
  --    mcid_arr = 'cccfcefe-1eaa-db11-8952-0018717a8c82'
  ---Now take the mcids which are only in ufdm arr for tat
,
  tat_3 as (
    select t2.mcid_tat,
      t2.product_family_tat,
      t2.snapshot_date_tat,
      t2."MAX Snapshot Date of TAT",
      t2."Sum by Product Family and Date - TAT"
    from tat_2 t2
    where t2.mcid_tat in (
        select distinct mcid_arr
        from ufdm_arr_2
      ) --Get Rid of Any PF that has 0 data
      and t2."Sum by Product Family and Date - TAT" > 0
  ) --select
  --    distinct mcid_tat
  --from
  --    tat_2
  --where
  --    mcid_tat = 'cccfcefe-1eaa-db11-8952-0018717a8c82'
,
  combined_table_1 as (
    select coalesce(t2.mcid_tat, u1.mcid_arr) as "Combined MCID",
      coalesce(t2.product_family_tat, u1.product_family_arr) as "Combined Product Family",
      t2.mcid_tat,
      t2.product_family_tat,
      t2.snapshot_date_tat,
      t2."MAX Snapshot Date of TAT",
      t2."Sum by Product Family and Date - TAT",
      u1.mcid_arr,
      u1.product_family_arr,
      u1.sku,
      u1.snapshot_date_arr,
      u1."Sum by Product Family,SKU and Date - UFDM ARR",
      u1."Start Date in UFDM ARR"
    from tat_3 t2
      full join ufdm_arr_2 u1 on t2.mcid_tat = u1.mcid_arr
      and t2.product_family_tat = u1.product_family_arr
  ) --   select
  --      *
  --   from
  --      combined_table_1
  --   where mcid_arr  = '0033532c-9f42-dd11-93be-0018717a8c82'
,
  combined_table_2 as (
    select "Combined MCID",
      "Combined Product Family",
      case
        when "Combined Product Family" = product_family_tat
        and "Combined Product Family" = product_family_arr then 'Present in Both TAT and UFDM ARR'
        when "Combined Product Family" = product_family_tat
        and product_family_arr is null then 'Present in TAT only - Loss of PF in Bridge'
        when product_family_tat is null
        and "Combined Product Family" = product_family_arr then 'Present in UFDM ARR only - New PF in Bridge'
      end as "Presence of PF in 2 Tables",
      coalesce(
        "Sum by Product Family,SKU and Date - UFDM ARR",
        0
      ) - coalesce("Sum by Product Family and Date - TAT", 0) as "Difference between UFDM and TAT -- ARR",
      mcid_tat,
      product_family_tat,
      snapshot_date_tat,
      "MAX Snapshot Date of TAT",
      "Sum by Product Family and Date - TAT",
      mcid_arr,
      product_family_arr,
      sku,
      snapshot_date_arr,
      "Sum by Product Family,SKU and Date - UFDM ARR",
      "Start Date in UFDM ARR"
    from combined_table_1
  ),
  combined_table_3 as (
    select "Combined MCID",
      "Combined Product Family",
      coalesce(
        snapshot_date_arr,
        (snapshot_date_tat + interval '1 Month')
      )::DATE as "Combined Date -- ARR",
      "Difference between UFDM and TAT -- ARR",
      "Presence of PF in 2 Tables",
      case
        when "Presence of PF in 2 Tables" = 'Present in Both TAT and UFDM ARR'
        and "Sum by Product Family and Date - TAT" > 1
        and "Sum by Product Family,SKU and Date - UFDM ARR" > 1
        and "Difference between UFDM and TAT -- ARR" >= 1 then 'Upsell'
        when "Presence of PF in 2 Tables" = 'Present in Both TAT and UFDM ARR'
        and "Sum by Product Family and Date - TAT" > 1
        and "Sum by Product Family,SKU and Date - UFDM ARR" > 1
        and "Difference between UFDM and TAT -- ARR" <= -1 then 'Partial Churn'
        when "Presence of PF in 2 Tables" = 'Present in Both TAT and UFDM ARR'
        and ABS("Difference between UFDM and TAT -- ARR") < 1 then 'Flat'
        when --Churn case 1
        (
          "Presence of PF in 2 Tables" = 'Present in TAT only - Loss of PF in Bridge'
          and "Difference between UFDM and TAT -- ARR" <= -1
        ) then 'Churn' --Churn case 2
        when "Presence of PF in 2 Tables" = 'Present in Both TAT and UFDM ARR'
        and "Sum by Product Family and Date - TAT" > 1
        and "Sum by Product Family,SKU and Date - UFDM ARR" < 1 then 'Churn' --New case 1
        when (
          "Presence of PF in 2 Tables" = 'Present in UFDM ARR only - New PF in Bridge'
          and "Difference between UFDM and TAT -- ARR" >= 1
        ) then 'New' --New case 2
        when "Presence of PF in 2 Tables" = 'Present in Both TAT and UFDM ARR'
        and "Sum by Product Family and Date - TAT" < 1
        and "Sum by Product Family,SKU and Date - UFDM ARR" > 1 then 'New'
      end as "Product Bridge",
      mcid_tat,
      product_family_tat,
      snapshot_date_tat,
      "MAX Snapshot Date of TAT",
      "Sum by Product Family and Date - TAT",
      mcid_arr,
      product_family_arr,
      sku,
      snapshot_date_arr,
      "Sum by Product Family,SKU and Date - UFDM ARR",
      "Start Date in UFDM ARR"
    from combined_table_2
  ),
  combined_table_4 as (
    select "Combined MCID",
      "Combined Product Family",
      "Combined Date -- ARR",
      "Presence of PF in 2 Tables",
      "Difference between UFDM and TAT -- ARR",
      "Product Bridge",
      --Flag only flat customers
      case
        when sum(
          case
            when "Product Bridge" = 'Flat' then 0
            else 1
          end
        ) over(partition by "Combined MCID") = 0 then 1
        else 0
      end as "Flat Customers Only (1 if yes)",
      --Flag only Upsell/Partial Churn Customers
      case
        when sum(
          case
            when "Product Bridge" in ('Upsell', 'Partial Churn') then 0
            else 1
          end
        ) over(partition by "Combined MCID") = 0 then 1
        else 0
      end as "Upsell/Partial Customers Only (1 if yes)",
      --Flag only New/Churn Customers
      case
        when sum(
          case
            when "Product Bridge" in ('New', 'Churn') then 0
            else 1
          end
        ) over(partition by "Combined MCID") = 0 then 1
        else 0
      end as "New/Churn Customers Only (1 if yes)",
      mcid_tat,
      product_family_tat,
      snapshot_date_tat,
      "MAX Snapshot Date of TAT",
      "Sum by Product Family and Date - TAT",
      mcid_arr,
      product_family_arr,
      sku,
      snapshot_date_arr,
      "Sum by Product Family,SKU and Date - UFDM ARR",
      "Start Date in UFDM ARR"
    from combined_table_3
  ) --Find out customers whose total ARR does not change but the ratio changes -- this can be taken care of by dragging the ratio
,
  combined_table_5 as (
    select "Combined MCID",
      "Combined Product Family",
      "Combined Date -- ARR",
      "Presence of PF in 2 Tables",
      "Difference between UFDM and TAT -- ARR",
      "Product Bridge",
      --Flag only flat customers
      "Flat Customers Only (1 if yes)",
      --Flag only Upsell/Partial Churn Customers
      "Upsell/Partial Customers Only (1 if yes)",
      --Flag only New/Churn Customers
      "New/Churn Customers Only (1 if yes)",
      --Flag customers who have -- ARR between the 2 tables is the same, PF Makeup is the same but Ratio is Different
      case
        when --same ARR
        ABS(
          coalesce(
            sum("Sum by Product Family and Date - TAT") over(partition by "Combined MCID"),
            0
          ) - coalesce(
            sum("Sum by Product Family,SKU and Date - UFDM ARR") over(partition by "Combined MCID"),
            0
          )
        ) < 5 -- and --not a flat customer
        -- "Flat Customers Only (1 if yes)" != 1
        and --product families present in both
        sum(
          case
            when "Presence of PF in 2 Tables" in ('Present in Both TAT and UFDM ARR') then 0
            else 1
          end
        ) over(partition by "Combined MCID") = 0 then 1
        else 0
      end as "Same ARR & PF But Different Ratio (1 if yes)",
      --Flag customers who have -- ARR between the 2 tables is the same, PF Makeup is the Different & Ratio is Different
      case
        when --same ARR
        ABS(
          coalesce(
            sum("Sum by Product Family and Date - TAT") over(partition by "Combined MCID"),
            0
          ) - coalesce(
            sum("Sum by Product Family,SKU and Date - UFDM ARR") over(partition by "Combined MCID"),
            0
          )
        ) < 5 -- and --not a flat customer
        -- "Flat Customers Only (1 if yes)" != 1
        and --Different product families in both
        sum(
          case
            when "Presence of PF in 2 Tables" in ('Present in Both TAT and UFDM ARR') then 0
            else 1
          end
        ) over(partition by "Combined MCID") != 0 then 1
        else 0
      end as "Same ARR & But Different PF and Ratio (1 if yes)",
      --Identify Customers who have Different ARR and Different Makeup
      case
        when --Different ARR
        ABS(
          coalesce(
            sum("Sum by Product Family and Date - TAT") over(partition by "Combined MCID"),
            0
          ) - coalesce(
            sum("Sum by Product Family,SKU and Date - UFDM ARR") over(partition by "Combined MCID"),
            0
          )
        ) > 5 -- and --not a flat customer
        -- "Flat Customers Only (1 if yes)" != 1
        and --Different product families in both
        sum(
          case
            when "Presence of PF in 2 Tables" in ('Present in Both TAT and UFDM ARR') then 0
            else 1
          end
        ) over(partition by "Combined MCID") != 0 then 1
        else 0
      end as "Different ARR & PF in Both Tables(1 if yes)",
      --Identify Customers who have Different ARR and Same Makeup
      case
        when --different ARR
        ABS(
          coalesce(
            sum("Sum by Product Family and Date - TAT") over(partition by "Combined MCID"),
            0
          ) - coalesce(
            sum("Sum by Product Family,SKU and Date - UFDM ARR") over(partition by "Combined MCID"),
            0
          )
        ) > 5 -- and --not a flat customer
        -- "Flat Customers Only (1 if yes)" != 1
        and --Same product families in both
        sum(
          case
            when "Presence of PF in 2 Tables" in ('Present in Both TAT and UFDM ARR') then 0
            else 1
          end
        ) over(partition by "Combined MCID") = 0 then 1
        else 0
      end as "Different ARR But Same PF in Both Tables(1 if yes)",
      mcid_tat,
      product_family_tat,
      snapshot_date_tat,
      "MAX Snapshot Date of TAT",
      "Sum by Product Family and Date - TAT",
      mcid_arr,
      product_family_arr,
      sku,
      snapshot_date_arr,
      "Sum by Product Family,SKU and Date - UFDM ARR",
      "Start Date in UFDM ARR"
    from combined_table_4 --where
      --    "Combined MCID" = '50661331-d24e-e811-813c-70106fa6f451'
  ) --Find out if the churn was true churn or churn due to different source. Join it to previous month's data
  --Make a sub-table of UFDM ARR with previous month's data
,
  ufdm_arr_2b as (
    select ua1.mcid_arr as mcid_prev_month,
      ua1.product_family_arr as pf_arr_prev_month,
      ua1.sku AS sku_prev_month,
      ua1.snapshot_date_arr as snapshot_date_arr_pmonth,
      ua1."Sum by Product Family,SKU and Date - UFDM ARR" as "Sum by Product Family,SKU and Date - UFDM ARR Prev. Month"
    from ufdm_arr_1 ua1
      inner join tat_dates td on ua1.mcid_arr = td.mcid
      and --use same snapshot date
      (ua1.snapshot_date_arr) = td."MAX Snapshot Date of TAT"
  ) --Join this to the combined table above on TAT mcid, date and product family
,
  combined_table_6 as (
    select ct5."Combined MCID" as "Combined MCID after Transition",
      coalesce(ct5.mcid_tat, u2b.mcid_prev_month) as "Combined MCID Before Transition",
      ct5."Combined Date -- ARR" as "Combined Date After Transition",
      coalesce(
        ct5.snapshot_date_tat,
        u2b.snapshot_date_arr_pmonth
      ) as "Combine Date Before Transition",
      ct5."Presence of PF in 2 Tables",
      ct5."Difference between UFDM and TAT -- ARR",
      ct5."Product Bridge",
      --Flag only flat customers
      ct5."Flat Customers Only (1 if yes)",
      --Flag only Upsell/Partial Churn Customers
      ct5."Upsell/Partial Customers Only (1 if yes)",
      --Flag only New/Churn Customers
      ct5."New/Churn Customers Only (1 if yes)",
      --Flag Customers who have Same ARR and PF but Ratio is Different
      ct5."Same ARR & PF But Different Ratio (1 if yes)",
      --Flag Customers who have Same ARR but different PF and Ratio
      "Same ARR & But Different PF and Ratio (1 if yes)",
      --Flag Customers who have Different ARR and PF in Both Tables
      ct5."Different ARR & PF in Both Tables(1 if yes)",
      --Flag Customers who have Different ARR but Same PF Makeup in both Tables
      ct5."Different ARR But Same PF in Both Tables(1 if yes)",
      ct5.mcid_tat,
      ct5.product_family_tat,
      ct5.snapshot_date_tat,
      ct5."MAX Snapshot Date of TAT",
      ct5."Sum by Product Family and Date - TAT",
      ct5.mcid_arr,
      ct5.product_family_arr,
      ct5.sku,
      ct5.snapshot_date_arr,
      ct5."Sum by Product Family,SKU and Date - UFDM ARR",
      ct5."Start Date in UFDM ARR",
      u2b.mcid_prev_month,
      u2b.pf_arr_prev_month,
      u2b.sku_prev_month,
      u2b.snapshot_date_arr_pmonth,
      u2b."Sum by Product Family,SKU and Date - UFDM ARR Prev. Month"
    from combined_table_5 ct5
      full join ufdm_arr_2b u2b on ct5.mcid_tat = u2b.mcid_prev_month
      and ct5.product_family_tat = u2b.pf_arr_prev_month
      and ct5.snapshot_date_tat = u2b.snapshot_date_arr_pmonth
      and ct5.sku = u2b.sku_prev_month
  ) --Start calculating true churn
  --   select * from combined_table_6 where mcid_arr= '0033532c-9f42-dd11-93be-0018717a8c82'
,
  combined_table_7 as (
    select "Combined MCID after Transition",
      "Combined MCID Before Transition",
      coalesce(
        "Combined MCID after Transition",
        "Combined MCID Before Transition"
      ) as "Combined MCID Before and After Transition",
      "Combined Date After Transition",
      "Combine Date Before Transition",
      "Presence of PF in 2 Tables",
      "Difference between UFDM and TAT -- ARR",
      ABS("Difference between UFDM and TAT -- ARR") as "Absolute Diff. between UFDM and TAT--ARR",
      "Product Bridge",
      --Flag only flat customers
      "Flat Customers Only (1 if yes)",
      --Flag only Upsell/Partial Churn Customers
      "Upsell/Partial Customers Only (1 if yes)",
      --Flag only New/Churn Customers
      "New/Churn Customers Only (1 if yes)",
      --Flag Customers who have Same ARR and PF but Ratio is Different
      "Same ARR & PF But Different Ratio (1 if yes)",
      --Flag Customers who have Same ARR but different PF and Ratio
      "Same ARR & But Different PF and Ratio (1 if yes)",
      --Flag Customers who have Different ARR and PF in Both Tables
      "Different ARR & PF in Both Tables(1 if yes)",
      --Flag Customers who have Different ARR but Same PF Makeup in both Tables
      "Different ARR But Same PF in Both Tables(1 if yes)",
      mcid_tat,
      product_family_tat,
      snapshot_date_tat,
      "MAX Snapshot Date of TAT",
      "Sum by Product Family and Date - TAT",
      mcid_arr,
      product_family_arr,
      sku,
      snapshot_date_arr,
      "Sum by Product Family,SKU and Date - UFDM ARR",
      "Start Date in UFDM ARR",
      mcid_prev_month,
      pf_arr_prev_month,
      sku_prev_month,
      snapshot_date_arr_pmonth,
      "Sum by Product Family,SKU and Date - UFDM ARR Prev. Month",
      --Identify True Bridge Customers
      --If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match
      case
        when sum (
          case
            when pf_arr_prev_month = product_family_tat --Same Product family in [re]
            and abs(
              "Sum by Product Family,SKU and Date - UFDM ARR Prev. Month" - "Sum by Product Family and Date - TAT"
            ) < 1 --same ARR
            then 0
            else 1
          end
        ) over(
          partition by "Combined MCID after Transition",
          "Combined Date After Transition"
        ) = 0 then 1
        else 0
      end as "No Diff between UFDM and TAT Prev. Month (1 if yes)"
    from combined_table_6
  ) --Join it to Campaign Welcome and Unbundling
,
  campaign AS (
    SELECT distinct mcid as mcid_camp,
      date_trunc('MONTH', snapshot_date) as snapshot_date_camp,
      '1' AS campaign_overages
    FROM sandbox.sst_temp
    WHERE record_source ilike '%ufdm_campaigns_dec2021%'
      AND overage_flag = 'Y'
    ORDER BY 1,
      2
  ),
  welcome AS (
    SELECT distinct mcid as mcid_welc,
      date_trunc('MONTH', snapshot_date) as snapshot_date_welc,
      '1' AS welcome_historicals
    FROM sandbox_pd.arr
    WHERE reference_number = 'Welcome Historicals'
    ORDER BY 1,
      2
  ),
  unbundling AS (
    SELECT distinct mcid as mcid_unbund,
      date_trunc('MONTH', snapshot_date) as snapshot_date_unbund,
      '1' AS unbundling
    FROM ufdm_blue.monthly_metrics
    WHERE modified_comments ILIKE '%unbundling%'
    ORDER BY 1,
      2
  ) ---Now left join all the flags to combined_table_7
,
  combined_table_8 as (
    select ct7."Combined MCID after Transition",
      ct7."Combined MCID Before Transition",
      ct7."Combined MCID Before and After Transition",
      ct7."Combined Date After Transition",
      ct7."Combine Date Before Transition",
      ct7."Presence of PF in 2 Tables",
      ct7."Difference between UFDM and TAT -- ARR",
      ct7."Absolute Diff. between UFDM and TAT--ARR",
      ct7."Product Bridge",
      --Flag only flat customers
      ct7."Flat Customers Only (1 if yes)",
      --Flag only Upsell/Partial Churn Customers
      ct7."Upsell/Partial Customers Only (1 if yes)",
      --Flag only New/Churn Customers
      ct7."New/Churn Customers Only (1 if yes)",
      --Flag Customers who have Same ARR and PF but Ratio is Different
      ct7."Same ARR & PF But Different Ratio (1 if yes)",
      --Flag Customers who have Same ARR but different PF and Ratio
      ct7."Same ARR & But Different PF and Ratio (1 if yes)",
      --Flag Customers who have Different ARR and PF in Both Tables
      ct7."Different ARR & PF in Both Tables(1 if yes)",
      --Flag Customers who have Different ARR but Same PF Makeup in both Tables
      ct7."Different ARR But Same PF in Both Tables(1 if yes)",
      ct7.mcid_tat,
      ct7.product_family_tat,
      ct7.snapshot_date_tat,
      ct7."MAX Snapshot Date of TAT",
      ct7."Sum by Product Family and Date - TAT",
      ct7.mcid_arr,
      ct7.product_family_arr,
      ct7.sku,
      ct7.snapshot_date_arr,
      ct7."Sum by Product Family,SKU and Date - UFDM ARR",
      ct7."Start Date in UFDM ARR",
      ct7.mcid_prev_month,
      ct7.pf_arr_prev_month,
      ct7.sku_prev_month,
      ct7.snapshot_date_arr_pmonth,
      ct7."Sum by Product Family,SKU and Date - UFDM ARR Prev. Month",
      --Identify True Bridge Customers
      --If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match
      ct7."No Diff between UFDM and TAT Prev. Month (1 if yes)",
      cap.campaign_overages as "Campaign Flag (1 if yes)",
      welc.welcome_historicals as "Wecome Flag (1 if yes)",
      ubund.unbundling as "Unbundling (1 if yes)"
    from combined_table_7 ct7
      left join campaign cap on ct7.mcid_arr = cap.mcid_camp
      and ct7.snapshot_date_arr = cap.snapshot_date_camp
      left join welcome welc on ct7.mcid_arr = welc.mcid_welc
      and ct7.snapshot_date_arr = welc.snapshot_date_welc
      left join unbundling ubund on ct7.mcid_arr = ubund.mcid_unbund
      and ct7.snapshot_date_arr = ubund.snapshot_date_unbund
  ) --Take customers who are not flat or true movement customers
  --End of Initial Bucket Analysis
,
  table_invest_1 as (
    select ct8."Combined MCID after Transition",
      ct8."Combined MCID Before Transition",
      ct8."Combined MCID Before and After Transition",
      ct8."Combined Date After Transition",
      ct8."Combine Date Before Transition",
      ct8."Presence of PF in 2 Tables",
      ct8."Difference between UFDM and TAT -- ARR",
      ct8."Absolute Diff. between UFDM and TAT--ARR",
      ct8."Product Bridge",
      --Flag only flat customers
      ct8."Flat Customers Only (1 if yes)",
      --Flag only Upsell/Partial Churn Customers
      ct8."Upsell/Partial Customers Only (1 if yes)",
      --Flag only New/Churn Customers
      ct8."New/Churn Customers Only (1 if yes)",
      --Flag Customers who have Same ARR and PF but Ratio is Different
      ct8."Same ARR & PF But Different Ratio (1 if yes)",
      --Flag Customers who have Same ARR but different PF and Ratio
      ct8."Same ARR & But Different PF and Ratio (1 if yes)",
      --Flag Customers who have Different ARR and PF in Both Tables
      ct8."Different ARR & PF in Both Tables(1 if yes)",
      --Flag Customers who have Different ARR but Same PF Makeup in both Tables
      ct8."Different ARR But Same PF in Both Tables(1 if yes)",
      ct8.mcid_tat,
      ct8.product_family_tat,
      ct8.snapshot_date_tat,
      ct8."MAX Snapshot Date of TAT",
      ct8."Sum by Product Family and Date - TAT",
      ct8.mcid_arr,
      ct8.product_family_arr,
      ct8.sku,
      ct8.snapshot_date_arr,
      ct8."Sum by Product Family,SKU and Date - UFDM ARR",
      ct8."Start Date in UFDM ARR",
      ct8.mcid_prev_month,
      ct8.pf_arr_prev_month,
      ct8.sku_prev_month,
      ct8.snapshot_date_arr_pmonth,
      ct8."Sum by Product Family,SKU and Date - UFDM ARR Prev. Month",
      --Identify True Bridge Customers
      --If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match
      ct8."No Diff between UFDM and TAT Prev. Month (1 if yes)",
      ct8."Campaign Flag (1 if yes)",
      ct8."Wecome Flag (1 if yes)",
      ct8."Unbundling (1 if yes)"
    from combined_table_8 ct8 -- where -- "Flat Customers Only (1 if yes)" = 0 and 
      --   "No Diff between UFDM and TAT Prev. Month (1 if yes)" = 0
  ) --select
  --    count(distinct "Combined MCID Before and After Transition")
  --from
  --    table_invest_1
  --Take TAT data for those mcids only
,
  table_invest_2 as (
    select distinct ti1.mcid_tat,
      ti1.snapshot_date_tat as "Max Snapshot Date of TAT"
    from table_invest_1 ti1
  ) --Take data from TAT for only these customers
,
  table_invest_3 as (
    select ti2.mcid_tat as "MCID TAT",
      ti2."Max Snapshot Date of TAT",
      t1.mcid_tat,
      t1.product_family_tat,
      t1.snapshot_date_tat,
      t1."Sum by Product Family and Date - TAT",
      t1."Sum by MICD & Date - TAT"
    from tat_1 t1
      inner join table_invest_2 ti2 on ti2.mcid_tat = t1.mcid_tat
  ) --select
  --    count(distinct "MCID TAT")
  --from
  --    table_invest_3
  --Now only keep data in tat where snapshot_date <= Max Snapshot Date of TAT
,
  table_invest_4 as (
    select ti3."MCID TAT",
      ti3."Max Snapshot Date of TAT",
      ti3.product_family_tat,
      ti3.snapshot_date_tat,
      min(ti3.snapshot_date_tat) over(partition by ti3."MCID TAT") as "Start Date of TAT",
      ti3."Sum by Product Family and Date - TAT",
      ti3."Sum by MICD & Date - TAT"
    from table_invest_3 ti3
    where ti3.snapshot_date_tat <= ti3."Max Snapshot Date of TAT"
      and ----Only take non null product families where ARR is greater than zero
      ti3."Sum by Product Family and Date - TAT" > 0
  ) --select
  --    *
  --from
  --    table_invest_4
  --where
  --    "MCID TAT" = '001ea07d-2184-df11-8804-0018717a8c82'
  --The count will drop as we get rid of all customers who do not have values greater than zero
  --select
  --    count(distinct "MCID TAT")
  --from
  --    table_invest_4
  --Now join current month's data to previous months -- using MCID and PF. Do a Full Join
,
  table_invest_5 as (
    select ti4."MCID TAT",
      ti4a."MCID TAT" as "Prev. Month MCID: TAT",
      coalesce(ti4."MCID TAT", ti4a."MCID TAT") as "Combined MCID: TAT",
      coalesce(
        ti4.snapshot_date_tat,
        ti4a.snapshot_date_tat + interval '1 month'
      ) as "Combined Date TAT",
      ti4."Max Snapshot Date of TAT",
      ti4."Start Date of TAT",
      ti4.product_family_tat,
      ti4a.product_family_tat as "Prev. Month PF: TAT",
      ti4.snapshot_date_tat,
      ti4a.snapshot_date_tat as "Prev Month Date: TAT",
      ti4."Sum by Product Family and Date - TAT",
      ti4."Sum by MICD & Date - TAT"
    from table_invest_4 ti4
      full join table_invest_4 ti4a on ti4."MCID TAT" = ti4a."MCID TAT"
      and ti4.snapshot_date_tat = ti4a.snapshot_date_tat + interval '1 month'
      and ti4.product_family_tat = ti4a.product_family_tat --stop taking data from the previous month where prev. month date in TAT < Max Date in TAT
      --where
      --    ti4a.snapshot_date_tat < ti4."Max Snapshot Date of TAT"
  ),
  table_invest_5a as (
    select ti5."Combined MCID: TAT",
      ti5."Combined Date TAT",
      ti5."MCID TAT",
      ti5."Prev. Month MCID: TAT",
      max(ti5."Max Snapshot Date of TAT") over(partition by ti5."Combined MCID: TAT") as "Max Snapshot Date of TAT",
      min(ti5."Start Date of TAT") over(partition by ti5."Combined MCID: TAT") as "Start Date of TAT",
      ti5.product_family_tat,
      ti5."Prev. Month PF: TAT",
      ti5.snapshot_date_tat,
      ti5."Prev Month Date: TAT",
      ti5."Sum by Product Family and Date - TAT",
      ti5."Sum by MICD & Date - TAT"
    from table_invest_5 ti5
  ),
  table_invest_5b as (
    select ti5."Combined MCID: TAT",
      ti5."Combined Date TAT",
      ti5."MCID TAT",
      ti5."Prev. Month MCID: TAT",
      ti5."Max Snapshot Date of TAT",
      ti5."Start Date of TAT",
      ti5.product_family_tat,
      ti5."Prev. Month PF: TAT",
      ti5.snapshot_date_tat,
      ti5."Prev Month Date: TAT",
      ti5."Sum by Product Family and Date - TAT",
      ti5."Sum by MICD & Date - TAT"
    from table_invest_5a ti5
    where ti5."Combined Date TAT" <= ti5."Max Snapshot Date of TAT"
  ) --select
  --    *
  --from
  --    table_invest_5b
  --where
  --    "Combined MCID: TAT" = '025c17f2-7b31-e411-9f63-0050568d2da8'
,
  table_invest_6 as (
    select ti5."Combined MCID: TAT",
      ti5."Combined Date TAT",
      ti5."MCID TAT",
      ti5."Prev. Month MCID: TAT",
      ti5."Max Snapshot Date of TAT",
      ti5."Start Date of TAT",
      ti5.product_family_tat,
      ti5."Prev. Month PF: TAT",
      ti5.snapshot_date_tat,
      ti5."Prev Month Date: TAT",
      ti5."Sum by Product Family and Date - TAT",
      ti5."Sum by MICD & Date - TAT",
      case
        when ti5.snapshot_date_tat = ti5."Start Date of TAT" then 0
        when ti5.snapshot_date_tat > ti5."Start Date of TAT"
        and ti5.product_family_tat is not null
        and ti5."Prev. Month PF: TAT" is not null then 0
        else 1
      end as "Prev Month and Current Month PF Matches (1 if no)"
    from table_invest_5b ti5
  ) --select
  --    *
  --from
  --    table_invest_6
  --where
  --    "Combined MCID: TAT" = '150b3417-2400-bed2-9fbb-0468b547aad4'
,
  table_invest_7 as (
    select ti6."Combined MCID: TAT",
      ti6."Combined Date TAT",
      ti6."MCID TAT",
      ti6."Prev. Month MCID: TAT",
      ti6."Max Snapshot Date of TAT",
      ti6."Start Date of TAT",
      ti6.product_family_tat,
      ti6."Prev. Month PF: TAT",
      ti6.snapshot_date_tat,
      ti6."Prev Month Date: TAT",
      ti6."Sum by Product Family and Date - TAT",
      ti6."Sum by MICD & Date - TAT",
      ti6."Prev Month and Current Month PF Matches (1 if no)",
      max(ti6."Combined Date TAT") filter(
        where ti6."Prev Month and Current Month PF Matches (1 if no)" = 1
      ) over(partition by ti6."Combined MCID: TAT") as "Last Date of History Change TAT",
      case
        when sum(
          ti6."Prev Month and Current Month PF Matches (1 if no)"
        ) over(partition by ti6."Combined MCID: TAT") > 0 then 1
        else 0
      end as "Mismatch between PF in TAT History (1 if yes)"
    from table_invest_6 ti6
  ) --select
  --    *
  --from
  --    table_invest_7
  --where
  --    "Combined MCID: TAT" = '046135a0-ebe5-e411-9afb-0050568d2da8'
  --End of Adding Flags for TAT History
,
  table_invest_7a as (
    select distinct "Combined MCID: TAT",
      "Mismatch between PF in TAT History (1 if yes)",
      "Last Date of History Change TAT",
      "Start Date of TAT"
    from table_invest_7
  ) --select
  --    *
  --from
  --    table_invest_7a
  --where
  --    "Combined MCID: TAT" = '046135a0-ebe5-e411-9afb-0050568d2da8'
  --Add them back to the original analysis
,
  combined_table_9 as (
    select ct8."Combined MCID after Transition",
      ct8."Combined MCID Before Transition",
      ct8."Combined MCID Before and After Transition",
      ct8."Combined Date After Transition",
      ct8."Combine Date Before Transition",
      ct8."Presence of PF in 2 Tables",
      ct8."Difference between UFDM and TAT -- ARR",
      ct8."Absolute Diff. between UFDM and TAT--ARR",
      ct8."Product Bridge",
      --Flag only flat customers
      ct8."Flat Customers Only (1 if yes)",
      --Flag only Upsell/Partial Churn Customers
      ct8."Upsell/Partial Customers Only (1 if yes)",
      --Flag only New/Churn Customers
      ct8."New/Churn Customers Only (1 if yes)",
      --Flag Customers who have Same ARR and PF but Ratio is Different
      ct8."Same ARR & PF But Different Ratio (1 if yes)",
      --Flag Customers who have Same ARR but different PF and Ratio
      ct8."Same ARR & But Different PF and Ratio (1 if yes)",
      --Flag Customers who have Different ARR and PF in Both Tables
      ct8."Different ARR & PF in Both Tables(1 if yes)",
      --Flag Customers who have Different ARR but Same PF Makeup in both Tables
      ct8."Different ARR But Same PF in Both Tables(1 if yes)",
      ct8.mcid_tat,
      ct8.product_family_tat,
      ct8.snapshot_date_tat,
      ct8."MAX Snapshot Date of TAT",
      ct8."Sum by Product Family and Date - TAT",
      ct8.mcid_arr,
      ct8.product_family_arr,
      ct8.sku,
      ct8.snapshot_date_arr,
      ct8."Sum by Product Family,SKU and Date - UFDM ARR",
      ct8."Start Date in UFDM ARR",
      ct8.mcid_prev_month,
      ct8.pf_arr_prev_month,
      ct8.sku_prev_month,
      ct8.snapshot_date_arr_pmonth,
      ct8."Sum by Product Family,SKU and Date - UFDM ARR Prev. Month",
      --Identify True Bridge Customers
      --If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match
      ct8."No Diff between UFDM and TAT Prev. Month (1 if yes)",
      ct8."Campaign Flag (1 if yes)",
      ct8."Wecome Flag (1 if yes)",
      ct8."Unbundling (1 if yes)",
      --Add Flags if it has the same history in TAT or it changes. Also add flags that
      ti7."Combined MCID: TAT",
      ti7."Mismatch between PF in TAT History (1 if yes)",
      ti7."Last Date of History Change TAT",
      ti7."Start Date of TAT"
    from combined_table_8 ct8
      left join table_invest_7a ti7 on ct8.mcid_tat = ti7."Combined MCID: TAT"
  ) --   select
  --      *
  --   from
  --      combined_table_9
  --   where
  --      "Combined MCID Before and After Transition" = '0033532c-9f42-dd11-93be-0018717a8c82'
  --Coalesnce Mismatch between PF in TAT History to have either 1 (if there is mismatch) or 0 (no mistmatch )
  --Also start using dense_rank for each of the product families in UFDM ARR
  --Also coalesce the max snapshot date, the start date and last date of change over all rows for a customer
,
  combined_table_10 as (
    select ct9."Combined MCID after Transition",
      ct9."Combined MCID Before Transition",
      ct9."Combined MCID Before and After Transition",
      ct9."Combined Date After Transition",
      ct9."Combine Date Before Transition",
      ct9."Presence of PF in 2 Tables",
      ct9."Difference between UFDM and TAT -- ARR",
      ct9."Absolute Diff. between UFDM and TAT--ARR",
      ct9."Product Bridge",
      --Flag only flat customers
      ct9."Flat Customers Only (1 if yes)",
      --Flag only Upsell/Partial Churn Customers
      ct9."Upsell/Partial Customers Only (1 if yes)",
      --Flag only New/Churn Customers
      ct9."New/Churn Customers Only (1 if yes)",
      --Flag Customers who have Same ARR and PF but Ratio is Different
      ct9."Same ARR & PF But Different Ratio (1 if yes)",
      --Flag Customers who have Same ARR but different PF and Ratio
      ct9."Same ARR & But Different PF and Ratio (1 if yes)",
      --Flag Customers who have Different ARR and PF in Both Tables
      ct9."Different ARR & PF in Both Tables(1 if yes)",
      --Flag Customers who have Different ARR but Same PF Makeup in both Tables
      ct9."Different ARR But Same PF in Both Tables(1 if yes)",
      ct9.mcid_tat,
      ct9.product_family_tat,
      ct9.snapshot_date_tat,
      case
        when ct9.product_family_tat is null then 0
        else dense_rank() over(
          partition by ct9.mcid_tat,
          ct9.snapshot_date_tat
          order by ct9.product_family_tat
        )
      end as "Dense Rank of PF TAT",
      max(ct9."MAX Snapshot Date of TAT") over(
        partition by ct9."Combined MCID Before and After Transition"
      ) as "MAX Snapshot Date of TAT",
      ct9."Sum by Product Family and Date - TAT",
      ct9.mcid_arr,
      ct9.product_family_arr,
      ct9.sku,
      ct9.snapshot_date_arr,
      case
        when ct9.product_family_arr is null then 0
        else dense_rank() over(
          partition by ct9.mcid_arr,
          ct9.snapshot_date_arr
          order by ct9.product_family_arr
        )
      end as "Dense Rank of PF UFDM ARR",
      ct9."Sum by Product Family,SKU and Date - UFDM ARR",
      ct9."Start Date in UFDM ARR",
      ct9.mcid_prev_month,
      ct9.pf_arr_prev_month,
      ct9.sku_prev_month,
      ct9.snapshot_date_arr_pmonth,
      ct9."Sum by Product Family,SKU and Date - UFDM ARR Prev. Month",
      --Identify True Bridge Customers
      --If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match
      ct9."No Diff between UFDM and TAT Prev. Month (1 if yes)",
      ct9."Campaign Flag (1 if yes)",
      ct9."Wecome Flag (1 if yes)",
      ct9."Unbundling (1 if yes)",
      --Take the max of history in TAT
      max(
        ct9."Mismatch between PF in TAT History (1 if yes)"
      ) over(
        partition by ct9."Combined MCID Before and After Transition"
      ) as "Mismatch between PF in TAT History (1 if yes)",
      max(ct9."Last Date of History Change TAT") over(
        partition by ct9."Combined MCID Before and After Transition"
      ) as "Last Date of History Change TAT",
      max(ct9."Start Date of TAT") over(
        partition by ct9."Combined MCID Before and After Transition"
      ) as "Start Date of TAT"
    from combined_table_9 ct9
  ) --select
  --    *
  --from
  --    combined_table_10
  --where
  --    "Combined MCID Before and After Transition" = '046135a0-ebe5-e411-9afb-0050568d2da8'
  --Now take the max of each dense rank to find the number of product families in TAT and UFDM ARR during Transition
  --Also take max of last date of change and start date of tat -- to fill up null rows where we have no TAT data
,
  combined_table_11 as (
    select ct10."Combined MCID after Transition",
      ct10."Combined MCID Before Transition",
      ct10."Combined MCID Before and After Transition",
      ct10."Combined Date After Transition",
      ct10."Combine Date Before Transition",
      ct10."Presence of PF in 2 Tables",
      ct10."Difference between UFDM and TAT -- ARR",
      ct10."Absolute Diff. between UFDM and TAT--ARR",
      ct10."Product Bridge",
      --Flag only flat customers
      ct10."Flat Customers Only (1 if yes)",
      --Flag only Upsell/Partial Churn Customers
      ct10."Upsell/Partial Customers Only (1 if yes)",
      --Flag only New/Churn Customers
      ct10."New/Churn Customers Only (1 if yes)",
      --Flag Customers who have Same ARR and PF but Ratio is Different
      ct10."Same ARR & PF But Different Ratio (1 if yes)",
      --Flag Customers who have Same ARR but different PF and Ratio
      ct10."Same ARR & But Different PF and Ratio (1 if yes)",
      --Flag Customers who have Different ARR and PF in Both Tables
      ct10."Different ARR & PF in Both Tables(1 if yes)",
      --Flag Customers who have Different ARR but Same PF Makeup in both Tables
      ct10."Different ARR But Same PF in Both Tables(1 if yes)",
      ct10.mcid_tat,
      ct10.product_family_tat,
      ct10.snapshot_date_tat,
      max(ct10."Dense Rank of PF TAT") over(
        partition by ct10.mcid_tat,
        ct10.snapshot_date_tat
      ) as "No of PF TAT",
      ct10."MAX Snapshot Date of TAT",
      ct10."Sum by Product Family and Date - TAT",
      ct10.mcid_arr,
      ct10.product_family_arr,
      ct10.sku,
      ct10.snapshot_date_arr,
      ct10."Dense Rank of PF UFDM ARR",
      max(ct10."Dense Rank of PF UFDM ARR") over(
        partition by ct10.mcid_arr,
        ct10.snapshot_date_arr
      ) as "No of PF ARR",
      ct10."Sum by Product Family,SKU and Date - UFDM ARR",
      ct10."Start Date in UFDM ARR",
      ct10.mcid_prev_month,
      ct10.pf_arr_prev_month,
      ct10.sku_prev_month,
      ct10.snapshot_date_arr_pmonth,
      ct10."Sum by Product Family,SKU and Date - UFDM ARR Prev. Month",
      --Identify True Bridge Customers
      --If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match
      ct10."No Diff between UFDM and TAT Prev. Month (1 if yes)",
      ct10."Campaign Flag (1 if yes)",
      ct10."Wecome Flag (1 if yes)",
      ct10."Unbundling (1 if yes)",
      --Add Flags if it has the same history in TAT or it changes
      coalesce(
        ct10."Mismatch between PF in TAT History (1 if yes)",
        0
      ) as "Mismatch between PF in TAT History (1 if yes)",
      ct10."Last Date of History Change TAT",
      ct10."Start Date of TAT"
    from combined_table_10 ct10
    order by "Combined MCID Before and After Transition"
  ) --select
  --    *
  --from
  --    combined_table_11
  --where
  --    "Combined MCID Before and After Transition" = '001ea07d-2184-df11-8804-0018717a8c82'
  --Take Max of each product family to makesure it is the same throughout the MCID
,
  combined_table_12 as (
    select ct11."Combined MCID after Transition",
      ct11."Combined MCID Before Transition",
      ct11."Combined MCID Before and After Transition",
      ct11."Combined Date After Transition",
      ct11."Combine Date Before Transition",
      ct11."Presence of PF in 2 Tables",
      ct11."Difference between UFDM and TAT -- ARR",
      ct11."Absolute Diff. between UFDM and TAT--ARR",
      ct11."Product Bridge",
      --Flag only flat customers
      ct11."Flat Customers Only (1 if yes)",
      --Flag only Upsell/Partial Churn Customers
      ct11."Upsell/Partial Customers Only (1 if yes)",
      --Flag only New/Churn Customers
      ct11."New/Churn Customers Only (1 if yes)",
      --Flag Customers who have Same ARR and PF but Ratio is Different
      ct11."Same ARR & PF But Different Ratio (1 if yes)",
      --Flag Customers who have Same ARR but different PF and Ratio
      ct11."Same ARR & But Different PF and Ratio (1 if yes)",
      --Flag Customers who have Different ARR and PF in Both Tables
      ct11."Different ARR & PF in Both Tables(1 if yes)",
      --Flag Customers who have Different ARR but Same PF Makeup in both Tables
      ct11."Different ARR But Same PF in Both Tables(1 if yes)",
      ct11.mcid_tat,
      ct11.product_family_tat,
      ct11.snapshot_date_tat,
      max(ct11."No of PF TAT") over(
        partition by ct11."Combined MCID Before and After Transition"
      ) as "No of PF TAT",
      ct11."MAX Snapshot Date of TAT",
      ct11."Sum by Product Family and Date - TAT",
      ct11.mcid_arr,
      ct11.product_family_arr,
      ct11.sku,
      ct11.snapshot_date_arr,
      ct11."Dense Rank of PF UFDM ARR",
      max(ct11."No of PF ARR") over(
        partition by ct11."Combined MCID Before and After Transition"
      ) as "No of PF ARR",
      ct11."Sum by Product Family,SKU and Date - UFDM ARR",
      ct11."Start Date in UFDM ARR",
      ct11.mcid_prev_month,
      ct11.pf_arr_prev_month,
      ct11.sku_prev_month,
      ct11.snapshot_date_arr_pmonth,
      ct11."Sum by Product Family,SKU and Date - UFDM ARR Prev. Month",
      --Identify True Bridge Customers
      --If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match
      ct11."No Diff between UFDM and TAT Prev. Month (1 if yes)",
      ct11."Campaign Flag (1 if yes)",
      ct11."Wecome Flag (1 if yes)",
      ct11."Unbundling (1 if yes)",
      --Add Flags if it has the same history in TAT or it changes
      ct11."Mismatch between PF in TAT History (1 if yes)",
      ct11."Last Date of History Change TAT",
      ct11."Start Date of TAT"
    from combined_table_11 ct11
    order by "Combined MCID Before and After Transition"
  ) --Start Looking at product family transition. Also look at ratios allocated to UFDM PF During Point of Transition
,
  combined_table_13 as (
    select ct12."Combined MCID after Transition",
      ct12."Combined MCID Before Transition",
      ct12."Combined MCID Before and After Transition",
      ct12."Combined Date After Transition",
      coalesce(
        ct12."Combined Date After Transition",
        ct12."Combine Date Before Transition" + interval '1 month'
      ) as "Combined Date Overall",
      ct12."Combine Date Before Transition",
      ct12."Presence of PF in 2 Tables",
      ct12."Difference between UFDM and TAT -- ARR",
      ct12."Absolute Diff. between UFDM and TAT--ARR",
      ct12."Product Bridge",
      --Flag only flat customers
      ct12."Flat Customers Only (1 if yes)",
      --Flag only Upsell/Partial Churn Customers
      ct12."Upsell/Partial Customers Only (1 if yes)",
      --Flag only New/Churn Customers
      ct12."New/Churn Customers Only (1 if yes)",
      --Flag Customers who have Same ARR and PF but Ratio is Different
      ct12."Same ARR & PF But Different Ratio (1 if yes)",
      --Flag Customers who have Same ARR but different PF and Ratio
      ct12."Same ARR & But Different PF and Ratio (1 if yes)",
      --Flag Customers who have Different ARR and PF in Both Tables
      ct12."Different ARR & PF in Both Tables(1 if yes)",
      --Flag Customers who have Different ARR but Same PF Makeup in both Tables
      ct12."Different ARR But Same PF in Both Tables(1 if yes)",
      ct12.mcid_tat,
      ct12.product_family_tat,
      ct12.snapshot_date_tat,
      ct12."No of PF TAT",
      ct12."MAX Snapshot Date of TAT",
      ct12."Sum by Product Family and Date - TAT",
      ct12.mcid_arr,
      ct12.product_family_arr,
      ct12.sku,
      ct12.snapshot_date_arr,
      ct12."Dense Rank of PF UFDM ARR",
      ct12."No of PF ARR",
      ct12."Sum by Product Family,SKU and Date - UFDM ARR",
      ct12."Sum by Product Family,SKU and Date - UFDM ARR" / nullif(
        (
          sum(
            ct12."Sum by Product Family,SKU and Date - UFDM ARR"
          ) over(
            partition by ct12.mcid_arr,
            ct12.snapshot_date_arr
          )
        ),
        0
      ) as "Ratio of ARR Allocated to PF UFDM ARR",
      ct12."Start Date in UFDM ARR",
      ct12.mcid_prev_month,
      ct12.pf_arr_prev_month,
      ct12.sku_prev_month,
      ct12.snapshot_date_arr_pmonth,
      ct12."Sum by Product Family,SKU and Date - UFDM ARR Prev. Month",
      --Identify True Bridge Customers
      --If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match
      ct12."No Diff between UFDM and TAT Prev. Month (1 if yes)",
      ct12."Campaign Flag (1 if yes)",
      ct12."Wecome Flag (1 if yes)",
      ct12."Unbundling (1 if yes)",
      --Add Flags if it has the same history in TAT or it changes
      ct12."Mismatch between PF in TAT History (1 if yes)",
      ct12."Last Date of History Change TAT",
      ct12."Start Date of TAT",
      --Add Flags for Number of PF from UFDM ARR to TAT
      --1) Single PF -> Single PF
      case
        when ct12."No of PF TAT" = 1
        and ct12."No of PF ARR" = 1 then 1
        else 0
      end as "Single PF to Single PF (1 if yes)",
      --2) Multi PF --> Single PF
      case
        when ct12."No of PF TAT" > 1
        and ct12."No of PF ARR" = 1 then 1
        else 0
      end as "Multi PF to Single PF (1 if yes)",
      --3) Single to Multi PF
      case
        when ct12."No of PF TAT" = 1
        and ct12."No of PF ARR" > 1 then 1
        else 0
      end as "Single PF to Multi PF (1 if yes)",
      --4) Multi to Multi PF
      case
        when ct12."No of PF TAT" > 1
        and ct12."No of PF ARR" > 1 then 1
        else 0
      end as "Multi PF to Multi PF (1 if yes)"
    from combined_table_12 ct12 --Get rid of any customers who don't have any data in TAT
    where ct12."No of PF TAT" > 0
  ) --select
  --    *
  --from
  --    combined_table_13
  --where
  --    "Combined MCID Before and After Transition" = '046135a0-ebe5-e411-9afb-0050568d2da8'
  --Product Family Transition
,
  combined_table_14 as (
    select ct13."Combined MCID after Transition",
      ct13."Combined MCID Before Transition",
      ct13."Combined MCID Before and After Transition",
      ct13."Combined Date After Transition",
      ct13."Combined Date Overall",
      date_trunc('YEAR', ct13."Combined Date Overall") as "Year",
      ct13."Combine Date Before Transition",
      ct13."Presence of PF in 2 Tables",
      ct13."Difference between UFDM and TAT -- ARR",
      ct13."Absolute Diff. between UFDM and TAT--ARR",
      ct13."Product Bridge",
      --Flag only flat customers
      ct13."Flat Customers Only (1 if yes)",
      --Flag only Upsell/Partial Churn Customers
      ct13."Upsell/Partial Customers Only (1 if yes)",
      --Flag only New/Churn Customers
      ct13."New/Churn Customers Only (1 if yes)",
      --Flag Customers who have Same ARR and PF but Ratio is Different
      ct13."Same ARR & PF But Different Ratio (1 if yes)",
      --Flag Customers who have Same ARR but different PF and Ratio
      ct13."Same ARR & But Different PF and Ratio (1 if yes)",
      --Flag Customers who have Different ARR and PF in Both Tables
      ct13."Different ARR & PF in Both Tables(1 if yes)",
      --Flag Customers who have Different ARR but Same PF Makeup in both Tables
      ct13."Different ARR But Same PF in Both Tables(1 if yes)",
      ct13.mcid_tat,
      ct13.product_family_tat,
      ct13.snapshot_date_tat,
      ct13."No of PF TAT",
      ct13."MAX Snapshot Date of TAT",
      ct13."Sum by Product Family and Date - TAT",
      ct13.mcid_arr,
      ct13.product_family_arr,
      ct13.sku,
      ct13.snapshot_date_arr,
      ct13."Dense Rank of PF UFDM ARR",
      ct13."No of PF ARR",
      ct13."Sum by Product Family,SKU and Date - UFDM ARR",
      ct13."Ratio of ARR Allocated to PF UFDM ARR",
      ct13."Start Date in UFDM ARR",
      ct13.mcid_prev_month,
      ct13.pf_arr_prev_month,
      ct13.sku_prev_month,
      ct13.snapshot_date_arr_pmonth,
      ct13."Sum by Product Family,SKU and Date - UFDM ARR Prev. Month",
      --Identify True Bridge Customers
      --If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match
      ct13."No Diff between UFDM and TAT Prev. Month (1 if yes)",
      ct13."Campaign Flag (1 if yes)",
      ct13."Wecome Flag (1 if yes)",
      ct13."Unbundling (1 if yes)",
      --Add Flags if it has the same history in TAT or it changes
      ct13."Mismatch between PF in TAT History (1 if yes)",
      ct13."Last Date of History Change TAT",
      ct13."Start Date of TAT",
      --Add Flags for Number of PF from UFDM ARR to TAT
      --1) Single PF -> Single PF
      case
        when ct13."Single PF to Single PF (1 if yes)" = 1 then 'Single PF to Single PF'
        when ct13."Multi PF to Single PF (1 if yes)" = 1 then 'Multi PF to Single PF'
        when ct13."Single PF to Multi PF (1 if yes)" = 1 then 'Single PF to Multi PF'
        when ct13."Multi PF to Multi PF (1 if yes)" = 1 then 'Multi PF to Multi PF'
        else null
      end as "Product Family Transition"
    from combined_table_13 ct13
  ) --Full Export
,
  combined_table_15 as (
    select ct14."Combined MCID after Transition",
      ct14."Combined MCID Before Transition",
      ct14."Combined MCID Before and After Transition",
      ct14."Combined Date After Transition",
      ct14."Combined Date Overall",
      ct14."Year",
      ct14."Combine Date Before Transition",
      ct14."Presence of PF in 2 Tables",
      ct14."Difference between UFDM and TAT -- ARR",
      ct14."Absolute Diff. between UFDM and TAT--ARR",
      ct14."Product Bridge",
      --Flag only flat customers
      ct14."Flat Customers Only (1 if yes)",
      --Identify True Bridge Customers
      --If you compare PF Makeup and ARR by PF between UFDM and TAT in the month before the transition month, the PF & ARR must match
      ct14."No Diff between UFDM and TAT Prev. Month (1 if yes)",
      --Flag only Upsell/Partial Churn Customers
      ct14."Upsell/Partial Customers Only (1 if yes)",
      --Flag only New/Churn Customers
      ct14."New/Churn Customers Only (1 if yes)",
      --Flag Customers who have Same ARR and PF but Ratio is Different
      ct14."Same ARR & PF But Different Ratio (1 if yes)",
      --Flag Customers who have Same ARR but different PF and Ratio
      ct14."Same ARR & But Different PF and Ratio (1 if yes)",
      --Flag Customers who have Different ARR and PF in Both Tables
      ct14."Different ARR & PF in Both Tables(1 if yes)",
      --Flag Customers who have Different ARR but Same PF Makeup in both Tables
      ct14."Different ARR But Same PF in Both Tables(1 if yes)",
      ct14.mcid_tat,
      ct14.product_family_tat,
      ct14.snapshot_date_tat,
      ct14."No of PF TAT",
      ct14."MAX Snapshot Date of TAT",
      ct14."Sum by Product Family and Date - TAT",
      ct14.mcid_arr,
      ct14.product_family_arr,
      ct14.sku,
      ct14.snapshot_date_arr,
      ct14."Dense Rank of PF UFDM ARR",
      ct14."No of PF ARR",
      ct14."Sum by Product Family,SKU and Date - UFDM ARR",
      ct14."Ratio of ARR Allocated to PF UFDM ARR",
      ct14."Start Date in UFDM ARR",
      ct14.mcid_prev_month,
      ct14.pf_arr_prev_month,
      ct14.sku_prev_month,
      ct14.snapshot_date_arr_pmonth,
      ct14."Sum by Product Family,SKU and Date - UFDM ARR Prev. Month",
      ct14."Campaign Flag (1 if yes)",
      ct14."Wecome Flag (1 if yes)",
      ct14."Unbundling (1 if yes)",
      --Add Flags if it has the same history in TAT or it changes
      ct14."Mismatch between PF in TAT History (1 if yes)",
      ct14."Last Date of History Change TAT",
      ct14."Start Date of TAT",
      --Add Flags for Number of PF from UFDM ARR to TAT
      --1) Single PF -> Single PF
      ct14."Product Family Transition"
    from combined_table_14 ct14
  ) --select
  --    *
  --from
  --    combined_table_15
  --where
  --    "Combined MCID Before and After Transition" = '5e479b1a-2251-e811-813c-70106fa51d21'
  --select
  --    *
  --from
  --    combined_table_15
  --where
  --    "Combined MCID Before and After Transition" = '2178614d-6aef-fea3-1223-983ef99e185d'
  --
  ----Full Export
  --select
  --    count(distinct "Combined MCID Before and After Transition")
  --from
  --    combined_table_15
  --where
  --    "Flat Customers Only (1 if yes)" = 0
  --    and
  --    "No Diff between UFDM and TAT Prev. Month (1 if yes)" = 0
  --select
  --
  --, all_mcid as
  --(
  --select
  --    distinct "Combined MCID Before and After Transition"
  --from
  --    combined_table_15
  --where
  --    mcid_tat is null
  --)
  --
  --, mcid_tat as
  --(
  --select
  --    distinct mcid_tat
  --from
  --    combined_table_15
  --)
  --
  --select
  --    am."Combined MCID Before and After Transition"
  --from
  --    all_mcid am
  --left join
  --    mcid_tat mt
  --          on
  --                mt.mcid_tat = am."Combined MCID Before and After Transition"
  --where
  --    mt.mcid_tat is null
  --    "Combined MCID Before and After Transition" = '426200e8-ae90-e611-9afb-0050568d2da8'
  --Prepare ratios for Scenario 1 & 2
,
  sc1_sc2 as (
    select mcid_arr,
      "MAX Snapshot Date of TAT",
      product_family_arr,
      sku,
      "Ratio of ARR Allocated to PF UFDM ARR",
      "Last Date of History Change TAT",
      "Start Date of TAT",
      "Mismatch between PF in TAT History (1 if yes)",
      "Start Date of TAT" as "Date to Drag to Under Scenario 1",
      case
        when "Mismatch between PF in TAT History (1 if yes)" = 1 then "Last Date of History Change TAT"
        else "Start Date of TAT"
      end as "Date to Drag Under Scenario 2",
      "Product Family Transition"
    from combined_table_15
    where -- "Flat Customers Only (1 if yes)" = 0 and
      -- "No Diff between UFDM and TAT Prev. Month (1 if yes)" = 0 and 
      mcid_arr is not null
    order by "Combined MCID Before and After Transition"
  ) --New code for drag ratio
  select mcid_arr,
    "MAX Snapshot Date of TAT",
    product_family_arr,
    sku,
    "Ratio of ARR Allocated to PF UFDM ARR",
    "Date to Drag to Under Scenario 1",
    "Date to Drag Under Scenario 2",
    "Product Family Transition"
  from sc1_sc2
  where "Ratio of ARR Allocated to PF UFDM ARR" is not null
);
+ DROP TABLE IF EXISTS sandbox.drag_ratio_with_sku_c2;
CREATE TABLE sandbox.drag_ratio_with_sku_c2 AS (
  with cartersian_table as (
    select a.mcid,
      b.snapshot_date
    from (
        select distinct mcid
        from ufdm.tat_upload_data tud
        where is_deleted IS DISTINCT
        FROM 1
          and "Overage Y/N" is distinct
        from 'Y'
          and not (
            date_trunc('month', snapshot_date) = '2021-12-01'::DATE
            AND product_family ilike '%Campaign%'
          )
      ) as a,
      (
        select distinct snapshot_date
        from ufdm.tat_upload_data tud
        where is_deleted IS DISTINCT
        FROM 1
          and "Overage Y/N" is distinct
        from 'Y'
          and not (
            date_trunc('month', snapshot_date) = '2021-12-01'::DATE
            AND product_family ilike '%Campaign%'
          )
      ) as b
  ),
  initial_tat as (
    select distinct mcid,
      snapshot_date,
      sum(arr_usd_ccfx) over(partition by mcid, snapshot_date) as sum_mcid_date
    from ufdm.tat_upload_data tud
    where is_deleted IS DISTINCT
    FROM 1
      and "Overage Y/N" is distinct
    from 'Y'
      and not (
        date_trunc('month', snapshot_date) = '2021-12-01'::DATE
        AND product_family ilike '%Campaign%'
      )
  ) --Now do a left join to absorb the data into the cartersian table 
,
  tat_0 as (
    select ct.mcid,
      ct.snapshot_date,
      coalesce(it.sum_mcid_date, 0) as sum_mcid_date
    from cartersian_table ct
      left join initial_tat it on ct.mcid = it.mcid
      and ct.snapshot_date = it.snapshot_date
  ) --Put a Limit on Date and Start Summing ARR From Now Till End and ARR Over the Entire Period 
,
  tat_1 as (
    select mcid,
      snapshot_date,
      sum_mcid_date,
      sum(sum_mcid_date) over(
        partition by mcid
        order by snapshot_date rows between current row
          and unbounded following
      ) as "ARR from Now Till End",
      sum(sum_mcid_date) over(partition by mcid) as "ARR Over the Entire Time Period",
      min(snapshot_date) filter(
        where sum_mcid_date > 0
      ) over(partition by mcid) as "Start of Data in TAT"
    from tat_0 --Put a limit on date 
    where snapshot_date < '2022-01-01'
  ) --Get rid of customers who've never had ARR 
  --Take the first date when ARR goes to zero for the rest of time 
,
  tat_2 as (
    select mcid,
      snapshot_date,
      sum_mcid_date,
      "Start of Data in TAT",
      "ARR from Now Till End",
      "ARR Over the Entire Time Period",
      min(snapshot_date) filter(
        where "ARR from Now Till End" = 0
      ) over(partition by mcid) as "Date of Churn"
    from tat_1
    where "ARR Over the Entire Time Period" > 0
  ) --Now find only churn customers 
,
  churn_cust as (
    select distinct mcid,
      "Start of Data in TAT",
      "Date of Churn"
    from tat_2
    where "Date of Churn" is not null
  ) --Test to make sure they are not in the TAT customers being changed 
  --select 
  --    cc.mcid 
  --from 
  --    churn_cust cc 
  --inner join 
  --    sandbox.drag_ration dr 
  --        on 
  --            dr.mcid = cc.mcid 
  --There are 2 scenarios with churn customers 1) They churned out from TAT and never appeared in UFDM 2) They churned out in TAT and were found later in UFDM 
  --Find Non-Fopti Customers in UFDM and find the minimum date they start 
,
  non_fopti_1 as (
    select mcid,
      snapshot_date,
      arr,
      min(snapshot_date) filter(
        where arr > 0
      ) over(partition by mcid) as "Start of SST Data from UFDM"
    from sandbox.sst_temp
    where record_source ilike 'ufdm_2022'
      and product_family not in (
        'Full Stack',
        'Web',
        'Recurring: Cloud: Intelligence Cloud: Web Experimentation and Personalization'
      )
  ),
  non_fopti_2 as (
    select distinct mcid,
      "Start of SST Data from UFDM"
    from non_fopti_1
    where "Start of SST Data from UFDM" is not null
    order by mcid
  ) --Now join the non-Fopti data to the TAT. This is the final table that shows TAT MCID, Date of Churn and When they Start in UFDM ARR 
,
  tat_final as (
    select cc.mcid,
      cc."Date of Churn",
      cc."Start of Data in TAT",
      date_trunc('MONTH', nfp2."Start of SST Data from UFDM")::DATE as "Start of SST Data from UFDM"
    from churn_cust cc
      left join non_fopti_2 nfp2 on cc.mcid = nfp2.mcid
  ) --Now look at UFDM ARR: Check the Non-Fopti Data in UFDM ARR 
  --Take absolute values 
,
  unbund_arr as (
    select *,
      abs(arr_usd_ccfx) as abs_arr_usd_ccfx
    from sandbox_pd.arr
  ),
  ufdm_arr_0 as (
    select distinct mcid,
      snapshot_date,
      product_family,
      sku,
      sum(arr_usd_ccfx) over(
        partition by mcid,
        snapshot_date,
        product_family,
        sku
      ) as sum_arr_pf,
      --Ratios with negative values as well 
      (
        sum(abs_arr_usd_ccfx) over(
          partition by mcid,
          snapshot_date,
          product_family,
          sku
        ) / nullif(
          sum(abs_arr_usd_ccfx) over(partition by mcid, snapshot_date),
          0
        )
      ) *(abs_arr_usd_ccfx / nullif(arr_usd_ccfx, 0)) as "Ratio to Each PF",
      sum(arr_usd_ccfx) over(partition by mcid, snapshot_date) as sum_ufdm_arr
    from unbund_arr
    where product_family not in (
        'Full Stack',
        'Web',
        'Recurring: Cloud: Intelligence Cloud: Web Experimentation and Personalization'
      )
  ) --Put a limit on the table to select only customers who have greater than arr > 0 
  --select 
  --  * 
  --from 
  --  ufdm_arr_0 
  --where 
  --  mcid = '43a42cfd-dc29-e011-915e-0018717a8c82'
,
  ufdm_arr_1 as (
    select mcid,
      date_trunc('MONTH', snapshot_date)::DATE as date_ufdm_arr,
      product_family,
      sku,
      sum_arr_pf,
      "Ratio to Each PF",
      sum_ufdm_arr
    from ufdm_arr_0
    where sum_ufdm_arr > 0
  ) --This is the final UFDM ARR that shows the Non-Fopti Data with ARR 
,
  ufdm_arr_2 as (
    select distinct mcid,
      date_ufdm_arr,
      sum_ufdm_arr
    from ufdm_arr_1
  ) --select 
  --    *
  --from
  --    ufdm_arr_1
  --where 
  --    mcid = '01052ec2-dae5-e411-9afb-0050568d2da8'
  --Now Join TAT to UFDM ARR on 2 things: +/-3 months and +/-6 months 
,
  combined_table_1 as (
    select tf.mcid,
      tf."Date of Churn",
      tf."Start of Data in TAT",
      ua2.date_ufdm_arr as "UFDM ARR Dates in +/- 6 Month Range: with ARR",
      abs(tf."Date of Churn" - ua2.date_ufdm_arr) as "Abs. Difference between Churn Date and 6 Month Range",
      tf."Start of SST Data from UFDM"
    from tat_final tf
      left join ufdm_arr_2 ua2 on ua2.mcid = tf.mcid
      and (
        (
          ua2.date_ufdm_arr >= tf."Date of Churn" - interval '6 month'
        )
        and (ua2.date_ufdm_arr <= tf."Date of Churn")
        or (
          ua2.date_ufdm_arr <= tf."Date of Churn" + interval '6 month'
        )
        and (ua2.date_ufdm_arr >= tf."Date of Churn")
      )
    order by tf.mcid
  ) --Only take the closest date from the +6 month range 
  --Rank the least difference first 
  --Rank the latest dates first 
,
  combined_table_2 as (
    select ct1.mcid,
      ct1."Date of Churn",
      ct1."Start of Data in TAT",
      ct1."UFDM ARR Dates in +/- 6 Month Range: with ARR",
      ct1."Abs. Difference between Churn Date and 6 Month Range",
      ct1."Start of SST Data from UFDM",
      rank() over(
        partition by mcid
        order by ct1."Abs. Difference between Churn Date and 6 Month Range",
          ct1."UFDM ARR Dates in +/- 6 Month Range: with ARR" desc
      ) as ranking_6_month
    from combined_table_1 ct1
  ) --Now only keep the rows where ranking = 1 
,
  combined_table_3 as (
    select ct2.mcid,
      ct2."Date of Churn",
      ct2."Start of Data in TAT",
      ct2."UFDM ARR Dates in +/- 6 Month Range: with ARR",
      ct2."Abs. Difference between Churn Date and 6 Month Range",
      ct2."Start of SST Data from UFDM",
      ct2.ranking_6_month
    from combined_table_2 ct2
    where ranking_6_month = 1
  ) --Test to make sure there are no duplicates 
  --select
  --    distinct mcid, 
  --    count(*) as no_of_obs
  --from 
  --    combined_table_3 
  --group by 
  --    mcid 
  --having 
  --    count(*) > 1
  --Now repeat this process for 2-month period 
,
  combined_table_4 as (
    select ct3.mcid,
      ct3."Date of Churn",
      ct3."Start of Data in TAT",
      ct3."UFDM ARR Dates in +/- 6 Month Range: with ARR",
      ct3."Start of SST Data from UFDM",
      ua3.date_ufdm_arr as "UFDM ARR Dates in +/- 2 Month Range: with ARR",
      abs(ct3."Date of Churn" - ua3.date_ufdm_arr) as "Abs. Difference between Churn Date and 2 Month Range"
    from combined_table_3 ct3
      left join ufdm_arr_2 ua3 on ua3.mcid = ct3.mcid
      and (
        (
          ua3.date_ufdm_arr >= ct3."Date of Churn" - interval '2 month'
        )
        and (ua3.date_ufdm_arr <= ct3."Date of Churn")
        or (
          ua3.date_ufdm_arr <= ct3."Date of Churn" + interval '2 month'
        )
        and (ua3.date_ufdm_arr >= ct3."Date of Churn")
      )
    order by ct3.mcid
  ) --Now start ranking the dates 
  --Rank the least difference first 
  --Rank the latest dates first 
,
  combined_table_5 as (
    select ct4.mcid,
      ct4."Date of Churn",
      ct4."Start of Data in TAT",
      ct4."UFDM ARR Dates in +/- 6 Month Range: with ARR",
      ct4."Start of SST Data from UFDM",
      ct4."UFDM ARR Dates in +/- 2 Month Range: with ARR",
      ct4."Abs. Difference between Churn Date and 2 Month Range",
      rank() over(
        partition by mcid
        order by ct4."Abs. Difference between Churn Date and 2 Month Range",
          ct4."UFDM ARR Dates in +/- 2 Month Range: with ARR" desc
      ) as ranking_2_month
    from combined_table_4 ct4
  ) --Now only take the dates which are ranked 1 
,
  final_table_1 as (
    select ct5.mcid,
      ct5."Start of SST Data from UFDM",
      ct5."Start of Data in TAT",
      ct5."Date of Churn",
      ct5."UFDM ARR Dates in +/- 6 Month Range: with ARR",
      ct5."UFDM ARR Dates in +/- 2 Month Range: with ARR",
      ct5."Abs. Difference between Churn Date and 2 Month Range"
    from combined_table_5 ct5
    where ranking_2_month = 1
  ) --Joining to to UFDM ARR with PF Makeup and Ratio. 
,
  final_table_2 as (
    select ft1.mcid,
      ft1."Start of SST Data from UFDM",
      ft1."Start of Data in TAT",
      ft1."Date of Churn",
      ft1."UFDM ARR Dates in +/- 6 Month Range: with ARR",
      ua1.product_family,
      ua1.sku,
      ua1.date_ufdm_arr,
      ua1."Ratio to Each PF"
    from final_table_1 ft1
      left join ufdm_arr_1 ua1 on ft1.mcid = ua1.mcid
      and ft1."UFDM ARR Dates in +/- 6 Month Range: with ARR" = ua1.date_ufdm_arr --Filter on Dates
    where ft1."UFDM ARR Dates in +/- 6 Month Range: with ARR" is not null
  ) --We will only we be using dates within the 6 month range as it covers the 2 month range as well and casts a wider net 
  --Produce a final table for the DEs with mcid, Date in UFDM ARR, Start of Data in TAT, Product Family Makeup and Ratio 
  --This is the drag ratio table
,
  drag_ratio_c2 as --test
  (
    select ft2.mcid,
      ft2."Start of Data in TAT" as "Start of Drag Ratio in TAT",
      ft2."Date of Churn" - interval '1 month' as "End of Drag Ratio in TAT",
      --end it before the churn date, 
      ft2."Date of Churn" as "Churn Date in TAT",
      (
        (
          ft2."UFDM ARR Dates in +/- 6 Month Range: with ARR" + interval '1 month'
        ) - interval '1 day'
      ) as "Date in UFDM ARR",
      ft2.product_family as "Product Family in UFDM ARR",
      ft2.sku,
      ft2.date_ufdm_arr as snapshot_date_arr,
      ft2."Ratio to Each PF" as "Ratio of ARR for Each PF in UFDM ARR"
    from final_table_2 ft2
  )
  select *
  from drag_ratio_c2
);
drop table if exists sandbox.drag_ratio_with_sku_with_refined_proposal;
create table sandbox.drag_ratio_with_sku_with_refined_proposal as (
  with combined_drag_ratio as (
    SELECT mcid_arr,
      "MAX Snapshot Date of TAT",
      product_family_arr,
      sku,
      "Ratio of ARR Allocated to PF UFDM ARR" AS "Ratio of ARR",
      "Date to Drag to Under Scenario 1" AS "Date to Drag: Sol. 1"
    FROM sandbox.drag_ratio_with_sku_c1_with_flat AS a
    UNION ALL
    SELECT mcid AS mcid_arr,
      "End of Drag Ratio in TAT" AS "Max Snapshot Date in TAT",
      "Product Family in UFDM ARR" AS product_family_arr,
      sku,
      "Ratio of ARR for Each PF in UFDM ARR" AS "Ratio of ARR",
      "Start of Drag Ratio in TAT" AS "Date to Drag: Sol. 1"
    FROM sandbox.drag_ratio_with_sku_c2
    WHERE mcid NOT IN(
        SELECT DISTINCT mcid_arr
        FROM sandbox.drag_ratio_with_sku_c1_with_flat
      )
  ),
  remove_customer AS (
    SELECT DISTINCT mcid_arr
    FROM (
        select distinct mcid_arr
        from sandbox.drag_ratio_with_sku_c1_with_flat
        where "Ratio of ARR Allocated to PF UFDM ARR" < 0
        UNION ALL
        select distinct mcid
        from sandbox.drag_ratio_with_sku_c2
        where "Ratio of ARR for Each PF in UFDM ARR" < 0
      ) AS a
  )
  select *
  from combined_drag_ratio
  WHERE mcid_arr NOT IN (
      SELECT mcid_arr
      FROM remove_customer
    )
);
Drop table if exists sandbox.drag_ration_with_sku_refined_proposal;
CREATE TABLE sandbox.drag_ration_with_sku_refined_proposal AS (
  with tat_info as (
    select distinct utu."customer_name_d&b",
      utu.parent_customer,
      utu.parent_master_customer_id,
      utu.customer_name,
      utu.end_customer,
      utu.mcid,
      utu."Overage Y/N",
      utu."NS ID",
      utu.subsidiary_name,
      --    utu.product_family, 
      utu.currency,
      utu.snapshot_date,
      --    utu.arr,
      utu.fx_rate_ccfx,
      --    utu.arr_usd_ccfx,   
      utu.ccfx_date,
      utu.mcid_old,
      utu.is_deleted,
      utu.modified_comments
    from ufdm.tat_upload_data utu
    where utu.currency is not null
      and utu.mcid is not null
      and utu.is_deleted IS DISTINCT
    FROM 1
      and coalesce(nullif(trim(utu."Overage Y/N"), ''), 'N') is distinct
    from 'Y'
      and not (
        date_trunc('month', snapshot_date) = '2021-12-01'::DATE
        AND product_family ilike '%Campaign%'
      )
  ),
  combined_table_1 as (
    select t1."customer_name_d&b",
      t1.parent_customer,
      t1.parent_master_customer_id,
      t1.customer_name,
      t1.end_customer,
      t1.mcid,
      t1."Overage Y/N",
      t1."NS ID",
      t1.subsidiary_name,
      --    t1.product_family,  
      t1.currency,
      t1.snapshot_date,
      --    t1.arr,
      t1.fx_rate_ccfx,
      --    t1.arr_usd_ccfx,    
      t1.ccfx_date,
      t1.mcid_old,
      t1.is_deleted,
      t1.modified_comments,
      dr."MAX Snapshot Date of TAT",
      dr.product_family_arr,
      dr.sku,
      dr."Ratio of ARR" as "Original Ratio",
      dr."Date to Drag: Sol. 1" as "Date to Drag to Under Scenario 1",
      row_number() over(
        partition by t1.mcid,
        t1.snapshot_date,
        dr.product_family_arr,
        dr.sku
        order by t1.mcid
      ) as "Row Number of PF"
    from tat_info t1
      inner join sandbox.drag_ratio_with_sku_with_refined_proposal dr on t1.mcid = dr.mcid_arr
  ),
  combined_table_2 as (
    select ct1."customer_name_d&b",
      ct1.parent_customer,
      ct1.parent_master_customer_id,
      ct1.customer_name,
      ct1.end_customer,
      ct1.mcid,
      ct1."Overage Y/N",
      ct1."NS ID",
      ct1.subsidiary_name,
      --    ct1.product_family, 
      ct1.currency,
      ct1.snapshot_date,
      --    ct1.arr,
      ct1.fx_rate_ccfx,
      --    ct1.arr_usd_ccfx,   
      ct1.ccfx_date,
      ct1.mcid_old,
      ct1.is_deleted,
      ct1.modified_comments,
      ct1."MAX Snapshot Date of TAT",
      ct1.product_family_arr,
      ct1.sku,
      ct1."Original Ratio",
      ct1."Date to Drag to Under Scenario 1",
      max(ct1."Row Number of PF") over(
        partition by mcid,
        ct1.snapshot_date,
        product_family_arr,
        sku
      ) as "No of Rows Per PF"
    from combined_table_1 ct1
  ),
  comibined_table_2a as (
    select ct1."customer_name_d&b",
      ct1.parent_customer,
      ct1.parent_master_customer_id,
      ct1.customer_name,
      ct1.end_customer,
      ct1.mcid,
      ct1."Overage Y/N",
      ct1."NS ID",
      ct1.subsidiary_name,
      --    ct1.product_family, 
      ct1.currency,
      ct1.snapshot_date,
      --    ct1.arr,
      ct1.fx_rate_ccfx,
      --    ct1.arr_usd_ccfx,   
      ct1.ccfx_date,
      ct1.mcid_old,
      ct1.is_deleted,
      ct1.modified_comments,
      ct1."MAX Snapshot Date of TAT",
      ct1.product_family_arr,
      ct1.sku,
      ct1."Original Ratio",
      ct1."Date to Drag to Under Scenario 1",
      ct1."No of Rows Per PF"
    from combined_table_2 ct1 --Get rid of Campaign in Dec 2021 if it's copied over from UFDM ARR. Campaign does not come from TAT in Dec 2021. Therefore ratios in Dec 2021 should be done without Campaign
    where not (
        date_trunc('month', snapshot_date) = '2021-12-01'::DATE
        AND product_family_arr ilike 'Recurring: Cloud: Other Bookings: Campaign'
      )
  ),
  combined_table_3 as (
    select ct2."customer_name_d&b",
      ct2.parent_customer,
      ct2.parent_master_customer_id,
      ct2.customer_name,
      ct2.end_customer,
      ct2.mcid,
      ct2."Overage Y/N",
      ct2."NS ID",
      ct2.subsidiary_name,
      --    ct2.product_family, 
      ct2.currency,
      ct2.snapshot_date,
      --    ct2.arr,
      ct2.fx_rate_ccfx,
      --    ct2.arr_usd_ccfx,   
      ct2.ccfx_date,
      ct2.mcid_old,
      ct2.is_deleted,
      ct2.modified_comments,
      ct2."MAX Snapshot Date of TAT",
      ct2.product_family_arr,
      ct2.sku,
      ct2."Original Ratio",
      ct2."Date to Drag to Under Scenario 1",
      --Calculate the new ratios 
      --If it's not Dec 2021, then original ratio divided by number of rows 
      --If it's Dec 2021, since we got rid of campaign, get ratio of ratios 
      case
        when date_trunc('MONTH', snapshot_date) != '2021-12-01' then ct2."Original Ratio" / ct2."No of Rows Per PF"
        when date_trunc('MONTH', snapshot_date) = '2021-12-01' then ct2."Original Ratio" /(
          sum(ct2."Original Ratio") over(partition by ct2.mcid, ct2.snapshot_date)
        )
      end as "New Ratio Per Date for TAT"
    from comibined_table_2a ct2
    where ct2.snapshot_date <= ct2."MAX Snapshot Date of TAT"::DATE
      and ct2.snapshot_date >= ct2."Date to Drag to Under Scenario 1"::DATE
  ) --This is the final drag ration table 
,
  drag_ration_unified as (
    select ct3."customer_name_d&b",
      ct3.parent_customer,
      ct3.parent_master_customer_id,
      ct3.customer_name,
      ct3.end_customer,
      ct3.mcid,
      ct3."Overage Y/N",
      ct3."NS ID",
      ct3.subsidiary_name,
      --    ct3.product_family, 
      ct3.currency,
      ct3.snapshot_date,
      --    ct3.arr,
      ct3.fx_rate_ccfx,
      --    ct3.arr_usd_ccfx,   
      ct3.ccfx_date,
      ct3.mcid_old,
      ct3.is_deleted,
      ct3.modified_comments,
      ct3."MAX Snapshot Date of TAT",
      ct3.product_family_arr,
      ct3.sku,
      ct3."Original Ratio",
      ct3."Date to Drag to Under Scenario 1",
      ct3."New Ratio Per Date for TAT",
      sum(ct3."New Ratio Per Date for TAT") over(partition by ct3.mcid, ct3.snapshot_date) as "Sum of Ratios Per MCID and Snapshot Date"
    from combined_table_3 ct3
  ) --Table for Drag Ration: Use this to create drag ration 
  select *
  from drag_ration_unified
);
DROP TABLE IF EXISTS sandbox.tat_with_sku_with_refined_proposal;
CREATE TABLE sandbox.tat_with_sku_with_refined_proposal as (
  with edit_tat as (
    select *
    from ufdm.tat_upload_data tat
    where tat.is_deleted IS DISTINCT
    FROM 1
      and coalesce(nullif(trim(tat."Overage Y/N"), ''), 'N') is distinct
    from 'Y'
      and not (
        date_trunc('month', snapshot_date) = '2021-12-01'::DATE
        AND product_family ilike '%Campaign%'
      )
  ),
  tat_change_1 as (
    select distinct mcid,
      snapshot_date,
      product_family,
      '' as sku,
      currency,
      sum(arr) over(
        partition by mcid,
        snapshot_date,
        product_family,
        currency
      ) as arr,
      sum(arr_usd_ccfx) over(
        partition by mcid,
        snapshot_date,
        product_family,
        currency
      ) as arr_usd_ccfx
    from edit_tat
  ),
  tat_change_2 as (
    select distinct tc1.mcid,
      tc1.snapshot_date,
      sum(tc1.arr) over(
        partition by tc1.mcid,
        tc1.snapshot_date
      ) as "ARR:Local Currency",
      sum(tc1.arr_usd_ccfx) over(
        partition by tc1.mcid,
        tc1.snapshot_date
      ) as "ARR:USD CCFX"
    from tat_change_1 tc1
    where arr >= 0
  ) --Now take the sandbox.drag_ration_3 table and append ending ARR to 
,
  tat_change_3 as (
    select sdr3."customer_name_d&b",
      sdr3.parent_customer,
      sdr3.parent_master_customer_id,
      sdr3.customer_name,
      sdr3.end_customer,
      sdr3.mcid,
      sdr3."Overage Y/N",
      sdr3."NS ID",
      sdr3.subsidiary_name,
      sdr3.currency,
      sdr3.snapshot_date,
      sdr3.fx_rate_ccfx,
      sdr3.ccfx_date,
      sdr3.mcid_old,
      sdr3.is_deleted,
      sdr3.modified_comments,
      sdr3."MAX Snapshot Date of TAT",
      sdr3.product_family_arr,
      sdr3.sku,
      sdr3."Original Ratio",
      sdr3."Date to Drag to Under Scenario 1",
      sdr3."New Ratio Per Date for TAT",
      sdr3."Sum of Ratios Per MCID and Snapshot Date",
      tc2."ARR:Local Currency",
      tc2."ARR:USD CCFX"
    from sandbox.drag_ration_with_sku_refined_proposal sdr3
      inner join tat_change_2 tc2 on tc2.mcid = sdr3.mcid
      and tc2.snapshot_date = sdr3.snapshot_date --For Scenario 1 
    where sdr3.snapshot_date >= sdr3."Date to Drag to Under Scenario 1"::DATE
  ) --select 
  --  *
  --from 
  --  tat_change_3 
  --where 
  --  mcid = 'd75409e0-c8f2-e711-811d-70106faa0841'
  --select 
  --  *
  --from 
  --  ufdm.tat_upload_data tud 
  --where 
  --  mcid = '03c69ff6-a949-ea11-a812-000d3a228882'
  --and 
  --  date_trunc('MONTH', snapshot_date) = '2021-02-01' 
  --This is the final tat to be changed. It has the same structure as original TAT 
,
  tat_change_4 as (
    select tc3."customer_name_d&b",
      tc3.parent_customer,
      tc3.parent_master_customer_id,
      tc3.customer_name,
      tc3.end_customer,
      tc3.mcid,
      tc3."Overage Y/N",
      tc3."NS ID",
      tc3.subsidiary_name,
      tc3.product_family_arr as product_family,
      tc3.sku,
      --new product family of TAT 
      tc3.currency,
      tc3.snapshot_date,
      (
        tc3."ARR:Local Currency" * tc3."New Ratio Per Date for TAT"
      ) as arr,
      -- new local currency arr of TAT 
      tc3.fx_rate_ccfx,
      (
        tc3."ARR:USD CCFX" * tc3."New Ratio Per Date for TAT"
      ) as arr_usd_ccfx,
      --new arr_usd_ccfx of TAT 
      tc3.ccfx_date,
      tc3.mcid_old,
      tc3.is_deleted,
      tc3.modified_comments
    from tat_change_3 tc3
  ) --select 
  --  *
  --from 
  --  tat_change_4 
  --where 
  --  mcid = 'abf9133d-75e4-e411-9afb-0050568d2da8'
  --select 
  --  *,
  --  sum(arr) over(partition by mcid, snapshot_date) as sum_arr,
  --  sum(arr_usd_ccfx) over(partition by mcid, snapshot_date) as sum_arr_usd_ccfx
  --from 
  --  tat_change_4 
  --where 
  --  mcid = '1f6370ef-dbaf-e311-a1cd-0050568d2da8'
  --  and 
  --  date_trunc('MONTH', snapshot_date) = '2019-01-01'
  --Make sure ending ARRs match 
  --Running Tests 
,
  test_1 as (
    select distinct mcid as mcid_nt,
      snapshot_date as snapshot_date_nt,
      sum(arr_usd_ccfx) over(partition by mcid, snapshot_date) as sum_new_tat_ccfx,
      sum(arr) over(partition by mcid, snapshot_date) as sum_new_tat_lc
    from tat_change_4
  ) --select 
  --  t1.mcid_nt,
  --  t1.snapshot_date_nt,
  --  t1.sum_new_tat_ccfx,
  --  t1.sum_new_tat_lc, 
  --  tc2."ARR:USD CCFX",
  --  tc2."ARR:Local Currency"
  --from 
  --  test_1 t1
  --inner join 
  --  tat_change_2 tc2
  --      on 
  --          t1.mcid_nt = tc2.mcid
  --          and 
  --          t1.snapshot_date_nt = tc2.snapshot_date 
  --where 
  --  abs(tc2."ARR:USD CCFX"-t1.sum_new_tat_ccfx) > 1
  --Now union to TAT that does not change. Take all the mcids and dates that are not present in sandbox ratio
,
  tat_no_change as (
    select st7."customer_name_d&b",
      st7.parent_customer,
      st7.parent_master_customer_id,
      st7.customer_name,
      st7.end_customer,
      st7.mcid,
      st7."Overage Y/N",
      st7."NS ID",
      st7.subsidiary_name,
      st7.product_family,
      ' ' AS sku,
      st7.currency,
      st7.snapshot_date,
      st7.arr,
      st7.fx_rate_ccfx,
      st7.arr_usd_ccfx,
      st7.ccfx_date,
      st7.mcid_old,
      st7.is_deleted,
      st7.modified_comments
    from edit_tat st7
      left join sandbox.drag_ration_with_sku_refined_proposal sdr2 on st7.mcid = sdr2.mcid
      and st7.snapshot_date = sdr2.snapshot_date --For Scenario 1 
    where sdr2.mcid is null
  ) --Now union the 2 tables 
,
  combined_table_1 as (
    (
      select tc4."customer_name_d&b",
        tc4.parent_customer,
        tc4.parent_master_customer_id,
        tc4.customer_name,
        tc4.end_customer,
        tc4.mcid,
        tc4."Overage Y/N",
        tc4."NS ID",
        tc4.subsidiary_name,
        tc4.product_family,
        --new product family of TAT 
        tc4.sku,
        tc4.currency,
        tc4.snapshot_date,
        tc4.arr,
        -- new local currency arr of TAT 
        tc4.fx_rate_ccfx,
        tc4.arr_usd_ccfx,
        --new arr_usd_ccfx of TAT 
        tc4.ccfx_date,
        tc4.mcid_old,
        tc4.is_deleted,
        tc4.modified_comments
      from tat_change_4 tc4
    )
    union all
    (
      select tcn."customer_name_d&b",
        tcn.parent_customer,
        tcn.parent_master_customer_id,
        tcn.customer_name,
        tcn.end_customer,
        tcn.mcid,
        tcn."Overage Y/N",
        tcn."NS ID",
        tcn.subsidiary_name,
        tcn.product_family,
        tcn.sku,
        tcn.currency,
        tcn.snapshot_date,
        tcn.arr,
        tcn.fx_rate_ccfx,
        tcn.arr_usd_ccfx,
        tcn.ccfx_date,
        tcn.mcid_old,
        tcn.is_deleted,
        tcn.modified_comments
      from tat_no_change tcn
    )
  ) --select 
  --  *
  --from 
  --  combined_table_1
,
  new_prod_tat as (
    select ct1."customer_name_d&b",
      ct1.parent_customer,
      ct1.parent_master_customer_id,
      ct1.customer_name,
      ct1.end_customer,
      ct1.mcid,
      ct1."Overage Y/N",
      ct1."NS ID",
      ct1.subsidiary_name,
      ct1.product_family,
      ct1.sku,
      ct1.currency,
      ct1.snapshot_date,
      ct1.arr,
      ct1.fx_rate_ccfx,
      ct1.arr_usd_ccfx,
      ct1.ccfx_date,
      ct1.mcid_old,
      ct1.is_deleted,
      ct1.modified_comments
    from combined_table_1 ct1
    order by ct1.mcid,
      ct1.snapshot_date
  ) --New TAT Table: Solution 1 for Cohort 2
  --
  select *
  from new_prod_tat
);