create or replace function sandbox_pd.sp_populate_price_ramps_for_bridges(var_period text) returns void language plpgsql as $$ 
BEGIN drop table if exists ramp;
create temp table ramp as with ramp as (
  select mm.reference_number,
    mm.subline_id,
    mm.SKU,
    mm.date_start,
    mm.date_end,
    latest_snapshot
  from (
      select distinct reference_number,
        subline_id,
        SKU,
        date_start,
        date_end,
        MAX(snapshot_date) as latest_snapshot
      from sandbox_pd.monthly_metrics mm
      where 1 = 1 --and snapshot_date = (select current_period from ufdm_grey.periods p  where evaluation_period = '2020M06')
        --snapshot_date >= '2023-03-01' AND snapshot_date <= '2023-06-30' AND
        and SKU not ilike '%-OVR%' --and reference_number in('7917','16614')
      group by 1,
        2,
        3,
        4,
        5
    ) mm
    left join (
      select distinct reference_number,
        subline_id,
        SKU,
        date_start,
        date_end
      from sandbox_pd.monthly_metrics mm
      where 1 = 1 --and snapshot_date = (select current_period from ufdm_grey.periods p  where evaluation_period = '2020M06')
        --snapshot_date >= '2023-03-01' AND snapshot_date <= '2023-06-30' AND
        and SKU not ilike '%-OVR%' --and reference_number in('7917','16614')
    ) mm1 on mm.reference_number = mm1.reference_number
    and mm.SKU = mm1.SKU
    and mm.subline_id = mm1.subline_id
  where mm.date_start <> mm1.date_start
    and mm.date_end <> mm1.date_end
)
SELECT *
FROM RAMP;
--select * from ramp;
drop table if exists arr;
create temp table arr as
select *,
  case
    when date_start = date_2 then arr_usd_ccfx
    else arr_usd_ccfx - firstvalue
  end as Price_Ramp,
  case
    when date_start = date_2 then arr_lcu
    else arr_lcu - firstvalue_lcu
  end as Price_Ramp_lcu
from (
    select c_name,
      mcid,
      coalesce(
        end_customer,
        parent_customer
      ) as name,
      r.*,
      MM.snapshot_date,
      case
        when snapshot_date < '2024-01-01' then arr_usd_ccfx_24_fx_rate
        else arr_usd_ccfx
      end as arr_usd_ccfx,
      baseline_arr_local_currency as arr_lcu,
      --        row_number() over (PARTITION BY r.date_start,r.date_end order by snapshot_date asc) as r_1,
      first_value(r.date_start) over (
        partition by r.reference_number,
        r.sku
        order by snapshot_date rows between 1 preceding and current row
      ) as date_2,
      first_value(
        case
          when snapshot_date < '2024-01-01' then arr_usd_ccfx_24_fx_rate
          else arr_usd_ccfx
        end
      ) over (
        partition by r.reference_number,
        r.sku
        order by snapshot_date rows between 1 preceding and current row
      ) as firstvalue,
      first_value(mm.baseline_arr_local_currency) over (
        partition by r.reference_number,
        r.sku
        order by snapshot_date rows between 1 preceding and current row
      ) as firstvalue_lcu
    from ramp r
      left join sandbox_pd.monthly_metrics mm on mm.reference_number = r.reference_number
      and mm.SKU = r.SKU
      and mm.subline_id = r.subline_id
      and mm.date_start = r.date_start
      and mm.date_end = r.date_end --AND mm.snapshot_date = r.latest_snapshot
      --where mm.reference_number in('7917','16614')
  ) x
where date_start != date_2
  and date_start > date_2
  and arr_usd_ccfx - firstvalue > 0 --GROUP By 1,2,3,4,5,6,7,8,9
;
drop table if exists Price_Ramps;
create temp table Price_Ramps as with bsa as (
  select distinct ba.billing_account_name,
    bs.subscription_number as reference_number_monthly_metrics,
    bs.date_start as Sub_startdate,
    bs.date_end as Sub_enddate
  from epi_netsuite.billing_subscriptions bs
    join epi_netsuite.billing_accounts ba on bs.billing_account_id = ba.billing_account_id --where bs.subscription_number in('16614')
)
select *
from (
    --case when Lag_de is null then 0 else arr_usd_ccfx-Lag_de end  as Priceramp from (
    select c_name,
      mcid,
      name,
      reference_number,
      subline_id,
      sku,
      date_start,
      date_end,
      snapshot_date,
      arr_usd_ccfx,
      Price_Ramp,
      Price_Ramp_lcu --                      RANK() OVER (PARTITION BY reference_number, subline_id, SKU ORDER BY date_start) AS rank_list,
      --                     lag(arr_usd_ccfx) over (partition by reference_number order by date_start)       as Lag_de
    from ARR
  ) x
  inner join bsa on reference_number = bsa.reference_number_monthly_metrics
where AGE(Sub_enddate, Sub_startdate) >= interval '1 year 11 months' -- where reference_number='16614'and arr_usd_ccfx-Lag_de>=0
;
drop table if exists sandbox_pd.Price_Ramps;
create table sandbox_pd.price_ramps as
select *
from Price_Ramps;
END;
$$;
select sandbox_pd.sp_populate_price_ramps_for_bridges('2020M06');
create table ufdm_archive.price_ramps_bkup_09072024 as
select *
from sandbox_pd.price_ramps;
select *
from sandbox_pd.price_ramps
order by snapshot_date desc;
truncate table sandbox_pd.price_ramps;
select *
from sandbox_pd.price_ramps
where sku in (
    select distinct a.sku
    from sandbox_pd.price_ramps a
      join ufdm_grey.sku_mapping_allocation_final b on a.sku = b.sku
    where b.product_category not in ('Web', 'Full Stack')
  );
select *
from sandbox_pd.price_ramps
where 1 = 1 --and mcid ilike '039fa%'
  and mcid = 'b6da5650-76bc-ea11-a812-000d3abb4345';
select *
from ufdm_grey.product_hierarchy_mappings
where "Product Code" = 'OPT-USEIMP0 - 3.75 Billion';