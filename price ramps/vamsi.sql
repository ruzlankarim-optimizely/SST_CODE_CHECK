--#############################################
--Price Ramps
--#############################################
drop table if exists temp_customer_bridge_price_ramps;
create temp table temp_customer_bridge_price_ramps as with cte as (
  select c_name,
    mcid,
    date_start,
    date_end,
    snapshot_date,
    sum(Price_Ramp) as PriceRamp_Value
  from sandbox_pd.Price_Ramps
  group by c_name,
    mcid,
    date_start,
    date_end,
    snapshot_date
)
select pr.evaluation_period,
  pr.prior_period,
  pr.current_period,
  pr.mcid,
  pr.prior_arr_usd_ccfx as prior_period_customer_arr_usd_ccfx,
  pr.current_arr_usd_ccfx as current_period_customer_arr_usd_ccfx,
  pr.customer_arr_change_ccfx,
  customer_bridge,
  PriceRamp_Value,
  cte.snapshot_date
from arr_bridge_tmp pr
  inner join cte on pr.mcid = cte.mcid
  and pr.current_period = cte.snapshot_date
where customer_bridge = 'Up Sell';
-- select * from temp_customer_bridge_price_ramps
--     where customer_arr_change_ccfx< PriceRamp_Value and PriceRamp_Value>0;
--
-- select * from temp_customer_bridge_price_ramps
--     where customer_arr_change_ccfx> PriceRamp_Value and PriceRamp_Value>0 --and mcid='6c3202c8-cb68-7ccd-f92d-378914e0ebfd'
--
-- select * from Price_Ramps where mcid='be8ab5f4-c33f-e511-9afb-0050568d2da8'
update arr_bridge_tmp a
set customer_bridge = 'Price Ramp'
from temp_customer_bridge_price_ramps b
where a.mcid = b.mcid
  and a.current_period = b.snapshot_date
  and coalesce(a.customer_arr_change_ccfx::numeric, 0) - coalesce(b.PriceRamp_Value::numeric, 0) BETWEEN -1 and 1;
drop table if exists temp_Price_Ramp_split;
create temp table temp_Price_Ramp_split as
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  a.prior_arr_usd_ccfx as prior_period_customer_arr_usd_ccfx,
  a.current_arr_usd_ccfx - b.PriceRamp_Value as current_period_customer_arr_usd_ccfx,
  a.customer_arr_change_ccfx - b.PriceRamp_Value as customer_arr_change_ccfx,
  a.prior_arr_lcu as prior_period_customer_arr_lcu,
  a.current_arr_lcu - b.PriceRamp_Value as current_period_customer_lcu,
  a.customer_arr_change_lcu - b.PriceRamp_Value as customer_arr_change_lcu,
  a.customer_bridge,
  null::text as winback_period_days,
  null::text as wip_flag
from arr_bridge_tmp a
  join temp_customer_bridge_price_ramps b on a.mcid = b.mcid
  and a.current_period = b.snapshot_date
where coalesce(a.customer_arr_change_ccfx::numeric, 0) - coalesce(b.PriceRamp_Value::numeric, 0) > 1 --a.mcid='dce00e69-883c-5e2f-b0ac-826f23cc3a18'
union all
select distinct a.evaluation_period,
  a.prior_period,
  a.current_period,
  a.current_master_customer_id,
  a.prior_master_customer_id,
  a.mcid,
  a.name,
  a.baseline_currency,
  a.subsidiary_entity_name,
  '0'::numeric as prior_period_customer_arr_usd_ccfx,
  b.PriceRamp_Value as current_period_customer_arr_usd_ccfx,
  b.PriceRamp_Value as customer_arr_change_ccfx,
  '0'::numeric as prior_period_customer_arr_lcu,
  b.PriceRamp_Value as current_period_customer_lcu,
  b.PriceRamp_Value as customer_arr_change_lcu,
  'Price Ramp' as customer_bridge,
  null::text as winback_period_days,
  null::text as wip_flag
from arr_bridge_tmp a
  join temp_customer_bridge_price_ramps b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.customer_bridge = b.customer_bridge
where coalesce(a.customer_arr_change_ccfx::numeric, 0) - coalesce(b.PriceRamp_Value::numeric, 0) > 1
order by mcid;
delete from arr_bridge_tmp a using temp_customer_bridge_price_ramps b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.current_period = b.snapshot_date --and a.evaluation_period = var_period
  and coalesce(a.customer_arr_change_ccfx::numeric, 0) - coalesce(b.PriceRamp_Value::numeric, 0) > 1;
insert into arr_bridge_tmp (
    evaluation_period,
    prior_period,
    current_period,
    current_master_customer_id,
    prior_master_customer_id,
    mcid,
    name,
    baseline_currency,
    subsidiary_entity_name,
    prior_arr_usd_ccfx,
    current_arr_usd_ccfx,
    customer_arr_change_ccfx,
    prior_arr_lcu,
    current_arr_lcu,
    customer_arr_change_lcu,
    customer_bridge --, winback_period_days, wip_flag
  )
select evaluation_period,
  prior_period,
  current_period,
  current_master_customer_id,
  prior_master_customer_id,
  mcid,
  name,
  baseline_currency,
  subsidiary_entity_name,
  prior_period_customer_arr_usd_ccfx,
  current_period_customer_arr_usd_ccfx,
  customer_arr_change_ccfx,
  prior_period_customer_arr_lcu,
  current_period_customer_lcu,
  customer_arr_change_lcu,
  customer_bridge --, winback_period_days, wip_flag
from temp_Price_Ramp_split;
* /