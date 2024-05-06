drop table if exists Future_Price_Ramps create temp table Future_Price_Ramps as
select distinct bs.subscription_number,
  bs.subscription_id,
  bs.original_coterm_subscription_,
  c.master_customer_id,
  e.master_customer_id AS "end_customer_master_customer_id",
  bsl.line_type,
  slpi.date_start_inclusive::date,
  slpi.date_end_exclusive::date,
  bsl.date_start::date,
  bsl.date_end::date,
  subline_id,
  skus,
  total_interval_value,
  esa.of_skus_c,
  product,
  i.full_name,
  cur.name as Currency,
  phmp.*
FROM epi_netsuite.billing_subscription_lines bsl
  LEFT JOIN epi_netsuite.billing_subscriptions bs ON bs.subscription_id = bsl.subscription_id
  AND bs."_fivetran_deleted" IS DISTINCT
FROM TRUE
  LEFT JOIN epi_netsuite.subscript_line_price_intervals slpi ON slpi.subscription_line_id = bsl.subline_id
  AND slpi."_fivetran_deleted" IS DISTINCT
FROM TRUE
  LEFT JOIN epi_netsuite.subsidiaries s ON s.subsidiary_id = bs.subsidiary_id
  AND s."_fivetran_deleted" IS DISTINCT
FROM TRUE
  LEFT JOIN epi_netsuite.currencies cur ON bs.currency = cur.name
  AND cur._fivetran_deleted IS DISTINCT
FROM TRUE
  LEFT JOIN epi_netsuite.billing_accounts ba ON ba.billing_account_id = bs.billing_account_id
  AND ba."_fivetran_deleted" IS DISTINCT
FROM TRUE
  LEFT JOIN epi_netsuite.billing_schedule_descriptions bsd ON bsd.billing_schedule_id = ba.billing_schedule_id
  AND bsd."_fivetran_deleted" IS DISTINCT
FROM TRUE
  LEFT JOIN epi_netsuite.companies c ON c.company_id = ba.customer_id
  AND c._fivetran_deleted IS DISTINCT
FROM TRUE
  LEFT JOIN epi_netsuite.companies e ON e.company_id = bs.end_customer_id
  AND e."_fivetran_deleted" IS DISTINCT
FROM TRUE
  LEFT JOIN epi_netsuite.items i ON i.item_id = bsl.item_id
  and i._fivetran_deleted IS DISTINCT
FROM TRUE
  LEFT JOIN ufdm_grey.product_hierarchy_mappings phmp on phmp."Product Code" = i.full_name --and phmp._fivetr is distinct from true
  LEFT JOIN (
    SELECT *,
      ROW_NUMBER() over(
        PARTITION BY COALESCE(aa.dynamics_id_c, aa.sf_guid_c)
      ) AS "row_ranking"
    FROM epi_salesforce.account aa
  ) esa on COALESCE(esa.dynamics_id_c, esa.sf_guid_c) = c.master_customer_id
  and esa.is_deleted IS DISTINCT
FROM TRUE
  AND esa.row_ranking = 1
  LEFT JOIN (
    SELECT *,
      ROW_NUMBER() over(
        PARTITION BY COALESCE(bb.dynamics_id_c, bb.sf_guid_c)
      ) AS "row_ranking"
    FROM epi_salesforce.account bb
  ) esb ON coalesce(esb.dynamics_id_c, esb.sf_guid_c) = e.master_customer_id
  AND esb.is_deleted IS DISTINCT
FROM TRUE
  AND esb.row_ranking = 1
  LEFT JOIN ufdm.account ua on ua.id = esa.id
  LEFT JOIN (
    SELECT *,
      ROW_NUMBER() over(
        PARTITION BY p2.product_code
        ORDER BY (
            p2.is_active::int + (not p2.is_deleted)::int + (
              p2.don_t_sync_with_net_suite_c is distinct
              from true
            )::int
          ) desc
      ) AS "row_ranking"
    FROM opti_salesforce.product_2 p2
  ) x ON x.product_code = i."name"
  AND x.row_ranking = 1
WHERE 1 = 1
  AND bsl."_fivetran_deleted" IS DISTINCT
FROM TRUE --   AND (
  --             bsl.date_start::date <= '2024-02-29'
  --         AND bsl.date_end::date >= '2024-02-29'
  --         AND COALESCE( bsl.date_termination::date , '2099-12-31' ) >= '2024-02-29'
  --     )
  AND bsl.status_id IN (
    'CLOSED',
    'ACTIVE',
    'TERMINATED',
    'PENDING_ACTIVATION',
    'SUSPENDED'
  )
  AND (
    --           slpi.date_start_inclusive::date <= '2024-02-29'
    --       AND slpi.date_end_exclusive::date >= '2024-02-29' AND
    slpi.status_id = 'ACTIVE'
  ) --and bs.original_coterm_subscription_ is not null
  --and c.master_customer_id='1b03b5ac-0d26-f3bb-f9a7-4c12285cf66e'
  and bs.subscription_id in('94824', '187720');
SELECT *,
  CASE
    WHEN date_start_inclusive = date_2 THEN total_interval_value
    ELSE total_interval_value - firstvalue
  END AS Price_Ramp,
  subscription_number
FROM (
    SELECT master_customer_id,
      total_interval_value,
(
        Total_Interval_Value / (r.date_end::date - r.date_start::date)
      ) * 365 as ARR,
      date_start,
      date_end,
      r.date_start_inclusive,
      r.date_end_exclusive,
      r.subscription_number,
      full_name,
      first_value(r.date_start_inclusive) OVER (
        PARTITION BY r.subscription_number,
        r.subline_id
        ORDER BY date_start_inclusive ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
      ) AS date_2,
      first_value(total_interval_value) OVER (
        PARTITION BY r.subscription_number,
        r.subline_id
        ORDER BY date_start_inclusive ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
      ) AS firstvalue
    FROM Future_Price_Ramps r
    where AGE(date_end, date_start) >= INTERVAL '1 year 11 months'
  ) AS x
WHERE date_start_inclusive != date_2
  AND date_start_inclusive > date_2
  AND total_interval_value - firstvalue > 0;
select *
from sandbox_pd.arr_fx_rates
where fx_date = '2022-12-31'
  and fx_type = 'ccfx'
  and source_table = 'MM' ---------
select distinct ba.billing_account_name,
  subscription_id,
  bs.subscription_number as reference_number_monthly_metrics,
  bs.date_start as Sub_startdate,
  bs.date_end as Sub_enddate
from epi_netsuite.billing_subscriptions bs
  join epi_netsuite.billing_accounts ba on bs.billing_account_id = ba.billing_account_id
where bs.subscription_number in('14199', '26967', '19203')
select *
from epi_netsuite.billing_subscription_lines bsl
where bsl.subscription_id = '94824'
select sandbox.dbt_mahmudnabi_audit.netsuite_subscription_detail()
select *
from epi_netsuite.billing_subscriptions bs
where bs.subscription_number = '14199'
select *
from epi_netsuite.subscript_line_price_intervals
limit 10
select *
from ufdm.netsuite_subscriptions_detail
where subscription_id = '94824'
select *
from ufdm_blue.monthly_metrics
where reference_number = '14199'
  and sku = 'ISS-IC-100,000'
select *
from sandbox_PD.monthly_metrics
WHERE reference_number = '19203'
  and sku like '%IDC-G%'
select *
from sandbox_PD.monthly_metrics
WHERE reference_number = '26967'
  and sku like '%IDC-G%'
select full_name,
  *
from epi_netsuite.items
where items.full_name ilike '%AZ-ECSOIA%'
select distinct sku,
  sku_name
from ufdm_blue.monthly_metrics
where sku ilike '%AZ-ECSOIA%'
select *
from epi_netsuite.
limit 10
select sandbox_pd.sp_populate_snapshot_unbundling('2024-01-31');
--------------------------------------------------------------------------------
drop table if exists Price_Ramps;
Create temp table Price_Ramps as WITH ramp AS (
  SELECT mm.reference_number,
    mm.subline_id,
    mm.SKU,
    mm.date_start,
    mm.date_end,
    latest_snapshot
  FROM (
      SELECT DISTINCT reference_number,
        subline_id,
        SKU,
        date_start,
        date_end,
        MAX(snapshot_date) AS latest_snapshot
      FROM ufdm_blue.monthly_metrics mm
      WHERE --snapshot_date >= '2023-03-01' AND snapshot_date <= '2023-06-30' AND
        SKU NOT ILIKE '%-OVR%' --and reference_number in('7917','16614')
      GROUP BY 1,
        2,
        3,
        4,
        5
    ) mm
    LEFT JOIN (
      SELECT DISTINCT reference_number,
        subline_id,
        SKU,
        date_start,
        date_end
      FROM ufdm_blue.monthly_metrics mm
      WHERE --snapshot_date >= '2023-03-01' AND snapshot_date <= '2023-06-30' AND
        SKU NOT ILIKE '%-OVR%' --and reference_number in('7917','16614')
    ) mm1 ON mm.reference_number = mm1.reference_number
    AND mm.SKU = mm1.SKU
    and mm.subline_id = mm1.subline_id
  WHERE mm.date_start <> mm1.date_start
    AND mm.date_end <> mm1.date_end
) --SELECT * FROM RAMP
,
ARR AS (
  select *,
    case
      when date_start = date_2 then arr_usd_ccfx
      else arr_usd_ccfx - firstvalue
    end as Price_Ramp
  from (
      SELECT c_name,
        mcid,
        COALESCE(end_customer, parent_customer) AS name,
        r.*,
        MM.snapshot_date,
        arr_usd_ccfx,
        --        row_number() over (PARTITION BY r.date_start,r.date_end order by snapshot_date asc) as r_1,
        first_value(r.date_start) over (
          partition by r.reference_number,
          r.sku
          order by snapshot_date rows between 1 preceding and current row
        ) as date_2,
        first_value(mm.arr_usd_ccfx) over (
          partition by r.reference_number,
          r.sku
          order by snapshot_date rows between 1 preceding and current row
        ) as firstvalue
      from ramp r
        LEFT JOIN ufdm_blue.monthly_metrics mm ON mm.reference_number = r.reference_number
        AND mm.SKU = r.SKU
        and mm.subline_id = r.subline_id
        AND mm.date_start = r.date_start
        AND mm.date_end = r.date_end --AND mm.snapshot_date = r.latest_snapshot
      where mm.reference_number in('14199', '26967')
    ) x --where date_start!=date_2 and date_start>date_2 and arr_usd_ccfx-firstvalue  >0
    --GROUP By 1,2,3,4,5,6,7,8,9
),
bsa as(
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
    SELECT c_name,
      mcid,
      name,
      reference_number,
      subline_id,
      sku,
      date_start,
      date_end,
      snapshot_date,
      arr_usd_ccfx,
      Price_Ramp --                      RANK() OVER (PARTITION BY reference_number, subline_id, SKU ORDER BY date_start) AS rank_list,
      --                     lag(arr_usd_ccfx) over (partition by reference_number order by date_start)       as Lag_de
    FROM ARR
  ) x
  inner join bsa on reference_number = bsa.reference_number_monthly_metrics
where AGE(Sub_enddate, Sub_startdate) >= INTERVAL '1 year 11 months' -- where reference_number='16614'and arr_usd_ccfx-Lag_de>=0
;
select snapshot_date,
  sum(arr_usd_ccfx)
from sandbox_pd.monthly_metrics
where reference_number = '26967'
  and sku = 'IDC-G'
group by snapshot_date --subline_id='302258'
  and snapshot_date >= '2022-01-31'
select *
from sandbox_pd.monthly_metrics
where reference_number = '26967'
  and sku = 'IDC-G' drop table sandbox.sst_customer_bridge_PR;
select * into sandbox.sst_customer_bridge_PR
from sandbox_pd.sst_customer_bridge
where 1 = 1 drop table if exists temp_customer_bridge_price_ramps;
create temp table temp_customer_bridge_price_ramps as with cte as (
  select c_name,
    mcid,
    date_start,
    --date_end,
    snapshot_date,
    sum (Price_Ramp) as PriceRamp_Value
  from Price_Ramps
  group by c_name,
    mcid,
    date_start,
    --date_end,
    snapshot_date
)
select pr.evaluation_period,
  pr.prior_period,
  pr.current_period,
  pr.mcid,
  pr.prior_period_customer_arr_usd_ccfx,
  pr.current_period_customer_arr_usd_ccfx,
  pr.customer_arr_change_ccfx,
  customer_bridge,
  PriceRamp_Value,
  cte.snapshot_date
from sandbox.sst_customer_bridge_PR pr
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
update sandbox.sst_customer_bridge_PR a
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
  a.prior_period_customer_arr_usd_ccfx,
  a.current_period_customer_arr_usd_ccfx - b.PriceRamp_Value as current_period_customer_arr_usd_ccfx,
  a.customer_arr_change_ccfx - b.PriceRamp_Value as customer_arr_change_ccfx,
  a.prior_period_customer_arr_lcu,
  a.current_period_customer_lcu - b.PriceRamp_Value as current_period_customer_lcu,
  a.customer_arr_change_lcu - b.PriceRamp_Value as customer_arr_change_lcu,
  a.customer_bridge,
  a.winback_period_days,
  a.wip_flag
from sandbox.sst_customer_bridge_PR a
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
  a.winback_period_days,
  a.wip_flag
from sandbox_pd.sst_customer_bridge a
  join temp_customer_bridge_price_ramps b on a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.customer_bridge = b.customer_bridge
where coalesce(a.customer_arr_change_ccfx::numeric, 0) - coalesce(b.PriceRamp_Value::numeric, 0) > 1
order by mcid;
delete from sandbox.sst_customer_bridge_PR a using temp_customer_bridge_price_ramps b
where 1 = 1
  and a.mcid = b.mcid
  and a.evaluation_period = b.evaluation_period
  and a.current_period = b.snapshot_date --and a.evaluation_period = var_period
  and coalesce(a.customer_arr_change_ccfx::numeric, 0) - coalesce(b.PriceRamp_Value::numeric, 0) > 1;
insert into sandbox.sst_customer_bridge_PR (
    evaluation_period,
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
    customer_bridge,
    winback_period_days,
    wip_flag
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
  customer_bridge,
  winback_period_days,
  wip_flag
from temp_Price_Ramp_split;
select sandbox_pd.sp_populate_snapshot_unbundling('2024-01-31');