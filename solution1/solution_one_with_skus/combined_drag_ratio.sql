drop table if exists sandbox.drag_ratio_with_sku;
create table sandbox.drag_ratio_with_sku as (
  with combined_drag_ratio as (
    SELECT mcid_arr,
      "MAX Snapshot Date of TAT",
      product_family_arr,
      sku ,
      "Ratio of ARR Allocated to PF UFDM ARR" AS "Ratio of ARR",
      "Date to Drag to Under Scenario 1" AS "Date to Drag: Sol. 1"
    FROM sandbox.drag_ratio_with_sku_c1 AS a
    UNION ALL
    SELECT mcid AS mcid_arr,
      "End of Drag Ratio in TAT" AS "Max Snapshot Date in TAT",
      "Product Family in UFDM ARR" AS product_family_arr,
      sku ,
      "Ratio of ARR for Each PF in UFDM ARR" AS "Ratio of ARR",
      "Start of Drag Ratio in TAT" AS "Date to Drag: Sol. 1"
    FROM sandbox.drag_ratio_with_sku_c2
    WHERE mcid NOT IN(
        SELECT DISTINCT mcid_arr
        FROM sandbox.drag_ratio_with_sku_c1
      )
  )
  select *
  from combined_drag_ratio
);