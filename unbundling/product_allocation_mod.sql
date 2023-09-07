drop table if exists ryzlan.pa;
create table ryzlan.pa as
select *
from ufdm_blue.product_allocated;

select ryzlan.sp_populate_snapshot_product_allocation_mod('2019-01-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2019-02-28');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2019-03-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2019-04-30');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2019-05-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2019-06-30');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2019-07-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2019-08-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2019-09-30');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2019-10-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2019-11-30');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2019-12-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2020-01-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2020-02-29');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2020-03-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2020-04-30');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2020-05-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2020-06-30');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2020-07-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2020-08-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2020-09-30');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2020-10-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2020-11-30');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2020-12-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2021-01-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2021-02-28');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2021-03-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2021-04-30');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2021-05-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2021-06-30');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2021-07-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2021-08-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2021-09-30');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2021-10-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2021-11-30');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2021-12-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2022-01-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2022-02-28');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2022-03-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2022-04-30');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2022-05-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2022-06-30');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2022-07-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2022-08-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2022-09-30');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2022-10-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2022-11-30');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2022-12-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2023-01-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2023-02-28');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2023-03-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2023-04-30');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2023-05-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2023-06-30');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2023-07-31');
select ryzlan.sp_populate_snapshot_product_allocation_mod('2023-08-31');

DROP TABLE IF EXISTS sandbox.pa_mod;
CREATE TABLE sandbox.pa_mod AS
SELECT *
FROM ryzlan.pa pa