CREATE TABLE ryzlan.sst_customer_bridge_pp AS
SELECT *
FROM sandbox_pd.sst_customer_bridge ;
CREATE TABLE ryzlan.sst_product_bridge_product_group_pp AS
SELECT *
FROM sandbox_pd.sst_product_bridge_product_group spbpg; 
CREATE TABLE ryzlan.sst_product_bridge_product_solution_pp AS
SELECT *
FROM sandbox_pd.sst_product_bridge_product_solution spbps; 