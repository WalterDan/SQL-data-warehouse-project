
/*
=================================================================================
DDl Script: Create Gold Views
=================================================================================
Script Purpose:
  This script creates views for the Gold layer in the data warehouse.
  The Gold layer represents the final dimension and fact tables (start schema).
  
  Each view performs transformations and combines data from the silver layer
  to produce a clean, enriched, and business-ready dataset.

Usage:
  - These views can be queried directly for analytics and reporting
==================================================================================
*/


/*
=================================================================================
Create Dimension: gold.dim_customers
==================================================================================
*/


-- create view object for the gold layer
CREATE VIEW gold.dim_customers AS


SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,-- generating a surrogate key for this dimension table
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'unknown' THEN ci.cst_gndr		-- CRM is the master table for gender info
		ELSE COALESCE(ca.gen, 'unknown')	-- value to use in case of a NULL
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
	
	

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON
ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON
ci.cst_key = la.cid 


/*
=================================================================================
Create Dimension: gold.dim_productss
==================================================================================
*/


-- create the gold.dim_products view

CREATE VIEW gold.dim_products AS

-- create the dim Products table
select 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,-- create surrogate keys
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
	
FROM silver.crm_prd_info pn

LEFT JOIN silver.erp_px_cat_g1v2 pc
ON
pn.cat_id = pc.id

WHERE prd_end_dt IS NULL -- selecting only current info i.e where the prd_end_dt is NULL


/*
=================================================================================
Create Dimension: gold.fact_sales
==================================================================================
*/

-- creating the gold.sales_details view

CREATE VIEW gold.fact_sales AS

SELECT 
sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sls_order_dt AS order_date,
sls_ship_dt AS shipping_date,
sls_due_dt AS due_date,
sls_sales AS sales_amount,
sls_quantity AS quantity,
sls_price AS price
FROM
silver.crm_sales_details sd

LEFT JOIN gold.dim_products pr
ON
sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON
sd.sls_cust_id = cu.customer_id
