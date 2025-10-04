/*
==========================================================================
Quality Checks
==========================================================================
Script Purpose:
  This script performs quality checks to validate the quality, consistency
  and accuracy of the goldlayer. These checks ensure: 
  - uniqueness of surrogate keys in dimension tables
  - referential integrity between fact and dimension tables
  - validation of relationship in the data model for analytical purposes.

Usage Notes:
  - Run time checks after loading silver layers.
  - Investigate and resolve any discrepancies found during during the checks

===========================================================================
*/






/*
======================================================================
checking the gold.dim_customers
=====================================================================
*/
-- Checking for uniqueness of customer_key 
SELECT
  customer_key,
  COUNT(*) AS duplicate_count
FROM gold.dim_customers
  GROUP BY customer_key
  HAVING COUNT(*) > 1;

-- there are two gender columns so we have to carry out data integration

SELECT DISTINCT
	
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr != 'unknown' THEN ci.cst_gndr		-- CRM is the master table for gender info
		ELSE COALESCE(ca.gen, 'unknown')	-- value to use in case of a NULL
	END AS new_gen

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON
ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON
ci.cst_key = la.cid 
ORDER BY 1, 2 

-- we have to ask the stakeholders the master table to determine which information is correct.
-- the crm table is said to be the one with accurate gender inormation of the customer




/*
======================================================================
checking the gold.dim_products
=====================================================================
*/


-- Checking for uniqueness of customer_key 
SELECT
  product_key,
  COUNT(*) AS duplicate_count
FROM gold.dim_products
  GROUP BY product_key
  HAVING COUNT(*) > 1;


/*
======================================================================
checking the gold.fact_sales
=====================================================================
*/

-- Foreign key integrity (dimensions)
select * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON
c.customer_key = f.customer_key	
LEFT JOIN gold.dim_products p
ON 
p.product_key = f.product_key
WHERE p.product_key IS NULL


