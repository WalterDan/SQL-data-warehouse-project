
/*
===========================================================================
Quality Checks
============================================================================
SCript Purpose:
    This script performs various checks for data consistency, accuracy, and 
    standardization across the 'silver' schema. It includes checks for:
    - Null or Duplicate primary keys  
    - Unwanted spaces in string fields
    - Data standardization and data consistency
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
  - Run these checks after loading the Silver layer.
- Investigate and resolve any discrepancies found during the checks
============================================================================
*/


-- ========================================================================
-- checking 'silver.crm_cust_info'
-- ========================================================================

-- check for NULLs or Duplicates in primary key
-- Expectation: No Result
SELECT 
cst_id,
count(*)
FROM silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is NULL



-- check for unwanted spaces
-- Expectation: No result

select cst_marital_status
FROM silver.crm_cust_info
where cst_marital_status != TRIM(cst_marital_status)		-- get values where the firstname is not equal to the value after triming unwanted spaces



-- Data Standardization & consistency

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info


SELECT *
FROM silver.crm_cust_info

-- ========================================================================
-- checking 'silver.crm_prd_info'
-- ========================================================================

-- check for Nulls or Duplicates

select
prd_id,
count(*)
from silver.crm_prd_info
group by prd_id
Having count(*) > 1 or prd_id is null


-- check for unwanted spaces
-- Expectation: No results

select prd_nm
from silver.crm_prd_info
where prd_nm != TRIM(prd_nm)

-- check for Nulls or negative numbers
-- Expectation: No Results

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data standardization & consistency

SELECT DISTINCT prd_line
FROM
silver.crm_prd_info

-- check for invalid order dates

select *
from
silver.crm_prd_info
where prd_end_dt < prd_start_dt -- result: about 200 rows have lower end dates than start dates. so we have to fix that by
-- making the start date of the next record for each product key the end date(minus 1 day) for the previous record

-- ========================================================================
-- checking 'silver.crm_sales_details'
-- ========================================================================


-- we have clean the sls_order_dt, sls_due_dt and sls_ship_dt
-- check for invalid dates

select 
NULLIF(sls_due_dt, 0) sls_due_dt
FROM bronze.crm_sales_details
where  sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8 
OR sls_due_dt > 20500101 
OR sls_due_dt < 19000101-- to check for negative values or has more or less than require numbers for date

-- CHECKING IF THE ORDER DATE IS GREATER THAN THE SHIPPING OR DUE DATE

SELECT *
FROM
bronze.crm_sales_details

WHERE sls_order_dt > sls_ship_dt 
OR sls_order_dt > sls_due_dt

-- sales must be equal to quantity * price

SELECT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,

CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != (sls_quantity * ABS(sls_price))
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity, 0) 
	ELSE sls_price
END AS sls_price

FROM 
bronze.crm_sales_details

WHERE

sls_sales != (sls_quantity * sls_price)
OR
sls_sales IS NULL OR sls_sales <= 0
OR
sls_quantity IS NULL OR sls_quantity <= 0
OR
sls_price IS NULL OR sls_price <= 0
ORDER BY sls_sales,sls_quantity, sls_price

-- some sls_sales are NULL, are negative, some are 0. in the price column, some are negative, some are null
-- to fix that, the Rules are:
-- if sales is -ve, zero or null, derive it using Qty and Price.
-- If price is zero or null, calculate it using sales and quantity
-- if price is negative, convert to positive values

IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date DATETIME DEFAULT GETDATE()
);





-- ========================================================================
-- checking 'silver.erp_cust_az12'
-- ========================================================================
-- checking the bdate column

select distinct
bdate
from bronze.erp_cust_az12
where bdate <'1924-01-01' OR  bdate  >  getdate()  -- very old birth dates or birthdates greater than the current date

-- checking the gender column : data standardization and consistency

select distinct 
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	ELSE 'unknown'
END AS gen
from bronze.erp_cust_az12


select distinct gen from silver.erp_cust_az12


select distinct
bdate
from silver.erp_cust_az12
where bdate <'1924-01-01' OR  bdate  >  getdate()




-- ========================================================================
-- checking 'silver.erp_loc_a101'
-- ========================================================================


-- remove the delimiter in cid so it matches values in crm_cust_info

select 

REPLACE (cid, '-','') cid,
cntry
from bronze.erp_loc_a101

-- -- checking data standardization and consistency in the cntry column

select distinct cntry
from silver.erp_loc_a101
order by cntry 



select * from silver.erp_loc_a101


-- ========================================================================
-- checking 'silver.erp_px_cat_g1v2'
-- ========================================================================


-- checking id column



select distinct id
from
bronze.erp_px_cat_g1v2

WHERE id IN 

(SELECT DISTINCT cat_id from silver.crm_prd_info)

-- checking the cat column
-- checking for unwanted spaces
select * from 
bronze.erp_px_cat_g1v2
where 
cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- checking data standardization

select 
distinct  maintenance
from bronze.erp_px_cat_g1v2


select * 
from silver.erp_px_cat_g1v2










